// loadgen sustains Put/Get traffic against the KV cluster for the public proof.
//
//	KV_PEERS=1=node1:8000,2=node2:8000,...
//	WRITE_QPS=1500
//	READ_QPS=10000
//	WRITE_WORKERS=64
//	READ_WORKERS=128
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
)

func main() {
	peers, err := parsePeers(env("KV_PEERS", ""))
	if err != nil || len(peers) == 0 {
		fatalf("KV_PEERS required (id=host:port,...)")
	}
	writeQPS := envFloat("WRITE_QPS", 1500)
	readQPS := envFloat("READ_QPS", 10000)
	writeWorkers := envInt("WRITE_WORKERS", 64)
	readWorkers := envInt("READ_WORKERS", 128)
	adapt := !strings.EqualFold(env("ADAPT", "true"), "false")
	minWriteQPS := envFloat("MIN_WRITE_QPS", 50)
	minReadQPS := envFloat("MIN_READ_QPS", 100)

	client := kv.NewClient(peers)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var writes, reads, writeErrs, readErrs atomic.Uint64
	// Each worker gets its own clientID: the cluster's exactly-once dedup is
	// keyed on (clientID, requestID), and workers count requestIDs
	// independently — a shared ID made their requestIDs collide, silently
	// deduping a large fraction of "successful" writes.
	go runPool(ctx, pool{name: "write", workers: writeWorkers, maxQPS: writeQPS, minQPS: minWriteQPS, adapt: adapt},
		&writes, &writeErrs, func(ctx context.Context, worker int, n uint64) error {
			key := fmt.Sprintf("load/w/%d", n%10_000)
			_, err := client.ExecuteOnce(ctx, fmt.Sprintf("loadgen-w-%d", worker), n, kv.Command{
				Op: kv.OpPut, Key: key, Value: []byte(fmt.Sprintf("%d", n)),
			})
			return err
		})
	go runPool(ctx, pool{name: "read", workers: readWorkers, maxQPS: readQPS, minQPS: minReadQPS, adapt: adapt},
		&reads, &readErrs, func(ctx context.Context, worker int, n uint64) error {
			key := fmt.Sprintf("load/w/%d", n%10_000)
			_, err := client.Get(ctx, fmt.Sprintf("loadgen-r-%d", worker), n, key)
			return err
		})

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		var prevW, prevR uint64
		prev := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				w, r := writes.Load(), reads.Load()
				dt := now.Sub(prev).Seconds()
				if dt > 0 {
					fmt.Printf("loadgen writes/s=%.0f reads/s=%.0f write_errs=%d read_errs=%d\n",
						float64(w-prevW)/dt, float64(r-prevR)/dt, writeErrs.Load(), readErrs.Load())
				}
				prevW, prevR, prev = w, r, now
			}
		}
	}()

	fmt.Printf("loadgen targeting writes=%.0f/s reads=%.0f/s workers=%d/%d\n",
		writeQPS, readQPS, writeWorkers, readWorkers)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("loadgen shutting down…")
	cancel()
	time.Sleep(200 * time.Millisecond)
}

// pool describes one traffic pool (writes or reads).
type pool struct {
	name    string
	workers int
	maxQPS  float64 // configured ceiling (WRITE_QPS / READ_QPS)
	minQPS  float64 // adaptive floor: keep a pulse even when unhealthy
	adapt   bool
}

// runPool paces fn at a target QPS via a token ticker, with an AIMD
// controller that watches the failure rate and backs off when the cluster is
// struggling. This closes the loop that caused the 2026-07-21 six-day
// election storm: a fixed-rate loadgen kept full pressure on a leaderless
// cluster, so the cluster could never get enough breathing room to elect,
// and the box starved its own sshd. Now sustained failures halve the offered
// rate (down to minQPS), and sustained health ramps it back up (+5% of the
// ceiling per check) toward maxQPS.
func runPool(ctx context.Context, p pool, ok, fail *atomic.Uint64, fn func(context.Context, int, uint64) error) {
	if p.workers < 1 {
		p.workers = 1
	}
	if p.maxQPS <= 0 {
		return
	}
	if p.minQPS <= 0 || p.minQPS > p.maxQPS {
		p.minQPS = p.maxQPS
	}
	intervalFor := func(qps float64) time.Duration {
		iv := time.Duration(float64(time.Second) / qps)
		if iv < time.Microsecond {
			iv = time.Microsecond
		}
		return iv
	}

	tokens := make(chan struct{}, p.workers*2)
	go func() {
		cur := p.maxQPS
		t := time.NewTicker(intervalFor(cur))
		defer t.Stop()
		ctrl := time.NewTicker(2 * time.Second)
		defer ctrl.Stop()
		var lastOK, lastFail uint64
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case tokens <- struct{}{}:
				default:
				}
			case <-ctrl.C:
				if !p.adapt {
					continue
				}
				o, f := ok.Load(), fail.Load()
				dOK, dFail := o-lastOK, f-lastFail
				lastOK, lastFail = o, f
				total := dOK + dFail
				if total == 0 {
					continue
				}
				rate := float64(dFail) / float64(total)
				switch {
				case rate > 0.20:
					// Multiplicative decrease: the cluster is failing; get
					// out of its way fast.
					next := max(cur/2, p.minQPS)
					if next != cur {
						cur = next
						t.Reset(intervalFor(cur))
						fmt.Printf("loadgen[%s] backoff: %.0f%% failures, target now %.0f/s\n",
							p.name, rate*100, cur)
					}
				case rate < 0.05 && cur < p.maxQPS:
					// Additive increase: healthy again; creep back toward
					// the headline rate.
					next := min(cur+0.05*p.maxQPS, p.maxQPS)
					cur = next
					t.Reset(intervalFor(cur))
					fmt.Printf("loadgen[%s] recover: target now %.0f/s\n", p.name, cur)
				}
			}
		}
	}()
	for i := 0; i < p.workers; i++ {
		go func(worker int) {
			var n uint64
			for {
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					n++
					cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					err := fn(cctx, worker, n)
					cancel()
					if err != nil {
						fail.Add(1)
					} else {
						ok.Add(1)
					}
				}
			}
		}(i)
	}
}

func parsePeers(s string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("want id=host:port, got %q", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, err
		}
		out[id] = strings.TrimSpace(addr)
	}
	return out, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	v := env(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(k string, def float64) float64 {
	v := env(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
