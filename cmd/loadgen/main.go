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

	client := kv.NewClient(peers)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var writes, reads, writeErrs, readErrs atomic.Uint64
	go runPool(ctx, writeWorkers, writeQPS, &writes, &writeErrs, func(ctx context.Context, n uint64) error {
		key := fmt.Sprintf("load/w/%d", n%10_000)
		_, err := client.ExecuteOnce(ctx, "loadgen-w", n, kv.Command{
			Op: kv.OpPut, Key: key, Value: []byte(fmt.Sprintf("%d", n)),
		})
		return err
	})
	go runPool(ctx, readWorkers, readQPS, &reads, &readErrs, func(ctx context.Context, n uint64) error {
		key := fmt.Sprintf("load/w/%d", n%10_000)
		_, err := client.Get(ctx, "loadgen-r", n, key)
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

func runPool(ctx context.Context, workers int, qps float64, ok, fail *atomic.Uint64, fn func(context.Context, uint64) error) {
	if workers < 1 {
		workers = 1
	}
	if qps <= 0 {
		return
	}
	interval := time.Duration(float64(time.Second) / qps)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	tokens := make(chan struct{}, workers*2)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}
	}()
	for i := 0; i < workers; i++ {
		go func() {
			var n uint64
			for {
				select {
				case <-ctx.Done():
					return
				case <-tokens:
					n++
					cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
					err := fn(cctx, n)
					cancel()
					if err != nil {
						fail.Add(1)
					} else {
						ok.Add(1)
					}
				}
			}
		}()
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
