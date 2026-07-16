// node is a single Raft + KV process for Docker (and bare-metal) deployments.
//
// Environment:
//
//	NODE_ID=1
//	DATA_DIR=/data
//	RAFT_ADDR=0.0.0.0:7000
//	KV_ADDR=0.0.0.0:8000
//	METRICS_ADDR=0.0.0.0:9100
//	PEERS=2=node2:7000,3=node3:7000          # other Raft peers (id=host:port)
//	KV_PEERS=1=node1:8000,2=node2:8000,...   # all KV endpoints (for traffic agent)
//	RUN_AGENT=true                           # only one node should set this
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
	"github.com/AnushSonone/kill-my-cluster/internal/metrics"
	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		fatalf("%v", err)
	}
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		fatalf("data dir: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transport := raft.NewGRPCTransport(cfg.raftPeers)
	cl, err := kv.NewCluster(kv.Config{
		ID: cfg.id, Peers: cfg.peerIDs, Dir: cfg.dataDir,
		Transport: transport,
	})
	if err != nil {
		fatalf("cluster: %v", err)
	}

	col := metrics.NewCollector(cfg.id)
	cl.SetTelemetry(col)
	go metrics.NewReporter(cl.Raft(), col, 250*time.Millisecond).Run(ctx)

	raftSrv, err := raft.NewServer(cl.Raft(), cfg.raftAddr)
	if err != nil {
		fatalf("raft server: %v", err)
	}
	kvSrv, err := kv.NewKVServer(cl, cfg.kvAddr)
	if err != nil {
		fatalf("kv server: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", col.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		rn := cl.Raft()
		term, isLeader := rn.Status()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":          true,
			"id":          cfg.id,
			"term":        term,
			"commitIndex": rn.CommitIndex(),
			"leaderId":    rn.LeaderID(),
			"isLeader":    isLeader,
			"role":        rn.Role().String(),
		})
	})
	httpSrv := &http.Server{Addr: cfg.metricsAddr, Handler: mux}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "metrics: %v\n", err)
		}
	}()

	fmt.Printf("node %d up\n", cfg.id)
	fmt.Printf("  raft    %s\n", raftSrv.Addr())
	fmt.Printf("  kv      %s\n", kvSrv.Addr())
	fmt.Printf("  metrics http://%s/metrics\n", cfg.metricsAddr)

	var kvClient *kv.Client
	if cfg.runAgent {
		kvClient = kv.NewClient(cfg.kvPeers)
		go runTraffic(ctx, kvClient)
		fmt.Println("  traffic agent starting (waits for quorum)…")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down…")
	cancel()
	if kvClient != nil {
		kvClient.Close()
	}
	_ = httpSrv.Close()
	kvSrv.Stop()
	raftSrv.Stop()
	cl.Stop()
	transport.Close()
}

type config struct {
	id          uint64
	dataDir     string
	raftAddr    string
	kvAddr      string
	metricsAddr string
	peerIDs     []uint64
	raftPeers   map[uint64]string
	kvPeers     map[uint64]string
	runAgent    bool
}

func loadConfig() (config, error) {
	id, err := strconv.ParseUint(env("NODE_ID", ""), 10, 64)
	if err != nil || id == 0 {
		return config{}, fmt.Errorf("NODE_ID must be a positive integer")
	}
	raftPeers, err := parsePeers(env("PEERS", ""))
	if err != nil {
		return config{}, fmt.Errorf("PEERS: %w", err)
	}
	peerIDs := make([]uint64, 0, len(raftPeers))
	for pid := range raftPeers {
		peerIDs = append(peerIDs, pid)
	}
	kvPeers, err := parsePeers(env("KV_PEERS", ""))
	if err != nil {
		return config{}, fmt.Errorf("KV_PEERS: %w", err)
	}
	runAgent := strings.EqualFold(env("RUN_AGENT", "false"), "true")
	if runAgent && len(kvPeers) == 0 {
		return config{}, fmt.Errorf("RUN_AGENT=true requires KV_PEERS")
	}
	return config{
		id:          id,
		dataDir:     env("DATA_DIR", "/data"),
		raftAddr:    env("RAFT_ADDR", "0.0.0.0:7000"),
		kvAddr:      env("KV_ADDR", "0.0.0.0:8000"),
		metricsAddr: env("METRICS_ADDR", "0.0.0.0:9100"),
		peerIDs:     peerIDs,
		raftPeers:   raftPeers,
		kvPeers:     kvPeers,
		runAgent:    runAgent,
	}, nil
}

func parsePeers(s string) (map[uint64]string, error) {
	out := make(map[uint64]string)
	s = strings.TrimSpace(s)
	if s == "" {
		return out, nil
	}
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

// runTraffic writes a heartbeat key on an interval so commit indexes / applies
// keep moving for the public proof (Grafana + mesh pulse).
func runTraffic(ctx context.Context, client *kv.Client) {
	interval := agentInterval()
	var seq uint64
	fmt.Printf("  traffic agent running (interval %s)\n", interval)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			seq++
			val := []byte(fmt.Sprintf("%d", time.Now().UnixNano()))
			_, err := client.ExecuteOnce(ctx, "traffic", seq, kv.Command{
				Op: kv.OpPut, Key: "demo/heartbeat", Value: val,
			})
			if err != nil {
				// No quorum yet — reuse the same request id on the next tick.
				seq--
			}
		}
	}
}

func agentInterval() time.Duration {
	if v := os.Getenv("AGENT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return 1500 * time.Millisecond
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
