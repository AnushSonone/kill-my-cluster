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
//	KV_PEERS=1=node1:8000,2=node2:8000,...   # all KV endpoints (for optional agent)
//	RUN_AGENT=true                           # only one node should set this
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/bank"
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
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
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
		go runAgent(ctx, kvClient, col)
		fmt.Println("  bank agent starting (waits for quorum)…")
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
		id: id,
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

func runAgent(ctx context.Context, client *kv.Client, col *metrics.Collector) {
	b := bank.NewBankStore(client)
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := b.Init(ctx); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		break
	}
	naive, err := bank.NewNaiveLedger()
	if err != nil {
		fmt.Fprintf(os.Stderr, "naive: %v\n", err)
		return
	}
	agent, err := bank.NewAgent(bank.AgentConfig{
		Bank: b, Naive: naive,
		Interval: 300 * time.Millisecond, DuplicateRate: 0.3, MaxAmountCents: 250,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		return
	}
	agent.Start(ctx)
	defer agent.Stop()
	fmt.Println("  bank agent running")
	reportBank(ctx, agent, col)
}

func reportBank(ctx context.Context, agent *bank.Agent, col *metrics.Collector) {
	var last uint64
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			snap, err := agent.Stats(ctx)
			if err != nil {
				continue
			}
			delta := snap.AgentTransfers - last
			last = snap.AgentTransfers
			col.SetBank(snap.RealTotalCents, snap.NaiveTotalCents, snap.DriftCents, delta)
		}
	}
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
