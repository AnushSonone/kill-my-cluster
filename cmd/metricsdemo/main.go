// metricsdemo runs a 3-node cluster with Prometheus /metrics endpoints and
// the bank tenant so Grafana has live series to graph.
//
// Usage (from repo root, with observability stack up — see deploy/observability):
//
//	go run ./cmd/metricsdemo
//
// Then open http://localhost:3000 (Grafana) and http://localhost:9090 (Prometheus).
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/bank"
	"github.com/AnushSonone/kill-my-cluster/internal/kv"
	"github.com/AnushSonone/kill-my-cluster/internal/metrics"
	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

const nNodes = 3

// metricsPorts are scraped by Prometheus (see deploy/observability/prometheus.yml).
var metricsPorts = []string{"9101", "9102", "9103"}

func main() {
	base, err := os.MkdirTemp("", "kill-my-cluster-metricsdemo-*")
	if err != nil {
		fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(base)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clusters, collectors, cleanup := startCluster(ctx, base)
	defer cleanup()

	fmt.Println("--- waiting for leader ---")
	waitForLeader(clusters, 5*time.Second)

	b := bank.NewBank(clusters...)
	if err := b.Init(ctx); err != nil {
		fatalf("bank init: %v", err)
	}
	naive, err := bank.NewNaiveLedger()
	if err != nil {
		fatalf("naive: %v", err)
	}
	agent, err := bank.NewAgent(bank.AgentConfig{
		Bank:           b,
		Naive:          naive,
		Interval:       300 * time.Millisecond,
		DuplicateRate:  0.3,
		MaxAmountCents: 250,
	})
	if err != nil {
		fatalf("agent: %v", err)
	}
	agent.Start(ctx)
	defer agent.Stop()

	// Bank gauges live on node 1's registry (one place — avoids triple-counting).
	go reportBank(ctx, agent, collectors[0])

	fmt.Println("\nKill My Cluster — metrics demo")
	fmt.Printf("  data: %s\n", base)
	for i, port := range metricsPorts {
		fmt.Printf("  node %d metrics: http://127.0.0.1:%s/metrics\n", i+1, port)
	}
	fmt.Println("  Prometheus: http://localhost:9090")
	fmt.Println("  Grafana:    http://localhost:3000  (admin / admin)")
	fmt.Println("Ctrl+C to stop.")
	fmt.Println()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
}

func startCluster(ctx context.Context, base string) ([]*kv.Cluster, []*metrics.Collector, func()) {
	addrs := make(map[uint64]string, nNodes)
	listeners := make([]net.Listener, nNodes)
	clusters := make([]*kv.Cluster, nNodes)
	servers := make([]*raft.Server, nNodes)
	collectors := make([]*metrics.Collector, nNodes)
	httpSrvs := make([]*http.Server, nNodes)

	for i := 0; i < nNodes; i++ {
		id := uint64(i + 1)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fatalf("listen: %v", err)
		}
		listeners[i] = lis
		addrs[id] = lis.Addr().String()
	}

	for i := 0; i < nNodes; i++ {
		id := uint64(i + 1)
		dir := filepath.Join(base, fmt.Sprintf("node%d", id))
		var peers []uint64
		peerAddrs := make(map[uint64]string)
		for pid, addr := range addrs {
			if pid == id {
				continue
			}
			peers = append(peers, pid)
			peerAddrs[pid] = addr
		}

		cl, err := kv.NewCluster(kv.Config{
			ID: id, Peers: peers, Dir: dir,
			Transport: raft.NewGRPCTransport(peerAddrs),
		})
		if err != nil {
			fatalf("cluster: %v", err)
		}
		col := metrics.NewCollector(id)
		cl.SetTelemetry(col)
		go metrics.NewReporter(cl.Raft(), col, 250*time.Millisecond).Run(ctx)

		mux := http.NewServeMux()
		mux.Handle("/metrics", col.Handler())
		hs := &http.Server{Addr: "127.0.0.1:" + metricsPorts[i], Handler: mux}
		go func(hs *http.Server, id uint64) {
			if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "metrics server node %d: %v\n", id, err)
			}
		}(hs, id)

		srv, err := raft.ServeOnListener(cl.Raft(), listeners[i])
		if err != nil {
			fatalf("raft server: %v", err)
		}
		clusters[i] = cl
		servers[i] = srv
		collectors[i] = col
		httpSrvs[i] = hs
	}

	return clusters, collectors, func() {
		for i := range clusters {
			_ = httpSrvs[i].Close()
			clusters[i].Stop()
			servers[i].Stop()
		}
	}
}

func reportBank(ctx context.Context, agent *bank.Agent, col *metrics.Collector) {
	var lastTransfers uint64
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
			delta := snap.AgentTransfers - lastTransfers
			lastTransfers = snap.AgentTransfers
			col.SetBank(snap.RealTotalCents, snap.NaiveTotalCents, snap.DriftCents, delta)
		}
	}
}

func waitForLeader(clusters []*kv.Cluster, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, cl := range clusters {
			if cl.IsLeader() {
				fmt.Printf("  leader: node %d\n", cl.Raft().ID())
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	fatalf("no leader within %v", timeout)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
