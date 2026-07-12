// bankdemo runs the Phase 4 tenant: 3-node cluster + continuous transfers.
//
// Usage:
//
//	go run ./cmd/bankdemo
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/bank"
	"github.com/AnushSonone/kill-my-cluster/internal/kv"
	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

const nNodes = 3

func main() {
	base, err := os.MkdirTemp("", "kill-my-cluster-bankdemo-*")
	if err != nil {
		fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(base)
	fmt.Printf("The First National Bank of KillMyCluster\n")
	fmt.Printf("data dir (ephemeral): %s\n\n", base)

	clusters, cleanup := startCluster(base)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("--- initializing ledger ($1,000.00) ---")
	if err := viaLeader(clusters, func(cl *kv.Cluster) error {
		return bank.NewBank(clusters...).Init(ctx)
	}); err != nil {
		fatalf("init: %v", err)
	}
	fmt.Printf("  seeded %d house accounts\n\n", len(bank.DefaultAccounts))

	naive, err := bank.NewNaiveLedger()
	if err != nil {
		fatalf("naive: %v", err)
	}
	agent, err := bank.NewAgent(bank.AgentConfig{
		Bank:           bank.NewBank(clusters...),
		Naive:          naive,
		Interval:       200 * time.Millisecond,
		DuplicateRate:  0.35,
		MaxAmountCents: 300,
	})
	if err != nil {
		fatalf("agent: %v", err)
	}
	agent.Start(ctx)
	defer agent.Stop()

	fmt.Println("--- running tenant (15 transfers) ---")
	for i := 0; i < 15; i++ {
		time.Sleep(250 * time.Millisecond)
		snap, err := agent.Stats(ctx)
		if err != nil {
			fatalf("stats: %v", err)
		}
		fmt.Printf("  [%02d] real=%s conserved=%v | naive=%s drift=+%s (%d duplicate credits)\n",
			i+1,
			bank.FormatTotal(snap.RealTotalCents), snap.Conserved,
			bank.FormatTotal(snap.NaiveTotalCents), bank.FormatTotal(snap.DriftCents),
			snap.NaiveDuplicates)
	}

	snap, _ := agent.Stats(ctx)
	fmt.Printf("\n--- final ---\n")
	fmt.Printf("  Real bank:  %s (conserved: %v)\n", bank.FormatTotal(snap.RealTotalCents), snap.Conserved)
	fmt.Printf("  Naive twin: %s (+%s created from duplicate credits)\n",
		bank.FormatTotal(snap.NaiveTotalCents), bank.FormatTotal(snap.DriftCents))
	fmt.Printf("  Transfers: %d real · %d naive duplicates\n", snap.AgentTransfers, snap.NaiveDuplicates)
	fmt.Println("\ndone.")
}

func startCluster(base string) ([]*kv.Cluster, func()) {
	addrs := make(map[uint64]string, nNodes)
	listeners := make([]net.Listener, nNodes)
	clusters := make([]*kv.Cluster, nNodes)
	servers := make([]*raft.Server, nNodes)

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
		srv, err := raft.ServeOnListener(cl.Raft(), listeners[i])
		if err != nil {
			fatalf("server: %v", err)
		}
		clusters[i] = cl
		servers[i] = srv
	}

	return clusters, func() {
		for i := range clusters {
			clusters[i].Stop()
			servers[i].Stop()
		}
	}
}

func viaLeader(clusters []*kv.Cluster, fn func(*kv.Cluster) error) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, cl := range clusters {
			if err := fn(cl); err != kv.ErrNotLeader {
				return err
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("no leader")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
