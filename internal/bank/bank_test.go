package bank

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

func bootKVCluster(t *testing.T, n int) ([]*kv.Cluster, func()) {
	t.Helper()
	base := t.TempDir()
	addrs := make(map[uint64]string)
	listeners := make([]net.Listener, n)
	clusters := make([]*kv.Cluster, n)
	servers := make([]*raft.Server, n)

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = lis
		addrs[id] = lis.Addr().String()
	}

	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		dir := filepath.Join(base, fmt.Sprintf("node%d", id))
		_ = os.MkdirAll(dir, 0o755)

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
			t.Fatal(err)
		}
		srv, err := raft.ServeOnListener(cl.Raft(), listeners[i])
		if err != nil {
			t.Fatal(err)
		}
		clusters[i] = cl
		servers[i] = srv
	}

	stop := func() {
		for i := range clusters {
			if clusters[i] != nil {
				clusters[i].Stop()
			}
			if servers[i] != nil {
				servers[i].Stop()
			}
		}
	}
	return clusters, stop
}

func waitKVLeader(t *testing.T, clusters []*kv.Cluster) *kv.Cluster {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, cl := range clusters {
			if cl != nil && cl.IsLeader() {
				return cl
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func viaLeader(clusters []*kv.Cluster, fn func(*kv.Cluster) error) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, cl := range clusters {
			if cl == nil {
				continue
			}
			if err := fn(cl); err != kv.ErrNotLeader {
				return err
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("no leader")
}

func TestBankConservationAndExactlyOnce(t *testing.T) {
	clusters, stop := bootKVCluster(t, 3)
	defer stop()
	waitKVLeader(t, clusters)
	ctx := context.Background()

	if err := viaLeader(clusters, func(cl *kv.Cluster) error {
		return NewBank(clusters...).Init(ctx)
	}); err != nil {
		t.Fatal(err)
	}

	// First transfer.
	if err := viaLeader(clusters, func(cl *kv.Cluster) error {
		_, err := NewBank(clusters...).Transfer(ctx, 100, "checking", "savings", 500)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Retry same request_id — must not move money again.
	if err := viaLeader(clusters, func(cl *kv.Cluster) error {
		res, err := NewBank(clusters...).Transfer(ctx, 100, "checking", "savings", 500)
		if err != nil {
			return err
		}
		if !res.Duplicate {
			t.Fatal("expected duplicate on retry")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var total int64
	if err := viaLeader(clusters, func(cl *kv.Cluster) error {
		var err error
		total, err = NewBank(clusters...).Total(ctx, 999)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if total != InitialTotalCents {
		t.Fatalf("total=%d want %d", total, InitialTotalCents)
	}
}

func TestAgentKeepsRealTotalConserved(t *testing.T) {
	clusters, stop := bootKVCluster(t, 3)
	defer stop()
	waitKVLeader(t, clusters)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := viaLeader(clusters, func(cl *kv.Cluster) error {
		return NewBank(clusters...).Init(ctx)
	}); err != nil {
		t.Fatal(err)
	}

	naive, err := NewNaiveLedger()
	if err != nil {
		t.Fatal(err)
	}
	agent, err := NewAgent(AgentConfig{
		Bank:           NewBank(clusters...),
		Naive:          naive,
		Interval:       50 * time.Millisecond,
		DuplicateRate:  0.5,
		MaxAmountCents: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent.Start(ctx)
	time.Sleep(800 * time.Millisecond)
	agent.Stop()

	snap, err := agent.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Conserved {
		t.Fatalf("real total drifted: %s", FormatTotal(snap.RealTotalCents))
	}
	if snap.AgentTransfers < 3 {
		t.Fatalf("expected several transfers, got %d", snap.AgentTransfers)
	}
	if snap.DriftCents <= 0 {
		t.Fatalf("naive should have leaked, drift=%d", snap.DriftCents)
	}
}
