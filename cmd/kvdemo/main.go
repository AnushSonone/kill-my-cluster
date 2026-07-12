// kvdemo demonstrates Phase 3: linearizable KV + exactly-once retries.
//
// Usage:
//
//	go run ./cmd/kvdemo
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

const nNodes = 3

type member struct {
	cluster *kv.Cluster
	raftSrv *raft.Server
}

func main() {
	base, err := os.MkdirTemp("", "kill-my-cluster-kvdemo-*")
	if err != nil {
		fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(base)
	fmt.Printf("data dir (ephemeral): %s\n\n", base)

	addrs := make(map[uint64]string, nNodes)
	listeners := make([]net.Listener, nNodes)
	for i := 0; i < nNodes; i++ {
		id := uint64(i + 1)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fatalf("listen: %v", err)
		}
		listeners[i] = lis
		addrs[id] = lis.Addr().String()
	}

	members := make([]member, nNodes)
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
			fatalf("cluster %d: %v", id, err)
		}
		_ = listeners[i].Close()
		rs, err := raft.NewServer(cl.Raft(), addrs[id])
		if err != nil {
			fatalf("raft server %d: %v", id, err)
		}
		members[i] = member{cluster: cl, raftSrv: rs}
		fmt.Printf("node %d raft on %s\n", id, rs.Addr())
	}
	defer func() {
		for _, m := range members {
			m.cluster.Stop()
			m.raftSrv.Stop()
		}
	}()

	fmt.Println("\n--- waiting for leader ---")
	waitForLeader(members, 3*time.Second)

	ctx := context.Background()
	fmt.Println("\n--- linearizable PUT color=blue ---")
	mustViaLeader(members, func(cl *kv.Cluster) error {
		_, err := cl.Put(ctx, "demo", 1, "color", []byte("blue"))
		return err
	})
	res := mustViaLeaderResult(members, func(cl *kv.Cluster) (kv.ApplyResult, error) {
		return cl.Get(ctx, "demo", 2, "color")
	})
	fmt.Printf("  GET color → found=%v value=%q\n", res.Found, string(res.Value))

	fmt.Println("\n--- exactly-once: same request_id twice (simulated retry) ---")
	var sideEffects int
	var mu sync.Mutex

	for attempt := 1; attempt <= 2; attempt++ {
		fmt.Printf("  attempt %d...\n", attempt)
		r := mustViaLeaderResult(members, func(cl *kv.Cluster) (kv.ApplyResult, error) {
			return cl.ExecuteOnce(ctx, "agent-transfer", 9001, kv.Command{
				Op: kv.OpPut, Key: "ledger", Value: []byte("paid-once"),
			})
		})
		if !r.Duplicate {
			mu.Lock()
			sideEffects++
			mu.Unlock()
		}
		fmt.Printf("    duplicate=%v side-effect count=%d\n", r.Duplicate, sideEffects)
	}
	fmt.Printf("\n  side effects executed: %d (want 1) — duplicate log entries, single mutation\n", sideEffects)

	fmt.Println("\n--- agent checkpoint ---")
	state := []byte(`{"step":3,"note":"resume here after crash"}`)
	mustViaLeader(members, func(cl *kv.Cluster) error {
		_, err := cl.Checkpoint(ctx, "agent-1", 10, state)
		return err
	})
	ck := mustViaLeaderResult(members, func(cl *kv.Cluster) (kv.ApplyResult, error) {
		return cl.ReadCheckpoint(ctx, "agent-1", 11)
	})
	fmt.Printf("  checkpoint: %q\n", string(ck.Value))

	fmt.Println("\ndone.")
}

func waitForLeader(members []member, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if m.cluster.IsLeader() {
				fmt.Printf("  leader: node (cluster on raft id %d)\n", m.cluster.Raft().ID())
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	fatalf("no leader within %v", timeout)
}

func mustViaLeader(members []member, fn func(*kv.Cluster) error) {
	if err := viaLeader(members, fn); err != nil {
		fatalf("%v", err)
	}
}

func mustViaLeaderResult(members []member, fn func(*kv.Cluster) (kv.ApplyResult, error)) kv.ApplyResult {
	res, err := viaLeaderResult(members, fn)
	if err != nil {
		fatalf("%v", err)
	}
	return res
}

func viaLeader(members []member, fn func(*kv.Cluster) error) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range members {
			if err := fn(m.cluster); err != kv.ErrNotLeader {
				return err
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("no leader accepted request")
}

func viaLeaderResult(members []member, fn func(*kv.Cluster) (kv.ApplyResult, error)) (kv.ApplyResult, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range members {
			res, err := fn(m.cluster)
			if err != kv.ErrNotLeader {
				return res, err
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return kv.ApplyResult{}, fmt.Errorf("no leader accepted request")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
