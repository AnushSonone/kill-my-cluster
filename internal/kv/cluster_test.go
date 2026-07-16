package kv

// cluster_test.go is the Phase 3 acceptance suite: linearizable KV over a
// real 3-node Raft cluster, plus exactly-once retry semantics.

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

type testKVCluster struct {
	t        *testing.T
	dirs     []string
	addrs    map[uint64]string
	clusters []*Cluster
	servers  []*raft.Server
	kvAddrs  map[uint64]string
}

func newTestKVCluster(t *testing.T, n int) *testKVCluster {
	t.Helper()
	c := &testKVCluster{t: t}
	base := t.TempDir()
	c.addrs = make(map[uint64]string)
	c.kvAddrs = make(map[uint64]string)
	c.dirs = make([]string, n)
	c.clusters = make([]*Cluster, n)
	c.servers = make([]*raft.Server, n)

	raftListeners := make([]net.Listener, n)
	kvListeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		rl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		kl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		raftListeners[i] = rl
		kvListeners[i] = kl
		c.addrs[id] = rl.Addr().String()
		c.kvAddrs[id] = kl.Addr().String()
		c.dirs[i] = filepath.Join(base, fmt.Sprintf("node%d", id))
		_ = os.MkdirAll(c.dirs[i], 0o755)
	}

	for i := 0; i < n; i++ {
		c.boot(i, raftListeners[i], kvListeners[i])
	}
	return c
}

func (c *testKVCluster) boot(i int, raftLis, kvLis net.Listener) {
	t := c.t
	id := uint64(i + 1)

	var peers []uint64
	peerAddrs := make(map[uint64]string)
	for pid, addr := range c.addrs {
		if pid == id {
			continue
		}
		peers = append(peers, pid)
		peerAddrs[pid] = addr
	}

	cl, err := NewCluster(Config{
		ID: id, Peers: peers, Dir: c.dirs[i],
		Transport: raft.NewGRPCTransport(peerAddrs),
	})
	if err != nil {
		t.Fatalf("cluster %d: %v", id, err)
	}

	raftSrv, err := raft.ServeOnListener(cl.Raft(), raftLis)
	if err != nil {
		t.Fatalf("raft server %d: %v", id, err)
	}
	kvSrv, err := serveKVOnListener(NewGRPCServer(cl), kvLis)
	if err != nil {
		t.Fatalf("kv server %d: %v", id, err)
	}
	_ = kvSrv

	c.clusters[i] = cl
	c.servers[i] = raftSrv
}

func (c *testKVCluster) stop() {
	for i := range c.clusters {
		if c.clusters[i] != nil {
			c.clusters[i].Stop()
		}
		if c.servers[i] != nil {
			c.servers[i].Stop()
		}
	}
}

func (c *testKVCluster) waitForLeader(timeout time.Duration) *Cluster {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, cl := range c.clusters {
			if cl != nil && cl.IsLeader() {
				return cl
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return nil
}

func (c *testKVCluster) proposeViaLeader(ctx context.Context, fn func(*Cluster) error) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, cl := range c.clusters {
			if cl == nil {
				continue
			}
			err := fn(cl)
			if err == ErrNotLeader {
				continue
			}
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("no leader accepted request")
}

func TestKVPutGetLinearizable(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	c.waitForLeader(2 * time.Second)

	ctx := context.Background()
	err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		_, err := cl.Put(ctx, "visitor", 1, "color", []byte("blue"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	for i, cl := range c.clusters {
		v, ok := cl.machine.Get("color")
		if !ok || string(v) != "blue" {
			t.Fatalf("node %d: got ok=%v val=%q", i+1, ok, v)
		}
	}
}

func TestExactlyOnceRetry(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	c.waitForLeader(2 * time.Second)

	ctx := context.Background()
	var sideEffect int
	var mu sync.Mutex

	run := func(cl *Cluster) (ApplyResult, error) {
		res, err := cl.ExecuteOnce(ctx, "agent-1", 1001, Command{
			Op: OpPut, Key: "counter", Value: []byte("increment"),
		})
		if err != nil {
			return res, err
		}
		if !res.Duplicate {
			mu.Lock()
			sideEffect++
			mu.Unlock()
		}
		return res, nil
	}

	// First execution.
	if err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		_, err := run(cl)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	// Simulate client retry (lost ACK) — same request_id, new log entry.
	if err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		res, err := run(cl)
		if err != nil {
			return err
		}
		if !res.Duplicate {
			t.Fatal("retry must be marked duplicate")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	n := sideEffect
	mu.Unlock()
	if n != 1 {
		t.Fatalf("side effect ran %d times, want 1", n)
	}

	// All nodes must converge before we assert global state.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, cl := range c.clusters {
			if v, ok := cl.machine.Get("counter"); !ok || string(v) != "increment" {
				all = false
				break
			}
		}
		if all {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for i, cl := range c.clusters {
		v, ok := cl.machine.Get("counter")
		if !ok || string(v) != "increment" {
			t.Fatalf("node %d state wrong: ok=%v %q", i+1, ok, v)
		}
	}
}

func TestCASAndCheckpoint(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	c.waitForLeader(2 * time.Second)
	ctx := context.Background()

	err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		_, err := cl.Put(ctx, "client", 1, "balance", []byte("1000"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = c.proposeViaLeader(ctx, func(cl *Cluster) error {
		res, err := cl.CAS(ctx, "client", 2, "balance", []byte("1000"), []byte("999"))
		if err != nil {
			return err
		}
		if !res.Found {
			t.Fatal("CAS should swap")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	state := []byte(`{"step":6,"done":["a","b"]}`)
	err = c.proposeViaLeader(ctx, func(cl *Cluster) error {
		_, err := cl.Checkpoint(ctx, "agent-1", 3, state)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = c.proposeViaLeader(ctx, func(cl *Cluster) error {
		res, err := cl.ReadCheckpoint(ctx, "agent-1", 4)
		if err != nil {
			return err
		}
		if !res.Found || string(res.Value) != string(state) {
			t.Fatalf("checkpoint read: %+v", res)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestWatchNotification(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	leader := c.waitForLeader(2 * time.Second)

	ch := leader.Watch("ticker")
	defer leader.Unwatch("ticker", ch)

	ctx := context.Background()
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = c.proposeViaLeader(ctx, func(cl *Cluster) error {
			_, err := cl.Put(ctx, "w", 1, "ticker", []byte("up"))
			return err
		})
	}()

	select {
	case ev := <-ch:
		if ev.Key != "ticker" || string(ev.Value) != "up" {
			t.Fatalf("watch: %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for watch")
	}
}
