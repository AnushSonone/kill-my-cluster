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
	clusters  []*Cluster
	servers   []*raft.Server
	kvServers []*KVServer
	kvAddrs   map[uint64]string
	// cfgTweak lets a test adjust each node's Config before boot
	// (e.g. aggressive snapshot cadence).
	cfgTweak func(*Config)
}

func newTestKVCluster(t *testing.T, n int) *testKVCluster {
	return newTestKVClusterCfg(t, n, nil)
}

func newTestKVClusterCfg(t *testing.T, n int, tweak func(*Config)) *testKVCluster {
	t.Helper()
	c := &testKVCluster{t: t, cfgTweak: tweak}
	base := t.TempDir()
	c.addrs = make(map[uint64]string)
	c.kvAddrs = make(map[uint64]string)
	c.dirs = make([]string, n)
	c.clusters = make([]*Cluster, n)
	c.servers = make([]*raft.Server, n)
	c.kvServers = make([]*KVServer, n)

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

	cfg := Config{
		ID: id, Peers: peers, Dir: c.dirs[i],
		Transport: raft.NewGRPCTransport(peerAddrs),
		// Fast clocks: same ratios as production, ~7x quicker suite.
		Timings: raft.Timings{
			ElectionTimeoutMin: 100 * time.Millisecond,
			ElectionTimeoutMax: 200 * time.Millisecond,
			HeartbeatInterval:  30 * time.Millisecond,
			TickInterval:       10 * time.Millisecond,
		},
	}
	if c.cfgTweak != nil {
		c.cfgTweak(&cfg)
	}
	cl, err := NewCluster(cfg)
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

	c.clusters[i] = cl
	c.servers[i] = raftSrv
	c.kvServers[i] = kvSrv
}

// restart stops node i and boots it again from its on-disk state on the same
// addresses (server first, then cluster — mirrors production shutdown order).
func (c *testKVCluster) restart(i int) {
	t := c.t
	id := uint64(i + 1)
	if c.servers[i] != nil {
		c.servers[i].Stop()
		c.servers[i] = nil
	}
	if c.kvServers[i] != nil {
		c.kvServers[i].Stop()
		c.kvServers[i] = nil
	}
	if c.clusters[i] != nil {
		c.clusters[i].Stop()
		c.clusters[i] = nil
	}
	rl, err := net.Listen("tcp", c.addrs[id])
	if err != nil {
		t.Fatalf("restart re-listen raft %d: %v", id, err)
	}
	kl, err := net.Listen("tcp", c.kvAddrs[id])
	if err != nil {
		t.Fatalf("restart re-listen kv %d: %v", id, err)
	}
	c.boot(i, rl, kl)
}

func (c *testKVCluster) stop() {
	for i := range c.clusters {
		if c.servers[i] != nil {
			c.servers[i].Stop()
		}
		if c.kvServers[i] != nil {
			c.kvServers[i].Stop()
		}
		if c.clusters[i] != nil {
			c.clusters[i].Stop()
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
	c.waitForLeader(5 * time.Second)

	ctx := context.Background()
	err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		_, err := cl.Put(ctx, "visitor", 1, "color", []byte("blue"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		allOK := true
		for i, cl := range c.clusters {
			v, ok := cl.machine.Get("color")
			if !ok || string(v) != "blue" {
				allOK = false
				if time.Now().After(deadline) {
					t.Fatalf("node %d: got ok=%v val=%q", i+1, ok, v)
				}
				break
			}
		}
		if allOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestExactlyOnceRetry(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	c.waitForLeader(5 * time.Second)

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
	c.waitForLeader(5 * time.Second)
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
	leader := c.waitForLeader(5 * time.Second)

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

func TestGetUsesReadIndexNotLog(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	leader := c.waitForLeader(5 * time.Second)

	ctx := context.Background()
	if err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		_, err := cl.Put(ctx, "visitor", 1, "color", []byte("red"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	before := leader.Raft().CommitIndex()
	for i := 0; i < 40; i++ {
		res, err := leader.Get(ctx, "reader", uint64(i+1), "color")
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !res.Found || string(res.Value) != "red" {
			t.Fatalf("Get %d: found=%v val=%q", i, res.Found, res.Value)
		}
	}
	after := leader.Raft().CommitIndex()
	if after != before {
		t.Fatalf("commit index moved %d -> %d after Gets; ReadIndex must not append", before, after)
	}
}

// TestAutoSnapshotUnderLoad pins the compaction wiring end-to-end: sustained
// ExecuteOnce load must trigger automatic snapshots (the 2026-07 incident's
// deepest defect was that compaction never ran — 21.6M entries in RAM), a
// restarted node must recover from snapshot + tail instead of a full replay,
// and exactly-once dedup must survive the snapshot round-trip.
func TestAutoSnapshotUnderLoad(t *testing.T) {
	c := newTestKVClusterCfg(t, 3, func(cfg *Config) {
		cfg.SnapshotEntries = 256
		cfg.SnapshotRetain = -1 // full compaction so FirstIndex moves visibly
	})
	defer c.stop()
	c.waitForLeader(5 * time.Second)
	ctx := context.Background()

	const writes = 700
	for n := 1; n <= writes; n++ {
		key := fmt.Sprintf("auto/%d", n%100)
		err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
			_, err := cl.ExecuteOnce(ctx, "snap-test", uint64(n), Command{
				Op: OpPut, Key: key, Value: []byte(fmt.Sprintf("%d", n)),
			})
			return err
		})
		if err != nil {
			t.Fatalf("write %d: %v", n, err)
		}
	}

	// Compaction ran on the leader.
	leader := c.waitForLeader(5 * time.Second)
	if fi := leader.Raft().FirstIndex(); fi == 0 {
		t.Fatal("no automatic snapshot after sustained load (FirstIndex still 0)")
	}

	// Dedup survives: a retried recent request must return its original
	// result, not re-execute.
	var dup ApplyResult
	err := c.proposeViaLeader(ctx, func(cl *Cluster) error {
		res, err := cl.ExecuteOnce(ctx, "snap-test", uint64(writes), Command{
			Op: OpPut, Key: "auto/0", Value: []byte("SHOULD-NOT-APPLY"),
		})
		dup = res
		return err
	})
	if err != nil {
		t.Fatalf("dedup retry: %v", err)
	}
	if !dup.Duplicate {
		t.Fatal("retried request re-executed; dedup lost across snapshots")
	}

	// A restarted follower recovers via snapshot restore + tail replay.
	victim := -1
	for i, cl := range c.clusters {
		if cl != nil && !cl.IsLeader() {
			victim = i
			break
		}
	}
	if victim < 0 {
		t.Fatal("no follower to restart")
	}
	c.restart(victim)

	target := leader.Raft().CommitIndex()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.clusters[victim].Raft().LastApplied() >= target {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := c.clusters[victim].Raft().LastApplied(); got < target {
		t.Fatalf("restarted node applied %d < leader commit %d", got, target)
	}
	// State correctness after restore: auto/0 was last written by write 700.
	if v, ok := c.clusters[victim].machine.Get("auto/0"); !ok || string(v) != "700" {
		t.Fatalf("restarted node has auto/0=%q ok=%v, want \"700\"", v, ok)
	}
	if fi := c.clusters[victim].Raft().FirstIndex(); fi == 0 {
		t.Fatal("restarted node shows no compaction (FirstIndex 0)")
	}
}
