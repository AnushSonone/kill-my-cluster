package raft

// cluster_test.go spins up real 3-node Raft clusters over gRPC on localhost.
// These are the Phase 2 acceptance tests: elect a leader, replicate writes,
// survive a leader kill with sub-second re-election, persist across restarts,
// and compact via snapshots.

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testCluster is a disposable Raft group wired through real gRPC.
type testCluster struct {
	t          *testing.T
	dirs       []string
	addrs      map[uint64]string // stable peer addresses, reused on restart
	nodes      []*Node
	servers    []*Server
	transports []*GRPCTransport
	apply      [][]ApplyMsg
	applyMu    sync.Mutex
}

func newTestCluster(t *testing.T, nNodes int) *testCluster {
	t.Helper()
	if nNodes < 3 {
		t.Fatalf("need at least 3 nodes, got %d", nNodes)
	}

	c := &testCluster{t: t}
	base := t.TempDir()
	c.addrs = make(map[uint64]string, nNodes)
	c.dirs = make([]string, nNodes)
	c.nodes = make([]*Node, nNodes)
	c.servers = make([]*Server, nNodes)
	c.transports = make([]*GRPCTransport, nNodes)
	c.apply = make([][]ApplyMsg, nNodes)

	// Reserve stable addresses up front so a restarted node can re-bind the
	// same host:port and peers' transport maps stay valid.
	listeners := make([]net.Listener, nNodes)
	for i := 0; i < nNodes; i++ {
		id := uint64(i + 1)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen node %d: %v", id, err)
		}
		listeners[i] = lis
		c.addrs[id] = lis.Addr().String()
		c.dirs[i] = filepath.Join(base, fmt.Sprintf("node%d", id))
		if err := os.MkdirAll(c.dirs[i], 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	for i := 0; i < nNodes; i++ {
		c.bootNode(i, listeners[i])
	}
	return c
}

func (c *testCluster) bootNode(i int, lis net.Listener) {
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

	ch := make(chan ApplyMsg, 128)
	go c.drainApply(i, ch)

	transport := NewGRPCTransport(peerAddrs)
	node, err := NewNode(Config{
		ID: id, Peers: peers, Dir: c.dirs[i],
		Transport: transport, ApplyCh: ch,
	})
	if err != nil {
		t.Fatalf("node %d: %v", id, err)
	}
	srv, err := serveOnListener(node, lis)
	if err != nil {
		t.Fatalf("server %d: %v", id, err)
	}

	c.nodes[i] = node
	c.servers[i] = srv
	c.transports[i] = transport
}

func (c *testCluster) drainApply(idx int, ch chan ApplyMsg) {
	for msg := range ch {
		c.applyMu.Lock()
		c.apply[idx] = append(c.apply[idx], msg)
		c.applyMu.Unlock()
	}
}

func (c *testCluster) stop() {
	for i := range c.nodes {
		c.stopIndex(i)
	}
}

func (c *testCluster) stopIndex(i int) {
	if c.nodes[i] != nil {
		c.nodes[i].Stop()
		c.nodes[i] = nil
	}
	if c.transports[i] != nil {
		c.transports[i].Close()
		c.transports[i] = nil
	}
	if c.servers[i] != nil {
		c.servers[i].Stop()
		c.servers[i] = nil
	}
}

// stopNode kills one node entirely (algorithm + gRPC). Simulates a crash.
func (c *testCluster) stopNode(id uint64) {
	c.stopIndex(int(id - 1))
	// Drop cached connections to the dead peer so survivors don't keep dialing
	// a closed socket until the next RPC error.
	for i, tr := range c.transports {
		if tr != nil {
			tr.InvalidatePeer(id)
		}
		_ = i
	}
}

// restartNode brings a previously stopped node back from its on-disk state
// on the same address it used before.
func (c *testCluster) restartNode(id uint64) {
	t := c.t
	i := int(id - 1)
	addr := c.addrs[id]

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen node %d at %s: %v", id, addr, err)
	}
	c.bootNode(i, lis)

	// Force everyone else to re-dial the restarted peer.
	for j, tr := range c.transports {
		if tr != nil && uint64(j+1) != id {
			tr.InvalidatePeer(id)
		}
	}
}

func (c *testCluster) leaderID(timeout time.Duration) (uint64, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n == nil {
				continue
			}
			if _, isLeader := n.Status(); isLeader {
				return n.ID(), true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return 0, false
}

func (c *testCluster) waitForLeader(timeout time.Duration) uint64 {
	c.t.Helper()
	id, ok := c.leaderID(timeout)
	if !ok {
		c.t.Fatal("timed out waiting for a leader")
	}
	return id
}

func (c *testCluster) proposeAll(data []byte) {
	c.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n == nil {
				continue
			}
			if _, _, ok := n.Propose(data); ok {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no leader accepted proposal %q", data)
}

func (c *testCluster) waitForCommitOnMajority(minIndex uint64, timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		committed := 0
		for _, n := range c.nodes {
			if n == nil {
				continue
			}
			if n.CommitIndex() >= minIndex {
				committed++
			}
		}
		if committed >= 2 { // majority of 3
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("index %d not committed on a majority within %v", minIndex, timeout)
}

func (c *testCluster) appliedData(nodeIdx int) [][]byte {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	var out [][]byte
	for _, msg := range c.apply[nodeIdx] {
		if msg.CommandValid {
			out = append(out, msg.Command)
		}
	}
	return out
}

func (c *testCluster) nodeByID(id uint64) *Node {
	for _, n := range c.nodes {
		if n != nil && n.ID() == id {
			return n
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestInitialElection(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leader := c.waitForLeader(2 * time.Second)
	t.Logf("elected leader: node %d", leader)
}

func TestReplication(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
	c.proposeAll([]byte("alpha"))
	c.waitForCommitOnMajority(1, 2*time.Second)

	for i := 0; i < 3; i++ {
		data := c.appliedData(i)
		if len(data) != 1 || !bytes.Equal(data[0], []byte("alpha")) {
			t.Fatalf("node %d applied %v, want [[alpha]]", i+1, data)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	oldLeader := c.waitForLeader(2 * time.Second)
	c.proposeAll([]byte("before-kill"))
	c.waitForCommitOnMajority(1, 2*time.Second)

	start := time.Now()
	c.stopNode(oldLeader)

	newLeader, ok := c.leaderID(2 * time.Second)
	if !ok {
		t.Fatal("no new leader after killing the old one")
	}
	elapsed := time.Since(start)
	if newLeader == oldLeader {
		t.Fatalf("same leader %d after kill", oldLeader)
	}
	t.Logf("failover: node %d → node %d in %v", oldLeader, newLeader, elapsed)
	if elapsed > 800*time.Millisecond {
		t.Fatalf("re-election took %v; want sub-second (~hundreds of ms)", elapsed)
	}

	c.proposeAll([]byte("after-kill"))
	c.waitForCommitOnMajority(2, 2*time.Second)
}

func TestPersistenceAcrossRestart(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
	c.proposeAll([]byte("durable"))
	c.waitForCommitOnMajority(1, 2*time.Second)

	c.stopNode(2)
	c.restartNode(2)
	time.Sleep(400 * time.Millisecond)

	data := c.appliedData(1)
	found := false
	for _, d := range data {
		if bytes.Equal(d, []byte("durable")) {
			found = true
		}
	}
	if !found {
		t.Fatalf("restarted node 2 did not apply durable entry; got %v", data)
	}
}

func TestSnapshotCompaction(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(2 * time.Second)

	for i := 0; i < 5; i++ {
		c.proposeAll([]byte(fmt.Sprintf("entry-%d", i)))
	}
	c.waitForCommitOnMajority(5, 3*time.Second)

	leader := c.nodeByID(leaderID)
	if err := leader.Snapshot(5, []byte("snap-state")); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	c.proposeAll([]byte("post-snap"))
	c.waitForCommitOnMajority(6, 3*time.Second)

	for i := 0; i < 3; i++ {
		if len(c.appliedData(i)) < 6 {
			t.Fatalf("node %d applied only %d entries after snapshot", i+1, len(c.appliedData(i)))
		}
	}
}

func TestLaggingFollowerInstallSnapshot(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	c.waitForLeader(2 * time.Second)
	c.stopNode(3)

	// The leader may have been node 3 — re-elect among the survivors first.
	leaderID := c.waitForLeader(2 * time.Second)

	for i := 0; i < 8; i++ {
		c.proposeAll([]byte(fmt.Sprintf("bulk-%d", i)))
	}
	c.waitForCommitOnMajority(8, 5*time.Second)

	leader := c.nodeByID(leaderID)
	if leader == nil {
		// Leadership may have moved during replication; grab whoever leads now.
		leaderID = c.waitForLeader(2 * time.Second)
		leader = c.nodeByID(leaderID)
	}
	if err := leader.Snapshot(8, []byte("bulk-snap")); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	c.restartNode(3)
	time.Sleep(800 * time.Millisecond)

	c.proposeAll([]byte("after-snap-rejoin"))
	c.waitForCommitOnMajority(9, 5*time.Second)

	data := c.appliedData(2)
	found := false
	for _, d := range data {
		if bytes.Equal(d, []byte("after-snap-rejoin")) {
			found = true
		}
	}
	if !found {
		t.Fatalf("rejoined node 3 never applied post-snapshot write; got %v", data)
	}
}
