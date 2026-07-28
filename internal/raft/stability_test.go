package raft

// stability_test.go is the election-storm regression suite, written after the
// 2026-07 Oracle incident: a 7-node cluster under fixed load entered a
// six-day election storm (term 36k+, ~39 elections/min, zero goodput). These
// tests pin the three mechanisms that prevent a recurrence: pre-vote,
// leader stickiness + CheckQuorum, and candidacy backoff.
//
// The partition primitive here is what cluster_test.go's stopNode cannot
// express: a node that is ALIVE and accumulating election attempts while
// unreachable. A cleanly stopped node's term is frozen on disk; only a
// partitioned one can build up the inflated term that used to dethrone a
// healthy leader on rejoin.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raftpb"
)

var errPartitioned = errors.New("raft test: partitioned")

// gateTransport wraps a real transport with an outbound gate. Blocking the
// gate plus stopping the node's gRPC server isolates it in both directions
// while its algorithm keeps running.
type gateTransport struct {
	inner   Transport
	blocked atomic.Bool
}

func (g *gateTransport) RequestVote(ctx context.Context, peer uint64, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	if g.blocked.Load() {
		return nil, errPartitioned
	}
	return g.inner.RequestVote(ctx, peer, req)
}

func (g *gateTransport) AppendEntries(ctx context.Context, peer uint64, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	if g.blocked.Load() {
		return nil, errPartitioned
	}
	return g.inner.AppendEntries(ctx, peer, req)
}

func (g *gateTransport) InstallSnapshot(ctx context.Context, peer uint64, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	if g.blocked.Load() {
		return nil, errPartitioned
	}
	return g.inner.InstallSnapshot(ctx, peer, req)
}

// partition isolates a node: outbound RPCs error, inbound server stops. The
// node itself keeps running (unlike stopNode).
func (c *testCluster) partition(id uint64) {
	i := int(id - 1)
	c.gates[i].blocked.Store(true)
	if c.servers[i] != nil {
		c.servers[i].Stop()
		c.servers[i] = nil
	}
}

// heal reverses partition: re-listen on the node's stable address, unblock
// outbound, and force peers to re-dial.
func (c *testCluster) heal(id uint64) {
	t := c.t
	i := int(id - 1)
	lis, err := net.Listen("tcp", c.addrs[id])
	if err != nil {
		t.Fatalf("heal re-listen node %d: %v", id, err)
	}
	srv, err := serveOnListener(c.nodes[i], lis)
	if err != nil {
		t.Fatalf("heal re-serve node %d: %v", id, err)
	}
	c.servers[i] = srv
	c.gates[i].blocked.Store(false)
	for j, tr := range c.transports {
		if tr != nil && uint64(j+1) != id {
			tr.InvalidatePeer(id)
		}
	}
}

// terms returns each live node's current term keyed by ID.
func (c *testCluster) terms() map[uint64]uint64 {
	out := make(map[uint64]uint64)
	for _, n := range c.nodes {
		if n == nil {
			continue
		}
		term, _ := n.Status()
		out[n.ID()] = term
	}
	return out
}

func maxTerm(m map[uint64]uint64) uint64 {
	var out uint64
	for _, t := range m {
		out = max(out, t)
	}
	return out
}

// TestPreVoteIsolatedNodeDoesNotDisruptOnRejoin is the direct regression for
// the storm's nastiest mode: under the old code, an isolated node bumped its
// term on every timeout, and on rejoin its inflated term instantly dethroned
// a healthy leader. With pre-vote, its term must stay frozen while isolated,
// and rejoin must disturb nothing.
func TestPreVoteIsolatedNodeDoesNotDisruptOnRejoin(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(5 * time.Second)
	// Pick a follower to isolate.
	var followerID uint64
	for _, n := range c.nodes {
		if n != nil && n.ID() != leaderID {
			followerID = n.ID()
			break
		}
	}
	follower := c.nodeByID(followerID)
	termBefore, _ := follower.Status()

	c.partition(followerID)
	// 6x the max election timeout: the old code would burn ~6+ terms here.
	time.Sleep(6 * testTimings().ElectionTimeoutMax)

	termIsolated, _ := follower.Status()
	if termIsolated != termBefore {
		t.Fatalf("isolated node's term moved %d -> %d; pre-vote must not bump terms",
			termBefore, termIsolated)
	}

	c.heal(followerID)
	time.Sleep(4 * testTimings().ElectionTimeoutMax)

	// The old leader must still lead, at the same term.
	if _, isLeader := c.nodeByID(leaderID).Status(); !isLeader {
		t.Fatalf("leader %d was dethroned by a rejoining node", leaderID)
	}
	termAfter, _ := c.nodeByID(leaderID).Status()
	if termAfter != termBefore {
		t.Fatalf("cluster term moved %d -> %d across an isolate/rejoin; want unchanged",
			termBefore, termAfter)
	}

	// And the cluster still commits.
	c.proposeAll([]byte("post-rejoin"))
	c.waitForCommitOnMajority(1, 2*time.Second)
}

// TestCheckQuorumLeaderStepsDown: a leader cut off from every peer must
// demote itself (zombie leaders would otherwise serve stale lease reads),
// and must do it WITHOUT a term bump — stepping down is not an election.
func TestCheckQuorumLeaderStepsDown(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()

	leaderID := c.waitForLeader(5 * time.Second)
	leader := c.nodeByID(leaderID)
	termBefore, _ := leader.Status()

	c.partition(leaderID)

	deadline := time.Now().Add(4 * testTimings().ElectionTimeoutMax)
	for time.Now().Before(deadline) {
		if leader.Role() != Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader.Role() == Leader {
		t.Fatal("partitioned leader never stepped down (CheckQuorum)")
	}
	if termNow, _ := leader.Status(); termNow != termBefore {
		t.Fatalf("step-down changed term %d -> %d; must be term-neutral", termBefore, termNow)
	}

	// Its ReadIndex must fail — no stale linearizable reads.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, ok := leader.ReadIndex(ctx); ok {
		t.Fatal("deposed leader still serves ReadIndex")
	}

	// The surviving majority elects a replacement.
	var newLeader uint64
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n != nil && n.ID() != leaderID {
				if _, isLeader := n.Status(); isLeader {
					newLeader = n.ID()
				}
			}
		}
		if newLeader != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == 0 {
		t.Fatal("survivors elected no replacement leader")
	}
}

// TestElectionStormRegression is the headline test: kill/restart churn under
// continuous proposals must cost ~one term per real leader kill, not a storm.
// Under the pre-fix code, term inflation from restarting nodes and dueling
// candidates made termDelta blow far past the iteration count.
func TestElectionStormRegression(t *testing.T) {
	c := newTestCluster(t, 5)
	defer c.stop()

	c.waitForLeader(5 * time.Second)
	termStart := maxTerm(c.terms())

	// Continuous background proposals, failures ignored — load must never
	// stop just because the cluster is electing (that is the demo's reality).
	stopLoad := make(chan struct{})
	defer close(stopLoad)
	go func() {
		var i int
		for {
			select {
			case <-stopLoad:
				return
			default:
			}
			for _, n := range c.liveNodes() {
				_, _, _ = n.Propose([]byte(fmt.Sprintf("storm-%d", i)))
			}
			i++
			time.Sleep(5 * time.Millisecond)
		}
	}()

	const iterations = 8
	commitFloor := uint64(0)
	for it := 0; it < iterations; it++ {
		victim := uint64(rand.Intn(5) + 1)
		c.stopNode(victim)
		time.Sleep(4 * testTimings().HeartbeatInterval)
		c.restartNode(victim)

		leaderID := c.waitForLeader(5 * time.Second)
		// Commit progress each round: the cluster must do useful work
		// between kills, not just survive them.
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if ci := c.nodeByID(leaderID).CommitIndex(); ci > commitFloor {
				commitFloor = ci
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	termEnd := maxTerm(c.terms())
	delta := termEnd - termStart
	// Each iteration can justify at most ~1 election (leader killed), plus
	// slack for unlucky pre-vote races. The storm signature is delta in the
	// tens-to-hundreds.
	if delta > iterations+4 {
		t.Fatalf("term advanced %d over %d kill cycles; election storm not damped", delta, iterations)
	}
	if commitFloor == 0 {
		t.Fatal("no commit progress across the whole churn run")
	}
	t.Logf("term delta %d over %d kill/restart cycles; commit reached %d", delta, iterations, commitFloor)
}

// TestCandidacyBackoff verifies the retry window widens with consecutive
// failed campaigns (capped at 8x spread) and that the floor never rises —
// backoff must slow re-campaigning, not leader-loss detection.
func TestCandidacyBackoff(t *testing.T) {
	c := newTestCluster(t, 3)
	defer c.stop()
	n := c.nodes[0]
	tm := testTimings()
	spread := tm.ElectionTimeoutMax - tm.ElectionTimeoutMin

	maxObserved := func(attempts, samples int) time.Duration {
		var out time.Duration
		n.mu.Lock()
		defer n.mu.Unlock()
		saved := n.electionAttempts
		n.electionAttempts = attempts
		for i := 0; i < samples; i++ {
			n.resetElectionTimerLocked()
			if n.electionTimeout < tm.ElectionTimeoutMin {
				c.t.Fatalf("timeout %v under floor %v", n.electionTimeout, tm.ElectionTimeoutMin)
			}
			out = max(out, n.electionTimeout)
		}
		n.electionAttempts = saved
		n.resetElectionTimerLocked()
		return out
	}

	base := maxObserved(0, 300)
	backed := maxObserved(3, 300)
	capped := maxObserved(10, 300)

	if base >= tm.ElectionTimeoutMin+spread {
		t.Fatalf("attempt-0 timeout %v exceeds 1x spread bound", base)
	}
	if backed <= tm.ElectionTimeoutMin+2*spread {
		t.Fatalf("attempt-3 max %v shows no widening (want > min+2x spread)", backed)
	}
	if capped >= tm.ElectionTimeoutMin+8*spread+time.Millisecond {
		t.Fatalf("attempt-10 timeout %v exceeds the 8x cap", capped)
	}
}
