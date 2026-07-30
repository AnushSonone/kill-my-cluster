// Package raft implements the Raft consensus algorithm from scratch: leader
// election, log replication, persistence, and snapshot-based log compaction,
// as described in "In Search of an Understandable Consensus Algorithm"
// (Ongaro & Ousterhout, 2014).
//
// ---------------------------------------------------------------------------
// The mental model
// ---------------------------------------------------------------------------
// Raft turns "several machines that can crash" into "one machine that
// doesn't". It does this by replicating a LOG: clients propose commands, the
// leader assigns them positions in the log, and an entry is COMMITTED once a
// majority of nodes have it durably on disk. Every node then applies committed
// entries, in order, to its local state machine — identical inputs in an
// identical order produce identical state on every node.
//
// At any moment a node plays one of three roles:
//
//	Follower  — passive; answers RPCs, and expects periodic heartbeats from a
//	            leader. If heartbeats stop, it assumes the leader died.
//	Candidate — a follower whose election timer fired; it asks the cluster to
//	            vote for it as the new leader.
//	Leader    — the single node that accepts client proposals and pushes
//	            AppendEntries to everyone else.
//
// Terms are the logical clock gluing this together: each election starts a new
// term, at most one leader can win any given term (each node votes once per
// term), and any node that ever sees a higher term than its own immediately
// steps down to follower and adopts it. Stale leaders are thereby neutralized
// the instant they talk to anyone newer.
//
// This file holds the node's state, lifecycle, election logic, and the three
// RPC handlers. The leader's replication machinery is in replicate.go, the
// in-memory log in log.go, durability in persist.go, and gRPC plumbing in
// transport.go.
package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raftpb"
)

// Role is the node's current position in the protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Election/heartbeat timing.
//
// The constraint (§5.6) is: broadcastTime << electionTimeout << MTBF.
// Under Docker compose + loadgen, scheduling jitter is much larger than bare
// localhost RPC, so we use longer timeouts than the classic 150-300ms lab
// values. That keeps the leader stable unless a machine is actually killed.
// Re-election after a real kill still lands in about 1-2s.
//
// The timeout is RANDOMIZED per election within [min, max): if every follower
// used the same timeout, they would all become candidates simultaneously,
// split the vote, and repeat forever. Randomization makes one node's timer
// usually fire first; it wins before the others even wake up.
const (
	electionTimeoutMin = 750 * time.Millisecond
	electionTimeoutMax = 1500 * time.Millisecond

	// The leader sends heartbeats at several times the election-timeout
	// frequency so a single dropped heartbeat can't trigger a needless
	// election.
	heartbeatInterval = 150 * time.Millisecond

	// How often the election-timer goroutine wakes up to check the clock.
	tickInterval = 25 * time.Millisecond

	// How long a leader may hold uncommitted work without commitIndex moving
	// before it deposes itself. See the commit-stall watchdog in
	// runElectionTimer: CheckQuorum proves we can still be HEARD, this proves
	// we can still make PROGRESS.
	//
	// Deliberately generous. A 15-minute local kill-churn soak at 10s produced
	// one false positive: under heavy CPU starvation a leader missed the
	// 10s window and was deposed, costing an extra election during load it
	// would have ridden out. The failure this defends against ran for 48
	// HOURS, so 30s versus 10s is irrelevant to recovery time while tripling
	// the margin against transient starvation. When in doubt, be slower.
	commitStallTimeout = 30 * time.Second
)

// Timings holds the protocol clocks. The zero value means "use the defaults
// above"; tests inject fast values, deployments tune via env without a
// rebuild (cmd/node reads RAFT_ELECTION_TIMEOUT_MIN and friends).
type Timings struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	TickInterval       time.Duration
	// CommitStallTimeout bounds how long a leader may sit on uncommitted
	// entries without commitIndex advancing. Zero means the default; NEGATIVE
	// disables the watchdog entirely (same convention as SnapshotRetain).
	CommitStallTimeout time.Duration
}

// DefaultTimings returns the production values.
func DefaultTimings() Timings {
	return Timings{
		ElectionTimeoutMin: electionTimeoutMin,
		ElectionTimeoutMax: electionTimeoutMax,
		HeartbeatInterval:  heartbeatInterval,
		TickInterval:       tickInterval,
		CommitStallTimeout: commitStallTimeout,
	}
}

// withDefaults fills zero fields and repairs inverted ranges. The spread
// (Max-Min) must stay positive: the timer randomization calls rand.Int63n on
// it, which panics on <= 0.
func (t Timings) withDefaults() Timings {
	d := DefaultTimings()
	if t.ElectionTimeoutMin <= 0 {
		t.ElectionTimeoutMin = d.ElectionTimeoutMin
	}
	if t.ElectionTimeoutMax <= t.ElectionTimeoutMin {
		t.ElectionTimeoutMax = t.ElectionTimeoutMin + t.ElectionTimeoutMin/2
		if t.ElectionTimeoutMax <= t.ElectionTimeoutMin {
			t.ElectionTimeoutMax = t.ElectionTimeoutMin + time.Millisecond
		}
	}
	if t.HeartbeatInterval <= 0 {
		t.HeartbeatInterval = d.HeartbeatInterval
	}
	if t.TickInterval <= 0 {
		t.TickInterval = d.TickInterval
	}
	// Only zero takes the default here: a negative value is a deliberate
	// "disable the watchdog" and must survive.
	if t.CommitStallTimeout == 0 {
		t.CommitStallTimeout = d.CommitStallTimeout
	}
	return t
}

// ApplyMsg is delivered on the apply channel for every committed log entry
// (CommandValid) or installed snapshot (SnapshotValid). The state machine —
// Phase 3's KV store — consumes these; Raft itself never interprets commands.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex uint64
	SnapshotTerm  uint64
}

// Transport abstracts how nodes reach each other, so the core algorithm can
// be driven by real gRPC (transport.go) in production and by an in-memory
// fake in unit tests without touching the protocol logic.
type Transport interface {
	RequestVote(ctx context.Context, peer uint64, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error)
	AppendEntries(ctx context.Context, peer uint64, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, peer uint64, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error)
}

// snapshotCache is the last snapshot this node persisted, held in memory so
// the leader can ship it without a disk read while holding mu. Shipping used
// to call persist.readSnapshot() under the mutex; with several followers past
// the compaction point at once, those reads serialized every propose and
// ReadIndex on the leader behind them — half of the 2026-07-28 wedge.
type snapshotCache struct {
	index uint64
	term  uint64
	data  []byte
}

// Node is one member of a Raft cluster.
type Node struct {
	// mu guards every mutable field below. Raft is notoriously easy to get
	// wrong under concurrency; a single coarse mutex (held only for in-memory
	// work and disk persistence, never across network calls) keeps the
	// invariants checkable. Network I/O always happens with mu released.
	mu sync.Mutex

	id        uint64   // this node's ID (1-based; 0 is reserved for "nobody")
	peers     []uint64 // IDs of the other nodes (not including self)
	transport Transport
	persist   *persister
	tm        Timings // protocol clocks; immutable after NewNode
	// snapshotRetain: entries kept behind a snapshot (see Config).
	snapshotRetain uint64
	// snapCache mirrors the on-disk snapshot so InstallSnapshot sends never
	// touch the disk while mu is held.
	snapCache snapshotCache

	// --- Persistent state (mirrored on disk before answering RPCs). ---
	currentTerm uint64
	votedFor    uint64 // noVote if none this term
	log         *raftLog

	// --- Volatile state. ---
	role        Role
	leaderID    uint64 // last known leader (0 if unknown); handy for clients
	commitIndex uint64 // highest index known committed
	lastApplied uint64 // highest index handed to the apply channel

	// electionReset marks the last moment we heard from a live leader or
	// granted a vote. The election timer measures from here.
	electionReset time.Time
	// electionTimeout is re-randomized each time the timer is reset.
	electionTimeout time.Duration
	// electionAttempts counts consecutive failed pre-vote/election rounds
	// since we last heard from a live leader. It widens the retry window
	// (candidacy backoff) so a cluster that cannot elect — starved CPU,
	// partition, split votes — probes gently instead of stampeding.
	electionAttempts int
	// preVoteRound stamps each pre-vote poll so responses from an abandoned
	// round are recognized as history and dropped.
	preVoteRound uint64

	// --- Leader-only state, reinitialized on every election win (§5.3). ---
	// nextIndex[peer]: index of the next entry to SEND to that peer (a guess,
	// repaired by AppendEntries rejections). matchIndex[peer]: highest index
	// KNOWN replicated on that peer (ground truth, only moves forward).
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64
	// lastContact[peer]: when that peer last answered one of our RPCs while
	// we led. CheckQuorum reads it: a leader that can't hear a majority for a
	// full election timeout steps down instead of lingering as a zombie —
	// required for the ReadIndex lease to stay honest, and doubly important
	// now that stickiness makes deposing a zombie by force harder.
	lastContact     map[uint64]time.Time
	lastQuorumCheck time.Time
	// Commit-stall watchdog. CheckQuorum above proves followers still ANSWER
	// us; these prove our log still MOVES. A leader whose replicators are
	// wedged keeps heartbeating (so CheckQuorum is satisfied) while committing
	// nothing, and PreVote + stickiness stop anyone else from deposing it —
	// the 2026-07-28 outage sat exactly there for 48 hours. lastProgressIndex
	// is the commitIndex as of lastCommitProgress; both are seeded on election
	// win and updated whenever commitIndex actually advances.
	lastCommitProgress   time.Time
	lastProgressIndex    uint64
	commitStallStepdowns uint64 // cumulative, for telemetry
	// repls holds this leadership's per-follower replication loops. Rebuilt
	// with fresh notify channels on every election win; loops from an older
	// term exit on their own via the (role, term) guards.
	repls []*replState

	// applyCh delivers committed entries to the state machine. applyCond
	// wakes the applier goroutine whenever commitIndex advances.
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// pendingSnapshot holds a snapshot received via InstallSnapshot that the
	// applier must hand to the state machine before any further entries.
	pendingSnapshot *ApplyMsg

	// propCh batches concurrent Propose calls so many entries share one WAL
	// fsync (group commit).
	propCh chan propReq
	// readCh batches ReadIndex confirmations so many linearizable local reads
	// share one majority heartbeat.
	readCh chan readReq
	// done is closed on Stop; batchers exit and Propose/ReadIndex fail fast.
	done chan struct{}
	// readLeaseUntil is when the leader may serve ReadIndex without a fresh
	// majority heartbeat (refreshed after each successful confirm).
	readLeaseUntil time.Time

	stopped bool
	wg      sync.WaitGroup
}

// propReq is one client Propose waiting for a group-committed log slot.
type propReq struct {
	data []byte
	ch   chan propResult
}

type propResult struct {
	index    uint64
	term     uint64
	isLeader bool
}

// readReq is one ReadIndex waiter (linearizable local-read barrier).
type readReq struct {
	ctx context.Context
	ch  chan readResult
}

type readResult struct {
	index uint64
	ok    bool
}

// Config bundles what a Node needs at startup.
type Config struct {
	ID        uint64
	Peers     []uint64 // other nodes' IDs, excluding ID
	Dir       string   // directory for this node's durable state
	Transport Transport
	// ApplyCh receives committed commands and snapshots. The consumer must
	// keep draining it; Raft blocks applying (never loses) if it stalls.
	ApplyCh chan ApplyMsg
	// Timings overrides the protocol clocks; zero fields use the defaults.
	Timings Timings
	// SnapshotRetain is how many committed entries to keep in the log BEHIND
	// a snapshot point. Without a margin, every snapshot empties the log and
	// any follower even one entry behind at that instant needs a full
	// InstallSnapshot instead of a cheap AppendEntries — snapshot-flapping.
	//
	// DELIBERATELY SMALL — raising this is not free. Measured on the Oracle
	// A1 (4 OCPU, nodes capped at 0.45), 2026-07-30:
	//
	//	retain  snapEvery  writes/s  reads/s
	//	  1024      10000      1034     1275   <- shipped
	//	 32768      10000       903      605
	//	 16384     100000       930      765
	//
	// Caveat on those two rows: each was sampled ~9 minutes after a redeploy.
	// This cluster takes ~10 minutes to settle after a restart (reads were
	// seen climbing 950 -> 1275 on an UNCHANGED config), so treat them as
	// indicative, not exact. The 1024 row is a settled back-to-back window
	// against the pre-change build, which measured 1039 w/s / 1202 r/s.
	//
	// Two independent costs, and you cannot dodge both: saveSnapshot rewrites
	// the whole retained tail, so retain/snapEvery is a write-amplification
	// ratio; and the retained entries stay in RAM on the scan paths that run
	// under the node mutex, so snapEvery+retain is a latency cost. Sizing
	// retention above HEAL_AFTER x write rate (~11.6k entries at demo load)
	// pays one or the other.
	//
	// So a healed node DOES still land past the tail and take a full
	// InstallSnapshot. That is fine now: the snapshot is served from an
	// in-memory cache rather than a disk read under mu, and the commit-stall
	// watchdog bounds any wedge that results. Correctness comes from the
	// watchdog, not from this number. Raise it only with measurements.
	//
	// 0 means the default (1024); negative keeps none (tests use this to
	// force the InstallSnapshot path deterministically).
	SnapshotRetain int
}

// NewNode recovers durable state from cfg.Dir and starts the node's
// background goroutines (election timer + applier). The node begins life as
// a follower — even a node that was leader before a crash must re-earn
// leadership through an election, because the cluster may have moved on.
func NewNode(cfg Config) (*Node, error) {
	p, st, err := openPersister(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("raft: open persister: %w", err)
	}

	n := &Node{
		id:          cfg.ID,
		peers:       cfg.Peers,
		transport:   cfg.Transport,
		persist:     p,
		tm:          cfg.Timings.withDefaults(),
		currentTerm: st.term,
		votedFor:    st.votedFor,
		log:         newRaftLog(st.snapIndex, st.snapTerm),
		role:        Follower,
		applyCh:     cfg.ApplyCh,
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.log.append(st.entries...)

	switch {
	case cfg.SnapshotRetain > 0:
		n.snapshotRetain = uint64(cfg.SnapshotRetain)
	case cfg.SnapshotRetain == 0:
		n.snapshotRetain = 1024
	default:
		n.snapshotRetain = 0 // negative: compact fully (tests)
	}

	// A recovered snapshot is already-applied state: the state machine will
	// receive it as its baseline, so both cursors start at the snapshot.
	n.commitIndex = st.snapIndex
	n.lastApplied = st.snapIndex
	if st.snapIndex > 0 {
		n.pendingSnapshot = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      st.snapData,
			SnapshotIndex: st.snapIndex,
			SnapshotTerm:  st.snapTerm,
		}
	}

	n.resetElectionTimerLocked()

	n.propCh = make(chan propReq, 1024)
	n.readCh = make(chan readReq, 1024)
	n.done = make(chan struct{})

	n.wg.Add(4)
	go n.runElectionTimer()
	go n.runApplier()
	go n.runProposeBatcher()
	go n.runReadIndexBatcher()
	return n, nil
}

// Stop shuts the node down: background goroutines exit, the WAL is closed.
// Safe to call once; the node is unusable afterwards.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.applyCond.Broadcast() // wake the applier so it can observe stopped
	close(n.done)
	n.mu.Unlock()

	n.wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	_ = n.persist.close()
}

// Status reports (term, isLeader) — the standard Raft introspection point.
func (n *Node) Status() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

// Role returns follower, candidate, or leader — used by metrics scrapers.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// LeaderID returns the last known leader's ID (0 if unknown). Clients use
// this to redirect requests to the leader.
func (n *Node) LeaderID() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// ID returns this node's cluster ID.
func (n *Node) ID() uint64 {
	return n.id
}

// CommitIndex returns the highest log index known committed on this node.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// Propose asks the cluster to commit data. Only the leader accepts proposals;
// followers return isLeader=false and the caller must retry against the
// leader. Concurrent Proposes are group-committed: many entries share one
// WAL fsync before replication.
//
// On success it returns the log position the command will occupy IF it
// commits — commitment is confirmed later, when the entry arrives on the
// apply channel.
func (n *Node) Propose(data []byte) (index uint64, term uint64, isLeader bool) {
	ch := make(chan propResult, 1)
	req := propReq{data: data, ch: ch}

	select {
	case <-n.done:
		return 0, 0, false
	case n.propCh <- req:
	}

	select {
	case <-n.done:
		return 0, 0, false
	case res := <-ch:
		return res.index, res.term, res.isLeader
	}
}

// LastApplied returns the highest index delivered to the apply channel.
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// ReadIndex confirms leadership (majority heartbeat) and returns the commit
// index that must be applied before a linearizable local read is safe.
// Concurrent callers share one confirmation round (batched). While a read
// lease is held, callers bypass the batcher and run in parallel.
func (n *Node) ReadIndex(ctx context.Context) (readIndex uint64, ok bool) {
	n.mu.Lock()
	if n.role == Leader && time.Now().Before(n.readLeaseUntil) {
		idx := n.commitIndex
		n.mu.Unlock()
		if n.WaitApplied(ctx, idx) {
			return idx, true
		}
		return 0, false
	}
	n.mu.Unlock()

	ch := make(chan readResult, 1)
	req := readReq{ctx: ctx, ch: ch}

	select {
	case <-n.done:
		return 0, false
	case <-ctx.Done():
		return 0, false
	case n.readCh <- req:
	}

	select {
	case <-n.done:
		return 0, false
	case <-ctx.Done():
		return 0, false
	case res := <-ch:
		return res.index, res.ok
	}
}

// WaitApplied blocks until lastApplied >= index, ctx cancels, or the node stops.
func (n *Node) WaitApplied(ctx context.Context, index uint64) bool {
	n.mu.Lock()
	if n.lastApplied >= index {
		n.mu.Unlock()
		return true
	}
	n.mu.Unlock()

	deadline := time.Now().Add(5 * time.Second)
	for {
		n.mu.Lock()
		ok := !n.stopped && n.lastApplied >= index
		stopped := n.stopped
		n.mu.Unlock()
		if ok {
			return true
		}
		if stopped || ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-n.done:
			return false
		case <-time.After(time.Millisecond):
			// 1ms poll: at 3000 reads/s the old 200µs poll cost thousands of
			// extra mutex acquisitions per second on the same mutex the
			// election timer and RPC handlers need.
		}
	}
}

// ---------------------------------------------------------------------------
// Election timer + becoming a candidate
// ---------------------------------------------------------------------------

// resetElectionTimerLocked restarts the countdown with a fresh random
// timeout. Called when we hear from a legitimate leader, grant a vote, or
// start an election ourselves. Caller holds mu.
func (n *Node) resetElectionTimerLocked() {
	n.electionReset = time.Now()
	// Candidacy backoff: each consecutive failed round doubles the random
	// spread (capped at 8x). The floor stays at ElectionTimeoutMin — we never
	// get slower to NOTICE a dead leader, only less aggressive about
	// re-campaigning when campaigns keep failing. Reset to 1x on any real
	// leader contact or on winning.
	spread := n.tm.ElectionTimeoutMax - n.tm.ElectionTimeoutMin
	shift := min(n.electionAttempts, 3)
	n.electionTimeout = n.tm.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(spread<<shift)))
}

// heardLeaderRecentlyLocked reports whether we believe a live leader exists:
// we know who it is and it (or a vote we granted) touched our election clock
// within the minimum election timeout. While true, elections are disruptions,
// not repairs — pre-vote polls and higher-term vote requests are refused.
// Caller holds mu.
func (n *Node) heardLeaderRecentlyLocked() bool {
	return n.leaderID != 0 && time.Since(n.electionReset) < n.tm.ElectionTimeoutMin
}

// runElectionTimer is the follower/candidate watchdog: if no heartbeat or
// granted vote resets the clock within the randomized timeout, assume the
// leader is dead and stand for election.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.tm.TickInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		// Leaders don't run election timeouts — but they do run CheckQuorum:
		// if no majority has answered us within a full election timeout, the
		// rest of the cluster has likely moved on (or we're partitioned).
		// Step down quietly rather than linger as a zombie serving stale
		// lease reads.
		if n.role == Leader {
			if time.Since(n.lastQuorumCheck) >= n.tm.ElectionTimeoutMax {
				n.lastQuorumCheck = time.Now()
				if !n.quorumActiveLocked() {
					n.stepDownLocked()
					n.mu.Unlock()
					continue
				}
			}
			n.checkCommitStallLocked()
			n.mu.Unlock()
			continue
		}
		if time.Since(n.electionReset) < n.electionTimeout {
			n.mu.Unlock()
			continue
		}
		n.startPreVoteLocked()
		n.mu.Unlock()
	}
}

// startPreVoteLocked polls the cluster with a NON-BINDING "could I win?"
// question before any real election (Raft thesis §9.6). Nothing changes on
// this node or the voters — no term bump, no votedFor, no fsync — so a node
// that cannot win (partitioned, starved, out-of-date log) disturbs nobody,
// and terms only advance when a majority already agrees the leader is gone.
// This is the storm-breaker: under the old code every timeout burned a term
// and an fsync on 7 nodes, and any node returning from a stall could dethrone
// a healthy leader with its inflated term. Caller holds mu.
func (n *Node) startPreVoteLocked() {
	n.electionAttempts++
	n.preVoteRound++
	n.resetElectionTimerLocked() // schedule the next attempt (with backoff)

	round := n.preVoteRound
	term := n.currentTerm
	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()

	// Count ourselves; a single-node cluster has nobody to poll or disturb,
	// so it may proceed straight to the real election.
	votes := 1
	majority := (len(n.peers)+1)/2 + 1
	if votes >= majority {
		n.startElectionLocked()
		return
	}

	for _, peer := range n.peers {
		go func(peer uint64) {
			req := &raftpb.RequestVoteRequest{
				Term:         term + 1, // the term we WOULD start
				CandidateId:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
				PreVote:      true,
			}
			ctx, cancel := context.WithTimeout(context.Background(), n.tm.ElectionTimeoutMax)
			defer cancel()
			resp, err := n.transport.RequestVote(ctx, peer, req)
			if err != nil {
				return // unreachable peer simply doesn't answer the poll
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term)
				return
			}
			// Only act if this node is still the same follower running the
			// same poll — an old round's answer is history, not information.
			if n.role != Follower || n.currentTerm != term ||
				n.preVoteRound != round || !resp.VoteGranted {
				return
			}
			votes++
			if votes == majority {
				n.startElectionLocked()
			}
		}(peer)
	}
}

// startElectionLocked transitions to candidate and solicits votes (§5.2).
// Caller holds mu.
func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id // vote for ourselves
	n.leaderID = 0
	n.resetElectionTimerLocked() // a fresh timeout for THIS election; if it
	// expires with no winner (split vote), the timer fires again and we start
	// a new election in a higher term.
	if err := n.persist.saveHardState(n.currentTerm, n.votedFor); err != nil {
		// Can't durably record our own candidacy — abort it. We'll retry on
		// the next timer expiry.
		n.role = Follower
		return
	}

	term := n.currentTerm
	lastIdx := n.log.lastIndex()
	lastTerm := n.log.lastTerm()

	// votes counts ourselves already. Responses arrive on goroutines; the
	// shared counter is guarded by n.mu.
	votes := 1
	majority := (len(n.peers)+1)/2 + 1
	if votes >= majority {
		// Single-node cluster: our own vote IS the majority.
		n.becomeLeaderLocked()
		return
	}

	for _, peer := range n.peers {
		go func(peer uint64) {
			req := &raftpb.RequestVoteRequest{
				Term:         term,
				CandidateId:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			// The deadline bounds how long a vote request can dangle; a dead
			// peer shouldn't hold RPC resources past the election's relevance.
			ctx, cancel := context.WithTimeout(context.Background(), n.tm.ElectionTimeoutMax)
			defer cancel()
			resp, err := n.transport.RequestVote(ctx, peer, req)
			if err != nil {
				return // unreachable peer simply doesn't vote
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			// The world may have moved while the RPC was in flight. Only
			// count the vote if we are STILL the candidate of that same term
			// — otherwise the response is history, not information.
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term)
				return
			}
			if n.role != Candidate || n.currentTerm != term || !resp.VoteGranted {
				return
			}
			votes++
			if votes == majority {
				n.becomeLeaderLocked()
			}
		}(peer)
	}
}

// noteContactLocked records that a peer answered one of our RPCs while we
// lead. Fed by every leader-side response path; consumed by CheckQuorum.
// Caller holds mu.
func (n *Node) noteContactLocked(peer uint64) {
	if n.lastContact != nil {
		n.lastContact[peer] = time.Now()
	}
}

// quorumActiveLocked reports whether a majority (counting ourselves) has
// answered us within the last full election timeout. Caller holds mu.
func (n *Node) quorumActiveLocked() bool {
	majority := (len(n.peers)+1)/2 + 1
	count := 1 // self
	for _, peer := range n.peers {
		if time.Since(n.lastContact[peer]) < n.tm.ElectionTimeoutMax {
			count++
		}
	}
	return count >= majority
}

// noteCommitProgressLocked records that commitIndex just moved. Fed by
// maybeAdvanceCommitLocked; consumed by the commit-stall watchdog. Caller
// holds mu.
func (n *Node) noteCommitProgressLocked() {
	n.lastCommitProgress = time.Now()
	n.lastProgressIndex = n.commitIndex
}

// checkCommitStallLocked deposes this leader if it is holding uncommitted
// entries that have stopped making progress, despite a reachable majority.
//
// CheckQuorum cannot catch this. A leader whose per-follower replicators are
// wedged still exchanges heartbeats, so quorumActiveLocked stays true forever
// while commitIndex never moves — and PreVote plus leader stickiness actively
// stop a healthy follower from taking over. Stepping down is the only exit,
// and it is the one a human performed by hand (killing the leader container)
// to end the 2026-07-28 outage after 48 hours.
//
// Three conditions must ALL hold, and the first is the load-bearing one:
//
//  1. Uncommitted work exists. An idle cluster makes no commit progress by
//     definition; without this guard the watchdog would depose the leader on
//     every quiet period and produce exactly the election churn the PreVote
//     work was written to prevent.
//  2. No progress for the full CommitStallTimeout.
//  3. A majority is still answering us. If it is not, this is an ordinary
//     partition and CheckQuorum above already owns the response.
//
// Caller holds mu.
func (n *Node) checkCommitStallLocked() {
	if n.tm.CommitStallTimeout < 0 {
		return // watchdog disabled
	}
	if n.log.lastIndex() <= n.commitIndex {
		// Nothing pending: fully caught up, or idle. Keep the clock fresh so
		// the first proposal after a quiet spell gets a full timeout.
		n.noteCommitProgressLocked()
		return
	}
	if n.lastCommitProgress.IsZero() || n.commitIndex != n.lastProgressIndex {
		// Commit moved since we last looked. Compare the index rather than
		// trusting every commit path to call the hook: this way a future path
		// that advances commitIndex without notifying cannot make the watchdog
		// fire on a cluster that is in fact healthy.
		n.noteCommitProgressLocked()
		return
	}
	if time.Since(n.lastCommitProgress) < n.tm.CommitStallTimeout {
		return
	}
	if !n.quorumActiveLocked() {
		return // unreachable majority is CheckQuorum's problem, not ours
	}
	n.commitStallStepdowns++
	n.stepDownLocked()
}

// CommitStallStepdowns reports how many times this node has deposed itself for
// lack of commit progress. Cumulative for the process lifetime; surfaced as a
// gauge so a wedge that recurs is visible instead of silent.
func (n *Node) CommitStallStepdowns() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitStallStepdowns
}

// stepDownLocked relinquishes leadership WITHOUT changing terms. This must
// not reuse becomeFollowerLocked: that helper resets votedFor, and erasing
// our own same-term vote (we voted for ourselves when elected) would let us
// grant a second vote in this term — a double-vote safety hole. Here nothing
// persistent changes: same term, same votedFor, no fsync. The heartbeat and
// replication goroutines observe role != Leader and exit on their own.
// Caller holds mu.
func (n *Node) stepDownLocked() {
	n.role = Follower
	n.leaderID = 0
	n.readLeaseUntil = time.Time{}
	n.resetElectionTimerLocked()
}

// becomeFollowerLocked adopts a higher term and steps down. This is the
// universal "someone knows more than us" reaction every RPC send/receive path
// funnels through. Caller holds mu.
func (n *Node) becomeFollowerLocked(term uint64) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = noVote
	n.readLeaseUntil = time.Time{}
	// Persist: forgetting this term after a crash would let us double-vote.
	_ = n.persist.saveHardState(n.currentTerm, n.votedFor)
	n.resetElectionTimerLocked()
}

// becomeLeaderLocked initializes leader state after winning an election.
// Caller holds mu.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.id
	n.electionAttempts = 0 // campaigns work again; drop the backoff

	// nextIndex starts optimistically at "just past my log" — the consistency
	// check walks it back only as far as each follower actually needs (§5.3).
	// matchIndex starts at 0: we KNOW nothing about followers until they ack.
	n.nextIndex = make(map[uint64]uint64, len(n.peers))
	n.matchIndex = make(map[uint64]uint64, len(n.peers))
	n.lastContact = make(map[uint64]time.Time, len(n.peers))
	now := time.Now()
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.log.lastIndex() + 1
		n.matchIndex[peer] = 0
		// Seed contact at "now" — a grace period so a freshly elected leader
		// isn't deposed by CheckQuorum before its first heartbeats land.
		n.lastContact[peer] = now
	}
	n.lastQuorumCheck = now
	// Seed the stall clock for the same reason lastContact is seeded above: a
	// brand-new leader must not be deposed for a stall it inherited.
	n.lastCommitProgress = now
	n.lastProgressIndex = n.commitIndex

	// Clear any prior read lease — leadership is new.
	n.readLeaseUntil = time.Time{}

	// One long-lived replicator loop per follower owns all sending for this
	// leadership: announcement, heartbeats, entries, catch-up, snapshots.
	// Its first pass fires immediately, announcing leadership before other
	// nodes' election timers can go off.
	n.repls = make([]*replState, 0, len(n.peers))
	for _, peer := range n.peers {
		rs := &replState{peer: peer, term: n.currentTerm, notify: make(chan struct{}, 1)}
		n.repls = append(n.repls, rs)
		n.wg.Add(1)
		go n.runReplicator(rs)
	}
}

// ---------------------------------------------------------------------------
// RPC handlers (the receiving side of the protocol)
// ---------------------------------------------------------------------------

// HandleRequestVote decides whether to vote for a candidate (§5.2, §5.4.1),
// or answers a non-binding pre-vote poll (thesis §9.6).
func (n *Node) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// A stopped node's persister is closed; an in-flight RPC that raced the
	// shutdown must not touch it.
	if n.stopped {
		return &raftpb.RequestVoteResponse{Term: n.currentTerm}
	}

	// --- Pre-vote: answer WITHOUT touching any state. ---
	// No term adoption, no votedFor, no fsync, no timer reset. Granting many
	// pre-votes in one term is safe precisely because a grant binds nothing.
	if req.PreVote {
		resp := &raftpb.RequestVoteResponse{Term: n.currentTerm}
		upToDate := req.LastLogTerm > n.log.lastTerm() ||
			(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
		switch {
		case req.Term <= n.currentTerm:
			// The term it would start doesn't even beat ours.
		case n.heardLeaderRecentlyLocked():
			// Leader stickiness: our leader is alive; an election now would
			// be disruption, not repair.
		case !upToDate:
			// It couldn't win the real election either (§5.4.1).
		default:
			resp.VoteGranted = true
		}
		return resp
	}

	// --- Real vote. ---
	// Leader stickiness, second layer: refuse to even ADOPT a higher term
	// while our leader is demonstrably alive. Without this, one disruptive
	// candidate (or a legacy/raw RequestVote) still dethrones a healthy
	// leader the moment its term is bigger.
	if req.Term > n.currentTerm && n.heardLeaderRecentlyLocked() {
		return &raftpb.RequestVoteResponse{Term: n.currentTerm}
	}

	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
	}

	resp := &raftpb.RequestVoteResponse{Term: n.currentTerm}

	// Stale candidate from an older term: refuse, and our Term in the
	// response tells it to step down.
	if req.Term < n.currentTerm {
		return resp
	}

	// One vote per term: grant only if we haven't voted, or already voted for
	// this same candidate (a retransmitted request must get the same answer).
	alreadyCommitted := n.votedFor != noVote && n.votedFor != req.CandidateId
	if alreadyCommitted {
		return resp
	}

	// Election Restriction (§5.4.1): only vote for a candidate whose log is
	// at least as up-to-date as ours — last terms compared first, then
	// lengths. Since an entry is committed once a MAJORITY holds it, and a
	// candidate needs a MAJORITY of votes, the overlap guarantees the winner
	// holds every committed entry. This one check is why Raft never needs to
	// "copy missing data to the new leader" like Paxos variants do.
	upToDate := req.LastLogTerm > n.log.lastTerm() ||
		(req.LastLogTerm == n.log.lastTerm() && req.LastLogIndex >= n.log.lastIndex())
	if !upToDate {
		return resp
	}

	n.votedFor = req.CandidateId
	if err := n.persist.saveHardState(n.currentTerm, n.votedFor); err != nil {
		// If the vote can't be made durable we must not grant it — a crash
		// could otherwise let us vote twice in this term.
		n.votedFor = noVote
		return resp
	}
	// Granting a vote means we believe an election is legitimately underway;
	// give the candidate time to win before we'd stand ourselves.
	n.resetElectionTimerLocked()
	resp.VoteGranted = true
	return resp
}

// HandleAppendEntries processes replication/heartbeat traffic from a leader
// (§5.3). This single handler does triple duty: suppresses elections
// (heartbeat), repairs divergent logs (consistency check + truncation), and
// advances the follower's commit point.
func (n *Node) HandleAppendEntries(req *raftpb.AppendEntriesRequest) *raftpb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Shutdown race guard: the persister is closed once stopped.
	if n.stopped {
		return &raftpb.AppendEntriesResponse{Term: n.currentTerm}
	}

	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
	}

	resp := &raftpb.AppendEntriesResponse{Term: n.currentTerm}

	// A stale leader from an older term. Refuse; the Term in our response
	// forces it to step down.
	if req.Term < n.currentTerm {
		return resp
	}

	// Same-term AppendEntries proves a live, legitimate leader exists (only
	// one node can win any term). If we were mid-candidacy in this term, we
	// lost — stand down.
	n.role = Follower
	n.leaderID = req.LeaderId
	n.electionAttempts = 0 // real leader contact; drop any candidacy backoff
	n.resetElectionTimerLocked()

	// --- Consistency check (the Log Matching enforcement, §5.3). ---
	// We must hold an entry at prevLogIndex with prevLogTerm; otherwise our
	// log diverges from the leader's and appending here would corrupt it.
	if req.PrevLogIndex > n.log.lastIndex() {
		// We're missing entries entirely. Tell the leader how long our log
		// actually is so it can jump straight there instead of probing
		// backwards one index per round-trip.
		resp.ConflictIndex = n.log.lastIndex() + 1
		return resp
	}
	if req.PrevLogIndex >= n.log.firstIndex() {
		ourTerm, ok := n.log.term(req.PrevLogIndex)
		if !ok || ourTerm != req.PrevLogTerm {
			// We have an entry there but from the wrong term. Report that
			// term and where it starts — the leader skips the whole term.
			resp.ConflictTerm = ourTerm
			resp.ConflictIndex = n.log.firstIndexOfTerm(ourTerm)
			if resp.ConflictIndex == 0 {
				resp.ConflictIndex = n.log.firstIndex() + 1
			}
			return resp
		}
	}
	// (prevLogIndex below firstIndex means it's inside our snapshot — those
	// entries are committed and immutable, so the check trivially passes.)

	// --- Append, resolving conflicts in the leader's favor. ---
	// Walk the incoming entries: skip those we already hold (an identical
	// (index, term) means identical content — Log Matching), truncate at the
	// first mismatch, then append the rest. We must NOT blindly truncate at
	// prevLogIndex+1: a delayed, duplicate AppendEntries carrying entries we
	// already extended past would then erase good entries.
	newEntries := req.Entries
	for i, e := range req.Entries {
		if e.Index <= n.log.firstIndex() {
			// Already compacted into the snapshot — necessarily committed,
			// necessarily identical.
			newEntries = req.Entries[i+1:]
			continue
		}
		if e.Index > n.log.lastIndex() {
			newEntries = req.Entries[i:]
			break
		}
		ourTerm, _ := n.log.term(e.Index)
		if ourTerm != e.Term {
			// Conflict: OUR entry was never committed (a committed entry
			// can't conflict with the leader's log), so discarding it and
			// everything after is safe — and required.
			n.log.truncateFrom(e.Index)
			if err := n.persist.truncateFrom(e.Index); err != nil {
				return resp // can't persist the truncation → can't ack
			}
			newEntries = req.Entries[i:]
			break
		}
		// Identical entry already present; nothing to write.
		newEntries = req.Entries[i+1:]
	}
	if len(newEntries) > 0 {
		n.log.append(newEntries...)
		// Disk BEFORE ack: our "success" is the leader's evidence that this
		// entry is on a majority of disks. Acking from memory would let a
		// crash silently shrink the majority and lose a committed entry.
		if err := n.persist.appendEntries(newEntries); err != nil {
			return resp
		}
	}

	// --- Advance our commit point. ---
	// The leader's commitIndex may exceed what IT knows we hold; cap by our
	// last new entry. Everything up to the new commitIndex is now safe to
	// apply locally.
	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, max(lastNew, n.commitIndex))
		n.applyCond.Broadcast()
	}

	resp.Success = true
	return resp
}

// HandleInstallSnapshot accepts a full state image from the leader (§7),
// used when this follower lags so far behind that the leader has already
// compacted away the log entries it would need.
func (n *Node) HandleInstallSnapshot(req *raftpb.InstallSnapshotRequest) *raftpb.InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Shutdown race guard: the persister is closed once stopped.
	if n.stopped {
		return &raftpb.InstallSnapshotResponse{Term: n.currentTerm}
	}

	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
	}
	resp := &raftpb.InstallSnapshotResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp
	}

	n.role = Follower
	n.leaderID = req.LeaderId
	n.electionAttempts = 0 // real leader contact; drop any candidacy backoff
	n.resetElectionTimerLocked()

	// A snapshot older than our commit point adds nothing (a delayed or
	// duplicate RPC) — everything in it is already reflected locally.
	if req.LastIncludedIndex <= n.commitIndex {
		return resp
	}

	// Persist the snapshot and rewrite our log around it. Entries beyond the
	// snapshot that we already hold and that agree with it are kept (§7's
	// "retain log entries covered by the snapshot... if consistent").
	n.log.compactTo(req.LastIncludedIndex, req.LastIncludedTerm)
	if err := n.persist.saveSnapshot(
		req.LastIncludedIndex, req.LastIncludedTerm, req.Data,
		n.currentTerm, n.votedFor, n.log.allEntries(),
	); err != nil {
		return resp
	}
	// Keep the cache in step so this node can ship the snapshot straight from
	// memory if it is elected later.
	n.snapCache = snapshotCache{index: req.LastIncludedIndex, term: req.LastIncludedTerm, data: req.Data}

	// The snapshot IS applied state: jump both cursors to it and queue it for
	// the state machine, which must load it before consuming further entries.
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex
	n.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotIndex: req.LastIncludedIndex,
		SnapshotTerm:  req.LastIncludedTerm,
	}
	n.applyCond.Broadcast()
	return resp
}

// ---------------------------------------------------------------------------
// Snapshot creation (called from above, by the state machine)
// ---------------------------------------------------------------------------

// Snapshot tells Raft the state machine has captured its state up to and
// including index, so the log prefix through index can be discarded. The
// state machine drives this (it alone knows when a snapshot is worth taking
// and what bytes represent its state); Raft handles the durability and
// bookkeeping.
func (n *Node) Snapshot(index uint64, data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.log.firstIndex() {
		return nil // already compacted this far
	}
	if index > n.lastApplied {
		return fmt.Errorf("raft: cannot snapshot at %d beyond lastApplied %d", index, n.lastApplied)
	}
	// Capture the snapshot point's term BEFORE compaction can discard it.
	snapTerm, ok := n.log.term(index)
	if !ok {
		return fmt.Errorf("raft: snapshot index %d not in log", index)
	}

	// Compact only to index-retain: the snapshot file covers through index,
	// but a small tail of already-snapshotted entries stays in the log so
	// followers a few entries behind keep using cheap AppendEntries instead
	// of full snapshot transfers. The margin evaporates on restart (recovery
	// drops replayed entries at or below the snapshot index) — harmless.
	cut := index
	if n.snapshotRetain > 0 {
		if index > n.snapshotRetain {
			cut = index - n.snapshotRetain
		} else {
			cut = 0
		}
	}
	if cut > n.log.firstIndex() {
		if cutTerm, ok := n.log.term(cut); ok {
			n.log.compactTo(cut, cutTerm)
		}
	}
	if err := n.persist.saveSnapshot(index, snapTerm, data, n.currentTerm, n.votedFor, n.log.allEntries()); err != nil {
		return err
	}
	// Serve future InstallSnapshot sends from here instead of from disk. The
	// caller hands us a freshly serialized image each time and does not retain
	// it, so keeping the reference is safe.
	n.snapCache = snapshotCache{index: index, term: snapTerm, data: data}
	return nil
}

// FirstIndex returns the log's compaction point (0 if never compacted).
// Introspection for tests and dashboards.
func (n *Node) FirstIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.firstIndex()
}

// ---------------------------------------------------------------------------
// The applier: committed entries → the state machine
// ---------------------------------------------------------------------------

// runApplier is the only goroutine that sends on applyCh. It sleeps until
// commitIndex moves past lastApplied (or a snapshot arrives), then delivers
// in strict order. Delivery happens with mu RELEASED — the state machine may
// take arbitrarily long, and blocking Raft's mutex on it would freeze
// heartbeats and elections.
func (n *Node) runApplier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for !n.stopped && n.pendingSnapshot == nil && n.commitIndex == n.lastApplied {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}

		// Snapshot first: it is the baseline any subsequent entries build on.
		if snap := n.pendingSnapshot; snap != nil {
			n.pendingSnapshot = nil
			n.mu.Unlock()
			n.applyCh <- *snap
			continue
		}

		// Collect the newly committed batch while holding the lock...
		var batch []ApplyMsg
		for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
			e := n.log.entry(i)
			batch = append(batch, ApplyMsg{
				CommandValid: true,
				Command:      e.Data,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			})
		}
		n.lastApplied = n.commitIndex
		n.mu.Unlock()

		// ...and deliver it without the lock.
		for _, msg := range batch {
			n.applyCh <- msg
		}
	}
}
