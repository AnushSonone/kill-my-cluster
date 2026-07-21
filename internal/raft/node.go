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
)

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

	// --- Leader-only state, reinitialized on every election win (§5.3). ---
	// nextIndex[peer]: index of the next entry to SEND to that peer (a guess,
	// repaired by AppendEntries rejections). matchIndex[peer]: highest index
	// KNOWN replicated on that peer (ground truth, only moves forward).
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

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
		currentTerm: st.term,
		votedFor:    st.votedFor,
		log:         newRaftLog(st.snapIndex, st.snapTerm),
		role:        Follower,
		applyCh:     cfg.ApplyCh,
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.log.append(st.entries...)

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
		case <-time.After(200 * time.Microsecond):
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
	n.electionTimeout = electionTimeoutMin +
		time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

// runElectionTimer is the follower/candidate watchdog: if no heartbeat or
// granted vote resets the clock within the randomized timeout, assume the
// leader is dead and stand for election.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		// Leaders don't time out — they're the ones sending heartbeats.
		if n.role == Leader {
			n.mu.Unlock()
			continue
		}
		if time.Since(n.electionReset) < n.electionTimeout {
			n.mu.Unlock()
			continue
		}
		n.startElectionLocked()
		n.mu.Unlock()
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
			ctx, cancel := context.WithTimeout(context.Background(), electionTimeoutMax)
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

	// nextIndex starts optimistically at "just past my log" — the consistency
	// check walks it back only as far as each follower actually needs (§5.3).
	// matchIndex starts at 0: we KNOW nothing about followers until they ack.
	n.nextIndex = make(map[uint64]uint64, len(n.peers))
	n.matchIndex = make(map[uint64]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.log.lastIndex() + 1
		n.matchIndex[peer] = 0
	}

	// Announce leadership immediately — the empty AppendEntries suppresses
	// other nodes' election timers before they can fire.
	go n.broadcastAppendEntries()

	// And keep the heartbeat drumbeat going for as long as we lead.
	// Clear any prior read lease — leadership is new.
	n.readLeaseUntil = time.Time{}

	n.wg.Add(1)
	go n.runHeartbeats()
}

// ---------------------------------------------------------------------------
// RPC handlers (the receiving side of the protocol)
// ---------------------------------------------------------------------------

// HandleRequestVote decides whether to vote for a candidate (§5.2, §5.4.1).
func (n *Node) HandleRequestVote(req *raftpb.RequestVoteRequest) *raftpb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

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

	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
	}
	resp := &raftpb.InstallSnapshotResponse{Term: n.currentTerm}
	if req.Term < n.currentTerm {
		return resp
	}

	n.role = Follower
	n.leaderID = req.LeaderId
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
	term, ok := n.log.term(index)
	if !ok {
		return fmt.Errorf("raft: snapshot index %d not in log", index)
	}

	n.log.compactTo(index, term)
	return n.persist.saveSnapshot(index, term, data, n.currentTerm, n.votedFor, n.log.allEntries())
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
