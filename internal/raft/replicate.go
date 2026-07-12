package raft

// replicate.go is the leader side of log replication (§5.3): the heartbeat
// drumbeat, per-follower catch-up via nextIndex/matchIndex, snapshot shipping
// for followers that lag past the compaction point, and the commit rule.
//
// The flow, per follower:
//
//   1. Send AppendEntries with everything from nextIndex[peer] onward,
//      preceded by (prevLogIndex, prevLogTerm) for the consistency check.
//   2. Follower says success → matchIndex catches up, maybe commit advances.
//      Follower says conflict → walk nextIndex back (fast, using the
//      follower's conflict hints) and try again.
//   3. If nextIndex has fallen behind our log's compaction point, the entries
//      the follower needs no longer exist — ship the snapshot instead.
//
// Everything here follows one concurrency rule: build the request under the
// mutex, do the network call WITHOUT the mutex, re-take it to process the
// response, and re-validate that the world (term, role) hasn't changed while
// the RPC was in flight. Stale responses must inform, never act.

import (
	"context"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raftpb"
)

// runHeartbeats sends AppendEntries to every follower on a fixed interval for
// as long as this node remains leader of the term it was elected in. Started
// by becomeLeaderLocked; exits on step-down or shutdown.
//
// Heartbeats are not a separate message type — they are ordinary
// AppendEntries that happen to carry zero entries (or, conveniently, whatever
// entries the follower is missing). This unification means every heartbeat
// also repairs lagging followers and re-delivers the leader's commitIndex.
func (n *Node) runHeartbeats() {
	defer n.wg.Done()

	n.mu.Lock()
	myTerm := n.currentTerm
	n.mu.Unlock()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.stopped || n.role != Leader || n.currentTerm != myTerm {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		n.broadcastAppendEntries()
	}
}

// broadcastAppendEntries kicks off one replication round to every peer.
// Called on the heartbeat tick, immediately on winning an election, and
// immediately on Propose (so commit latency is one RTT, not one heartbeat).
func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	n.mu.Unlock()

	for _, peer := range n.peers {
		go n.replicateToPeer(peer, term)
	}
}

// replicateToPeer sends one AppendEntries (or InstallSnapshot) to one peer
// and processes the response. term is the leadership term this round belongs
// to; if the node's term has moved on by the time we look again, the round is
// abandoned.
func (n *Node) replicateToPeer(peer uint64, term uint64) {
	n.mu.Lock()
	if n.stopped || n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	next := n.nextIndex[peer]

	// The follower needs entries we no longer have in the log (they were
	// compacted into a snapshot). Ship the whole snapshot; the log-based
	// path resumes from the snapshot's index afterwards.
	if next <= n.log.firstIndex() {
		n.sendSnapshotLocked(peer, term) // unlocks internally
		return
	}

	// prev(LogIndex|Term) anchor the consistency check: the follower accepts
	// the new entries only if it also holds THIS entry. next > firstIndex is
	// guaranteed above, so the term lookup cannot miss (the sentinel answers
	// for the compaction boundary itself).
	prevIdx := next - 1
	prevTerm, _ := n.log.term(prevIdx)

	req := &raftpb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.suffix(next),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval*2)
	defer cancel()
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	if err != nil {
		return // dead/unreachable peer; the next heartbeat retries
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	if resp.Term > n.currentTerm {
		// Someone out there has a newer term — we are yesterday's leader.
		n.becomeFollowerLocked(resp.Term)
		return
	}
	// Stale response guard: only act if we're still the same-term leader.
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {
		// The follower now provably holds everything we sent. Advance both
		// cursors — using the request's own contents, not current state,
		// because the log may have grown while the RPC flew.
		newMatch := req.PrevLogIndex + uint64(len(req.Entries))
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		if newMatch+1 > n.nextIndex[peer] {
			n.nextIndex[peer] = newMatch + 1
		}
		n.maybeAdvanceCommitLocked()
		return
	}

	// Rejected: the consistency check failed. Use the follower's conflict
	// hints to leap nextIndex backwards (§5.3 fast backup) instead of
	// decrementing one step per round-trip.
	if resp.ConflictTerm != 0 {
		// If we also have entries of conflictTerm, resume just past our last
		// one (the logs agree up to there at best). If we've never seen that
		// term, skip the follower's entire run of it.
		if last := n.log.lastIndexOfTerm(resp.ConflictTerm); last != 0 {
			n.nextIndex[peer] = last + 1
		} else {
			n.nextIndex[peer] = resp.ConflictIndex
		}
	} else if resp.ConflictIndex != 0 {
		// Follower's log is simply shorter; jump straight to its end.
		n.nextIndex[peer] = resp.ConflictIndex
	} else {
		// No hints (shouldn't happen with our follower) — classic decrement.
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
	// Retry immediately with the repaired nextIndex rather than waiting a
	// full heartbeat — divergent followers converge in a few round-trips.
	go n.replicateToPeer(peer, term)
}

// sendSnapshotLocked ships the current snapshot to a follower whose nextIndex
// predates our earliest retained log entry. Caller holds mu; this function
// releases it around the RPC.
func (n *Node) sendSnapshotLocked(peer uint64, term uint64) {
	snapIdx, snapTerm, data, err := n.persist.readSnapshot()
	if err != nil || snapIdx == 0 {
		n.mu.Unlock()
		return
	}
	req := &raftpb.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          n.id,
		LastIncludedIndex: snapIdx,
		LastIncludedTerm:  snapTerm,
		Data:              data,
	}
	n.mu.Unlock()

	// Snapshots can be large; give the transfer more room than a heartbeat.
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatInterval*10)
	defer cancel()
	resp, err := n.transport.InstallSnapshot(ctx, peer, req)
	if err != nil {
		return
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
	if n.role != Leader || n.currentTerm != term {
		return
	}
	// The follower now holds state through snapIdx; log replication resumes
	// from the entry right after it.
	if snapIdx > n.matchIndex[peer] {
		n.matchIndex[peer] = snapIdx
	}
	if snapIdx+1 > n.nextIndex[peer] {
		n.nextIndex[peer] = snapIdx + 1
	}
}

// maybeAdvanceCommitLocked applies the commit rule (§5.3, §5.4.2): find the
// highest index N such that a MAJORITY of nodes have matchIndex >= N and
// log[N].term == currentTerm, and commit through N. Caller holds mu.
//
// The currentTerm restriction is the subtle-but-critical part (the paper's
// Figure 8 scenario): an old-term entry can sit on a majority and STILL be
// overwritten by a later leader, so counting replicas alone is not enough.
// Committing only current-term entries — which implicitly commits every
// earlier entry beneath them — closes that hole.
func (n *Node) maybeAdvanceCommitLocked() {
	majority := (len(n.peers)+1)/2 + 1

	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		t, ok := n.log.term(idx)
		if !ok || t != n.currentTerm {
			// Below current term: stop — older entries commit implicitly
			// when a current-term entry above them does, never directly.
			break
		}
		count := 1 // the leader's own disk
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= majority {
			n.commitIndex = idx
			n.applyCond.Broadcast()
			// Followers learn the new commitIndex on the next heartbeat.
			return
		}
	}
}
