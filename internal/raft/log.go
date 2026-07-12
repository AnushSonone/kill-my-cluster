package raft

// log.go is the in-memory view of the replicated Raft log.
//
// The log is THE central data structure of Raft: an ordered sequence of
// entries, each holding (index, term, command bytes). Every safety argument in
// the paper is ultimately an argument about which log prefixes are identical
// on which machines. This file only manages the in-memory slice; durability
// lives in persist.go, and the rules for *changing* the log live in the RPC
// handlers (node.go / replicate.go).
//
// ---------------------------------------------------------------------------
// The sentinel trick
// ---------------------------------------------------------------------------
// After snapshot compaction, the log no longer starts at index 1 — everything
// up to some index has been folded into a snapshot and discarded. We keep a
// single "sentinel" entry at position 0 of the slice whose Index/Term record
// the last compacted entry (lastIncludedIndex/lastIncludedTerm), with no data.
//
// Why? AppendEntries' consistency check needs term(prevLogIndex) even when
// prevLogIndex is exactly the compaction point. With the sentinel, that lookup
// works uniformly with no special cases: entry i lives at slice position
// i - sentinel.Index, and the sentinel itself answers for the compacted
// boundary. A fresh, never-compacted log has sentinel (Index: 0, Term: 0),
// matching the paper's convention that the log starts at index 1.

import (
	pb "github.com/AnushSonone/kill-my-cluster/internal/raftpb"
)

// raftLog holds the suffix of the replicated log that has not yet been
// compacted into a snapshot. It is not safe for concurrent use; the owning
// Node's mutex guards all access.
type raftLog struct {
	// entries[0] is always the sentinel (see file comment). Real entries, if
	// any, follow at positions 1..n and carry consecutive indexes starting at
	// sentinel.Index + 1.
	entries []*pb.LogEntry
}

// newRaftLog returns a log whose compaction point is (index, term). For a
// brand-new node that is (0, 0): "nothing compacted, log starts at 1".
func newRaftLog(lastIncludedIndex, lastIncludedTerm uint64) *raftLog {
	return &raftLog{
		entries: []*pb.LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}},
	}
}

// firstIndex returns the compaction point: the index of the last entry that
// was folded into a snapshot. Entries at or below this index no longer exist
// in memory (only their combined effect, inside the snapshot).
func (l *raftLog) firstIndex() uint64 {
	return l.entries[0].Index
}

// lastIndex returns the index of the last entry in the log (or the compaction
// point if the log holds no real entries).
func (l *raftLog) lastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

// lastTerm returns the term of the last entry (or of the compaction point).
func (l *raftLog) lastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

// term returns the term of the entry at index i, and whether that index is
// available in memory. i == firstIndex is answerable thanks to the sentinel;
// anything older has been compacted away and returns (0, false), which callers
// treat as "you need the snapshot, not the log".
func (l *raftLog) term(i uint64) (uint64, bool) {
	if i < l.firstIndex() || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-l.firstIndex()].Term, true
}

// entry returns the entry at index i. Callers must ensure firstIndex < i <=
// lastIndex (the sentinel is not a real entry).
func (l *raftLog) entry(i uint64) *pb.LogEntry {
	return l.entries[i-l.firstIndex()]
}

// suffix returns copies of all entries with index >= from, for sending in
// AppendEntries. It returns pointer copies (entries are treated as immutable
// once appended — nothing ever mutates a LogEntry in place, so sharing is
// safe and avoids copying command payloads on every heartbeat).
func (l *raftLog) suffix(from uint64) []*pb.LogEntry {
	if from <= l.firstIndex() || from > l.lastIndex() {
		return nil
	}
	src := l.entries[from-l.firstIndex():]
	out := make([]*pb.LogEntry, len(src))
	copy(out, src)
	return out
}

// append adds entries at the tail. Callers guarantee the first new entry's
// index is exactly lastIndex()+1 (the handlers enforce this by truncating
// conflicts first).
func (l *raftLog) append(entries ...*pb.LogEntry) {
	l.entries = append(l.entries, entries...)
}

// truncateFrom drops the entry at index i and everything after it. Used when
// a follower discovers its log conflicts with the leader's: the leader's log
// is authoritative, so the conflicting suffix is discarded (§5.3).
func (l *raftLog) truncateFrom(i uint64) {
	if i <= l.firstIndex() {
		// Cannot truncate into compacted territory; callers never ask to.
		i = l.firstIndex() + 1
	}
	l.entries = l.entries[:i-l.firstIndex()]
}

// firstIndexOfTerm returns the smallest in-memory index whose entry has the
// given term, or 0 if no such entry exists. Used to build the conflict hints
// that let a leader skip back over a whole divergent term in one round-trip.
func (l *raftLog) firstIndexOfTerm(t uint64) uint64 {
	for _, e := range l.entries[1:] { // skip sentinel
		if e.Term == t {
			return e.Index
		}
	}
	return 0
}

// lastIndexOfTerm returns the largest in-memory index whose entry has the
// given term, or 0 if none. The leader uses this with a follower's
// conflictTerm hint (§5.3 fast backup).
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for i := len(l.entries) - 1; i >= 1; i-- {
		if l.entries[i].Term == t {
			return l.entries[i].Index
		}
	}
	return 0
}

// compactTo discards all entries up to and including index, making (index,
// term) the new sentinel. Called after a snapshot has been durably saved: the
// discarded entries' effects live on inside the snapshot.
//
// If the log still holds entries after index, they are retained (a follower
// that received a snapshot may still have valid entries past it).
func (l *raftLog) compactTo(index, term uint64) {
	if index <= l.firstIndex() {
		return // already compacted at least this far
	}
	if index < l.lastIndex() {
		// Keep the suffix beyond the snapshot. Re-slice against a fresh
		// backing array so the compacted prefix can actually be garbage
		// collected (a plain re-slice would pin the old array in memory).
		keep := l.entries[index-l.firstIndex()+1:]
		fresh := make([]*pb.LogEntry, 0, len(keep)+1)
		fresh = append(fresh, &pb.LogEntry{Index: index, Term: term})
		fresh = append(fresh, keep...)
		l.entries = fresh
		return
	}
	// Snapshot covers the entire log: drop everything, keep only the sentinel.
	l.entries = []*pb.LogEntry{{Index: index, Term: term}}
}

// allEntries returns the real (non-sentinel) entries currently in memory.
// Used by persistence when rewriting the on-disk log during compaction.
func (l *raftLog) allEntries() []*pb.LogEntry {
	return l.entries[1:]
}
