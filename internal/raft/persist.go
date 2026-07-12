package raft

// persist.go makes Raft's critical state durable, reusing the Phase 1 WAL.
//
// ---------------------------------------------------------------------------
// What must be persisted, and why exactly this set
// ---------------------------------------------------------------------------
// The Raft paper (Figure 2) requires three things on stable storage, updated
// BEFORE responding to any RPC:
//
//   - currentTerm — if a node forgot its term after a crash, it could vote
//     again in an election it already voted in, under a term it has already
//     seen. Two nodes could then win the same term: two leaders, split brain.
//   - votedFor    — same reason: forgetting a vote means double-voting.
//   - log[]       — the entries themselves. A committed entry is "committed"
//     precisely because a majority has it ON DISK; if nodes lost log entries
//     on crash, a committed entry could vanish from a majority and be
//     overwritten by a new leader. That is data loss.
//
// Everything else (commitIndex, lastApplied, nextIndex/matchIndex, current
// role) is deliberately volatile — it is either rediscoverable from the
// persisted state or re-established by normal protocol traffic after restart.
//
// ---------------------------------------------------------------------------
// How we store it: one WAL, three record types
// ---------------------------------------------------------------------------
// We reuse the storage package's crash-safe framing (uint32 length + CRC32C +
// fsync + torn-tail recovery) rather than writing a second WAL implementation.
// Each framed payload starts with a one-byte record type:
//
//   | type=1 | term: uint64 | votedFor: uint64 |          — hard state update
//   | type=2 | protobuf-marshaled LogEntry |               — one appended entry
//   | type=3 | fromIndex: uint64 |                         — truncate marker
//
// Replaying these records in order deterministically rebuilds (currentTerm,
// votedFor, log) — the same "state = replay of history" idea as Phase 1.
// Truncations are rare (only on follower/leader log conflicts), so recording
// them as markers keeps the common path append-only and fast.
//
// The snapshot (snapshot.go's atomic-rename dance, replicated here) also
// compacts this WAL: after a snapshot is durable, we rewrite the WAL to hold
// only the hard state and the entries that survive past the snapshot.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AnushSonone/kill-my-cluster/internal/raftpb"
	"github.com/AnushSonone/kill-my-cluster/internal/storage"
	"google.golang.org/protobuf/proto"
)

const (
	raftWALName     = "raft.wal"
	raftWALTmpName  = "raft.wal.tmp"
	raftSnapName    = "raft.snapshot"
	raftSnapTmpName = "raft.snapshot.tmp"

	recHardState byte = 1
	recEntry     byte = 2
	recTruncate  byte = 3

	// noVote is the persisted votedFor value meaning "have not voted this
	// term". Node IDs are 1-based, so 0 is safely out of band.
	noVote uint64 = 0
)

// persister owns the on-disk representation of one node's Raft state. It is
// not safe for concurrent use; the owning Node's mutex serializes access
// (which also guarantees WAL records land in the same order as the in-memory
// mutations they describe — the property that makes replay correct).
type persister struct {
	dir string
	wal *storage.WAL
}

// persistedState is everything recovered from disk at startup.
type persistedState struct {
	term     uint64
	votedFor uint64
	// entries are the live (non-compacted) log entries, in order.
	entries []*raftpb.LogEntry
	// Snapshot metadata + payload; index 0 means "no snapshot exists".
	snapIndex uint64
	snapTerm  uint64
	snapData  []byte
}

// openPersister opens (or creates) the durable state under dir and recovers
// whatever a previous incarnation of this node persisted.
//
// Recovery order mirrors Phase 1: load the snapshot first (the baseline),
// then replay the WAL on top. The WAL's hard-state records overwrite as they
// replay, so the LAST one wins — exactly the semantics we want.
func openPersister(dir string) (*persister, persistedState, error) {
	st := persistedState{votedFor: noVote}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, st, fmt.Errorf("raft: create dir: %w", err)
	}

	// --- Load snapshot (if any). ---
	snapPath := filepath.Join(dir, raftSnapName)
	if raw, err := os.ReadFile(snapPath); err == nil {
		if len(raw) < 16 {
			return nil, st, fmt.Errorf("raft: snapshot file too short: %d bytes", len(raw))
		}
		st.snapIndex = binary.BigEndian.Uint64(raw[0:8])
		st.snapTerm = binary.BigEndian.Uint64(raw[8:16])
		st.snapData = raw[16:]
	} else if !os.IsNotExist(err) {
		return nil, st, fmt.Errorf("raft: read snapshot: %w", err)
	}

	// --- Replay the WAL. ---
	// We rebuild the entry list with a map-free approach: entries arrive in
	// index order; a truncate marker drops the suffix; appends after a
	// truncate re-extend. This mirrors exactly what the in-memory log did
	// while the records were being written.
	walPath := filepath.Join(dir, raftWALName)
	validOffset, err := storage.ReplayWAL(walPath, func(payload []byte) error {
		if len(payload) < 1 {
			return fmt.Errorf("raft: empty wal record")
		}
		switch payload[0] {
		case recHardState:
			if len(payload) != 17 {
				return fmt.Errorf("raft: bad hardstate record len %d", len(payload))
			}
			st.term = binary.BigEndian.Uint64(payload[1:9])
			st.votedFor = binary.BigEndian.Uint64(payload[9:17])
		case recEntry:
			e := &raftpb.LogEntry{}
			if err := proto.Unmarshal(payload[1:], e); err != nil {
				return fmt.Errorf("raft: unmarshal wal entry: %w", err)
			}
			st.entries = append(st.entries, e)
		case recTruncate:
			if len(payload) != 9 {
				return fmt.Errorf("raft: bad truncate record len %d", len(payload))
			}
			from := binary.BigEndian.Uint64(payload[1:9])
			// Drop every entry with index >= from.
			for len(st.entries) > 0 && st.entries[len(st.entries)-1].Index >= from {
				st.entries = st.entries[:len(st.entries)-1]
			}
		default:
			return fmt.Errorf("raft: unknown wal record type %d", payload[0])
		}
		return nil
	})
	if err != nil {
		return nil, st, err
	}

	// Truncate any torn tail so future appends start on a clean boundary
	// (same crash-recovery contract as the Phase 1 store).
	if fi, statErr := os.Stat(walPath); statErr == nil && fi.Size() > validOffset {
		if err := os.Truncate(walPath, validOffset); err != nil {
			return nil, st, fmt.Errorf("raft: truncate torn wal tail: %w", err)
		}
	}

	// Entries at or below the snapshot point are redundant (their effect is
	// inside the snapshot) — this happens if we crashed between "snapshot
	// saved" and "WAL rewritten". Dropping them here is the idempotent replay.
	if st.snapIndex > 0 {
		firstLive := 0
		for firstLive < len(st.entries) && st.entries[firstLive].Index <= st.snapIndex {
			firstLive++
		}
		st.entries = st.entries[firstLive:]
	}

	w, err := storage.OpenWAL(walPath)
	if err != nil {
		return nil, st, err
	}
	return &persister{dir: dir, wal: w}, st, nil
}

// saveHardState durably records (currentTerm, votedFor). Must complete before
// the node answers the RPC that caused the change — otherwise a crash could
// roll back a vote or a term, breaking election safety.
func (p *persister) saveHardState(term, votedFor uint64) error {
	rec := make([]byte, 17)
	rec[0] = recHardState
	binary.BigEndian.PutUint64(rec[1:9], term)
	binary.BigEndian.PutUint64(rec[9:17], votedFor)
	if err := p.wal.Append(rec); err != nil {
		return err
	}
	return p.wal.Sync()
}

// appendEntries durably appends log entries. All entries in one call share a
// single fsync (group commit) — the same throughput lever as Phase 1.
func (p *persister) appendEntries(entries []*raftpb.LogEntry) error {
	for _, e := range entries {
		body, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("raft: marshal entry: %w", err)
		}
		rec := make([]byte, 1+len(body))
		rec[0] = recEntry
		copy(rec[1:], body)
		if err := p.wal.Append(rec); err != nil {
			return err
		}
	}
	return p.wal.Sync()
}

// truncateFrom durably records that entries at index >= from are discarded
// (a follower resolving a conflict with the leader's authoritative log).
func (p *persister) truncateFrom(from uint64) error {
	rec := make([]byte, 9)
	rec[0] = recTruncate
	binary.BigEndian.PutUint64(rec[1:9], from)
	if err := p.wal.Append(rec); err != nil {
		return err
	}
	return p.wal.Sync()
}

// saveSnapshot durably installs a snapshot and compacts the WAL.
//
// Ordering is load-bearing, same as Phase 1's snapshot.go:
//  1. Write snapshot to a temp file, fsync, atomically rename into place,
//     fsync the directory. After this instant the snapshot is the durable
//     baseline no crash can corrupt.
//  2. Rewrite the WAL (also via temp + rename) to hold only the current hard
//     state and the entries that survive past the snapshot. If we crash
//     between 1 and 2, recovery replays stale WAL entries over the snapshot
//     and openPersister discards the overlap — harmless, only speed suffers.
func (p *persister) saveSnapshot(index, term uint64, data []byte, hardTerm, votedFor uint64, keep []*raftpb.LogEntry) error {
	// --- Step 1: atomic snapshot install. ---
	snapTmp := filepath.Join(p.dir, raftSnapTmpName)
	snapFinal := filepath.Join(p.dir, raftSnapName)

	buf := make([]byte, 16+len(data))
	binary.BigEndian.PutUint64(buf[0:8], index)
	binary.BigEndian.PutUint64(buf[8:16], term)
	copy(buf[16:], data)

	if err := writeFileSync(snapTmp, buf); err != nil {
		return err
	}
	if err := os.Rename(snapTmp, snapFinal); err != nil {
		return fmt.Errorf("raft: rename snapshot: %w", err)
	}
	if err := storage.SyncDir(p.dir); err != nil {
		return err
	}

	// --- Step 2: rewrite the WAL without the compacted prefix. ---
	// Build the replacement WAL in a temp file using the same framing (we go
	// through a fresh storage.WAL so the format stays identical), then swap.
	walTmp := filepath.Join(p.dir, raftWALTmpName)
	_ = os.Remove(walTmp) // stale temp from an earlier crash, if any

	tmpWAL, err := storage.OpenWAL(walTmp)
	if err != nil {
		return err
	}
	hs := make([]byte, 17)
	hs[0] = recHardState
	binary.BigEndian.PutUint64(hs[1:9], hardTerm)
	binary.BigEndian.PutUint64(hs[9:17], votedFor)
	if err := tmpWAL.Append(hs); err != nil {
		tmpWAL.Close()
		return err
	}
	for _, e := range keep {
		body, err := proto.Marshal(e)
		if err != nil {
			tmpWAL.Close()
			return fmt.Errorf("raft: marshal entry: %w", err)
		}
		rec := make([]byte, 1+len(body))
		rec[0] = recEntry
		copy(rec[1:], body)
		if err := tmpWAL.Append(rec); err != nil {
			tmpWAL.Close()
			return err
		}
	}
	if err := tmpWAL.Sync(); err != nil {
		tmpWAL.Close()
		return err
	}
	if err := tmpWAL.Close(); err != nil {
		return err
	}

	// Swap the new WAL into place. Close the live handle first (macOS/Windows
	// are stricter about renaming over open files than Linux).
	walPath := filepath.Join(p.dir, raftWALName)
	if err := p.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(walTmp, walPath); err != nil {
		return fmt.Errorf("raft: rename wal: %w", err)
	}
	if err := storage.SyncDir(p.dir); err != nil {
		return err
	}
	w, err := storage.OpenWAL(walPath)
	if err != nil {
		return err
	}
	p.wal = w
	return nil
}

// readSnapshot returns the current snapshot payload (for sending via
// InstallSnapshot to a lagging follower).
func (p *persister) readSnapshot() (index, term uint64, data []byte, err error) {
	raw, err := os.ReadFile(filepath.Join(p.dir, raftSnapName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil, nil
		}
		return 0, 0, nil, fmt.Errorf("raft: read snapshot: %w", err)
	}
	if len(raw) < 16 {
		return 0, 0, nil, fmt.Errorf("raft: snapshot file too short")
	}
	return binary.BigEndian.Uint64(raw[0:8]), binary.BigEndian.Uint64(raw[8:16]), raw[16:], nil
}

// close releases the WAL handle.
func (p *persister) close() error {
	if p.wal == nil {
		return nil
	}
	err := p.wal.Close()
	p.wal = nil
	return err
}

// writeFileSync writes data to path and fsyncs it — the "make it durable
// before renaming it into place" half of the atomic-install pattern.
func writeFileSync(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("raft: create %q: %w", path, err)
	}
	bw := bufio.NewWriter(f)
	if _, err := bw.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
