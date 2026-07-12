package storage

import (
	"os"
	"path/filepath"
	"sync"
)

// -----------------------------------------------------------------------------
// Store: a durable, crash-safe key-value store.
//
// BIG PICTURE (why this design looks the way it does)
//
// A crash-safe store has to answer one hard question: "if the machine loses
// power at the worst possible instant, what state do we come back up in?"
//
// The classic answer is a Write-Ahead Log (WAL) plus periodic snapshots:
//
//   - The WAL is an append-only file. Every mutation (Put/Delete) is first
//     written to the log AND flushed to stable storage (fsync) BEFORE we
//     consider the write "done". This gives us durability: once we tell a
//     caller "your write succeeded", that write survives a crash, because it
//     is physically on disk in the log.
//
//   - The in-memory map (`data`) is just a cache of the current state. It is
//     rebuilt on startup by replaying the log. The map is the "state
//     machine"; the log is the authoritative history of commands that produced
//     it. This is exactly the model Raft uses later: a replicated log of
//     commands, deterministically applied to a state machine on every node.
//
//   - Snapshots are periodic compaction. Replaying the entire log from the
//     beginning of time would make startup slower and slower forever. A
//     snapshot writes the *current* map to disk as a single compact image;
//     afterwards we can safely truncate the log, so recovery only ever has to
//     load "snapshot + a bounded tail of log".
//
// RECOVERY ORDER MATTERS. On startup we always: (1) load the snapshot to get a
// baseline state, then (2) replay the WAL *on top of* it to fold in every
// mutation that happened after the snapshot was taken. Do it in the other
// order and you'd throw away recent writes.
// -----------------------------------------------------------------------------

// File names living inside the store's directory. Everything the store needs
// to recover is contained in this one directory, which makes backups and
// reasoning about durability simple.
const (
	// walFileName is the append-only write-ahead log. It holds every command
	// applied since the last snapshot (or since the beginning, if no snapshot
	// has been taken yet).
	walFileName = "wal.log"

	// snapshotFileName is the last successfully compacted point-in-time image
	// of the entire map.
	snapshotFileName = "snapshot.bin"

	// snapshotTmpName is where a new snapshot is written first. We only rename
	// it into place once it is fully written and fsync'd, so a crash mid-write
	// can never corrupt the real snapshot. See snapshot.go.
	snapshotTmpName = "snapshot.tmp"
)

// Store is a durable crash-safe key-value store backed by a WAL and periodic
// snapshots. All exported methods are safe for concurrent use.
type Store struct {
	// mu guards data (and coordinates with WAL writes). We use an RWMutex so
	// many readers (Get/Len) can proceed in parallel while writers are
	// serialized. Serializing writers also guarantees the WAL is appended to in
	// the exact same order the map is mutated, which is what makes replay
	// deterministic.
	mu sync.RWMutex

	// dir is the directory that holds the WAL and snapshot files.
	dir string

	// data is the in-memory state machine: the current value for every live
	// key. This is a cache derived entirely from (snapshot + WAL); it is never
	// the source of truth.
	data map[string][]byte

	// wal is the write-ahead log we append every mutation to before applying
	// it in memory.
	wal *WAL
}

// Open opens (or creates) a Store rooted at dir and performs crash recovery so
// that the returned Store reflects every acknowledged write, in order.
//
// The recovery sequence below is deliberate and its ORDER is load-bearing.
func Open(dir string) (*Store, error) {
	// Step 1: Make sure the directory exists. 0o755 = owner rwx, group/other
	// rx. All of our state files live inside here.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	s := &Store{
		dir: dir,
		// Step 2: Start from an empty state machine...
		data: make(map[string][]byte),
	}

	// ...then load the last compacted snapshot on top of the empty map. After
	// this call, s.data holds the state as of the moment the snapshot was
	// taken. If there is no snapshot yet, loadSnapshot returns nil and we stay
	// empty.
	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}

	// Step 3: Replay the WAL ON TOP of the snapshot.
	//
	// The snapshot captured state up to some point in the past; the WAL
	// contains every mutation that happened *after* that point (plus possibly
	// some that overlap the snapshot — that's fine, see the idempotency note
	// below). Folding the log in over the snapshot reconstructs the exact
	// state at the moment of the last acknowledged write.
	//
	// IDEMPOTENCY / SAFETY NOTE: if we crashed between "snapshot renamed into
	// place" and "WAL truncated" (see Snapshot in snapshot.go), then the WAL
	// still contains commands that are *already* reflected in the snapshot. We
	// replay them anyway. That is safe because Put and Delete are deterministic
	// and idempotent when replayed in their original order: setting a key to
	// the same value, or deleting an already-deleted key, yields the identical
	// final state. So double-applying the overlap changes nothing.
	walPath := filepath.Join(dir, walFileName)

	// replayWAL walks valid records and returns the byte offset just past the
	// last fully-valid record. A crash can leave a "torn" (partially written)
	// record at the tail; replayWAL stops cleanly at the last good one and
	// tells us where that boundary is.
	validOffset, err := replayWAL(walPath, func(payload []byte) error {
		cmd, err := decodeCommand(payload)
		if err != nil {
			return err
		}
		// Single-threaded during recovery: no lock needed, and apply() mutates
		// the state machine just as it would during live operation.
		s.apply(cmd)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// If the file on disk is longer than the last valid record, the extra bytes
	// are a torn tail from a crash mid-append. Truncate them away so that our
	// next Append starts writing at a clean record boundary. Without this, a
	// new append would sit *after* garbage bytes, and a future replay would
	// choke on (or skip past) the corruption.
	if fi, statErr := os.Stat(walPath); statErr == nil {
		if fi.Size() > validOffset {
			if err := os.Truncate(walPath, validOffset); err != nil {
				return nil, err
			}
		}
	}

	// Step 4: Open the WAL for appending. From here on, every mutation goes
	// through the log first. OpenWAL positions us to append after the (now
	// clean) existing contents.
	w, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}
	s.wal = w

	return s, nil
}

// apply mutates the in-memory state machine to reflect a single command.
//
// This is THE STATE MACHINE. Its defining property is determinism: given the
// same starting state and the same command, it always produces the same
// resulting state, with no side effects on disk. That property is what lets us
// rebuild state by replaying the log, and later lets every node in a Raft
// cluster arrive at identical state by applying the identical command log.
//
// The caller must hold the write lock (during live operation) or be the
// single-threaded recovery path (during Open). apply itself does no locking.
func (s *Store) apply(cmd Command) {
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
}

// logAndApply is the durability-critical write path. It runs the three steps
// that make this store crash-safe, in this exact order:
//
//  1. Append the encoded command to the WAL.
//  2. Sync (fsync) the WAL so the bytes are physically on stable storage.
//  3. Apply the command to the in-memory state machine.
//
// WHY FSYNC BEFORE APPLY/ACK? This ordering is the entire point of a
// write-ahead log. We only make a change "visible" (apply it in memory) and
// return success to the caller AFTER the log record is durably on disk. That
// guarantees the following invariant:
//
//	If a caller was told "your write succeeded", the write cannot be lost —
//	even if the power dies one nanosecond later — because it is already in the
//	log, and recovery replays the log.
//
// If we applied first and fsync'd later (or never), the OS could acknowledge
// the write, we'd return success, and a crash before the fsync flushed the
// page cache would silently lose an "acknowledged" write. That's data loss,
// and it's exactly what WAL exists to prevent. The cost is real: fsync is
// slow because it waits for the disk, but correctness demands we pay it before
// promising durability.
func (s *Store) logAndApply(cmd Command) error {
	if err := s.wal.Append(encodeCommand(cmd)); err != nil {
		return err
	}
	if err := s.wal.Sync(); err != nil {
		return err
	}
	s.apply(cmd)
	return nil
}

// Put durably associates key with value. It returns only after the write is
// safely on disk (see logAndApply).
func (s *Store) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Defensive copy: the caller owns `value` and may mutate or reuse the
	// slice after Put returns. If we stored the caller's slice directly, a
	// later mutation by the caller would silently corrupt our state (and would
	// disagree with what we logged). Copying gives the store its own private,
	// immutable-to-callers copy.
	valCopy := make([]byte, len(value))
	copy(valCopy, value)

	return s.logAndApply(Command{Op: OpPut, Key: key, Value: valCopy})
}

// Delete durably removes key (a no-op on the map if it wasn't present, but the
// delete is still logged so the intent is recorded and replayable).
func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.logAndApply(Command{Op: OpDelete, Key: key})
}

// Get returns the value for key and whether it exists.
//
// It returns a COPY of the stored bytes. Returning the internal slice would
// let a caller mutate our state machine out from under the lock, breaking the
// invariant that state changes only ever happen through the logged write path.
// Aliasing internal state is a classic source of subtle, hard-to-reproduce
// corruption bugs, so we never do it.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Len returns the number of live keys currently in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Close flushes and closes the underlying WAL. After Close the Store must not
// be used again.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Guard against double-close: nil out s.wal after closing so a second Close
	// (or a Close racing shutdown paths) doesn't call Close on an already-closed
	// file handle.
	if s.wal == nil {
		return nil
	}
	err := s.wal.Close()
	s.wal = nil
	return err
}
