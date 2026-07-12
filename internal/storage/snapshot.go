package storage

import (
	"bufio"
	"encoding/gob"
	"os"
	"path/filepath"
)

// -----------------------------------------------------------------------------
// Snapshots: log compaction for bounded recovery.
//
// The WAL grows forever if left alone: every Put/Delete appends another record.
// Two problems follow:
//
//   1. Startup gets slower and slower, because recovery replays the whole log.
//   2. The log file grows without bound on disk, even for keys that were
//      overwritten or deleted long ago.
//
// A snapshot solves both. It writes the *entire current state* to a single
// compact file. Once that file is safely on disk, every command currently in
// the WAL is redundant (its effect is already baked into the snapshot), so we
// can truncate the WAL to empty. Recovery then only ever needs to load
// "snapshot + whatever has been appended since" — a bounded amount of work.
//
// The tricky part is doing all of this crash-safely. A snapshot is only useful
// if a crash can never leave us with a half-written one. The technique is the
// atomic-rename dance, explained step-by-step in Snapshot below.
// -----------------------------------------------------------------------------

// Snapshot compacts the store: it writes a consistent point-in-time image of
// the map to disk, atomically installs it as the current snapshot, then
// truncates the WAL. It is safe to call concurrently with reads/writes (it
// takes the write lock for the duration).
func (s *Store) Snapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Step 1: Take a consistent point-in-time image.
	//
	// We copy the map while holding the lock so that the snapshot reflects one
	// well-defined instant, with no writes tearing through it half-applied.
	// (We copy rather than serialize under lock the whole time only for
	// clarity here; the copy itself must happen under the lock.)
	snap := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		vc := make([]byte, len(v))
		copy(vc, v)
		snap[k] = vc
	}

	tmpPath := filepath.Join(s.dir, snapshotTmpName)
	finalPath := filepath.Join(s.dir, snapshotFileName)

	// Step 2: Write the image to a TEMPORARY file first, then fsync it.
	//
	// We never write directly to the real snapshot file, because if we crashed
	// mid-write we'd destroy the previous good snapshot and leave garbage. By
	// writing to snapshot.tmp we keep the old snapshot fully intact until the
	// new one is completely written and durable.
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	// bufio.Writer batches many small gob writes into larger syscalls — a big
	// throughput win. But buffering means bytes may still be sitting in the
	// buffer (not even handed to the OS yet), so we MUST Flush before Sync.
	bw := bufio.NewWriter(f)
	enc := gob.NewEncoder(bw)
	if err := enc.Encode(snap); err != nil {
		f.Close()
		return err
	}
	// Flush: push buffered bytes from bufio into the OS.
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	// Sync (fsync): force the OS to flush its page cache to the physical disk.
	// Flush alone only moves bytes into the kernel; a crash could still lose
	// them from the page cache. fsync is what actually makes the data durable.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	// Step 3: Atomically swap the new snapshot into place.
	//
	// rename(2) is ATOMIC on POSIX filesystems: at any instant, finalPath
	// refers to EITHER the complete old snapshot OR the complete new one, never
	// a blend and never a truncated file. So no matter when a crash strikes,
	// recovery finds a fully-valid snapshot. This "write temp, fsync, rename"
	// pattern is the standard recipe for atomic file replacement.
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}

	// Step 4: fsync the DIRECTORY.
	//
	// A subtle gotcha: the rename modifies the directory's contents (which name
	// points at which inode), and that directory metadata is itself buffered.
	// fsync'ing the file does NOT guarantee the directory entry is durable. If
	// we skipped this and crashed, the file's data could be on disk while the
	// directory still pointed at the old name — losing the rename. Syncing the
	// directory makes the rename itself durable.
	if err := syncDir(s.dir); err != nil {
		return err
	}

	// Step 5: Compact (truncate) the WAL.
	//
	// Every command in the WAL is now redundant: its effect is captured in the
	// snapshot we just made durable. Truncating to zero bounds future recovery
	// time — we won't replay history that predates the snapshot.
	//
	// CRASH WINDOW / IDEMPOTENCY: there is a gap between "rename done + dir
	// synced" (Step 4) and "WAL truncated" (Step 5). If we crash inside that
	// window, on the next Open we will load the new snapshot AND replay the
	// not-yet-truncated WAL entries a second time. That's harmless: the
	// commands are deterministic and idempotent when replayed in order, so the
	// final state is identical either way. Correctness never depends on the
	// truncate succeeding — only recovery *speed* does.
	walPath := filepath.Join(s.dir, walFileName)
	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := os.Truncate(walPath, 0); err != nil {
		return err
	}
	w, err := OpenWAL(walPath)
	if err != nil {
		return err
	}
	s.wal = w

	return nil
}

// loadSnapshot loads the most recent compacted snapshot into s.data. It is
// called during Open, before the WAL is replayed, to establish the baseline
// state on top of which the log is folded.
//
// If no snapshot exists yet (a brand-new store, or one that has never been
// compacted), that is not an error — we simply return nil and leave s.data
// empty for the WAL replay to populate.
func (s *Store) loadSnapshot() error {
	path := filepath.Join(s.dir, snapshotFileName)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No snapshot to load: this is the normal case for a fresh store.
			return nil
		}
		return err
	}
	defer f.Close()

	var snap map[string][]byte
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return err
	}

	// Copy entries into the store's live map. (We copy into s.data rather than
	// replacing the pointer so the map allocated in Open stays the one in use.)
	for k, v := range snap {
		s.data[k] = v
	}
	return nil
}

// syncDir fsyncs a directory so that changes to its entries — most importantly
// a rename — are durably recorded on disk. On POSIX you make a directory's
// metadata durable by opening the directory itself and calling Sync on the
// handle.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	// Sync the directory, then close. We check the Sync error first because
	// that's the operation whose failure means our rename might not be durable.
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
