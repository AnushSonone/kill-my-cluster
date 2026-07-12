package storage

// This test file lives in `package storage` (an "in-package" or "white-box"
// test) rather than `package storage_test`. That is a deliberate choice: some
// of what we want to verify — the exact on-disk WAL file name and its framing
// layout — are unexported implementation details (walFileName, frameHeaderSize).
// A durable storage engine's correctness is defined by its behaviour under
// crashes and corruption, and the only honest way to test that is to reach past
// the public API and manipulate the raw log bytes the way a real crash or a
// flaky disk would. Being in-package lets us do exactly that.

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// mustOpen opens a Store in dir and fails the test immediately if Open errors.
//
// Recovery is the one operation a storage engine performs on *every* startup —
// it reloads a snapshot and replays the WAL on top. If that fails, nothing else
// in a test is meaningful, so we treat an Open error as fatal. Centralizing this
// in a helper keeps each test focused on the property it is actually asserting
// rather than on error-handling boilerplate.
func mustOpen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("Open(%q) failed: %v", dir, err)
	}
	return s
}

// fileSize returns the size in bytes of the file at path, failing the test if it
// cannot be stat'd.
//
// Several durability properties are only observable as changes in the physical
// size of wal.log: a torn tail should *shrink* the file (truncation), and a
// snapshot should compact it to exactly zero. Measuring raw bytes on disk — not
// just the logical key/value state — is what proves those mechanisms actually
// ran, rather than merely appearing to work at the API level.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q failed: %v", path, err)
	}
	return fi.Size()
}

// TestPutGetDelete verifies the most basic contract of a key-value store: a
// value you write can be read back, a key you never wrote does not exist, and a
// key you delete stops existing.
//
// Distributed-systems relevance: this is the "linearizable point-in-time"
// behaviour of a single node — within one process, reads reflect the latest
// completed writes. Every higher-level guarantee (replication, consensus,
// snapshots) is built on top of a local store that at minimum gets this right.
// The exists flag (comma-ok) is important on its own: distinguishing "present
// with an empty value" from "absent" is a common source of correctness bugs.
func TestPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir)
	defer s.Close()

	// A value written should be readable, byte-for-byte.
	if err := s.Put("alpha", []byte("one")); err != nil {
		t.Fatalf("Put(alpha) failed: %v", err)
	}
	got, ok := s.Get("alpha")
	if !ok {
		t.Fatalf("Get(alpha): expected key to exist")
	}
	if !bytes.Equal(got, []byte("one")) {
		t.Fatalf("Get(alpha): got %q, want %q", got, "one")
	}

	// A key that was never written must report absent, not an empty value.
	if _, ok := s.Get("missing"); ok {
		t.Fatalf("Get(missing): expected key to be absent")
	}

	// Delete must actually remove the key from the visible state.
	if err := s.Delete("alpha"); err != nil {
		t.Fatalf("Delete(alpha) failed: %v", err)
	}
	if _, ok := s.Get("alpha"); ok {
		t.Fatalf("Get(alpha) after Delete: expected key to be absent")
	}

	// Len should reflect the number of live keys (zero after the delete).
	if n := s.Len(); n != 0 {
		t.Fatalf("Len after delete: got %d, want 0", n)
	}
}

// TestRecoveryAfterReopen proves that the store's state is not merely held in
// memory but is *reconstructed from the WAL* after a clean shutdown and restart.
//
// Distributed-systems relevance: this is the essence of durability. A store is
// only "crash-safe" if the acknowledged writes survive the process going away.
// Here we exercise the realistic mix an application produces — inserts, an
// overwrite (last-writer-wins for a key), and a delete — then throw the in-memory
// map away by closing and reopening. If the replayed log rebuilds exactly the
// state we left, we have confirmed the WAL is a faithful, ordered record of
// every mutation. This same "apply an ordered command log to rebuild state" is
// precisely how a Raft follower or a database replica catches up.
func TestRecoveryAfterReopen(t *testing.T) {
	dir := t.TempDir()

	s := mustOpen(t, dir)
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatalf("Put(a) failed: %v", err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatalf("Put(b) failed: %v", err)
	}
	if err := s.Put("c", []byte("3")); err != nil {
		t.Fatalf("Put(c) failed: %v", err)
	}
	// Overwrite "a": recovery must honour ordering so the LATER value wins.
	if err := s.Put("a", []byte("100")); err != nil {
		t.Fatalf("Put(a overwrite) failed: %v", err)
	}
	// Delete "b": recovery must replay the tombstone so "b" stays gone.
	if err := s.Delete("b"); err != nil {
		t.Fatalf("Delete(b) failed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen: everything below is served purely from replayed WAL state.
	s2 := mustOpen(t, dir)
	defer s2.Close()

	if got, ok := s2.Get("a"); !ok || !bytes.Equal(got, []byte("100")) {
		t.Fatalf("after reopen Get(a): got %q ok=%v, want %q true", got, ok, "100")
	}
	if _, ok := s2.Get("b"); ok {
		t.Fatalf("after reopen Get(b): expected deleted key to be absent")
	}
	if got, ok := s2.Get("c"); !ok || !bytes.Equal(got, []byte("3")) {
		t.Fatalf("after reopen Get(c): got %q ok=%v, want %q true", got, ok, "3")
	}
	// Live keys are "a" and "c" only.
	if n := s2.Len(); n != 2 {
		t.Fatalf("after reopen Len: got %d, want 2", n)
	}
}

// TestTornTailRecovery proves that a crash *mid-append* — leaving a partial,
// unfinished record at the end of the WAL — never corrupts data that was already
// safely committed, and that the store self-heals by truncating that torn tail.
//
// Distributed-systems relevance: appends are not atomic at the hardware level. A
// power loss can happen after some bytes of a record reach the disk but before
// the whole record (and its fsync) complete. A correct WAL must treat an
// incomplete trailing record as if it never happened: the write was never
// acknowledged, so discarding it is safe, while every prior acknowledged write
// must remain intact. We simulate the crash by appending 3 garbage bytes —
// fewer than the frameHeaderSize (8) needed for even a complete header — so the
// tail cannot possibly be a valid frame. On reopen we assert both original keys
// survive AND the file shrank by exactly those 3 bytes, proving the truncation.
func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()

	s := mustOpen(t, dir)
	if err := s.Put("k1", []byte("v1")); err != nil {
		t.Fatalf("Put(k1) failed: %v", err)
	}
	if err := s.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("Put(k2) failed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	walPath := filepath.Join(dir, walFileName)
	sizeBefore := fileSize(t, walPath)

	// Simulate a crash mid-write: append a stub of a record that is too short
	// to even be a header. This is exactly what a torn final append looks like.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal for torn append failed: %v", err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE}); err != nil { // 3 bytes < frameHeaderSize (8)
		t.Fatalf("torn append write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close wal after torn append failed: %v", err)
	}

	sizeTorn := fileSize(t, walPath)
	if sizeTorn != sizeBefore+3 {
		t.Fatalf("sanity: torn wal size got %d, want %d", sizeTorn, sizeBefore+3)
	}

	// Reopen: the store must replay the two good records and truncate the tail.
	s2 := mustOpen(t, dir)
	defer s2.Close()

	if got, ok := s2.Get("k1"); !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get(k1) after torn recovery: got %q ok=%v, want %q true", got, ok, "v1")
	}
	if got, ok := s2.Get("k2"); !ok || !bytes.Equal(got, []byte("v2")) {
		t.Fatalf("Get(k2) after torn recovery: got %q ok=%v, want %q true", got, ok, "v2")
	}

	// The torn 3 bytes must be gone: the file should be back to its pre-crash size.
	sizeAfter := fileSize(t, walPath)
	if sizeAfter != sizeBefore {
		t.Fatalf("torn tail not truncated: wal size got %d, want %d (shrank by exactly 3)", sizeAfter, sizeBefore)
	}
}

// TestCorruptRecordRecovery proves that CRC checksums reject a record whose bytes
// were silently corrupted, so garbage is never applied to the state machine.
//
// Distributed-systems relevance: the torn-tail case handles *incomplete* writes,
// but storage can also hand back *complete-looking* records whose contents are
// wrong — bit rot, misdirected writes, or a partially-overwritten frame. A
// length + payload with no integrity check would be blindly replayed as a real
// command. By storing a CRC32C over the payload and refusing any frame whose
// stored checksum does not match the recomputed one, the WAL turns silent data
// corruption into a detectable, ignorable event. Here we hand-craft a fully
// framed record (length says 4, payload is "XXXX") but write a deliberately
// wrong CRC of zero. On reopen the original key must survive and "XXXX" must not
// appear anywhere in the state.
func TestCorruptRecordRecovery(t *testing.T) {
	dir := t.TempDir()

	s := mustOpen(t, dir)
	if err := s.Put("good", []byte("value")); err != nil {
		t.Fatalf("Put(good) failed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	walPath := filepath.Join(dir, walFileName)

	// Build a complete frame with a valid length header but an INVALID CRC.
	// Frame layout: | payloadLen uint32 BE | crc32c uint32 BE | payload |
	payload := []byte("XXXX")
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload))) // length = 4 (correct)
	binary.BigEndian.PutUint32(frame[4:8], 0)                    // CRC = 0 (deliberately wrong)
	copy(frame[8:], payload)

	// Confirm our crafted CRC really is wrong, so the test is meaningful and not
	// accidentally valid (guards against the astronomically unlikely case that
	// the real checksum of "XXXX" is zero).
	if crc32.Checksum(payload, crcTable) == 0 {
		t.Fatalf("test setup invalid: real CRC of payload happens to be 0")
	}

	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal for corrupt append failed: %v", err)
	}
	if _, err := f.Write(frame); err != nil {
		t.Fatalf("corrupt frame write failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close wal after corrupt append failed: %v", err)
	}

	// Reopen: the corrupt frame must be rejected by the CRC check.
	s2 := mustOpen(t, dir)
	defer s2.Close()

	if got, ok := s2.Get("good"); !ok || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("Get(good) after corrupt record: got %q ok=%v, want %q true", got, ok, "value")
	}
	// The corrupt payload was "XXXX"; decoded it would set some key. Whatever it
	// might have been, it must not have been applied — only the good key lives.
	if n := s2.Len(); n != 1 {
		t.Fatalf("Len after corrupt record: got %d, want 1 (corrupt record must be ignored)", n)
	}
}

// TestSnapshotCompaction proves that Snapshot() folds the accumulated WAL into a
// compact on-disk snapshot (truncating wal.log to zero), and — crucially — that
// recovery afterwards correctly combines the snapshot with any WAL records
// written *after* the snapshot.
//
// Distributed-systems relevance: a WAL grows without bound; replaying millions
// of records on every restart would make startup unbearably slow. Snapshotting
// (a.k.a. log compaction / checkpointing) captures the current materialized
// state so the log before it can be discarded. This is exactly what etcd and
// Raft implementations do to cap recovery time. The subtle correctness hazard is
// the boundary: after a snapshot, new writes go to a fresh WAL, and recovery
// must load the snapshot FIRST and then replay only the newer WAL on top. We
// verify both halves: the WAL is truly zero bytes post-snapshot, and a value
// written after the snapshot wins after a reopen.
func TestSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()

	s := mustOpen(t, dir)

	// Write the same key many times: the WAL grows with every append even though
	// only the final value matters — precisely the redundancy snapshots remove.
	for i := 0; i < 100; i++ {
		if err := s.Put("counter", []byte(strconv.Itoa(i))); err != nil {
			t.Fatalf("Put(counter=%d) failed: %v", i, err)
		}
	}

	walPath := filepath.Join(dir, walFileName)
	sizeBeforeSnapshot := fileSize(t, walPath)
	if sizeBeforeSnapshot == 0 {
		t.Fatalf("sanity: expected non-empty wal after 100 puts, got 0 bytes")
	}

	// Compact: state is captured into snapshot.bin and wal.log is reset to 0.
	if err := s.Snapshot(); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}
	if sz := fileSize(t, walPath); sz != 0 {
		t.Fatalf("wal size after Snapshot: got %d, want 0 (compaction should truncate wal)", sz)
	}

	// One more write AFTER the snapshot: this lands in the now-empty WAL and must
	// be layered on top of the snapshot during the next recovery.
	if err := s.Put("counter", []byte("final")); err != nil {
		t.Fatalf("Put(counter=final) failed: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen: recovery = load snapshot.bin, then replay the post-snapshot WAL.
	s2 := mustOpen(t, dir)
	defer s2.Close()

	if got, ok := s2.Get("counter"); !ok || !bytes.Equal(got, []byte("final")) {
		t.Fatalf("Get(counter) after snapshot+reopen: got %q ok=%v, want %q true", got, ok, "final")
	}
	if n := s2.Len(); n != 1 {
		t.Fatalf("Len after snapshot+reopen: got %d, want 1", n)
	}
}
