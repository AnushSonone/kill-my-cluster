package storage

// wal.go implements the crash-safe, append-only write-ahead log.
//
// ---------------------------------------------------------------------------
// Why a write-ahead log?
// ---------------------------------------------------------------------------
// A storage engine must answer a hard question: how do we tell a client "your
// write is committed" and actually mean it, even if the power dies one
// microsecond later? In-memory data structures vanish on crash. Writing them
// out lazily risks losing acknowledged writes. The WAL solves this by turning
// every mutation into a small, sequential, append-only record that we push to
// stable storage *before* acknowledging the write. Sequential appends are the
// fastest thing a disk can do, and an append-only file is simple to reason
// about for crash recovery: on startup we just read it front-to-back and
// replay the operations to rebuild state.
//
// ---------------------------------------------------------------------------
// On-disk frame format
// ---------------------------------------------------------------------------
// Each record is stored as a self-describing "frame":
//
//	| payloadLen: uint32 big-endian (4 bytes) | crc32c(payload): uint32 big-endian (4 bytes) | payload: payloadLen bytes |
//
// The two header fields (frameHeaderSize == 8 bytes total) are what make the
// log recoverable:
//   - payloadLen tells the reader exactly how many payload bytes follow, so it
//     knows where this frame ends and the next begins. This is "framing":
//     without it, a stream of concatenated payloads would be ambiguous.
//   - crc32c(payload) lets the reader verify the payload was written completely
//     and correctly. If a crash happened mid-write, the length or CRC will not
//     match the bytes on disk, and we can detect the damage instead of
//     replaying corrupt data.
//
// The payload itself is opaque to the WAL — it is produced by encodeCommand in
// record.go. The WAL deliberately knows nothing about Commands; it just stores
// and returns byte blobs. This separation keeps each layer simple and testable.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL is a handle to an open write-ahead log file.
//
// It wraps the underlying *os.File in a bufio.Writer. Buffering matters because
// many small writes (an 8-byte header + a small payload, over and over) would
// otherwise become many small system calls; the buffer coalesces them into
// larger, more efficient writes. Crucially, buffering is NOT durability — bytes
// sitting in the bufio.Writer or in the OS page cache can still be lost on a
// crash. Durability comes only from Sync (see below).
type WAL struct {
	f    *os.File
	w    *bufio.Writer
	path string
}

// OpenWAL opens (or creates) the WAL file at path for appending.
//
// Flags explained:
//   - os.O_CREATE: create the file if it does not yet exist.
//   - os.O_RDWR:   open for read+write. (Appends are writes; RDWR keeps us
//     flexible and matches typical WAL handles.)
//   - os.O_APPEND: every write is atomically positioned at the current end of
//     file by the kernel. This is a key safety property: even with concurrent
//     writers or interleaving, an append can never overwrite existing log data,
//     it only ever extends the log. Our log is strictly append-only.
//
// Mode 0o644 gives the owner read/write and everyone else read — standard for a
// data file. We wrap the file in a bufio.Writer for efficient batched writes.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: open wal %q: %w", path, err)
	}
	return &WAL{
		f:    f,
		w:    bufio.NewWriter(f),
		path: path,
	}, nil
}

// Append writes one framed record for payload into the buffered writer.
//
// It writes the 8-byte header (4-byte big-endian length, then 4-byte big-endian
// CRC32C of the payload) followed by the payload bytes. It does NOT fsync — see
// Sync. Separating Append from Sync is what lets callers batch many logical
// writes and pay for a single expensive fsync, which is the primary throughput
// lever of a WAL (group commit).
//
// Note on partial writes: if a crash happens midway through these buffered
// writes, the on-disk frame may be short or its CRC may not match. That is
// fine — replayWAL is built to detect and discard such a torn tail.
func (w *WAL) Append(payload []byte) error {
	var header [frameHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.Checksum(payload, crcTable))

	if _, err := w.w.Write(header[:]); err != nil {
		return fmt.Errorf("storage: append header: %w", err)
	}
	if _, err := w.w.Write(payload); err != nil {
		return fmt.Errorf("storage: append payload: %w", err)
	}
	return nil
}

// Sync flushes buffered data and forces it to stable storage.
//
// There are two layers of buffering between an Append and the physical platter/
// flash:
//  1. Our in-process bufio.Writer. w.w.Flush() pushes those bytes into the OS
//     via the write(2) syscall.
//  2. The operating system's page cache. Even after write(2) returns, the data
//     may only be in RAM the OS is holding on our behalf; a power loss now would
//     lose it. w.f.Sync() issues fsync(2), which tells the kernel to push those
//     bytes all the way down to the storage device and not return until the
//     device reports the data is durable.
//
// Only after fsync succeeds may we honestly tell a client "your write is
// committed" — this is the moment durability is achieved and the reason a WAL
// can survive a crash. Because fsync is expensive (it waits on the physical
// device), we do NOT call it on every Append. Instead callers Append many
// records and Sync once, amortizing the cost across a batch. This is the
// classic durability/throughput trade-off every real database exposes.
func (w *WAL) Sync() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("storage: flush wal: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync wal: %w", err)
	}
	return nil
}

// Close flushes any buffered data and closes the underlying file.
//
// It flushes (but does not fsync) so buffered bytes are not silently dropped on
// close. If callers need a durability guarantee at shutdown they should call
// Sync explicitly before Close.
func (w *WAL) Close() error {
	if err := w.w.Flush(); err != nil {
		// Still attempt to close the file so we don't leak the descriptor.
		_ = w.f.Close()
		return fmt.Errorf("storage: flush wal on close: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("storage: close wal: %w", err)
	}
	return nil
}

// replayWAL reads the log at path from the beginning and invokes fn for every
// fully-written, CRC-valid frame, in order. It returns the byte offset of the
// end of the last valid frame (the "valid offset") and an error.
//
// ---------------------------------------------------------------------------
// Why tolerate a torn tail?
// ---------------------------------------------------------------------------
// A crash can strike at any instant, including in the middle of appending a
// frame. When that happens the file's final record may be:
//   - a short header (fewer than 8 bytes were written),
//   - a header that promises N payload bytes but fewer than N are present, or
//   - a complete-looking frame whose payload does not match its CRC (bytes were
//     only partially flushed).
//
// This damaged final record is called a "torn write". It is NOT corruption we
// need to panic over — it is the *expected* consequence of crashing mid-append,
// and it can only ever be the *last* record (everything before it was fully
// written and fsynced before the next append began). The correct recovery
// behavior is therefore: replay every fully-valid frame, and the moment we hit
// an incomplete or CRC-mismatched frame, STOP and report the offset of the last
// good frame. The caller can then truncate the file to that offset, cleanly
// discarding the torn tail, and resume appending. This is exactly how etcd,
// RocksDB, and PostgreSQL treat the end of their logs after a crash.
//
// A CRC mismatch *before* the final record would indicate genuine mid-file
// corruption; here we conservatively stop at the first bad frame and let the
// caller decide, since we cannot safely skip an unknown-length gap.
//
// If the file does not exist, there is simply nothing to replay: return (0, nil).
// If fn returns an error, we stop and return (validOffset, thatError) so the
// caller learns how far application succeeded before the failure.
func replayWAL(path string, fn func(payload []byte) error) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// No log yet — a fresh store. Nothing to recover.
			return 0, nil
		}
		return 0, fmt.Errorf("storage: open wal for replay %q: %w", path, err)
	}
	defer f.Close()

	// Buffer reads for efficiency, just as we buffer writes.
	r := bufio.NewReader(f)

	// validOffset tracks the total number of bytes belonging to frames we have
	// fully read and validated. It always points to a frame boundary, so it is
	// exactly the length the caller should truncate to in order to drop any
	// torn tail.
	var validOffset int64

	for {
		// --- Read the fixed-size frame header. ---
		var header [frameHeaderSize]byte
		_, err := io.ReadFull(r, header[:])
		if err != nil {
			// io.EOF means we stopped precisely on a frame boundary: a clean end
			// of log. io.ErrUnexpectedEOF means only part of a header made it to
			// disk: a torn header from a crash. Either way there is no further
			// valid frame; return what we have with no error.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return validOffset, nil
			}
			return validOffset, fmt.Errorf("storage: read wal header: %w", err)
		}

		payloadLen := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])

		// --- Read exactly payloadLen payload bytes. ---
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			// A short payload means the crash happened after the header was
			// written but before (all of) the payload landed: a torn tail.
			// Discard it by returning the last valid offset.
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return validOffset, nil
			}
			return validOffset, fmt.Errorf("storage: read wal payload: %w", err)
		}

		// --- Verify the CRC. ---
		// If the checksum does not match, the payload's bytes were not all
		// written durably (or were corrupted). Treat it as the torn tail and
		// stop; the caller truncates here.
		if crc32.Checksum(payload, crcTable) != wantCRC {
			return validOffset, nil
		}

		// The frame is complete and valid. Apply it.
		if err := fn(payload); err != nil {
			return validOffset, err
		}

		// Advance past this frame. Only now is it "committed" to our running
		// offset, guaranteeing validOffset always sits on a good boundary.
		validOffset += int64(frameHeaderSize) + int64(payloadLen)
	}
}
