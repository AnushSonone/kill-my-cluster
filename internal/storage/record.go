// Package storage implements a from-scratch, crash-safe key-value store.
//
// The heart of any durable storage engine is the write-ahead log (WAL). Before
// we ever mutate our in-memory state (or an on-disk table), we first append a
// description of that mutation to an append-only log and flush it to stable
// storage. That way, if the process crashes or the machine loses power, we can
// replay the log on startup and reconstruct exactly the state we had promised
// to the caller. This is the "write-ahead" principle: the intent to change is
// made durable *before* the change is considered committed.
//
// This file (record.go) is only concerned with the *logical* unit of work — a
// Command — and how to turn one into a flat byte payload and back again. The
// WAL layer (wal.go) is concerned with *framing* those payloads on disk
// (adding length + checksum) and with durability (fsync) and recovery (replay).
// Keeping these two concerns separate is deliberate: the record layer knows
// nothing about files, and the file layer knows nothing about the meaning of
// the bytes it stores. This is a classic layering seen in real engines such as
// LevelDB/RocksDB, etcd's WAL, and PostgreSQL's WAL.
package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// OpType identifies which kind of mutation a Command represents.
//
// We use a single byte because there are only a handful of operations and a
// compact on-disk encoding matters: a WAL is written on the hot path of every
// write, so every byte we save is a byte we don't have to push to disk and
// fsync. Keeping the type explicit (rather than a bare byte) also documents
// intent at call sites and lets the compiler catch mistakes.
type OpType uint8

const (
	// OpPut sets Key to Value (insert or overwrite).
	OpPut OpType = 1
	// OpDelete removes Key. Value is ignored/empty for deletes.
	OpDelete OpType = 2
)

// Command is a single mutation to the key-value state.
//
// Everything the store does to change its data can be expressed as a stream of
// Commands. That is what makes a WAL possible: instead of trying to snapshot
// arbitrary in-memory structures on every write, we record the *operations*
// that produced the state. Replaying the same operations in the same order
// deterministically reproduces the same state — the same idea that later powers
// Raft's replicated log, where each node applies an identical ordered sequence
// of commands to arrive at an identical state machine.
type Command struct {
	Op    OpType
	Key   string
	Value []byte
}

// frameHeaderSize is the size in bytes of the on-disk frame header used by the
// WAL: a 4-byte big-endian payload length followed by a 4-byte big-endian CRC32
// of the payload. It lives here because both record.go and wal.go reason about
// framing, and having a single named constant avoids magic numbers.
const frameHeaderSize = 8 // 4-byte length + 4-byte CRC

// crcTable is the polynomial table used for all checksums in this package.
//
// We use CRC-32 with the Castagnoli polynomial (a.k.a. CRC32C). Why a checksum
// at all? Disks, controllers, and filesystems can hand back bytes that are not
// what we wrote: bit rot, partially written ("torn") records after a crash, or
// a truncated tail. A checksum lets us *detect* such corruption instead of
// silently replaying garbage into our state machine. Why Castagnoli
// specifically? It has excellent error-detection properties and is hardware
// accelerated on most modern CPUs (SSE4.2 / ARM CRC instructions), so it is
// effectively free on the write path. Precomputing the table once and reusing
// it avoids rebuilding it on every checksum.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// encodeCommand serializes a Command into a flat payload with the layout:
//
//	| Op: 1 byte | keyLen: uint32 big-endian (4 bytes) | key bytes | value bytes (remaining) |
//
// Design notes:
//   - We store keyLen explicitly so the decoder can find where the key ends and
//     the value begins. We do NOT store a value length: the value is simply
//     "everything after the key". The WAL frame already records the total
//     payload length, so the decoder knows exactly how many bytes it was given,
//     and value length is derivable. This keeps the record encoding minimal.
//   - We use big-endian ("network byte order") for the length. The choice is
//     arbitrary for a single machine, but fixing an endianness makes the format
//     portable and self-consistent across architectures — important for a
//     store that may later ship data between nodes in a cluster.
func encodeCommand(c Command) []byte {
	key := []byte(c.Key)

	// Total size: 1 byte op + 4 byte key length + key + value.
	buf := make([]byte, 1+4+len(key)+len(c.Value))

	buf[0] = byte(c.Op)
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(key)))
	copy(buf[5:5+len(key)], key)
	copy(buf[5+len(key):], c.Value)

	return buf
}

// decodeCommand is the inverse of encodeCommand. It parses a payload produced by
// encodeCommand back into a Command, validating that the buffer is well-formed.
//
// Defensive parsing matters here: this payload may have come off disk after a
// crash. Even though the WAL verifies a CRC before handing us the payload, we
// still bounds-check every field so a malformed or unexpected buffer produces a
// clear error rather than a panic (an out-of-range slice) that would take down
// the whole process during recovery.
func decodeCommand(payload []byte) (Command, error) {
	// Need at least the fixed header: 1 byte op + 4 byte key length.
	if len(payload) < 5 {
		return Command{}, fmt.Errorf("storage: command payload too short: got %d bytes, need at least 5", len(payload))
	}

	op := OpType(payload[0])
	keyLen := binary.BigEndian.Uint32(payload[1:5])

	// The declared key length must fit within the bytes we actually have.
	// Without this check, slicing below could panic on corrupt/truncated data.
	if int(keyLen) > len(payload)-5 {
		return Command{}, fmt.Errorf("storage: command keyLen %d exceeds available payload %d", keyLen, len(payload)-5)
	}

	key := string(payload[5 : 5+keyLen])

	// The value is whatever remains after the key. We copy it into a fresh
	// slice rather than aliasing the input: the caller may reuse or mutate the
	// underlying payload buffer (e.g. a reader's scratch buffer during replay),
	// and we do not want the decoded Command to share storage with it. Aliasing
	// here would be a subtle data-corruption bug.
	valueSrc := payload[5+keyLen:]
	value := make([]byte, len(valueSrc))
	copy(value, valueSrc)

	return Command{Op: op, Key: key, Value: value}, nil
}
