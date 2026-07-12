// Package kv implements the replicated key-value state machine (Phase 3).
//
// Raft decides *order*; this package gives commands *meaning*. Every node
// applies the same committed log entries in the same order and arrives at
// the same map — that's state-machine replication. Linearizability comes from
// routing reads and writes through the Raft log (nothing is served from stale
// local state on a follower without going through consensus).
//
// Exactly-once is enforced inside the state machine: each mutating command
// carries a (clientID, requestID) pair. If requestID was already applied for
// that client, we return the cached outcome and skip the mutation — so client
// retries that create duplicate log entries still have at-most-once effect.
package kv

import (
	"encoding/binary"
	"fmt"
)

// OpType identifies which KV operation a Command performs.
type OpType uint8

const (
	OpGet        OpType = 1 // linearizable read (still goes through the Raft log)
	OpPut        OpType = 2 // unconditional write
	OpCAS        OpType = 3 // compare-and-swap: write only if current == expect
	OpCheckpoint OpType = 4 // agent checkpoint blob stored under a session key
)

// Command is one state-machine operation carried inside a Raft log entry.
//
// ClientID + RequestID are the exactly-once tuple. RequestID may be 0 for
// ops that don't need dedup (rare); mutating client APIs always set it.
// GET uses the tuple too so a retried read returns the same answer without
// re-executing side effects (there are none on GET, but the contract is uniform).
type Command struct {
	Op        OpType
	ClientID  string
	RequestID uint64
	Key       string
	Value     []byte // PUT/CHECKPOINT: new value; GET: unused
	Expect    []byte // CAS only: value that must match for swap to succeed
}

// ApplyResult is what the state machine returns after applying one command.
// It is cached for exactly-once replays keyed by (ClientID, RequestID).
type ApplyResult struct {
	// Found is true for GET when the key exists, or CAS when swap succeeded.
	Found bool
	// Value holds GET result, CAS/PUT previous value (if any), or checkpoint ack.
	Value []byte
	// Duplicate is true when this (client, request) was already applied — the
	// caller's retry hit the dedup table and no mutation ran again.
	Duplicate bool
}

// checkpointKey is where OpCheckpoint stores agent state for a session.
func checkpointKey(clientID string) string {
	return "_checkpoint/" + clientID
}

// Encode serializes cmd for appending to the Raft log.
//
// Wire layout (big-endian throughout, like Phase 1 storage/record.go):
//
//	| op: 1 | clientLen: u32 | client | requestId: u64 |
//	| keyLen: u32 | key | expectLen: u32 | expect | value (remaining) |
//
// expectLen is 0 except for CAS. Value length is implicit (rest of buffer).
func Encode(cmd Command) []byte {
	client := []byte(cmd.ClientID)
	key := []byte(cmd.Key)

	size := 1 + 4 + len(client) + 8 + 4 + len(key) + 4 + len(cmd.Expect) + len(cmd.Value)
	buf := make([]byte, size)
	off := 0

	buf[off] = byte(cmd.Op)
	off++

	binary.BigEndian.PutUint32(buf[off:], uint32(len(client)))
	off += 4
	copy(buf[off:], client)
	off += len(client)

	binary.BigEndian.PutUint64(buf[off:], cmd.RequestID)
	off += 8

	binary.BigEndian.PutUint32(buf[off:], uint32(len(key)))
	off += 4
	copy(buf[off:], key)
	off += len(key)

	binary.BigEndian.PutUint32(buf[off:], uint32(len(cmd.Expect)))
	off += 4
	copy(buf[off:], cmd.Expect)
	off += len(cmd.Expect)

	copy(buf[off:], cmd.Value)
	return buf
}

// Decode parses a payload produced by Encode.
func Decode(payload []byte) (Command, error) {
	if len(payload) < 1+4+8+4+4 {
		return Command{}, fmt.Errorf("kv: command payload too short: %d bytes", len(payload))
	}
	off := 0

	op := OpType(payload[off])
	off++

	if off+4 > len(payload) {
		return Command{}, fmt.Errorf("kv: truncated clientLen")
	}
	clientLen := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	if off+clientLen+8+4 > len(payload) {
		return Command{}, fmt.Errorf("kv: truncated client")
	}
	clientID := string(payload[off : off+clientLen])
	off += clientLen

	requestID := binary.BigEndian.Uint64(payload[off:])
	off += 8

	if off+4 > len(payload) {
		return Command{}, fmt.Errorf("kv: truncated keyLen")
	}
	keyLen := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	if off+keyLen+4 > len(payload) {
		return Command{}, fmt.Errorf("kv: truncated key")
	}
	key := string(payload[off : off+keyLen])
	off += keyLen

	if off+4 > len(payload) {
		return Command{}, fmt.Errorf("kv: truncated expectLen")
	}
	expectLen := int(binary.BigEndian.Uint32(payload[off:]))
	off += 4
	if off+expectLen > len(payload) {
		return Command{}, fmt.Errorf("kv: truncated expect")
	}
	expect := make([]byte, expectLen)
	copy(expect, payload[off:off+expectLen])
	off += expectLen

	value := make([]byte, len(payload)-off)
	copy(value, payload[off:])

	return Command{
		Op: op, ClientID: clientID, RequestID: requestID,
		Key: key, Expect: expect, Value: value,
	}, nil
}
