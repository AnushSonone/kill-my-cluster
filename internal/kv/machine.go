package kv

// machine.go is the deterministic KV state machine every Raft node applies.
//
// ---------------------------------------------------------------------------
// Why a separate in-memory machine (not Phase 1 storage.Store)?
// ---------------------------------------------------------------------------
// Phase 1's Store has its own WAL — durability lives there. In the cluster,
// the Raft log IS the durability layer; each node only needs an in-memory
// map rebuilt by replaying committed entries. One WAL (Raft's) is enough.
//
// ---------------------------------------------------------------------------
// Exactly-once via session dedup
// ---------------------------------------------------------------------------
// Clients attach (clientID, requestID) to mutating commands. Before executing,
// we look up that pair in `applied`. A hit means "this request already ran"
// — return the stored ApplyResult and skip the mutation. Retries that get
// re-proposed and re-committed as new log entries are therefore harmless:
// the duplicate log entry exists, but the state does not change twice.
//
// This is the standard replicated-state-machine idempotency pattern used by
// agent systems that must survive lost ACKs without double-paying.

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

// DedupKey identifies one logical client request for exactly-once tracking.
// Exported fields are required so gob can round-trip snapshots.
type DedupKey struct {
	Client    string
	RequestID uint64
}

// WatchEvent is delivered to subscribers when a watched key changes.
type WatchEvent struct {
	Key   string
	Value []byte
	Found bool // false means the key was deleted / CAS failed to find
}

// Machine is the replicated KV state. Apply is deterministic; Snapshot and
// Restore round-trip the full state for Raft log compaction.
type Machine struct {
	mu sync.RWMutex

	data    map[string][]byte
	applied map[DedupKey]ApplyResult

	// watchers[key] holds channels that receive an event when key changes.
	// We never block Apply on a slow watcher — sends are non-blocking drops.
	watchers map[string][]chan WatchEvent
}

// NewMachine returns an empty state machine.
func NewMachine() *Machine {
	return &Machine{
		data:     make(map[string][]byte),
		applied:  make(map[DedupKey]ApplyResult),
		watchers: make(map[string][]chan WatchEvent),
	}
}

// Apply executes one committed command and returns its outcome. The same
// command bytes applied at the same log position on every node produce the
// same post-state — Raft's safety guarantee depends on this determinism.
func (m *Machine) Apply(cmd Command) ApplyResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	// --- Exactly-once gate (all op types, so GET retries are uniform too). ---
	if cmd.ClientID != "" && cmd.RequestID != 0 {
		if prev, ok := m.applied[DedupKey{cmd.ClientID, cmd.RequestID}]; ok {
			out := prev
			out.Duplicate = true
			return out
		}
	}

	var res ApplyResult

	switch cmd.Op {
	case OpGet:
		v, ok := m.data[cmd.Key]
		res.Found = ok
		if ok {
			res.Value = copyBytes(v)
		}

	case OpPut:
		prev, had := m.data[cmd.Key]
		if had {
			res.Value = copyBytes(prev)
		}
		m.data[cmd.Key] = copyBytes(cmd.Value)
		res.Found = true
		m.notifyLocked(cmd.Key, cmd.Value, true)

	case OpCAS:
		cur, ok := m.data[cmd.Key]
		if !ok {
			// Key missing: swap only if client expected empty (nil/empty slice).
			if len(cmd.Expect) != 0 {
				res.Found = false
				break
			}
			m.data[cmd.Key] = copyBytes(cmd.Value)
			res.Found = true
			m.notifyLocked(cmd.Key, cmd.Value, true)
			break
		}
		res.Value = copyBytes(cur)
		if bytes.Equal(cur, cmd.Expect) {
			m.data[cmd.Key] = copyBytes(cmd.Value)
			res.Found = true
			m.notifyLocked(cmd.Key, cmd.Value, true)
		} else {
			res.Found = false
		}

	case OpCheckpoint:
		// Agent recovery point: store opaque state under a well-known session key.
		ck := checkpointKey(cmd.ClientID)
		m.data[ck] = copyBytes(cmd.Value)
		res.Found = true
		res.Value = copyBytes(cmd.Value)

	default:
		// Unknown ops are no-ops with empty result — safer than panicking on
		// a corrupted log entry during replay.
		res.Found = false
	}

	if cmd.ClientID != "" && cmd.RequestID != 0 {
		stored := res
		stored.Duplicate = false
		m.applied[DedupKey{cmd.ClientID, cmd.RequestID}] = stored
	}
	return res
}

// Get reads key from local state without going through Raft. Used by
// Cluster.Get after a ReadIndex barrier, and by tests/diagnostics.
func (m *Machine) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return copyBytes(v), ok
}

// Watch registers ch to receive events when key changes. The caller must
// eventually call Unwatch. Events are best-effort (slow consumers may drop).
func (m *Machine) Watch(key string, ch chan WatchEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.watchers[key] = append(m.watchers[key], ch)
}

// Unwatch removes ch from key's subscriber list.
func (m *Machine) Unwatch(key string, ch chan WatchEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	subs := m.watchers[key]
	for i, c := range subs {
		if c == ch {
			m.watchers[key] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
}

func (m *Machine) notifyLocked(key string, value []byte, found bool) {
	ev := WatchEvent{Key: key, Value: copyBytes(value), Found: found}
	for _, ch := range m.watchers[key] {
		select {
		case ch <- ev:
		default:
		}
	}
}

// snapshotData is the gob-encoded image written into Raft snapshots.
type snapshotData struct {
	Data    map[string][]byte
	Applied map[DedupKey]ApplyResult
}

// Snapshot exports the full machine state for Raft compaction.
func (m *Machine) Snapshot() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dataCopy := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		dataCopy[k] = copyBytes(v)
	}
	appliedCopy := make(map[DedupKey]ApplyResult, len(m.applied))
	for k, v := range m.applied {
		appliedCopy[k] = v
		if v.Value != nil {
			appliedCopy[k] = ApplyResult{
				Found: v.Found, Value: copyBytes(v.Value), Duplicate: false,
			}
		}
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshotData{Data: dataCopy, Applied: appliedCopy}); err != nil {
		return nil, fmt.Errorf("kv: encode snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore replaces machine state from a Raft snapshot payload.
func (m *Machine) Restore(raw []byte) error {
	var snap snapshotData
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&snap); err != nil {
		return fmt.Errorf("kv: decode snapshot: %w", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = snap.Data
	m.applied = snap.Applied
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	if m.applied == nil {
		m.applied = make(map[DedupKey]ApplyResult)
	}
	return nil
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
