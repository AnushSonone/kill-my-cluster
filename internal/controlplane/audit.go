package controlplane

// audit.go is the cluster's memory of its own bad days.
//
// On 2026-07-28 a visitor kill wedged the cluster: zero writes and zero reads
// for 48 hours with all seven containers up and every scrape target green.
// Reconstructing it afterwards was nearly impossible, because the two places
// that knew anything both forget. Prometheus retains 24h, so the window holding
// the transition had already rolled off. The control plane's event feed
// (addEvent) is a RAM ring buffer that dies with the process, and it only ever
// recorded operator actions, never health.
//
// So this writes state CHANGES to a file that outlives both: append-only JSONL,
// one self-contained line per transition, carrying enough context that a line
// read months later still explains itself. It answers "when did it go bad, and
// what did it look like". It does not tell anyone that it is happening — there
// is deliberately no notifier here.

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	// auditMaxBytes rotates the log before it can threaten the root disk. The
	// unbounded-log lesson was learned the hard way here: default json-file
	// docker logging filled the Oracle disk during the 2026-07-21 incident and
	// left sshd unable to write.
	auditMaxBytes = 8 << 20 // 8MB, then rotate to .1 (two files kept)

	// auditStallAfter is how long the commit index may sit still, with quorum
	// held and a leader in office, before it counts as a stall.
	//
	// Deliberately longer than the raft commit-stall watchdog (30s): if that
	// watchdog is doing its job the leader deposes itself and commit resumes
	// well inside this window, so nothing is written. A progress_stalled line
	// therefore means the watchdog did NOT save it, which is exactly the fact
	// worth having later.
	auditStallAfter = 60 * time.Second
)

// AuditEntry is one line of the log. Flat and self-contained on purpose: a
// single line should be readable without the ones around it.
type AuditEntry struct {
	Time         time.Time `json:"t"`
	Kind         string    `json:"kind"`
	Detail       string    `json:"detail,omitempty"`
	Alive        int       `json:"alive"`
	Total        int       `json:"total"`
	Quorum       bool      `json:"quorum"`
	Term         uint64    `json:"term,omitempty"`
	LeaderID     uint64    `json:"leader,omitempty"`
	CommitIndex  uint64    `json:"commitIndex,omitempty"`
	WritesPerSec float64   `json:"writesPerSec"`
	ReadsPerSec  float64   `json:"readsPerSec"`
	HostCPUPct   *float64  `json:"hostCpuPct,omitempty"`
}

// auditLog appends transitions to disk and remembers just enough previous state
// to recognise an edge. A nil *auditLog is a working no-op, so a bad path
// disables auditing instead of breaking the demo.
type auditLog struct {
	path string

	mu   sync.Mutex
	f    *os.File
	size int64

	// Previous observation, for edge detection.
	started     bool
	lastQuorum  bool
	lastLeader  uint64
	lastCommit  uint64
	commitSince time.Time // when lastCommit was first seen
	stalled     bool      // currently inside a reported stall
	lastStepdow uint64    // cumulative watchdog step-downs already reported
	seenStepdow bool
}

// openAuditLog opens path for appending. An empty path, or any failure to open,
// returns nil with an error: callers treat that as "auditing off" and carry on.
// Losing the audit log must never stop the cluster from serving.
func openAuditLog(path string) (*auditLog, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	var size int64
	if st, err := f.Stat(); err == nil {
		size = st.Size()
	}
	return &auditLog{path: path, f: f, size: size}, nil
}

func (a *auditLog) Close() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil {
		return nil
	}
	err := a.f.Close()
	a.f = nil
	return err
}

// write appends one entry and flushes it. fsync per line is affordable because
// these are transitions, not a stream: a busy day is a few dozen lines.
func (a *auditLog) write(e AuditEntry) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	b = append(b, '\n')
	n, err := a.f.Write(b)
	if err != nil {
		return
	}
	a.size += int64(n)
	_ = a.f.Sync()
	if a.size >= auditMaxBytes {
		a.rotateLocked()
	}
}

// rotateLocked moves the current file to .1 and starts a fresh one, keeping two
// generations. Caller holds mu.
func (a *auditLog) rotateLocked() {
	if a.f == nil {
		return
	}
	_ = a.f.Close()
	a.f = nil
	if err := os.Rename(a.path, a.path+".1"); err != nil {
		// Cannot rotate (read-only mount, permissions). Reopen and keep going
		// rather than silently stopping: an oversized log beats no log.
		if f, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			a.f = f
		}
		return
	}
	f, err := os.OpenFile(a.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	a.f = f
	a.size = 0
}

// entryFrom flattens a Snapshot into a log line. maxCommit is the highest commit
// index any node reports, which is the cluster's real progress marker.
func entryFrom(kind, detail string, snap Snapshot, maxCommit uint64) AuditEntry {
	return AuditEntry{
		Time:         time.Now().UTC(),
		Kind:         kind,
		Detail:       detail,
		Alive:        snap.Alive,
		Total:        snap.Total,
		Quorum:       snap.Quorum,
		Term:         snap.Term,
		LeaderID:     snap.LeaderID,
		CommitIndex:  maxCommit,
		WritesPerSec: snap.WritesPerSec,
		ReadsPerSec:  snap.ReadsPerSec,
		HostCPUPct:   snap.HostCpuBusyPct,
	}
}

// maxCommitIndex is the furthest any node has committed. Taking the max, not the
// leader's, keeps the marker meaningful even while leadership is changing hands.
func maxCommitIndex(nodes []Status) uint64 {
	var m uint64
	for _, n := range nodes {
		if n.CommitIndex > m {
			m = n.CommitIndex
		}
	}
	return m
}

// observe compares this snapshot against the previous one and writes a line for
// every edge it finds. Called on the reconcile tick; writes nothing on a
// steady-state tick, which is the normal case.
//
// stepdowns is the cluster-wide cumulative count of raft commit-stall
// step-downs, and ok reports whether that number could be read at all.
func (a *auditLog) observe(snap Snapshot, stepdowns uint64, stepdownsOK bool, now time.Time) {
	if a == nil {
		return
	}
	commit := maxCommitIndex(snap.Nodes)

	a.mu.Lock()
	first := !a.started
	a.started = true
	prevQuorum, prevLeader := a.lastQuorum, a.lastLeader
	prevCommit, since, wasStalled := a.lastCommit, a.commitSince, a.stalled
	prevStep, sawStep := a.lastStepdow, a.seenStepdow
	a.mu.Unlock()

	if first {
		a.write(entryFrom("controlplane_started", "audit log opened", snap, commit))
		a.mu.Lock()
		a.lastQuorum, a.lastLeader = snap.Quorum, snap.LeaderID
		a.lastCommit, a.commitSince, a.stalled = commit, now, false
		a.lastStepdow, a.seenStepdow = stepdowns, stepdownsOK
		a.mu.Unlock()
		return
	}

	if snap.Quorum != prevQuorum {
		if snap.Quorum {
			a.write(entryFrom("quorum_regained", "", snap, commit))
		} else {
			a.write(entryFrom("quorum_lost",
				fmt.Sprintf("%d of %d alive", snap.Alive, snap.Total), snap, commit))
		}
	}

	// Leader 0 means "nobody knows yet", which is the normal gap during an
	// election. Logging it would emit a pair of lines for every ordinary
	// failover, so only real handovers are recorded.
	if snap.LeaderID != 0 && snap.LeaderID != prevLeader {
		detail := fmt.Sprintf("node %d now leader at term %d", snap.LeaderID, snap.Term)
		if prevLeader != 0 {
			detail = fmt.Sprintf("node %d replaced node %d at term %d",
				snap.LeaderID, prevLeader, snap.Term)
		}
		a.write(entryFrom("leader_changed", detail, snap, commit))
	}

	// Progress. Only meaningful when the cluster claims it could serve: with
	// quorum and a leader. Without those it is failing for a reason that is
	// already recorded above.
	moved := commit != prevCommit
	stalled := wasStalled
	switch {
	case moved:
		if wasStalled {
			a.write(entryFrom("progress_resumed",
				fmt.Sprintf("commit index moving again after %s stalled at %d",
					now.Sub(since).Round(time.Second), prevCommit), snap, commit))
		}
		since = now
		stalled = false
	case snap.Quorum && snap.LeaderID != 0 && !wasStalled && now.Sub(since) >= auditStallAfter:
		a.write(entryFrom("progress_stalled",
			fmt.Sprintf("commit index frozen at %d for %s with quorum and a leader",
				commit, now.Sub(since).Round(time.Second)), snap, commit))
		stalled = true
	}

	// The raft watchdog deposing a leader for lack of progress is worth a line:
	// once means it rescued the cluster, repeatedly means its timeout is wrong.
	if stepdownsOK {
		if sawStep && stepdowns > prevStep {
			a.write(entryFrom("watchdog_stepdown",
				fmt.Sprintf("raft commit-stall watchdog deposed a leader (%d total since node start)",
					stepdowns), snap, commit))
		}
		prevStep, sawStep = stepdowns, true
	}

	a.mu.Lock()
	a.lastQuorum, a.lastLeader = snap.Quorum, snap.LeaderID
	a.lastCommit, a.commitSince, a.stalled = commit, since, stalled
	a.lastStepdow, a.seenStepdow = prevStep, sawStep
	a.mu.Unlock()
}

// tailAudit returns the last limit lines of the log, oldest first. Reads the
// file rather than keeping a second copy in memory, so it reflects what actually
// survived on disk.
func tailAudit(path string, limit int) ([]json.RawMessage, error) {
	if path == "" {
		return nil, fmt.Errorf("audit log disabled")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []json.RawMessage
	start := 0
	for i := 0; i < len(b); i++ {
		if b[i] != '\n' {
			continue
		}
		if line := b[start:i]; len(line) > 0 {
			out = append(out, json.RawMessage(append([]byte(nil), line...)))
		}
		start = i + 1
	}
	if start < len(b) {
		out = append(out, json.RawMessage(append([]byte(nil), b[start:]...)))
	}
	if limit > 0 && len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}
