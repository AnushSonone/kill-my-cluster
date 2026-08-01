package controlplane

// The behaviour under test is the one the 2026-07-28 incident needed and did
// not have: a durable line saying the cluster stopped committing while it still
// looked entirely healthy. The subtle half is the negative case — a wedge that
// writes a line every tick is as useless as no line at all.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// snap builds a healthy-looking snapshot with a given commit index.
func snap(alive int, quorum bool, leader uint64, term, commit uint64) Snapshot {
	return Snapshot{
		Nodes: []Status{
			{ID: 1, CommitIndex: commit},
			{ID: 2, CommitIndex: commit - 1},
		},
		Alive:    alive,
		Total:    7,
		Quorum:   quorum,
		LeaderID: leader,
		Term:     term,
	}
}

func newTestAudit(t *testing.T) (*auditLog, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	a, err := openAuditLog(path)
	if err != nil {
		t.Fatalf("openAuditLog: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, path
}

func readKinds(t *testing.T, path string) []string {
	t.Helper()
	lines, err := tailAudit(path, 0)
	if err != nil {
		t.Fatalf("tailAudit: %v", err)
	}
	var kinds []string
	for _, l := range lines {
		var e AuditEntry
		if err := json.Unmarshal(l, &e); err != nil {
			t.Fatalf("bad json line %q: %v", string(l), err)
		}
		kinds = append(kinds, e.Kind)
	}
	return kinds
}

func TestAuditRecordsStallOnceAndResume(t *testing.T) {
	a, path := newTestAudit(t)
	t0 := time.Now()

	// Boot, then two healthy ticks with commit advancing.
	a.observe(snap(7, true, 1, 5, 100), 0, true, t0)
	a.observe(snap(7, true, 1, 5, 200), 0, true, t0.Add(5*time.Second))
	a.observe(snap(7, true, 1, 5, 300), 0, true, t0.Add(10*time.Second))

	if got := readKinds(t, path); len(got) != 1 || got[0] != "controlplane_started" {
		t.Fatalf("healthy ticks should write nothing but the boot line; got %v", got)
	}

	// Commit index freezes. Everything else still looks fine, which is exactly
	// the incident's shape.
	frozen := snap(7, true, 1, 5, 300)
	a.observe(frozen, 0, true, t0.Add(30*time.Second)) // under threshold
	if got := readKinds(t, path); len(got) != 1 {
		t.Fatalf("stall under %s must not report; got %v", auditStallAfter, got)
	}

	a.observe(frozen, 0, true, t0.Add(75*time.Second)) // past threshold
	a.observe(frozen, 0, true, t0.Add(80*time.Second)) // still stalled
	a.observe(frozen, 0, true, t0.Add(85*time.Second))

	got := readKinds(t, path)
	if len(got) != 2 || got[1] != "progress_stalled" {
		t.Fatalf("want exactly one progress_stalled, got %v", got)
	}

	// Recovery.
	a.observe(snap(7, true, 1, 5, 400), 0, true, t0.Add(90*time.Second))
	got = readKinds(t, path)
	if len(got) != 3 || got[2] != "progress_resumed" {
		t.Fatalf("want progress_resumed, got %v", got)
	}
}

func TestAuditIgnoresIdleClusterWithoutLeader(t *testing.T) {
	a, path := newTestAudit(t)
	t0 := time.Now()

	// No leader and no quorum: frozen commit is explained by the outage, which
	// quorum_lost already records. Reporting a stall too would be noise.
	a.observe(snap(2, false, 0, 5, 300), 0, true, t0)
	for i := 1; i <= 40; i++ {
		a.observe(snap(2, false, 0, 5, 300), 0, true, t0.Add(time.Duration(i*5)*time.Second))
	}
	for _, k := range readKinds(t, path) {
		if k == "progress_stalled" {
			t.Fatal("must not report a stall while quorum is lost")
		}
	}
}

func TestAuditRecordsQuorumLeaderAndWatchdogEdges(t *testing.T) {
	a, path := newTestAudit(t)
	t0 := time.Now()

	a.observe(snap(7, true, 1, 5, 100), 0, true, t0)
	a.observe(snap(3, false, 0, 5, 100), 0, true, t0.Add(5*time.Second)) // quorum_lost
	a.observe(snap(7, true, 2, 6, 200), 0, true, t0.Add(10*time.Second)) // regained + leader
	a.observe(snap(7, true, 2, 6, 300), 1, true, t0.Add(15*time.Second)) // watchdog fired
	a.observe(snap(7, true, 2, 6, 400), 1, true, t0.Add(20*time.Second)) // same count, silent

	got := readKinds(t, path)
	want := []string{
		"controlplane_started", "quorum_lost", "quorum_regained",
		"leader_changed", "watchdog_stepdown",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("entry %d = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestAuditSurvivesReopenAndCarriesContext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")

	a, err := openAuditLog(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	a.observe(snap(7, true, 1, 5, 100), 0, true, time.Now())
	_ = a.Close()

	// A control-plane restart must append, never truncate: the 48h wedge
	// outlived several CP restarts and each one erased the in-memory feed.
	b, err := openAuditLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = b.Close() }()
	b.observe(snap(7, true, 1, 5, 100), 0, true, time.Now())

	lines, err := tailAudit(path, 0)
	if err != nil {
		t.Fatalf("tailAudit: %v", err)
	}
	if len(lines) != 2 {
		t.Fatalf("want 2 lines across a restart, got %d", len(lines))
	}

	var e AuditEntry
	if err := json.Unmarshal(lines[0], &e); err != nil {
		t.Fatalf("json: %v", err)
	}
	// A line has to stand alone months later.
	if e.CommitIndex != 100 || e.Total != 7 || !e.Quorum || e.LeaderID != 1 {
		t.Fatalf("entry lost context: %+v", e)
	}
	if e.Time.IsZero() {
		t.Fatal("entry has no timestamp")
	}
}

func TestAuditDisabledIsSafe(t *testing.T) {
	a, err := openAuditLog("")
	if err != nil || a != nil {
		t.Fatalf("empty path should disable cleanly, got (%v, %v)", a, err)
	}
	// Every method must be nil-safe: auditing off must never panic the cluster.
	a.observe(snap(7, true, 1, 5, 100), 0, true, time.Now())
	a.write(AuditEntry{Kind: "x"})
	if err := a.Close(); err != nil {
		t.Fatalf("Close on nil: %v", err)
	}
}

func TestAuditRotatesAtCap(t *testing.T) {
	a, path := newTestAudit(t)
	// Force the rotation path without writing 8MB.
	a.mu.Lock()
	a.size = auditMaxBytes
	a.mu.Unlock()
	a.write(AuditEntry{Time: time.Now().UTC(), Kind: "filler"})

	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected rotated file at %s.1: %v", path, err)
	}
	a.write(AuditEntry{Time: time.Now().UTC(), Kind: "after"})
	kinds := readKinds(t, path)
	if len(kinds) != 1 || kinds[0] != "after" {
		t.Fatalf("post-rotation log should start fresh, got %v", kinds)
	}
}
