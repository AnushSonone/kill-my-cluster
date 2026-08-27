package controlplane

// The monkey must be safe before it is interesting: it may only kill a node
// when the cluster is fully up, led, progressing, and untouched by visitors.
// Everything here is pure; there is no docker fake and none is wanted.

import (
	"testing"
	"time"
)

func healthyNodes(n int, leader uint64) []Status {
	out := make([]Status, 0, n)
	for i := 1; i <= n; i++ {
		id := uint64(i)
		out = append(out, Status{ID: id, Running: true, IsLeader: id == leader, LeaderID: leader})
	}
	return out
}

func healthySnap() Snapshot {
	return Snapshot{
		Nodes: healthyNodes(7, 3), Alive: 7, Total: 7, Quorum: true,
		LeaderID: 3, WritesPerSec: 700, ReadsPerSec: 800,
	}
}

func TestChaosCandidatesExcludeDownPartitionedAndHealing(t *testing.T) {
	nodes := healthyNodes(7, 1)
	nodes[1].Running = false
	nodes[2].Partitioned = true
	nodes[3].HealDueMs = 4000
	nodes[3].HealKind = "start"
	got := chaosCandidates(nodes)
	if len(got) != 4 {
		t.Fatalf("want 4 candidates, got %d: %+v", len(got), got)
	}
	for _, n := range got {
		switch n.ID {
		case 2, 3, 4:
			t.Fatalf("node %d should not be a candidate", n.ID)
		}
	}
	// The leader is a legitimate victim: elections are the demo.
	if got[0].ID != 1 || !got[0].IsLeader {
		t.Fatalf("leader should remain a candidate, got %+v", got[0])
	}
}

func TestChaosSkipsUnlessFullyHealthy(t *testing.T) {
	long := time.Now().Add(-time.Hour)
	cases := map[string]func(s *Snapshot){
		"one node down":    func(s *Snapshot) { s.Nodes[4].Running = false; s.Alive = 6 },
		"one node healing": func(s *Snapshot) { s.Nodes[4].HealDueMs = 3000; s.Nodes[4].HealKind = "start" },
		"partitioned":      func(s *Snapshot) { s.Nodes[0].Partitioned = true; s.Alive = 6 },
		"no quorum":        func(s *Snapshot) { s.Quorum = false },
		"no leader":        func(s *Snapshot) { s.LeaderID = 0 },
		"no progress":      func(s *Snapshot) { s.WritesPerSec, s.ReadsPerSec = 0, 0 },
		"empty":            func(s *Snapshot) { *s = Snapshot{} },
	}
	for name, mutate := range cases {
		s := healthySnap()
		mutate(&s)
		if reason := chaosBlocked(s, true, long, time.Now()); reason == "" {
			t.Errorf("%s: monkey should be blocked", name)
		}
	}
	if reason := chaosBlocked(healthySnap(), true, long, time.Now()); reason != "" {
		t.Fatalf("healthy cluster should allow chaos, got %q", reason)
	}
	// Without a rate cache the progress guard cannot be evaluated and must not
	// pin the monkey off forever.
	s := healthySnap()
	s.WritesPerSec, s.ReadsPerSec = 0, 0
	if reason := chaosBlocked(s, false, long, time.Now()); reason != "" {
		t.Fatalf("no rate cache should skip the progress guard, got %q", reason)
	}
}

func TestChaosYieldsAfterVisitorKill(t *testing.T) {
	now := time.Now()
	if reason := chaosBlocked(healthySnap(), true, now.Add(-5*time.Second), now); reason == "" {
		t.Fatal("5s after a visitor kill the monkey should yield")
	}
	if reason := chaosBlocked(healthySnap(), true, now.Add(-40*time.Second), now); reason != "" {
		t.Fatalf("40s after a visitor kill the monkey should act, got %q", reason)
	}
	if reason := chaosBlocked(healthySnap(), true, time.Time{}, now); reason != "" {
		t.Fatalf("no visitor kill ever should allow chaos, got %q", reason)
	}
}

func TestChaosWaitStaysWithinJitter(t *testing.T) {
	base := time.Minute
	for _, u := range []float64{0, 0.5, 0.999} {
		w := chaosWait(base, u)
		if w < 45*time.Second || w > 75*time.Second {
			t.Fatalf("u=%v: wait %v outside [45s,75s]", u, w)
		}
	}
	if chaosWait(base, 0.5) != base {
		t.Fatalf("midpoint should be the base interval, got %v", chaosWait(base, 0.5))
	}
}

func TestAddRingEventDoesNotAudit(t *testing.T) {
	a, path := newTestAudit(t)
	e := &Engine{audit: a, eventCap: 4}
	e.addRingEvent("chaos", "Machine 2 killed by the chaos monkey")
	e.addEvent("kill", "Machine 5 killed")
	kinds := readKinds(t, path)
	if len(kinds) != 1 || kinds[0] != "kill" {
		t.Fatalf("audit should hold only the visitor kill, got %v", kinds)
	}
	evs := e.Events()
	if len(evs) != 2 || evs[0].Kind != "chaos" || evs[1].Kind != "kill" {
		t.Fatalf("ring should hold both events in order, got %+v", evs)
	}
}

func TestChaosInfoNilWhenOff(t *testing.T) {
	e := &Engine{}
	if e.chaosInfo(time.Now()) != nil {
		t.Fatal("chaos off should report nil")
	}
	e.chaosInterval = time.Minute
	info := e.chaosInfo(time.Now())
	if info == nil || info.Node != 0 || info.AgeMs != -1 || info.IntervalMs != 60000 {
		t.Fatalf("chaos on with no kill yet: %+v", info)
	}
	e.lastChaos = chaosKill{Node: 4, At: time.Now().Add(-12 * time.Second)}
	info = e.chaosInfo(time.Now())
	if info.Node != 4 || info.AgeMs < 11900 || info.AgeMs > 13000 {
		t.Fatalf("chaos info after a kill: %+v", info)
	}
}

func TestEntryFromCarriesChaosContext(t *testing.T) {
	s := healthySnap()
	s.Chaos = &ChaosInfo{Node: 6, AgeMs: 2500, IntervalMs: 60000}
	got := entryFrom("leader_changed", "3 -> 4", s, 100)
	if got.ChaosNode != 6 || got.ChaosAgeMs != 2500 {
		t.Fatalf("recent chaos kill should be stamped, got %+v", got)
	}
	s.Chaos.AgeMs = (10 * time.Minute).Milliseconds()
	if got := entryFrom("leader_changed", "", s, 100); got.ChaosNode != 0 {
		t.Fatalf("stale chaos kill should not be stamped, got %+v", got)
	}
	s.Chaos = &ChaosInfo{AgeMs: -1, IntervalMs: 60000}
	if got := entryFrom("leader_changed", "", s, 100); got.ChaosNode != 0 || got.ChaosAgeMs != 0 {
		t.Fatalf("no kill yet should not be stamped, got %+v", got)
	}
	s.Chaos = nil
	if got := entryFrom("leader_changed", "", s, 100); got.ChaosNode != 0 {
		t.Fatalf("chaos off should not be stamped, got %+v", got)
	}
}
