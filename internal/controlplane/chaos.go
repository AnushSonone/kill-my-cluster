package controlplane

// The chaos monkey keeps the public demo demonstrating. With no visitors the
// HUD, the event feed and every Grafana panel sit flat, which reads as
// "nothing is happening" rather than "this cluster survives failure". So the
// control plane kills one random healthy node itself, roughly once a minute.
//
// It is deliberately timid. Incident 2 (2026-07-28, RECOVERY.md) was triggered
// by exactly this kill/heal/InstallSnapshot cycle, and the monkey replays it
// ~1,400 times a day. The commit-stall watchdog in the raft package is the
// real defence; the monkey's job is to never make an outage worse, so it only
// acts on a cluster that is fully up, has a leader, is making progress, and
// has not been touched by a visitor for chaosSettle.

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

// killSource says who asked for a kill: a visitor through the HTTP API, or
// the chaos monkey inside this process. It decides rate limiting (visitors
// only) and whether the kill and its heal are mirrored to the audit log
// (visitors only; monkey activity is routine by design).
type killSource int

const (
	killSourceVisitor killSource = iota
	killSourceChaos
)

const (
	// chaosSettle is how long the monkey stays quiet after any visitor kill or
	// partition. A visitor forcing quorum loss should not find the monkey
	// helping; a visitor watching their own kill heal should see only that.
	chaosSettle = 30 * time.Second

	// chaosJitter widens each interval by this fraction either way so the
	// feed does not look like a metronome.
	chaosJitter = 0.25

	// chaosAuditContext bounds how long after a monkey kill the audit log
	// still stamps it onto transition lines (see entryFrom).
	chaosAuditContext = 5 * time.Minute
)

// chaosKill is the last thing the monkey did.
type chaosKill struct {
	Node uint64
	At   time.Time
}

// ChaosInfo is the HUD and audit view of the monkey. Present on the Snapshot
// only when chaos is enabled.
type ChaosInfo struct {
	// Node is the last machine the monkey killed; 0 if none yet.
	Node uint64 `json:"node,omitempty"`
	// AgeMs is how long ago that was; -1 if none yet.
	AgeMs      int64 `json:"ageMs"`
	IntervalMs int64 `json:"intervalMs"`
}

// StartChaos begins the monkey loop. interval <= 0 leaves it off.
func (e *Engine) StartChaos(interval time.Duration) {
	if interval <= 0 {
		return
	}
	e.chaosMu.Lock()
	e.chaosInterval = interval
	e.chaosMu.Unlock()
	e.chaosStop = make(chan struct{})
	go func() {
		for {
			wait := chaosWait(interval, rand.Float64())
			select {
			case <-e.chaosStop:
				return
			case <-time.After(wait):
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			e.chaosOnce(ctx)
			cancel()
		}
	}()
}

// chaosWait spreads interval over [1-chaosJitter, 1+chaosJitter] using u in
// [0,1). Pure so the spread is testable.
func chaosWait(interval time.Duration, u float64) time.Duration {
	f := 1 - chaosJitter + 2*chaosJitter*u
	return time.Duration(float64(interval) * f)
}

// chaosOnce takes one look at the cluster and kills one node if every guard
// passes. Any miss is silent: the next tick will look again.
func (e *Engine) chaosOnce(ctx context.Context) {
	snap := e.Snapshot(ctx)
	e.chaosMu.Lock()
	lastVisitor := e.lastVisitorKill
	e.chaosMu.Unlock()
	if reason := chaosBlocked(snap, e.rates != nil, lastVisitor, time.Now()); reason != "" {
		return
	}
	victims := chaosCandidates(snap.Nodes)
	if len(victims) == 0 {
		return
	}
	victim := victims[rand.IntN(len(victims))]
	if err := e.kill(ctx, victim.ID, killSourceChaos); err != nil {
		e.addRingEvent("chaos", fmt.Sprintf("Chaos monkey could not kill machine %d: %v", victim.ID, err))
		return
	}
	e.chaosMu.Lock()
	e.lastChaos = chaosKill{Node: victim.ID, At: time.Now()}
	e.chaosMu.Unlock()
}

// chaosCandidates is every machine the monkey may pick: running, on the
// network, and not inside a heal window. The leader is included on purpose;
// an election is the visible half of the demo.
func chaosCandidates(nodes []Status) []Status {
	out := make([]Status, 0, len(nodes))
	for _, n := range nodes {
		if n.Running && !n.Partitioned && n.HealDueMs == 0 && n.HealKind == "" {
			out = append(out, n)
		}
	}
	return out
}

// chaosBlocked returns why the monkey must not act right now, or "" when it
// may. ratesKnown is whether a Prometheus rate cache exists at all; without
// one the progress guard cannot be evaluated and is skipped.
func chaosBlocked(snap Snapshot, ratesKnown bool, lastVisitorKill, now time.Time) string {
	if snap.Total == 0 {
		return "no nodes"
	}
	if len(chaosCandidates(snap.Nodes)) != snap.Total {
		return "a machine is down, partitioned or healing"
	}
	if !snap.Quorum {
		return "no quorum"
	}
	if snap.LeaderID == 0 {
		return "no leader"
	}
	if ratesKnown && snap.WritesPerSec <= 0 && snap.ReadsPerSec <= 0 {
		return "no progress"
	}
	if !lastVisitorKill.IsZero() && now.Sub(lastVisitorKill) < chaosSettle {
		return "visitor kill too recent"
	}
	return ""
}

// noteVisitorKill records that a visitor disrupted the cluster, which makes
// the monkey stand down for chaosSettle.
func (e *Engine) noteVisitorKill() {
	e.chaosMu.Lock()
	e.lastVisitorKill = time.Now()
	e.chaosMu.Unlock()
}

// chaosInfo reports the monkey's state for the Snapshot; nil when disabled.
func (e *Engine) chaosInfo(now time.Time) *ChaosInfo {
	e.chaosMu.Lock()
	defer e.chaosMu.Unlock()
	if e.chaosInterval <= 0 {
		return nil
	}
	info := &ChaosInfo{AgeMs: -1, IntervalMs: e.chaosInterval.Milliseconds()}
	if e.lastChaos.Node != 0 {
		info.Node = e.lastChaos.Node
		info.AgeMs = now.Sub(e.lastChaos.At).Milliseconds()
	}
	return info
}
