package controlplane

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestAllowDisruptPerIPCooldown(t *testing.T) {
	e := &Engine{
		nodes:      map[uint64]Node{1: {ID: 1, ContainerName: "n1"}},
		ipCooldown: 2 * time.Second,
		ipLast:     make(map[string]time.Time),
		heals:      make(map[uint64]*healJob),
	}
	ctx := context.Background()
	if err := e.allowDisrupt(ctx, "1.1.1.1"); err != nil {
		t.Fatalf("first kill: %v", err)
	}
	if err := e.allowDisrupt(ctx, "1.1.1.1"); err == nil {
		t.Fatal("expected per-IP cooldown")
	}
	if err := e.allowDisrupt(ctx, "2.2.2.2"); err != nil {
		t.Fatalf("other IP should be allowed: %v", err)
	}
}

func TestUnknownNodeRejected(t *testing.T) {
	e := &Engine{
		nodes: map[uint64]Node{1: {ID: 1, ContainerName: "n1"}},
		heals: make(map[uint64]*healJob),
	}
	err := e.Do(t.Context(), "127.0.0.1", 99, ActionKill)
	if err == nil {
		t.Fatal("expected whitelist error")
	}
}

func TestScheduleHealCancel(t *testing.T) {
	e := &Engine{
		nodes:     map[uint64]Node{1: {ID: 1, ContainerName: "n1"}},
		healAfter: 50 * time.Millisecond,
		heals:     make(map[uint64]*healJob),
		eventCap:  8,
	}
	e.scheduleHeal(1, "start")
	e.healMu.Lock()
	_, ok := e.heals[1]
	e.healMu.Unlock()
	if !ok {
		t.Fatal("expected pending heal")
	}
	e.cancelHeal(1)
	e.healMu.Lock()
	_, ok = e.heals[1]
	e.healMu.Unlock()
	if ok {
		t.Fatal("heal should be cancelled")
	}
}

func TestPartitionRequiresNetwork(t *testing.T) {
	e := &Engine{
		nodes: map[uint64]Node{1: {ID: 1, ContainerName: "n1"}},
		heals: make(map[uint64]*healJob),
	}
	err := e.Do(t.Context(), "127.0.0.1", 1, ActionPartition)
	if err == nil || !strings.Contains(err.Error(), "CONTROL_NETWORK") {
		t.Fatalf("expected network required error, got %v", err)
	}
}

func TestNoteQuorumTracksTimeSinceLoss(t *testing.T) {
	e := &Engine{}
	if got := e.noteQuorum(true); got != -1 {
		t.Fatalf("never lost: want -1, got %d", got)
	}
	if got := e.noteQuorum(true); got != -1 {
		t.Fatalf("still never lost: want -1, got %d", got)
	}
	if got := e.noteQuorum(false); got < 0 {
		t.Fatalf("on loss: want >=0, got %d", got)
	}
	time.Sleep(20 * time.Millisecond)
	mid := e.noteQuorum(false)
	if mid < 15 {
		t.Fatalf("during loss: want age >=15ms, got %d", mid)
	}
	time.Sleep(20 * time.Millisecond)
	after := e.noteQuorum(true)
	if after < mid {
		t.Fatalf("after recover: timer should keep advancing, got %d < %d", after, mid)
	}
	// Next true→false edge resets the clock.
	_ = e.noteQuorum(true)
	_ = e.noteQuorum(false)
	if got := e.noteQuorum(false); got > 50 {
		t.Fatalf("fresh loss should reset clock, got %d", got)
	}
}
