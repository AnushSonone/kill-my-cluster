package controlplane

import (
	"strings"
	"testing"
	"time"
)

func TestAllowDisruptRateLimits(t *testing.T) {
	e := &Engine{
		nodes:       map[uint64]Node{1: {ID: 1, ContainerName: "n1"}},
		globalEvery: 200 * time.Millisecond,
		ipCooldown:   500 * time.Millisecond,
		ipLast:      make(map[string]time.Time),
		heals:       make(map[uint64]*healJob),
	}
	if err := e.allowDisrupt("1.1.1.1"); err != nil {
		t.Fatalf("first kill: %v", err)
	}
	if err := e.allowDisrupt("1.1.1.1"); err == nil {
		t.Fatal("expected per-IP or global limit")
	}
	if err := e.allowDisrupt("2.2.2.2"); err == nil {
		t.Fatal("expected global rate limit")
	}
	time.Sleep(220 * time.Millisecond)
	if err := e.allowDisrupt("2.2.2.2"); err != nil {
		t.Fatalf("after global window: %v", err)
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
