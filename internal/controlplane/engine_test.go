package controlplane

import (
	"testing"
	"time"
)

func TestAllowKillRateLimits(t *testing.T) {
	e := &Engine{
		nodes:       map[uint64]Node{1: {ID: 1, ContainerName: "n1"}},
		globalEvery: 200 * time.Millisecond,
		ipCooldown:   500 * time.Millisecond,
		ipLast:      make(map[string]time.Time),
	}
	if err := e.allowKill("1.1.1.1"); err != nil {
		t.Fatalf("first kill: %v", err)
	}
	if err := e.allowKill("1.1.1.1"); err == nil {
		t.Fatal("expected per-IP or global limit")
	}
	// Different IP still blocked by global bucket immediately.
	if err := e.allowKill("2.2.2.2"); err == nil {
		t.Fatal("expected global rate limit")
	}
	time.Sleep(220 * time.Millisecond)
	if err := e.allowKill("2.2.2.2"); err != nil {
		t.Fatalf("after global window: %v", err)
	}
}

func TestUnknownNodeRejected(t *testing.T) {
	e := &Engine{nodes: map[uint64]Node{1: {ID: 1, ContainerName: "n1"}}}
	err := e.Do(t.Context(), "127.0.0.1", 99, ActionKill)
	if err == nil || err.Error() == "" {
		t.Fatal("expected whitelist error")
	}
}
