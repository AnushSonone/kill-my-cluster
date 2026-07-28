package kv

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cmd := Command{
		Op: OpCAS, ClientID: "agent-1", RequestID: 42,
		Key: "balance", Expect: []byte("100"), Value: []byte("50"),
	}
	got, err := Decode(Encode(cmd))
	if err != nil {
		t.Fatal(err)
	}
	if got.Op != cmd.Op || got.ClientID != cmd.ClientID || got.RequestID != cmd.RequestID ||
		got.Key != cmd.Key || !bytes.Equal(got.Expect, cmd.Expect) || !bytes.Equal(got.Value, cmd.Value) {
		t.Fatalf("round trip mismatch: %+v vs %+v", cmd, got)
	}
}

func TestPutGet(t *testing.T) {
	m := NewMachine()
	m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 1, Key: "k", Value: []byte("v")})
	res := m.Apply(Command{Op: OpGet, ClientID: "c", RequestID: 2, Key: "k"})
	if !res.Found || string(res.Value) != "v" {
		t.Fatalf("get: %+v", res)
	}
}

func TestExactlyOnceDedup(t *testing.T) {
	m := NewMachine()
	put := Command{Op: OpPut, ClientID: "agent", RequestID: 99, Key: "x", Value: []byte("1")}

	r1 := m.Apply(put)
	if r1.Duplicate {
		t.Fatal("first apply should not be duplicate")
	}
	v, ok := m.Get("x")
	if !ok || string(v) != "1" {
		t.Fatalf("key not set: %v %q", ok, v)
	}

	r2 := m.Apply(put)
	if !r2.Duplicate {
		t.Fatal("second apply must be duplicate")
	}
	// State must not have changed (still "1", not double-written).
	v, _ = m.Get("x")
	if string(v) != "1" {
		t.Fatalf("duplicate mutated state: %q", v)
	}
}

func TestCAS(t *testing.T) {
	m := NewMachine()
	m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 1, Key: "k", Value: []byte("old")})

	ok := m.Apply(Command{Op: OpCAS, ClientID: "c", RequestID: 2, Key: "k", Expect: []byte("old"), Value: []byte("new")})
	if !ok.Found {
		t.Fatal("CAS should succeed")
	}
	v, _ := m.Get("k")
	if string(v) != "new" {
		t.Fatalf("want new, got %q", v)
	}

	fail := m.Apply(Command{Op: OpCAS, ClientID: "c", RequestID: 3, Key: "k", Expect: []byte("old"), Value: []byte("bad")})
	if fail.Found {
		t.Fatal("CAS should fail on wrong expect")
	}
	v, _ = m.Get("k")
	if string(v) != "new" {
		t.Fatalf("failed CAS must not change value, got %q", v)
	}
}

func TestSnapshotRestore(t *testing.T) {
	m := NewMachine()
	m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 1, Key: "a", Value: []byte("1")})
	m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 2, Key: "b", Value: []byte("2")})

	raw, err := m.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	m2 := NewMachine()
	if err := m2.Restore(raw); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a": "1", "b": "2"}
	for k, w := range want {
		v, ok := m2.Get(k)
		if !ok || string(v) != w {
			t.Fatalf("key %s: ok=%v val=%q want %q", k, ok, v, w)
		}
	}
	// Dedup table survives too — retry must still be duplicate.
	dup := m2.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 1, Key: "z", Value: []byte("9")})
	if !dup.Duplicate {
		t.Fatal("restored dedup table should block replay")
	}
}

func TestWatch(t *testing.T) {
	m := NewMachine()
	ch := make(chan WatchEvent, 1)
	m.Watch("k", ch)
	m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 1, Key: "k", Value: []byte("v")})
	select {
	case ev := <-ch:
		if ev.Key != "k" || string(ev.Value) != "v" || !ev.Found {
			t.Fatalf("watch event: %+v", ev)
		}
	default:
		t.Fatal("expected watch event")
	}
}

// TestDedupSweepBounded pins the deterministic dedup bound: the table must
// stay near dedupHighWater under unbounded distinct requests, recent retries
// must still dedup, and requests older than the window re-execute (the
// documented trade-off).
func TestDedupSweepBounded(t *testing.T) {
	m := NewMachine()
	total := dedupHighWater + dedupSweepEvery + dedupWindow + 100
	for i := 1; i <= total; i++ {
		m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: uint64(i), Key: "k", Value: []byte("v")})
	}
	// Sweeps are paced (dedupSweepEvery), so the bound carries that slack.
	if len(m.applied) > dedupHighWater+dedupSweepEvery {
		t.Fatalf("dedup table %d entries; bound is %d", len(m.applied), dedupHighWater+dedupSweepEvery)
	}
	res := m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: uint64(total), Key: "k", Value: []byte("x")})
	if !res.Duplicate {
		t.Fatal("recent retry was not deduped")
	}
	res = m.Apply(Command{Op: OpPut, ClientID: "c", RequestID: 1, Key: "k", Value: []byte("y")})
	if res.Duplicate {
		t.Fatal("out-of-window retry claimed Duplicate; sweep should have dropped it")
	}
}

// TestDedupSweepManyClients is the regression for the 2026-07-28 production
// smother: with ~100 clients (per-worker loadgen IDs), clients x window must
// stay under the high-water mark or the sweep can never shrink the table and
// (pre-fix) ran a full-table scan on every apply across all replicas.
func TestDedupSweepManyClients(t *testing.T) {
	const clients = 96
	if clients*dedupWindow >= dedupHighWater {
		t.Fatalf("invariant broken: clients(%d) x dedupWindow(%d) = %d must stay under dedupHighWater(%d)",
			clients, dedupWindow, clients*dedupWindow, dedupHighWater)
	}
	m := NewMachine()
	// Interleave clients like concurrent workers; enough traffic to cross
	// high-water several times over.
	perClient := (dedupHighWater*3)/clients + 1
	for r := 1; r <= perClient; r++ {
		for c := 0; c < clients; c++ {
			m.Apply(Command{
				Op: OpPut, ClientID: fmt.Sprintf("w-%d", c), RequestID: uint64(r),
				Key: "k", Value: []byte("v"),
			})
		}
	}
	if got, bound := len(m.applied), clients*dedupWindow+dedupSweepEvery; got > bound {
		t.Fatalf("dedup table %d entries with %d clients; want <= %d (clients x window + sweep slack)",
			got, clients, bound)
	}
}
