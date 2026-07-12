package bank

import (
	"testing"
)

func TestLedgerTransferConservesTotal(t *testing.T) {
	l, err := NewLedger(InitialTotalCents, DefaultAccounts)
	if err != nil {
		t.Fatal(err)
	}
	next, err := l.Transfer("checking", "vault", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if next.Total() != InitialTotalCents {
		t.Fatalf("total changed: %d", next.Total())
	}
	if next.Balances["checking"] != l.Balances["checking"]-1500 {
		t.Fatalf("checking balance wrong")
	}
}

func TestLedgerMarshalRoundTrip(t *testing.T) {
	l, _ := NewLedger(InitialTotalCents, DefaultAccounts)
	raw, err := l.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalLedger(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Total() != InitialTotalCents {
		t.Fatalf("total %d", got.Total())
	}
}

func TestNaiveDuplicateLeaks(t *testing.T) {
	n, err := NewNaiveLedger()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.ApplyTransfer("checking", "savings", 100); err != nil {
		t.Fatal(err)
	}
	if n.Total() != InitialTotalCents {
		t.Fatalf("first transfer leaked: %d", n.Total())
	}
	if err := n.SimulateDuplicateRetry("checking", "savings", 100); err != nil {
		t.Fatal(err)
	}
	if n.Drift() != 100 {
		t.Fatalf("drift want 100 got %d", n.Drift())
	}
	_, dup, leaked, _ := n.Stats()
	if dup != 1 || leaked != 100 {
		t.Fatalf("stats dup=%d leaked=%d", dup, leaked)
	}
}

func TestFormatTotal(t *testing.T) {
	if FormatTotal(100_000) != "$1000.00" {
		t.Fatalf("got %s", FormatTotal(100_000))
	}
}
