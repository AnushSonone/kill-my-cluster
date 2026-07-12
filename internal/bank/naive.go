package bank

// naive.go is the deliberately broken twin bank for the Part 7 baseline.
//
// It mirrors the same transfer stream as the real bank but simulates
// at-least-once delivery bugs: on a "retry", it credits the destination
// again without debiting the source a second time — money materializes from
// nothing. This is the failure mode that exactly-once dedup prevents.

import (
	"fmt"
	"sync"
)

// NaiveLedger tracks an in-memory ledger that drifts when duplicates land.
// It is NOT stored in the cluster — it exists only to show visitors what
// happens without idempotency.
type NaiveLedger struct {
	mu sync.Mutex

	balances       map[string]int64
	transferCount  uint64
	duplicateCount uint64
	leakedCents    int64 // cumulative money created by duplicate credits
}

// NewNaiveLedger starts from the same even split as the real bank.
func NewNaiveLedger() (*NaiveLedger, error) {
	ledger, err := NewLedger(InitialTotalCents, DefaultAccounts)
	if err != nil {
		return nil, err
	}
	return &NaiveLedger{balances: ledger.Balances}, nil
}

// Total returns the current sum — will exceed InitialTotalCents after leaks.
func (n *NaiveLedger) Total() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	var sum int64
	for _, b := range n.balances {
		sum += b
	}
	return sum
}

// ApplyTransfer runs one correct transfer (debit + credit).
func (n *NaiveLedger) ApplyTransfer(from, to string, cents int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.transferLocked(from, to, cents); err != nil {
		return err
	}
	n.transferCount++
	return nil
}

// SimulateDuplicateRetry models a lost-ACK retry that re-applies only the
// credit leg — the classic double-payment / money-printing bug.
func (n *NaiveLedger) SimulateDuplicateRetry(from, to string, cents int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	dst, ok := n.balances[to]
	if !ok {
		return fmt.Errorf("bank: naive unknown account %q", to)
	}
	// Bug: credit again without debiting source.
	n.balances[to] = dst + cents
	n.duplicateCount++
	n.leakedCents += cents
	return nil
}

func (n *NaiveLedger) transferLocked(from, to string, cents int64) error {
	l := &Ledger{Balances: n.balances}
	next, err := l.Transfer(from, to, cents)
	if err != nil {
		return err
	}
	n.balances = next.Balances
	return nil
}

// Stats returns counters for the demo UI and Part 7 metrics.
func (n *NaiveLedger) Stats() (transfers, duplicates uint64, leaked int64, total int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var sum int64
	for _, b := range n.balances {
		sum += b
	}
	return n.transferCount, n.duplicateCount, n.leakedCents, sum
}

// Drift returns how far the naive total has diverged from the canonical $1,000.
func (n *NaiveLedger) Drift() int64 {
	return n.Total() - InitialTotalCents
}
