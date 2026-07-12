package bank

// agent.go is the 24/7 tenant: it continuously moves money between house
// accounts on the real bank (exactly-once) while feeding the same transfers
// to the naive twin (with simulated duplicate credits) so the demo always
// has live traffic and a visible leak baseline.

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

// AgentConfig controls the tenant loop.
type AgentConfig struct {
	Bank  *Bank
	Naive *NaiveLedger

	// Interval between transfer attempts. Production would use ~1–5s; demos
	// can go faster.
	Interval time.Duration

	// DuplicateRate is the fraction of transfers where the naive twin also
	// gets a SimulateDuplicateRetry (0.0–1.0). Default 0.25 makes drift
	// visible within seconds without being absurd.
	DuplicateRate float64

	// MaxAmountCents caps a single transfer size (keeps accounts liquid).
	MaxAmountCents int64
}

// Agent runs until Stop or ctx cancel.
type Agent struct {
	cfg AgentConfig

	requestSeq atomic.Uint64
	transfers  atomic.Uint64

	stop     chan struct{}
	stopOnce chan struct{}
}

// NewAgent validates config and returns a stopped agent.
func NewAgent(cfg AgentConfig) (*Agent, error) {
	if cfg.Bank == nil || cfg.Naive == nil {
		return nil, fmt.Errorf("bank: agent needs Bank and Naive")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.MaxAmountCents <= 0 {
		cfg.MaxAmountCents = 500 // $5.00 max per hop
	}
	if cfg.DuplicateRate < 0 || cfg.DuplicateRate > 1 {
		return nil, fmt.Errorf("bank: DuplicateRate must be 0–1")
	}
	return &Agent{
		cfg:      cfg,
		stop:     make(chan struct{}),
		stopOnce: make(chan struct{}),
	}, nil
}

// Start begins the transfer loop in a background goroutine.
func (a *Agent) Start(ctx context.Context) {
	go func() {
		defer close(a.stopOnce)
		ticker := time.NewTicker(a.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-a.stop:
				return
			case <-ticker.C:
				a.runOnce(ctx)
			}
		}
	}()
}

// Stop ends the loop and waits for exit.
func (a *Agent) Stop() {
	close(a.stop)
	<-a.stopOnce
}

// TransferCount is the number of successful real-bank transfers.
func (a *Agent) TransferCount() uint64 {
	return a.transfers.Load()
}

func (a *Agent) runOnce(ctx context.Context) {
	from, to, amount, err := a.pickTransfer()
	if err != nil {
		return
	}
	reqID := a.requestSeq.Add(1)

	res, err := a.cfg.Bank.Transfer(ctx, reqID, from, to, amount)
	if err != nil {
		return
	}
	if !res.Duplicate {
		a.transfers.Add(1)
	}

	_ = a.cfg.Naive.ApplyTransfer(from, to, amount)
	if rand.Float64() < a.cfg.DuplicateRate {
		_ = a.cfg.Naive.SimulateDuplicateRetry(from, to, amount)
	}
}

func (a *Agent) pickTransfer() (from, to string, cents int64, err error) {
	accts := DefaultAccounts
	if len(accts) < 2 {
		return "", "", 0, fmt.Errorf("bank: need 2+ accounts")
	}
	from = accts[rand.Intn(len(accts))]
	for {
		to = accts[rand.Intn(len(accts))]
		if to != from {
			break
		}
	}
	max := a.cfg.MaxAmountCents
	if max > 2000 {
		max = 2000
	}
	cents = int64(rand.Intn(int(max))) + 1
	return from, to, cents, nil
}

// Snapshot reports live totals for the demo headline and Part 7 metrics.
type Snapshot struct {
	RealTotalCents  int64
	NaiveTotalCents int64
	DriftCents      int64
	LeakedCents     int64
	TransferCount   uint64
	NaiveDuplicates uint64
	AgentTransfers  uint64
	Conserved       bool
}

// Stats reads real bank total from cluster and naive counters from memory.
func (a *Agent) Stats(ctx context.Context) (Snapshot, error) {
	readID := a.requestSeq.Load() + 1_000_000
	real, err := a.cfg.Bank.Total(ctx, readID)
	if err != nil {
		return Snapshot{}, err
	}
	tc, dup, leaked, naiveTotal := a.cfg.Naive.Stats()
	return Snapshot{
		RealTotalCents:  real,
		NaiveTotalCents: naiveTotal,
		DriftCents:      naiveTotal - InitialTotalCents,
		LeakedCents:     leaked,
		TransferCount:   tc,
		NaiveDuplicates: dup,
		AgentTransfers:  a.transfers.Load(),
		Conserved:       real == InitialTotalCents,
	}, nil
}
