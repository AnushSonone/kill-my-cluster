package bank

// bank.go is the real bank: ledger state lives in the replicated KV cluster
// and every transfer goes through ExecuteOnce so retries cannot double-move money.

import (
	"context"
	"fmt"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
)

// Bank stores the canonical ledger in the linearizable KV cluster.
type Bank struct {
	clusters []*kv.Cluster
}

// NewBank wraps one or more KV cluster nodes. Requests retry until they hit
// the leader — the agent can talk to any node.
func NewBank(clusters ...*kv.Cluster) *Bank {
	return &Bank{clusters: clusters}
}

// Init seeds the ledger with InitialTotalCents split across DefaultAccounts.
// Idempotent: if the ledger key already exists, it does nothing.
func (b *Bank) Init(ctx context.Context) error {
	return b.viaLeader(ctx, func(cl *kv.Cluster) error {
		res, err := cl.Get(ctx, "bank-init", 1, ledgerKey)
		if err != nil {
			return err
		}
		if res.Found {
			return nil
		}
		ledger, err := NewLedger(InitialTotalCents, DefaultAccounts)
		if err != nil {
			return err
		}
		raw, err := ledger.Marshal()
		if err != nil {
			return err
		}
		_, err = cl.ExecuteOnce(ctx, "bank-init", 2, kv.Command{
			Op: kv.OpPut, Key: ledgerKey, Value: raw,
		})
		return err
	})
}

// Load reads the current ledger linearizably from the cluster.
func (b *Bank) Load(ctx context.Context, readID uint64) (*Ledger, error) {
	var ledger *Ledger
	err := b.viaLeader(ctx, func(cl *kv.Cluster) error {
		res, err := cl.Get(ctx, "bank-read", readID, ledgerKey)
		if err != nil {
			return err
		}
		if !res.Found {
			return fmt.Errorf("bank: ledger not initialized")
		}
		ledger, err = UnmarshalLedger(res.Value)
		return err
	})
	return ledger, err
}

// Transfer moves cents between house accounts exactly once per requestID.
func (b *Bank) Transfer(ctx context.Context, requestID uint64, from, to string, cents int64) (kv.ApplyResult, error) {
	var result kv.ApplyResult
	err := b.viaLeader(ctx, func(cl *kv.Cluster) error {
		res, err := cl.Get(ctx, "bank-read", requestID, ledgerKey)
		if err != nil {
			return err
		}
		if !res.Found {
			return fmt.Errorf("bank: ledger not initialized")
		}
		ledger, err := UnmarshalLedger(res.Value)
		if err != nil {
			return err
		}
		next, err := ledger.Transfer(from, to, cents)
		if err != nil {
			return err
		}
		raw, err := next.Marshal()
		if err != nil {
			return err
		}
		result, err = cl.ExecuteOnce(ctx, "bank", requestID, kv.Command{
			Op: kv.OpPut, Key: ledgerKey, Value: raw,
		})
		return err
	})
	return result, err
}

// Total returns the conserved total in cents (linearizable read).
func (b *Bank) Total(ctx context.Context, readID uint64) (int64, error) {
	var total int64
	err := b.viaLeader(ctx, func(cl *kv.Cluster) error {
		res, err := cl.Get(ctx, "bank-read", readID, ledgerKey)
		if err != nil {
			return err
		}
		if !res.Found {
			return fmt.Errorf("bank: ledger not initialized")
		}
		ledger, err := UnmarshalLedger(res.Value)
		if err != nil {
			return err
		}
		total = ledger.Total()
		return nil
	})
	return total, err
}

func (b *Bank) viaLeader(ctx context.Context, fn func(*kv.Cluster) error) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, cl := range b.clusters {
			if cl == nil {
				continue
			}
			if err := fn(cl); err != kv.ErrNotLeader {
				return err
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("bank: no leader available")
}
