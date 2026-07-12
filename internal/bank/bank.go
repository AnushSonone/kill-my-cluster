package bank

// bank.go is the real bank: ledger state lives in the replicated KV cluster
// and every transfer goes through ExecuteOnce so retries cannot double-move money.

import (
	"context"
	"fmt"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kv"
)

// Store is the KV surface the bank needs — either in-process clusters or a
// remote gRPC client when nodes run in separate containers.
type Store interface {
	Get(ctx context.Context, clientID string, requestID uint64, key string) (kv.ApplyResult, error)
	ExecuteOnce(ctx context.Context, clientID string, requestID uint64, cmd kv.Command) (kv.ApplyResult, error)
}

// Bank stores the canonical ledger in the linearizable KV cluster.
type Bank struct {
	store Store
}

// NewBank wraps one or more in-process KV nodes. Requests retry until they
// hit the leader.
func NewBank(clusters ...*kv.Cluster) *Bank {
	return &Bank{store: &clusterStore{clusters: clusters}}
}

// NewBankStore wraps any Store (e.g. *kv.Client for Docker deployments).
func NewBankStore(store Store) *Bank {
	return &Bank{store: store}
}

// Init seeds the ledger with InitialTotalCents split across DefaultAccounts.
// Idempotent: if the ledger key already exists, it does nothing.
func (b *Bank) Init(ctx context.Context) error {
	res, err := b.store.Get(ctx, "bank-init", 1, ledgerKey)
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
	_, err = b.store.ExecuteOnce(ctx, "bank-init", 2, kv.Command{
		Op: kv.OpPut, Key: ledgerKey, Value: raw,
	})
	return err
}

// Load reads the current ledger linearizably from the cluster.
func (b *Bank) Load(ctx context.Context, readID uint64) (*Ledger, error) {
	res, err := b.store.Get(ctx, "bank-read", readID, ledgerKey)
	if err != nil {
		return nil, err
	}
	if !res.Found {
		return nil, fmt.Errorf("bank: ledger not initialized")
	}
	return UnmarshalLedger(res.Value)
}

// Transfer moves cents between house accounts exactly once per requestID.
func (b *Bank) Transfer(ctx context.Context, requestID uint64, from, to string, cents int64) (kv.ApplyResult, error) {
	res, err := b.store.Get(ctx, "bank-read", requestID, ledgerKey)
	if err != nil {
		return kv.ApplyResult{}, err
	}
	if !res.Found {
		return kv.ApplyResult{}, fmt.Errorf("bank: ledger not initialized")
	}
	ledger, err := UnmarshalLedger(res.Value)
	if err != nil {
		return kv.ApplyResult{}, err
	}
	next, err := ledger.Transfer(from, to, cents)
	if err != nil {
		return kv.ApplyResult{}, err
	}
	raw, err := next.Marshal()
	if err != nil {
		return kv.ApplyResult{}, err
	}
	return b.store.ExecuteOnce(ctx, "bank", requestID, kv.Command{
		Op: kv.OpPut, Key: ledgerKey, Value: raw,
	})
}

// Total returns the conserved total in cents (linearizable read).
func (b *Bank) Total(ctx context.Context, readID uint64) (int64, error) {
	ledger, err := b.Load(ctx, readID)
	if err != nil {
		return 0, err
	}
	return ledger.Total(), nil
}

// clusterStore fans out to in-process Cluster handles until one is leader.
type clusterStore struct {
	clusters []*kv.Cluster
}

func (s *clusterStore) Get(ctx context.Context, clientID string, requestID uint64, key string) (kv.ApplyResult, error) {
	var out kv.ApplyResult
	err := s.viaLeader(ctx, func(cl *kv.Cluster) error {
		res, err := cl.Get(ctx, clientID, requestID, key)
		if err != nil {
			return err
		}
		out = res
		return nil
	})
	return out, err
}

func (s *clusterStore) ExecuteOnce(ctx context.Context, clientID string, requestID uint64, cmd kv.Command) (kv.ApplyResult, error) {
	var out kv.ApplyResult
	err := s.viaLeader(ctx, func(cl *kv.Cluster) error {
		res, err := cl.ExecuteOnce(ctx, clientID, requestID, cmd)
		if err != nil {
			return err
		}
		out = res
		return nil
	})
	return out, err
}

func (s *clusterStore) viaLeader(ctx context.Context, fn func(*kv.Cluster) error) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, cl := range s.clusters {
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
