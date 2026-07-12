package bank

// ledger.go models the First National Bank of KillMyCluster's books.
//
// ---------------------------------------------------------------------------
// The invariant
// ---------------------------------------------------------------------------
// The bank holds a fixed pool of money split across house accounts. Every
// transfer moves cents from one account to another — nothing is created or
// destroyed. The public demo headline ("Total: $1,000.00 — unchanged after
// N transfers and M kills") is this number: sum(balances) must always equal
// InitialTotalCents when the real bank applies transfers exactly once.
//
// A naive at-least-once twin (naive.go) deliberately breaks the credit/debit
// pairing on simulated retries so visitors can *see* money appear from thin air
// when dedup is missing — the Part 7 baseline.

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// InitialTotalCents is $1,000.00 — the demo's fixed money supply.
const InitialTotalCents int64 = 100_000

// ledgerKey is the single KV key holding the canonical ledger blob.
const ledgerKey = "bank/ledger/v1"

// DefaultAccounts are the house accounts the tenant agent moves money between.
var DefaultAccounts = []string{"checking", "savings", "vault", "operations"}

// Ledger is a map of account → balance in cents. Transfers preserve Total().
type Ledger struct {
	Balances map[string]int64
}

// NewLedger splits totalCents evenly across accounts. Any remainder goes to the
// first account so the sum is exact.
func NewLedger(totalCents int64, accounts []string) (*Ledger, error) {
	if len(accounts) == 0 {
		return nil, fmt.Errorf("bank: need at least one account")
	}
	if totalCents < int64(len(accounts)) {
		return nil, fmt.Errorf("bank: total %d too small for %d accounts", totalCents, len(accounts))
	}
	per := totalCents / int64(len(accounts))
	rem := totalCents % int64(len(accounts))

	l := &Ledger{Balances: make(map[string]int64, len(accounts))}
	for i, name := range accounts {
		l.Balances[name] = per
		if i == 0 {
			l.Balances[name] += rem
		}
	}
	return l, nil
}

// Total returns the sum of all balances — the number the demo displays.
func (l *Ledger) Total() int64 {
	var sum int64
	for _, b := range l.Balances {
		sum += b
	}
	return sum
}

// Transfer moves cents from one house account to another. It returns a new
// Ledger (immutable update pattern) so the caller can CAS the blob in KV.
func (l *Ledger) Transfer(from, to string, cents int64) (*Ledger, error) {
	if cents <= 0 {
		return nil, fmt.Errorf("bank: transfer amount must be positive, got %d", cents)
	}
	if from == to {
		return nil, fmt.Errorf("bank: cannot transfer from %q to itself", from)
	}
	src, ok := l.Balances[from]
	if !ok {
		return nil, fmt.Errorf("bank: unknown account %q", from)
	}
	dst, ok := l.Balances[to]
	if !ok {
		return nil, fmt.Errorf("bank: unknown account %q", to)
	}
	if src < cents {
		return nil, fmt.Errorf("bank: insufficient funds in %q: have %d need %d", from, src, cents)
	}

	out := &Ledger{Balances: make(map[string]int64, len(l.Balances))}
	for k, v := range l.Balances {
		out.Balances[k] = v
	}
	out.Balances[from] = src - cents
	out.Balances[to] = dst + cents
	return out, nil
}

// Marshal gob-encodes the ledger for storage in the KV cluster.
func (l *Ledger) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(l.Balances); err != nil {
		return nil, fmt.Errorf("bank: marshal ledger: %w", err)
	}
	return buf.Bytes(), nil
}

// UnmarshalLedger decodes a ledger blob from the KV store.
func UnmarshalLedger(raw []byte) (*Ledger, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("bank: empty ledger")
	}
	var balances map[string]int64
	if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&balances); err != nil {
		return nil, fmt.Errorf("bank: unmarshal ledger: %w", err)
	}
	return &Ledger{Balances: balances}, nil
}

// FormatTotal renders cents as a dollar string for logs and the demo UI.
func FormatTotal(cents int64) string {
	dollars := cents / 100
	rem := cents % 100
	if rem < 0 {
		rem = -rem
	}
	return fmt.Sprintf("$%d.%02d", dollars, rem)
}
