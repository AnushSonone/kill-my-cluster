package controlplane

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateCache periodically queries Prometheus for cluster write/read QPS.
type RateCache struct {
	baseURL string
	client  *http.Client

	mu     sync.RWMutex
	writes float64
	reads  float64
}

// NewRateCache scrapes baseURL (e.g. http://prometheus:9090). Empty URL disables.
func NewRateCache(prometheusURL string) *RateCache {
	return &RateCache{
		baseURL: strings.TrimRight(prometheusURL, "/"),
		client:  &http.Client{Timeout: 2 * time.Second},
	}
}

// Run refreshes rates until ctx is done.
func (r *RateCache) Run(ctx context.Context) {
	if r == nil || r.baseURL == "" {
		return
	}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	r.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.refresh(ctx)
		}
	}
}

// Rates returns the last scraped writes/s and reads/s.
func (r *RateCache) Rates() (writes, reads float64) {
	if r == nil {
		return 0, 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.writes, r.reads
}

func (r *RateCache) refresh(ctx context.Context) {
	// Leader-only so follower applies do not multiply throughput by N.
	w := r.query(ctx, `sum(rate(kmc_kv_writes_total[15s]) * on(node_id) group_left() kmc_raft_is_leader)`)
	rd := r.query(ctx, `sum(rate(kmc_kv_reads_total[15s]) * on(node_id) group_left() kmc_raft_is_leader)`)
	r.mu.Lock()
	r.writes = w
	r.reads = rd
	r.mu.Unlock()
}

func (r *RateCache) query(ctx context.Context, expr string) float64 {
	u := r.baseURL + "/api/v1/query?query=" + url.QueryEscape(expr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0
	}
	if len(parsed.Data.Result) == 0 || len(parsed.Data.Result[0].Value) < 2 {
		return 0
	}
	s, ok := parsed.Data.Result[0].Value[1].(string)
	if !ok {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}
