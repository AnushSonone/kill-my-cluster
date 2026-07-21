package controlplane

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateCache periodically queries Prometheus for cluster QPS and host stats.
type RateCache struct {
	baseURL string
	client  *http.Client

	mu     sync.RWMutex
	writes float64
	reads  float64

	hostOK        bool
	cpuBusyPct    float64
	memUsedBytes  float64
	memTotalBytes float64
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

// Host returns VM host CPU busy % and memory bytes from node_exporter.
// ok is false when metrics are missing (exporter/Prom down or not scraped yet).
func (r *RateCache) Host() (cpuBusyPct, memUsedBytes, memTotalBytes float64, ok bool) {
	if r == nil {
		return 0, 0, 0, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cpuBusyPct, r.memUsedBytes, r.memTotalBytes, r.hostOK
}

func (r *RateCache) refresh(ctx context.Context) {
	// Leader-only so follower applies do not multiply throughput by N.
	// Join on instance (scrape target), not node_id: duplicate node_id labels
	// under kill/restart churn make on(node_id) many-to-many and return 0.
	w := r.query(ctx, `sum(rate(kmc_kv_writes_total[15s]) and on(instance) (kmc_raft_is_leader == 1))`)
	rd := r.query(ctx, `sum(rate(kmc_kv_reads_total[15s]) and on(instance) (kmc_raft_is_leader == 1))`)

	cpu, cpuOK := r.queryOK(ctx, `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[30s])))`)
	memTotal, memTotalOK := r.queryOK(ctx, `node_memory_MemTotal_bytes`)
	memAvail, memAvailOK := r.queryOK(ctx, `node_memory_MemAvailable_bytes`)
	hostOK := cpuOK && memTotalOK && memAvailOK && memTotal > 0
	var memUsed float64
	if hostOK {
		memUsed = memTotal - memAvail
		if memUsed < 0 {
			memUsed = 0
		}
		if math.IsNaN(cpu) || math.IsInf(cpu, 0) {
			hostOK = false
		}
	}

	r.mu.Lock()
	r.writes = w
	r.reads = rd
	r.hostOK = hostOK
	if hostOK {
		r.cpuBusyPct = cpu
		r.memUsedBytes = memUsed
		r.memTotalBytes = memTotal
	} else {
		r.cpuBusyPct = 0
		r.memUsedBytes = 0
		r.memTotalBytes = 0
	}
	r.mu.Unlock()
}

func (r *RateCache) query(ctx context.Context, expr string) float64 {
	f, _ := r.queryOK(ctx, expr)
	return f
}

func (r *RateCache) queryOK(ctx context.Context, expr string) (float64, bool) {
	u := r.baseURL + "/api/v1/query?query=" + url.QueryEscape(expr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, false
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, false
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0, false
	}
	if len(parsed.Data.Result) == 0 || len(parsed.Data.Result[0].Value) < 2 {
		return 0, false
	}
	s, ok := parsed.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}
