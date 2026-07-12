// Package metrics exports kill-my-cluster telemetry for Prometheus.
//
// Each node process exposes HTTP GET /metrics. Prometheus scrapes those
// endpoints; Grafana graphs the results. Labels always include node_id so
// multi-node dashboards stay readable.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector holds all process-level series. Create one per node process.
type Collector struct {
	reg *prometheus.Registry

	isLeader    prometheus.Gauge
	term        prometheus.Gauge
	commitIndex prometheus.Gauge
	role        prometheus.Gauge // 0=follower, 1=candidate, 2=leader, -1=dead
	leaderID    prometheus.Gauge

	proposalsTotal prometheus.Counter
	appliesTotal   prometheus.Counter
	proposeLatency prometheus.Histogram

	bankRealCents  prometheus.Gauge
	bankNaiveCents prometheus.Gauge
	bankDriftCents prometheus.Gauge
	bankTransfers  prometheus.Counter
}

// NewCollector registers series for the given node ID.
func NewCollector(nodeID uint64) *Collector {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	id := strconv.FormatUint(nodeID, 10)
	labels := prometheus.Labels{"node_id": id}

	c := &Collector{reg: reg}

	c.isLeader = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "kmc_raft_is_leader",
		Help:        "1 if this node is the Raft leader, else 0",
		ConstLabels: labels,
	})
	c.term = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "kmc_raft_term",
		Help:        "Current Raft term on this node",
		ConstLabels: labels,
	})
	c.commitIndex = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "kmc_raft_commit_index",
		Help:        "Highest committed log index on this node",
		ConstLabels: labels,
	})
	c.role = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "kmc_raft_role",
		Help:        "Raft role: 0=follower, 1=candidate, 2=leader",
		ConstLabels: labels,
	})
	c.leaderID = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "kmc_raft_leader_id",
		Help:        "This node's view of the current leader ID (0 if unknown)",
		ConstLabels: labels,
	})
	c.proposalsTotal = factory.NewCounter(prometheus.CounterOpts{
		Name:        "kmc_raft_proposals_total",
		Help:        "Proposals accepted by this node while leader",
		ConstLabels: labels,
	})
	c.appliesTotal = factory.NewCounter(prometheus.CounterOpts{
		Name:        "kmc_kv_applies_total",
		Help:        "Committed commands applied to the KV state machine",
		ConstLabels: labels,
	})
	c.proposeLatency = factory.NewHistogram(prometheus.HistogramOpts{
		Name:        "kmc_propose_latency_seconds",
		Help:        "Time from Propose to local apply (linearizable write latency)",
		ConstLabels: labels,
		Buckets:     []float64{.001, .002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})
	c.bankRealCents = factory.NewGauge(prometheus.GaugeOpts{
		Name: "kmc_bank_real_total_cents",
		Help: "Real bank ledger total in cents (should stay at 100000)",
	})
	c.bankNaiveCents = factory.NewGauge(prometheus.GaugeOpts{
		Name: "kmc_bank_naive_total_cents",
		Help: "Naive twin ledger total in cents (drifts upward on duplicate credits)",
	})
	c.bankDriftCents = factory.NewGauge(prometheus.GaugeOpts{
		Name: "kmc_bank_drift_cents",
		Help: "Naive total minus the canonical $1,000 (100000 cents)",
	})
	c.bankTransfers = factory.NewCounter(prometheus.CounterOpts{
		Name: "kmc_bank_transfers_total",
		Help: "Successful real-bank transfers observed by the metrics process",
	})

	return c
}

// Handler returns the Prometheus scrape endpoint for this collector.
func (c *Collector) Handler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{})
}

// SetRaft updates gauges from a node's current Raft view.
func (c *Collector) SetRaft(term uint64, isLeader bool, role int, commitIndex, leaderID uint64) {
	if isLeader {
		c.isLeader.Set(1)
	} else {
		c.isLeader.Set(0)
	}
	c.term.Set(float64(term))
	c.commitIndex.Set(float64(commitIndex))
	c.role.Set(float64(role))
	c.leaderID.Set(float64(leaderID))
}

// ObservePropose records one completed propose→apply round-trip.
func (c *Collector) ObservePropose(d time.Duration) {
	c.proposalsTotal.Inc()
	c.proposeLatency.Observe(d.Seconds())
}

// IncApply increments the applied-command counter.
func (c *Collector) IncApply() { c.appliesTotal.Inc() }

// SetBank updates the tenant headline gauges. Call from one process only
// (the metricsdemo agent owner) to avoid double-counting transfers.
func (c *Collector) SetBank(realCents, naiveCents, driftCents int64, transfersDelta uint64) {
	c.bankRealCents.Set(float64(realCents))
	c.bankNaiveCents.Set(float64(naiveCents))
	c.bankDriftCents.Set(float64(driftCents))
	if transfersDelta > 0 {
		c.bankTransfers.Add(float64(transfersDelta))
	}
}

// RoleInt maps raft.Role string values to the gauge encoding.
func RoleInt(role string) int {
	switch role {
	case "follower":
		return 0
	case "candidate":
		return 1
	case "leader":
		return 2
	default:
		return -1
	}
}
