package controlplane

// Package controlplane is the safe kill switch for the demo cluster.
//
// It can ONLY operate on a whitelist of Docker containers mapped from node
// IDs. Docker is driven through fixed Engine API calls on the mounted socket
// (never a shell). Rate limits + auto-heal keep a crowd from permanently
// destroying quorum.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Action is a whitelisted control operation.
type Action string

const (
	ActionKill      Action = "kill"
	ActionRestart   Action = "restart"
	ActionPartition Action = "partition"
)

// Node maps a public node ID to a Docker container name.
type Node struct {
	ID            uint64
	ContainerName string
}

// Engine drives Docker through the Engine API on the mounted daemon socket.
type Engine struct {
	docker    *dockerAPI
	network   string // compose network name for partition (e.g. kmc_kmc)
	nodes     map[uint64]Node
	startedAt time.Time
	rates     *RateCache

	// Per-client kill cooldown (same browser/IP).
	ipCooldown time.Duration
	ipMu       sync.Mutex
	ipLast     map[string]time.Time

	healAfter time.Duration
	healMu    sync.Mutex
	heals     map[uint64]*healJob

	eventMu  sync.Mutex
	events   []Event
	eventCap int

	viewersMu sync.Mutex
	viewers   int

	// Snapshot cache (see Snapshot). snapMu guards the cached view and is
	// never held across a build; buildMu keeps builds from overlapping.
	snapMu     sync.Mutex
	snapCached Snapshot
	snapJSON   []byte    // snapCached encoded once, shared by every viewer
	snapAt     time.Time // zero means stale
	snapHave   bool
	snapGen    uint64 // bumped by invalidateSnapshot
	refreshing bool   // a background rebuild is in flight
	buildMu    sync.Mutex
	// buildFn replaces buildSnapshot in tests.
	buildFn func(context.Context) Snapshot

	// probe is the shared keep-alive client for node health probes.
	probe *http.Client
	// probeURL replaces the node health endpoint in tests.
	probeURL func(id uint64) string

	// reconStop ends the reconcile loop (see StartReconciler).
	reconStop chan struct{}
	reconOnce sync.Once

	// audit is the durable incident record (see audit.go). nil = disabled.
	audit     *auditLog
	auditPath string

	// Chaos monkey (see chaos.go). chaosStop ends the loop; chaosMu guards
	// the rest. lastVisitorKill is what the monkey yields to.
	chaosStop       chan struct{}
	chaosStopOnce   sync.Once
	chaosMu         sync.Mutex
	chaosInterval   time.Duration
	lastChaos       chaosKill
	lastVisitorKill time.Time

	// Quorum edge tracking for HUD "time since last quorum loss".
	quorumMu         sync.Mutex
	sawQuorum        bool // true after first successful quorum observation
	lastHadQuorum    bool
	lastQuorumLossAt time.Time // zero => never lost quorum since CP start
}

type healJob struct {
	kind   string // "start" or "reconnect"
	due    time.Time
	timer  *time.Timer
	cancel chan struct{}
	// audit mirrors the heal line to the durable log. False for chaos-monkey
	// kills, which would otherwise write ~1,400 heal lines a day.
	audit bool
}

// Event is a short feed line for the UI.
type Event struct {
	Time    time.Time `json:"time"`
	Kind    string    `json:"kind"`
	Message string    `json:"message"`
}

// Config wires the engine.
type Config struct {
	Nodes []Node

	// DockerSocket is the Docker Engine API unix socket. Default
	// /var/run/docker.sock, which compose mounts into the container.
	DockerSocket string
	// Network is the Docker network used for partition (disconnect/connect).
	Network string

	// IPCooldown between kills from the same client IP (default 2s).
	IPCooldown time.Duration
	// HealAfter is how long a killed/partitioned machine stays down (default 10s).
	// Not advertised on the public wiki UI.
	HealAfter time.Duration
	// Rates is optional Prometheus-backed write/read QPS.
	Rates *RateCache
	// AuditPath is the append-only incident log (see audit.go). Empty disables
	// it; so does any open failure, which is logged and then ignored. The demo
	// must still start when its logging cannot.
	AuditPath string
}

// NewEngine validates the whitelist. Docker connectivity is checked lazily.
func NewEngine(cfg Config) (*Engine, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("controlplane: need at least one whitelisted node")
	}
	nodes := make(map[uint64]Node, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		if n.ID == 0 || n.ContainerName == "" {
			return nil, fmt.Errorf("controlplane: invalid node %+v", n)
		}
		if strings.ContainsAny(n.ContainerName, " \t\n;$|&;<>") {
			return nil, fmt.Errorf("controlplane: unsafe container name %q", n.ContainerName)
		}
		nodes[n.ID] = n
	}
	ipCD := cfg.IPCooldown
	if ipCD <= 0 {
		ipCD = 2 * time.Second
	}
	heal := cfg.HealAfter
	if heal <= 0 {
		heal = 10 * time.Second
	}
	al, err := openAuditLog(cfg.AuditPath)
	if err != nil {
		// Not fatal on purpose: no audit log is bad, a cluster that refuses to
		// boot because of one is worse.
		fmt.Fprintf(os.Stderr, "controlplane: audit log disabled (%s): %v\n", cfg.AuditPath, err)
		al = nil
	}
	return &Engine{
		docker:     newDockerAPI(cfg.DockerSocket),
		probe:      newProbeClient(),
		network:    cfg.Network,
		nodes:      nodes,
		startedAt:  time.Now().UTC(),
		rates:      cfg.Rates,
		ipCooldown: ipCD,
		ipLast:     make(map[string]time.Time),
		healAfter:  heal,
		heals:      make(map[uint64]*healJob),
		eventCap:   64,
		audit:      al,
		auditPath:  cfg.AuditPath,
	}, nil
}

// AuditPath is where the incident log is written ("" when disabled).
func (e *Engine) AuditPath() string { return e.auditPath }

// auditOnce samples the cluster and records any state change. Runs on the
// reconcile tick; on a healthy tick it writes nothing.
func (e *Engine) auditOnce(ctx context.Context) {
	if e.audit == nil {
		return
	}
	snap := e.Snapshot(ctx) // served from cache; a transition lands at most one build late
	var stepdowns uint64
	var ok bool
	if e.rates != nil {
		stepdowns, ok = e.rates.Stepdowns()
	}
	e.audit.observe(snap, stepdowns, ok, time.Now())
}

// AddViewer increments the live SSE presence count.
func (e *Engine) AddViewer() {
	e.viewersMu.Lock()
	e.viewers++
	e.viewersMu.Unlock()
}

// RemoveViewer decrements the live SSE presence count.
func (e *Engine) RemoveViewer() {
	e.viewersMu.Lock()
	if e.viewers > 0 {
		e.viewers--
	}
	e.viewersMu.Unlock()
}

func (e *Engine) activeUsers() int {
	e.viewersMu.Lock()
	defer e.viewersMu.Unlock()
	return e.viewers
}

// Close stops the reconciler and cancels pending heal timers.
func (e *Engine) Close() error {
	defer func() { _ = e.audit.Close() }()
	e.reconOnce.Do(func() {
		if e.reconStop != nil {
			close(e.reconStop)
		}
	})
	e.chaosStopOnce.Do(func() {
		if e.chaosStop != nil {
			close(e.chaosStop)
		}
	})
	e.healMu.Lock()
	defer e.healMu.Unlock()
	for id, job := range e.heals {
		e.cancelHealLocked(id, job)
	}
	return nil
}

// StartReconciler begins a background loop that repairs drift between
// desired state ("every whitelisted node running and connected") and actual
// Docker state. Nodes inside an active heal window are intentional outages
// and are left alone.
//
// This is what the old heal design could not cover: heal timers lived only
// in this process's memory and only for CP-initiated kills. A container that
// crashed on its own, or whose heal timer died with a CP restart, stayed
// down forever (node-3 in the 2026-07 incident). The reconciler needs no
// persistent state — desired state is implied by the whitelist.
func (e *Engine) StartReconciler(interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	e.reconStop = make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-e.reconStop:
				return
			case <-t.C:
				e.reconcileOnce()
				e.auditOnce(context.Background())
			}
		}
	}()
}

func (e *Engine) reconcileOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// One list call covers the healthy case, which is almost every tick.
	states, err := e.docker.containers(ctx)
	if err != nil {
		return // docker unavailable; try next tick
	}
	for _, n := range e.Nodes() {
		if e.healPending(n.ID) {
			continue // intentional outage; the heal timer owns it
		}
		cs, ok := states[n.ContainerName]
		if !ok {
			continue // container unknown; try next tick
		}
		if st := e.statusFrom(n, cs); st.Running && !st.Partitioned {
			continue
		}
		// The list can be seconds old by the time the loop gets here (a start
		// takes about a second, and a heal may have fired meanwhile), so look
		// again before acting on a node that seems wrong.
		st, err := e.inspect(ctx, n)
		if err != nil || e.healPending(n.ID) {
			continue
		}
		if !st.Running {
			if err := e.docker.start(ctx, n.ContainerName); err != nil {
				e.addEvent("heal", fmt.Sprintf("Node %d reconcile start failed: %v", n.ID, err))
				continue
			}
			if e.network != "" {
				_ = e.connectNetwork(ctx, n.ContainerName)
			}
			e.invalidateSnapshot()
			e.addEvent("heal", fmt.Sprintf("Node %d reconciled (was down outside any heal window)", n.ID))
			continue
		}
		if st.Partitioned {
			if err := e.connectNetwork(ctx, n.ContainerName); err == nil {
				e.invalidateSnapshot()
				e.addEvent("heal", fmt.Sprintf("Node %d reconciled (rejoined network)", n.ID))
			}
		}
	}
}

func (e *Engine) healPending(id uint64) bool {
	e.healMu.Lock()
	defer e.healMu.Unlock()
	_, ok := e.heals[id]
	return ok
}

// HealAfter returns the configured auto-heal delay.
func (e *Engine) HealAfter() time.Duration { return e.healAfter }

// Nodes returns the whitelist ordered by ID.
func (e *Engine) Nodes() []Node {
	out := make([]Node, 0, len(e.nodes))
	for id := uint64(1); id <= 64; id++ {
		if n, ok := e.nodes[id]; ok {
			out = append(out, n)
		}
	}
	return out
}

// Status is one machine's live Docker + Raft state.
type Status struct {
	ID            uint64 `json:"id"`
	ContainerName string `json:"container"`
	Running       bool   `json:"running"`
	Status        string `json:"status"`
	Partitioned   bool   `json:"partitioned"`
	HealDueMs     int64  `json:"healDueMs"` // ms until auto-heal; 0 if none
	HealKind      string `json:"healKind,omitempty"`
	Term          uint64 `json:"term,omitempty"`
	CommitIndex   uint64 `json:"commitIndex,omitempty"`
	LeaderID      uint64 `json:"leaderId,omitempty"`
	IsLeader      bool   `json:"isLeader,omitempty"`
	Role          string `json:"role,omitempty"`
}

// Snapshot is the full cluster view for SSE/UI.
type Snapshot struct {
	Nodes       []Status `json:"nodes"`
	Alive       int      `json:"alive"`
	Total       int      `json:"total"`
	Quorum      bool     `json:"quorum"`
	HealAfterMs int64    `json:"healAfterMs"`
	LeaderID    uint64   `json:"leaderId,omitempty"`
	Term        uint64   `json:"term,omitempty"`
	UptimeMs    int64    `json:"uptimeMs"`
	// SinceLastQuorumLossMs is ms since quorum last dropped false.
	// -1 means never lost quorum since this control-plane process started.
	// While quorum is currently false, this is age of the ongoing loss.
	SinceLastQuorumLossMs int64   `json:"sinceLastQuorumLossMs"`
	ActiveUsers           int     `json:"activeUsers"`
	WritesPerSec          float64 `json:"writesPerSec"`
	ReadsPerSec           float64 `json:"readsPerSec"`
	// Host* are VM-level (node_exporter). Omitted when Prom/exporter unavailable.
	HostCpuBusyPct    *float64 `json:"hostCpuBusyPct,omitempty"`
	HostMemUsedBytes  *float64 `json:"hostMemUsedBytes,omitempty"`
	HostMemTotalBytes *float64 `json:"hostMemTotalBytes,omitempty"`
	// Chaos is the chaos monkey's state; nil when CHAOS_INTERVAL is unset.
	Chaos  *ChaosInfo `json:"chaos,omitempty"`
	Events []Event    `json:"events"`
}

const (
	// snapshotMaxAge is how old a served view may be before a rebuild starts.
	snapshotMaxAge = 500 * time.Millisecond
	// snapshotBuildTimeout bounds a background rebuild, which must not borrow
	// the context of whichever request happened to trigger it.
	snapshotBuildTimeout = 10 * time.Second
)

// Snapshot returns the current cluster view without waiting on a build.
//
// It serves the last build immediately. If that build is older than 500ms it
// also starts one background rebuild, and never more than one, so any number
// of viewers cost at most one build per 500ms and none of them waits for it.
// Only the very first call, before any build exists, blocks.
//
// The cache this replaced held its lock across the build. When a build took
// seconds on the 0.08-CPU Oracle cap, every SSE tick, API call, audit tick and
// chaos tick queued behind it.
func (e *Engine) Snapshot(ctx context.Context) Snapshot {
	snap, _ := e.snapshot(ctx)
	return snap
}

// SnapshotJSON is Snapshot already encoded, shared by every viewer. Nil only
// if the view could not be encoded.
func (e *Engine) SnapshotJSON(ctx context.Context) []byte {
	_, data := e.snapshot(ctx)
	return data
}

// SnapshotFresh returns a view no older than 500ms, building one if needed.
// For decisions that must not act on stale state, such as the chaos
// monkey's "everyone is healthy" guard.
func (e *Engine) SnapshotFresh(ctx context.Context) Snapshot {
	snap, _ := e.refresh(ctx)
	return snap
}

func (e *Engine) snapshot(ctx context.Context) (Snapshot, []byte) {
	e.snapMu.Lock()
	if !e.snapHave {
		e.snapMu.Unlock()
		return e.refresh(ctx)
	}
	snap, data := e.snapCached, e.snapJSON
	if time.Since(e.snapAt) >= snapshotMaxAge && !e.refreshing {
		e.refreshing = true
		go e.refreshInBackground()
	}
	e.snapMu.Unlock()
	return snap, data
}

func (e *Engine) refreshInBackground() {
	defer func() {
		e.snapMu.Lock()
		e.refreshing = false
		e.snapMu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), snapshotBuildTimeout)
	defer cancel()
	e.refresh(ctx)
}

// refresh builds a new view unless a fresh one appeared while it waited for
// the build lock. Builds never overlap.
func (e *Engine) refresh(ctx context.Context) (Snapshot, []byte) {
	e.buildMu.Lock()
	defer e.buildMu.Unlock()

	e.snapMu.Lock()
	if e.snapHave && time.Since(e.snapAt) < snapshotMaxAge {
		snap, data := e.snapCached, e.snapJSON
		e.snapMu.Unlock()
		return snap, data
	}
	gen := e.snapGen
	e.snapMu.Unlock()

	build := e.buildFn
	if build == nil {
		build = e.buildSnapshot
	}
	snap := build(ctx)
	data, err := json.Marshal(snap)
	if err != nil {
		data = nil
	}

	e.snapMu.Lock()
	e.snapCached, e.snapJSON, e.snapHave = snap, data, true
	if e.snapGen == gen {
		e.snapAt = time.Now()
	} else {
		// Something changed the cluster mid-build; serve this view but
		// rebuild on the next tick.
		e.snapAt = time.Time{}
	}
	e.snapMu.Unlock()
	return snap, data
}

// invalidateSnapshot marks the cached view stale after an action changed the
// cluster, so the next viewer tick starts a rebuild instead of waiting 500ms.
func (e *Engine) invalidateSnapshot() {
	e.snapMu.Lock()
	e.snapGen++
	e.snapAt = time.Time{}
	e.snapMu.Unlock()
}

// buildSnapshot assembles the cluster view from live Docker + Raft state.
func (e *Engine) buildSnapshot(ctx context.Context) Snapshot {
	list, _ := e.List(ctx)
	alive := 0
	var leaderID, term uint64
	for _, n := range list {
		if n.Running && !n.Partitioned {
			alive++
		}
		if n.IsLeader {
			leaderID = n.ID
			term = n.Term
		}
		if leaderID == 0 && n.LeaderID != 0 {
			leaderID = n.LeaderID
			term = n.Term
		}
	}
	total := len(list)
	quorum := alive > total/2
	sinceLoss := e.noteQuorum(quorum)
	writes, reads := 0.0, 0.0
	if e.rates != nil {
		writes, reads = e.rates.Rates()
	}
	var hostCPU, hostMemUsed, hostMemTotal *float64
	if e.rates != nil {
		cpu, used, total, ok := e.rates.Host()
		if ok {
			hostCPU = &cpu
			hostMemUsed = &used
			hostMemTotal = &total
		}
	}
	started := e.startedAt
	if started.IsZero() {
		started = time.Now().UTC()
	}
	return Snapshot{
		Nodes:                 list,
		Alive:                 alive,
		Total:                 total,
		Quorum:                quorum,
		HealAfterMs:           e.healAfter.Milliseconds(),
		LeaderID:              leaderID,
		Term:                  term,
		UptimeMs:              time.Since(started).Milliseconds(),
		SinceLastQuorumLossMs: sinceLoss,
		ActiveUsers:           e.activeUsers(),
		WritesPerSec:          writes,
		ReadsPerSec:           reads,
		HostCpuBusyPct:        hostCPU,
		HostMemUsedBytes:      hostMemUsed,
		HostMemTotalBytes:     hostMemTotal,
		Chaos:                 e.chaosInfo(time.Now()),
		Events:                e.Events(),
	}
}

// noteQuorum records quorum true→false edges. Returns ms since last loss, or -1 if never.
func (e *Engine) noteQuorum(quorum bool) int64 {
	e.quorumMu.Lock()
	defer e.quorumMu.Unlock()
	now := time.Now()
	if !e.sawQuorum {
		e.sawQuorum = true
		e.lastHadQuorum = quorum
		if !quorum {
			e.lastQuorumLossAt = now
		}
	} else if e.lastHadQuorum && !quorum {
		e.lastQuorumLossAt = now
		e.lastHadQuorum = false
	} else if quorum {
		e.lastHadQuorum = true
	}
	if e.lastQuorumLossAt.IsZero() {
		return -1
	}
	return now.Sub(e.lastQuorumLossAt).Milliseconds()
}

// List reports running/exited for every whitelisted machine: one Docker list
// call, then every reachable node's health probe at once.
func (e *Engine) List(ctx context.Context) ([]Status, error) {
	nodes := e.Nodes()
	out := make([]Status, len(nodes))
	states, err := e.docker.containers(ctx)
	var wg sync.WaitGroup
	for i, n := range nodes {
		cs, ok := states[n.ContainerName]
		if err != nil || !ok {
			out[i] = Status{
				ID: n.ID, ContainerName: n.ContainerName,
				Running: false, Status: "unknown",
			}
		} else {
			out[i] = e.statusFrom(n, cs)
		}
		e.attachHeal(&out[i])
		if out[i].Running && !out[i].Partitioned {
			wg.Add(1)
			go func(st *Status) {
				defer wg.Done()
				e.attachRaft(ctx, st)
			}(&out[i])
		}
	}
	wg.Wait()
	return out, nil
}

// Do runs a whitelisted action. clientIP is used for rate limiting.
func (e *Engine) Do(ctx context.Context, clientIP string, id uint64, action Action) error {
	n, ok := e.nodes[id]
	if !ok {
		return fmt.Errorf("controlplane: node %d not in whitelist", id)
	}
	switch action {
	case ActionKill:
		if err := e.allowDisrupt(ctx, clientIP); err != nil {
			return err
		}
		e.noteVisitorKill()
		return e.kill(ctx, id, killSourceVisitor)

	case ActionRestart:
		e.cancelHeal(id)
		st, err := e.inspect(ctx, n)
		if err != nil {
			return err
		}
		if !st.Running {
			if err := e.docker.start(ctx, n.ContainerName); err != nil {
				return err
			}
		}
		if st.Partitioned || e.network != "" {
			_ = e.connectNetwork(ctx, n.ContainerName) // best-effort
		}
		e.invalidateSnapshot()
		e.addEvent("restart", fmt.Sprintf("Machine %d restarted", id))
		return nil

	case ActionPartition:
		if e.network == "" {
			return fmt.Errorf("controlplane: partition requires CONTROL_NETWORK")
		}
		if err := e.allowDisrupt(ctx, clientIP); err != nil {
			return err
		}
		e.noteVisitorKill()
		e.cancelHeal(id)
		st, err := e.inspect(ctx, n)
		if err != nil {
			return err
		}
		if !st.Running {
			return fmt.Errorf("controlplane: machine %d is not running", id)
		}
		if st.Partitioned {
			return fmt.Errorf("controlplane: machine %d already partitioned", id)
		}
		// Heal first for the same reconciler reason as ActionKill.
		e.scheduleHeal(id, "reconnect", true)
		if err := e.docker.networkDisconnect(ctx, e.network, n.ContainerName); err != nil {
			e.cancelHeal(id)
			return err
		}
		e.invalidateSnapshot()
		e.addEvent("partition", fmt.Sprintf("Machine %d partitioned", id))
		return nil

	default:
		return fmt.Errorf("controlplane: unknown action %q", action)
	}
}

// kill stops a whitelisted machine abruptly and schedules its heal. Shared by
// the visitor path (Do, after the per-IP cooldown) and the chaos monkey, which
// skips the cooldown so it can never rate-limit a real visitor or vice versa.
func (e *Engine) kill(ctx context.Context, id uint64, src killSource) error {
	n, ok := e.nodes[id]
	if !ok {
		return fmt.Errorf("controlplane: node %d not in whitelist", id)
	}
	e.cancelHeal(id)
	// Register the heal BEFORE stopping: the reconcile loop treats a
	// pending heal as "this outage is intentional" — scheduling first
	// means there is never a moment where a freshly killed container
	// looks like an accident and gets insta-restarted.
	e.scheduleHeal(id, "start", src == killSourceVisitor)
	// -t 1 ≈ abrupt crash (Raft's intended failure mode).
	if err := e.docker.stop(ctx, n.ContainerName, 1); err != nil {
		e.cancelHeal(id)
		return err
	}
	e.invalidateSnapshot()
	if src == killSourceChaos {
		// Ring only. The audit log gets chaos context stamped onto the edge
		// lines it already writes (entryFrom), not a line per monkey kill.
		e.addRingEvent("chaos", fmt.Sprintf("Machine %d killed by the chaos monkey", id))
	} else {
		e.addEvent("kill", fmt.Sprintf("Machine %d killed", id))
	}
	return nil
}

// ResetAll brings every whitelisted node back (start + reconnect). Not rate-limited.
func (e *Engine) ResetAll(ctx context.Context) error {
	var errs []string
	for _, n := range e.Nodes() {
		e.cancelHeal(n.ID)
		st, err := e.inspect(ctx, n)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if !st.Running {
			if err := e.docker.start(ctx, n.ContainerName); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if e.network != "" {
			_ = e.connectNetwork(ctx, n.ContainerName)
		}
	}
	e.invalidateSnapshot()
	e.addEvent("reset", "Reset all: every node started and rejoined the network")
	if len(errs) > 0 {
		return fmt.Errorf("controlplane: reset partial failures: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Events returns recent feed lines (oldest first).
func (e *Engine) Events() []Event {
	e.eventMu.Lock()
	defer e.eventMu.Unlock()
	out := make([]Event, len(e.events))
	copy(out, e.events)
	return out
}

// addEvent appends a feed line and mirrors it to the durable log. The RAM ring
// is for the UI and dies with the process; visitor kills and heals are exactly
// the context that made the 2026-07-28 timeline reconstructible, so they belong
// on disk too.
func (e *Engine) addEvent(kind, message string) {
	ev := e.addRingEvent(kind, message)
	if e.audit != nil {
		e.audit.write(AuditEntry{Time: ev.Time.UTC(), Kind: kind, Detail: message})
	}
}

// addRingEvent appends a feed line to the UI ring only. Used for chaos-monkey
// activity, which is routine by design and must not bury real incidents in
// the audit log.
func (e *Engine) addRingEvent(kind, message string) Event {
	e.eventMu.Lock()
	defer e.eventMu.Unlock()
	ev := Event{Time: time.Now(), Kind: kind, Message: message}
	if len(e.events) < e.eventCap {
		e.events = append(e.events, ev)
		return ev
	}
	copy(e.events, e.events[1:])
	e.events[len(e.events)-1] = ev
	return ev
}

func (e *Engine) attachHeal(st *Status) {
	e.healMu.Lock()
	defer e.healMu.Unlock()
	if job, ok := e.heals[st.ID]; ok {
		st.HealKind = job.kind
		ms := time.Until(job.due).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		st.HealDueMs = ms
	}
}

// attachRaft pulls live term/commit/leader from the machine metrics port
// (compose service nodeN:9100). Best-effort — Docker state still drives kill UX.
func (e *Engine) attachRaft(ctx context.Context, st *Status) {
	url := fmt.Sprintf("http://node%d:9100/healthz", st.ID)
	if e.probeURL != nil {
		url = e.probeURL(st.ID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	client := e.probe
	if client == nil {
		client = fallbackProbe
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return
	}
	var body struct {
		Term        uint64 `json:"term"`
		CommitIndex uint64 `json:"commitIndex"`
		LeaderID    uint64 `json:"leaderId"`
		IsLeader    bool   `json:"isLeader"`
		Role        string `json:"role"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return
	}
	st.Term = body.Term
	st.CommitIndex = body.CommitIndex
	st.LeaderID = body.LeaderID
	st.IsLeader = body.IsLeader
	st.Role = body.Role
}

func (e *Engine) scheduleHeal(id uint64, kind string, audit bool) {
	e.healMu.Lock()
	defer e.healMu.Unlock()
	if old, ok := e.heals[id]; ok {
		e.cancelHealLocked(id, old)
	}
	due := time.Now().Add(e.healAfter)
	cancel := make(chan struct{})
	job := &healJob{kind: kind, due: due, cancel: cancel, audit: audit}
	job.timer = time.AfterFunc(e.healAfter, func() {
		select {
		case <-cancel:
			return
		default:
		}
		ctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		e.runHeal(ctx, id, kind, audit)
		e.healMu.Lock()
		delete(e.heals, id)
		e.healMu.Unlock()
	})
	e.heals[id] = job
}

func (e *Engine) runHeal(ctx context.Context, id uint64, kind string, audit bool) {
	n, ok := e.nodes[id]
	if !ok {
		return
	}
	// A failed heal is always audited: the node stays down and the reconciler
	// takes over, which is worth a line whoever asked for the kill.
	emit := func(msg string) {
		if audit {
			e.addEvent("heal", msg)
		} else {
			e.addRingEvent("heal", msg)
		}
	}
	switch kind {
	case "start":
		st, err := e.inspect(ctx, n)
		if err == nil && st.Running {
			return
		}
		if err := e.docker.start(ctx, n.ContainerName); err != nil {
			e.addEvent("heal", fmt.Sprintf("Node %d heal start failed: %v", id, err))
			return
		}
		if e.network != "" {
			_ = e.connectNetwork(ctx, n.ContainerName)
		}
		e.invalidateSnapshot()
		emit(fmt.Sprintf("Node %d auto-healed (restarted)", id))
	case "reconnect":
		if err := e.connectNetwork(ctx, n.ContainerName); err != nil {
			e.addEvent("heal", fmt.Sprintf("Node %d heal reconnect failed: %v", id, err))
			return
		}
		e.invalidateSnapshot()
		emit(fmt.Sprintf("Node %d auto-healed (rejoined network)", id))
	}
}

func (e *Engine) cancelHeal(id uint64) {
	e.healMu.Lock()
	defer e.healMu.Unlock()
	if job, ok := e.heals[id]; ok {
		e.cancelHealLocked(id, job)
	}
}

func (e *Engine) cancelHealLocked(id uint64, job *healJob) {
	if job.timer != nil {
		job.timer.Stop()
	}
	select {
	case <-job.cancel:
	default:
		close(job.cancel)
	}
	delete(e.heals, id)
}

func (e *Engine) connectNetwork(ctx context.Context, container string) error {
	if e.network == "" {
		return nil
	}
	// Idempotent: ignore "already connected" style errors.
	err := e.docker.networkConnect(ctx, e.network, container)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "already") {
		return nil
	}
	return err
}

func (e *Engine) inspect(ctx context.Context, n Node) (Status, error) {
	cs, err := e.docker.inspect(ctx, n.ContainerName)
	if err != nil {
		return Status{}, err
	}
	return e.statusFrom(n, cs), nil
}

// statusFrom turns Docker's view of a container into a machine Status. A
// running machine that is off the cluster network is partitioned.
func (e *Engine) statusFrom(n Node, cs containerState) Status {
	st := Status{
		ID:            n.ID,
		ContainerName: n.ContainerName,
		Running:       cs.Running,
		Status:        cs.Status,
	}
	if e.network != "" && st.Running {
		st.Partitioned = true
		for _, net := range cs.Networks {
			if net == e.network {
				st.Partitioned = false
				break
			}
		}
	}
	return st
}

// newProbeClient is shared by every health probe so connections to the seven
// nodes are reused instead of dialled afresh each build.
func newProbeClient() *http.Client {
	return &http.Client{
		Timeout: 250 * time.Millisecond,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}
}

// fallbackProbe serves Engines built without NewEngine (tests).
var fallbackProbe = newProbeClient()

func (e *Engine) allowDisrupt(_ context.Context, clientIP string) error {
	now := time.Now()
	if clientIP == "" {
		clientIP = "unknown"
	}
	e.ipMu.Lock()
	defer e.ipMu.Unlock()
	if last, ok := e.ipLast[clientIP]; ok {
		if wait := e.ipCooldown - now.Sub(last); wait > 0 {
			return fmt.Errorf("controlplane: per-IP cooldown - retry in %dms", wait.Milliseconds())
		}
	}
	e.ipLast[clientIP] = now
	return nil
}
