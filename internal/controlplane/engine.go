package controlplane

// Package controlplane is the safe kill switch for the demo cluster.
//
// It can ONLY operate on a whitelist of Docker containers mapped from node
// IDs. Commands are fixed argv arrays (never a shell). Rate limits + auto-heal
// keep a crowd from permanently destroying quorum.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
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

// Engine runs docker CLI against the mounted daemon socket.
type Engine struct {
	dockerBin string
	network   string // compose network name for partition (e.g. kmc_kmc)
	nodes     map[uint64]Node
	startedAt time.Time
	rates     *RateCache

	globalEvery time.Duration
	globalMu    sync.Mutex
	globalNext  time.Time

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
}

type healJob struct {
	kind   string // "start" or "reconnect"
	due    time.Time
	timer  *time.Timer
	cancel chan struct{}
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

	DockerBin string
	// Network is the Docker network used for partition (disconnect/connect).
	Network string

	GlobalKillsPerSec float64
	IPCooldown        time.Duration
	// HealAfter is how long a killed/partitioned machine stays down (default 10s).
	HealAfter time.Duration
	// Rates is optional Prometheus-backed write/read QPS.
	Rates *RateCache
}

// NewEngine validates the whitelist. Docker connectivity is checked lazily.
func NewEngine(cfg Config) (*Engine, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("controlplane: need at least one whitelisted node")
	}
	bin := cfg.DockerBin
	if bin == "" {
		bin = "docker"
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
	rate := cfg.GlobalKillsPerSec
	if rate <= 0 {
		rate = 1.5
	}
	ipCD := cfg.IPCooldown
	if ipCD <= 0 {
		ipCD = 2 * time.Second
	}
	heal := cfg.HealAfter
	if heal <= 0 {
		heal = 10 * time.Second
	}
	return &Engine{
		dockerBin:   bin,
		network:     cfg.Network,
		nodes:       nodes,
		startedAt:   time.Now().UTC(),
		rates:       cfg.Rates,
		globalEvery: time.Duration(float64(time.Second) / rate),
		ipCooldown:  ipCD,
		ipLast:      make(map[string]time.Time),
		healAfter:   heal,
		heals:       make(map[uint64]*healJob),
		eventCap:    64,
	}, nil
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

// Close cancels pending heal timers.
func (e *Engine) Close() error {
	e.healMu.Lock()
	defer e.healMu.Unlock()
	for id, job := range e.heals {
		e.cancelHealLocked(id, job)
	}
	return nil
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
	Nodes         []Status `json:"nodes"`
	Alive         int      `json:"alive"`
	Total         int      `json:"total"`
	Quorum        bool     `json:"quorum"`
	HealAfterMs   int64    `json:"healAfterMs"`
	LeaderID      uint64   `json:"leaderId,omitempty"`
	Term          uint64   `json:"term,omitempty"`
	UptimeMs      int64    `json:"uptimeMs"`
	ActiveUsers   int      `json:"activeUsers"`
	WritesPerSec  float64  `json:"writesPerSec"`
	ReadsPerSec   float64  `json:"readsPerSec"`
	Events        []Event  `json:"events"`
}

// Snapshot builds the current cluster view.
func (e *Engine) Snapshot(ctx context.Context) Snapshot {
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
	writes, reads := 0.0, 0.0
	if e.rates != nil {
		writes, reads = e.rates.Rates()
	}
	started := e.startedAt
	if started.IsZero() {
		started = time.Now().UTC()
	}
	return Snapshot{
		Nodes:        list,
		Alive:        alive,
		Total:        total,
		Quorum:       alive > total/2,
		HealAfterMs:  e.healAfter.Milliseconds(),
		LeaderID:     leaderID,
		Term:         term,
		UptimeMs:     time.Since(started).Milliseconds(),
		ActiveUsers:  e.activeUsers(),
		WritesPerSec: writes,
		ReadsPerSec:  reads,
		Events:       e.Events(),
	}
}

// List reports running/exited for every whitelisted machine.
func (e *Engine) List(ctx context.Context) ([]Status, error) {
	nodes := e.Nodes()
	out := make([]Status, 0, len(nodes))
	for _, n := range nodes {
		st, err := e.inspect(ctx, n)
		if err != nil {
			st = Status{
				ID: n.ID, ContainerName: n.ContainerName,
				Running: false, Status: "unknown",
			}
		}
		e.attachHeal(&st)
		if st.Running && !st.Partitioned {
			e.attachRaft(ctx, &st)
		}
		out = append(out, st)
	}
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
		if err := e.allowDisrupt(clientIP); err != nil {
			return err
		}
		e.cancelHeal(id)
		// -t 1 ≈ abrupt crash (Raft's intended failure mode).
		if err := e.run(ctx, "stop", "-t", "1", n.ContainerName); err != nil {
			return err
		}
		e.addEvent("kill", fmt.Sprintf("Machine %d killed — auto-heal in %s", id, e.healAfter))
		e.scheduleHeal(id, "start")
		return nil

	case ActionRestart:
		e.cancelHeal(id)
		st, err := e.inspect(ctx, n)
		if err != nil {
			return err
		}
		if !st.Running {
			if err := e.run(ctx, "start", n.ContainerName); err != nil {
				return err
			}
		}
		if st.Partitioned || e.network != "" {
			_ = e.connectNetwork(ctx, n.ContainerName) // best-effort
		}
		e.addEvent("restart", fmt.Sprintf("Machine %d restarted", id))
		return nil

	case ActionPartition:
		if e.network == "" {
			return fmt.Errorf("controlplane: partition requires CONTROL_NETWORK")
		}
		if err := e.allowDisrupt(clientIP); err != nil {
			return err
		}
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
		if err := e.run(ctx, "network", "disconnect", e.network, n.ContainerName); err != nil {
			return err
		}
		e.addEvent("partition", fmt.Sprintf("Machine %d partitioned — reconnect in %s", id, e.healAfter))
		e.scheduleHeal(id, "reconnect")
		return nil

	default:
		return fmt.Errorf("controlplane: unknown action %q", action)
	}
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
			if err := e.run(ctx, "start", n.ContainerName); err != nil {
				errs = append(errs, err.Error())
			}
		}
		if e.network != "" {
			_ = e.connectNetwork(ctx, n.ContainerName)
		}
	}
	e.addEvent("reset", "Reset all — every node started and rejoined the network")
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

func (e *Engine) addEvent(kind, message string) {
	e.eventMu.Lock()
	defer e.eventMu.Unlock()
	ev := Event{Time: time.Now(), Kind: kind, Message: message}
	if len(e.events) < e.eventCap {
		e.events = append(e.events, ev)
		return
	}
	copy(e.events, e.events[1:])
	e.events[len(e.events)-1] = ev
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
	client := &http.Client{Timeout: 250 * time.Millisecond}
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

func (e *Engine) scheduleHeal(id uint64, kind string) {
	e.healMu.Lock()
	defer e.healMu.Unlock()
	if old, ok := e.heals[id]; ok {
		e.cancelHealLocked(id, old)
	}
	due := time.Now().Add(e.healAfter)
	cancel := make(chan struct{})
	job := &healJob{kind: kind, due: due, cancel: cancel}
	job.timer = time.AfterFunc(e.healAfter, func() {
		select {
		case <-cancel:
			return
		default:
		}
		ctx, c := context.WithTimeout(context.Background(), 30*time.Second)
		defer c()
		e.runHeal(ctx, id, kind)
		e.healMu.Lock()
		delete(e.heals, id)
		e.healMu.Unlock()
	})
	e.heals[id] = job
}

func (e *Engine) runHeal(ctx context.Context, id uint64, kind string) {
	n, ok := e.nodes[id]
	if !ok {
		return
	}
	switch kind {
	case "start":
		st, err := e.inspect(ctx, n)
		if err == nil && st.Running {
			return
		}
		if err := e.run(ctx, "start", n.ContainerName); err != nil {
			e.addEvent("heal", fmt.Sprintf("Node %d heal start failed: %v", id, err))
			return
		}
		if e.network != "" {
			_ = e.connectNetwork(ctx, n.ContainerName)
		}
		e.addEvent("heal", fmt.Sprintf("Node %d auto-healed (restarted)", id))
	case "reconnect":
		if err := e.connectNetwork(ctx, n.ContainerName); err != nil {
			e.addEvent("heal", fmt.Sprintf("Node %d heal reconnect failed: %v", id, err))
			return
		}
		e.addEvent("heal", fmt.Sprintf("Node %d auto-healed (rejoined network)", id))
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
	err := e.run(ctx, "network", "connect", e.network, container)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "already") {
		return nil
	}
	return err
}

func (e *Engine) inspect(ctx context.Context, n Node) (Status, error) {
	out, err := e.output(ctx, "inspect", "-f", "{{.State.Running}} {{.State.Status}}", n.ContainerName)
	if err != nil {
		return Status{}, err
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) < 2 {
		return Status{}, fmt.Errorf("controlplane: bad inspect output %q", out)
	}
	st := Status{
		ID:            n.ID,
		ContainerName: n.ContainerName,
		Running:       fields[0] == "true",
		Status:        fields[1],
	}
	if e.network != "" && st.Running {
		nets, err := e.output(ctx, "inspect", "-f", "{{json .NetworkSettings.Networks}}", n.ContainerName)
		if err == nil {
			st.Partitioned = !strings.Contains(nets, `"`+e.network+`"`)
		}
	}
	return st, nil
}

func (e *Engine) run(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, e.dockerBin, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("controlplane: docker %v: %s", args, msg)
	}
	return nil
}

func (e *Engine) output(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, e.dockerBin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("controlplane: docker %v: %s", args, msg)
	}
	return stdout.String(), nil
}

func (e *Engine) allowDisrupt(clientIP string) error {
	now := time.Now()

	e.globalMu.Lock()
	if now.Before(e.globalNext) {
		wait := e.globalNext.Sub(now)
		e.globalMu.Unlock()
		return fmt.Errorf("controlplane: global kill rate limit — retry in %dms", wait.Milliseconds())
	}
	e.globalNext = now.Add(e.globalEvery)
	e.globalMu.Unlock()

	if clientIP == "" {
		clientIP = "unknown"
	}
	e.ipMu.Lock()
	defer e.ipMu.Unlock()
	if last, ok := e.ipLast[clientIP]; ok {
		if wait := e.ipCooldown - now.Sub(last); wait > 0 {
			return fmt.Errorf("controlplane: per-IP cooldown — retry in %dms", wait.Milliseconds())
		}
	}
	e.ipLast[clientIP] = now
	return nil
}
