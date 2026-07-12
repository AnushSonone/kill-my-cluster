package controlplane

// Package controlplane is the safe kill switch for the demo cluster.
//
// It can ONLY stop/start a whitelist of Docker containers mapped from
// node IDs. Commands are fixed argv arrays (never a shell). Rate limits
// keep a crowd from killing a majority faster than the cluster can heal.

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Action is a whitelisted control operation.
type Action string

const (
	ActionKill    Action = "kill"
	ActionRestart Action = "restart"
)

// Node maps a public node ID to a Docker container name.
type Node struct {
	ID            uint64
	ContainerName string
}

// Engine runs docker CLI against the mounted daemon socket.
type Engine struct {
	dockerBin string
	nodes     map[uint64]Node

	globalEvery time.Duration
	globalMu    sync.Mutex
	globalNext  time.Time

	ipCooldown time.Duration
	ipMu      sync.Mutex
	ipLast    map[string]time.Time
}

// Config wires the engine.
type Config struct {
	Nodes []Node

	// DockerBin defaults to "docker" on PATH.
	DockerBin string

	GlobalKillsPerSec float64
	IPCooldown        time.Duration
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
	return &Engine{
		dockerBin:   bin,
		nodes:       nodes,
		globalEvery: time.Duration(float64(time.Second) / rate),
		ipCooldown:   ipCD,
		ipLast:      make(map[string]time.Time),
	}, nil
}

// Close is a no-op (kept for API symmetry).
func (e *Engine) Close() error { return nil }

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

// Status is one node's live Docker state.
type Status struct {
	ID            uint64 `json:"id"`
	ContainerName string `json:"container"`
	Running       bool   `json:"running"`
	Status        string `json:"status"`
}

// List reports running/exited for every whitelisted node.
func (e *Engine) List(ctx context.Context) ([]Status, error) {
	nodes := e.Nodes()
	out := make([]Status, 0, len(nodes))
	for _, n := range nodes {
		st, err := e.inspect(ctx, n)
		if err != nil {
			out = append(out, Status{
				ID: n.ID, ContainerName: n.ContainerName,
				Running: false, Status: "unknown",
			})
			continue
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
		if err := e.allowKill(clientIP); err != nil {
			return err
		}
		// -t 1 ≈ abrupt crash (Raft's intended failure mode).
		return e.run(ctx, "stop", "-t", "1", n.ContainerName)
	case ActionRestart:
		st, err := e.inspect(ctx, n)
		if err != nil {
			return err
		}
		if st.Running {
			return fmt.Errorf("controlplane: node %d already running", id)
		}
		return e.run(ctx, "start", n.ContainerName)
	default:
		return fmt.Errorf("controlplane: unknown action %q", action)
	}
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
	return Status{
		ID:            n.ID,
		ContainerName: n.ContainerName,
		Running:       fields[0] == "true",
		Status:        fields[1],
	}, nil
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

func (e *Engine) allowKill(clientIP string) error {
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
