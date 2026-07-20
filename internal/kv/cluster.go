package kv

// cluster.go wires one Raft node to the KV state machine and exposes the
// client/agent API: Get, Put, CAS, ExecuteOnce, Checkpoint, Watch.
//
// ---------------------------------------------------------------------------
// End-to-end flow (e.g. a Put)
// ---------------------------------------------------------------------------
//  1. Client calls Cluster.Put on any node.
//  2. If this node isn't leader, ErrNotLeader — client retries elsewhere.
//  3. Leader encodes the command and calls raft.Propose.
//  4. Client registers a waiter keyed by log index, blocks until apply.
//  5. The apply loop (started at NewCluster) reads raft.ApplyMsg, runs
//     machine.Apply, delivers the result to the waiter.
//  6. Only after the entry is committed on a majority AND applied locally
//     does the client get success — that's linearizability.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/raft"
)

// ErrNotLeader means the request hit a follower; retry against LeaderID.
var ErrNotLeader = errors.New("kv: not the leader")

// Cluster is one KV node: a Raft peer plus the state machine it applies.
type Cluster struct {
	raft    *raft.Node
	machine *Machine

	applyCh chan raft.ApplyMsg

	// waiters maps log index → channel receiving the apply result for that entry.
	waitMu  sync.Mutex
	waiters map[uint64]chan ApplyResult

	// telemetry is optional; set via SetTelemetry for Prometheus.
	telemetry Telemetry

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// Telemetry is the optional metrics sink (implemented by internal/metrics).
type Telemetry interface {
	ObservePropose(d time.Duration)
	IncApply()
	IncWrite()
	IncRead()
}

// Config bundles what NewCluster needs. It mirrors raft.Config but owns the
// apply channel internally so callers cannot forget to drain it.
type Config struct {
	ID        uint64
	Peers     []uint64
	Dir       string
	Transport raft.Transport
}

// NewCluster starts a Raft node and the goroutine that applies committed
// entries to the KV state machine.
func NewCluster(cfg Config) (*Cluster, error) {
	ch := make(chan raft.ApplyMsg, 256)
	rn, err := raft.NewNode(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers, Dir: cfg.Dir,
		Transport: cfg.Transport, ApplyCh: ch,
	})
	if err != nil {
		return nil, err
	}

	c := &Cluster{
		raft:    rn,
		machine: NewMachine(),
		applyCh: ch,
		waiters: make(map[uint64]chan ApplyResult),
		stop:    make(chan struct{}),
	}
	c.wg.Add(1)
	go c.runApplier()
	return c, nil
}

// Raft returns the underlying Raft node (for serving node-to-node RPCs).
func (c *Cluster) Raft() *raft.Node { return c.raft }

// SetTelemetry attaches a metrics sink (e.g. Prometheus collector). Safe to
// call once after NewCluster, before serving traffic.
func (c *Cluster) SetTelemetry(t Telemetry) { c.telemetry = t }

// Stop shuts down the apply loop and the Raft node.
func (c *Cluster) Stop() {
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	c.wg.Wait()
	c.raft.Stop()
}

// LeaderID returns the current leader (0 if unknown).
func (c *Cluster) LeaderID() uint64 { return c.raft.LeaderID() }

// IsLeader reports whether this node is the Raft leader.
func (c *Cluster) IsLeader() bool {
	_, ok := c.raft.Status()
	return ok
}

// runApplier drains raft.ApplyMsg and feeds the state machine. This is the
// only goroutine that mutates machine (besides Restore from snapshots).
func (c *Cluster) runApplier() {
	defer c.wg.Done()
	for {
		select {
		case <-c.stop:
			return
		case msg, ok := <-c.applyCh:
			if !ok {
				return
			}
			c.handleApply(msg)
		}
	}
}

func (c *Cluster) handleApply(msg raft.ApplyMsg) {
	if msg.SnapshotValid {
		if err := c.machine.Restore(msg.Snapshot); err != nil {
			// Log corruption would be catastrophic; panic is appropriate in dev.
			panic(fmt.Sprintf("kv: restore snapshot at %d: %v", msg.SnapshotIndex, err))
		}
		return
	}
	if !msg.CommandValid {
		return
	}

	cmd, err := Decode(msg.Command)
	if err != nil {
		panic(fmt.Sprintf("kv: decode at index %d: %v", msg.CommandIndex, err))
	}
	res := c.machine.Apply(cmd)
	if c.telemetry != nil {
		c.telemetry.IncApply()
		switch cmd.Op {
		case OpGet:
			c.telemetry.IncRead()
		case OpPut, OpCAS:
			c.telemetry.IncWrite()
		}
	}

	c.waitMu.Lock()
	if ch, ok := c.waiters[msg.CommandIndex]; ok {
		delete(c.waiters, msg.CommandIndex)
		c.waitMu.Unlock()
		ch <- res
		return
	}
	c.waitMu.Unlock()
}

// propose encodes cmd, sends it through Raft, and waits for local apply.
func (c *Cluster) propose(ctx context.Context, cmd Command) (ApplyResult, error) {
	data := Encode(cmd)
	start := time.Now()

	// Retry loop: followers redirect by returning ErrNotLeader.
	for {
		if err := ctx.Err(); err != nil {
			return ApplyResult{}, err
		}
		index, _, ok := c.raft.Propose(data)
		if !ok {
			return ApplyResult{}, ErrNotLeader
		}

		ch := make(chan ApplyResult, 1)
		c.waitMu.Lock()
		c.waiters[index] = ch
		c.waitMu.Unlock()

		select {
		case res := <-ch:
			if c.telemetry != nil {
				c.telemetry.ObservePropose(time.Since(start))
			}
			return res, nil
		case <-ctx.Done():
			c.waitMu.Lock()
			delete(c.waiters, index)
			c.waitMu.Unlock()
			return ApplyResult{}, ctx.Err()
		case <-time.After(5 * time.Second):
			// Leader accepted but we never applied — likely lost leadership
			// mid-flight; retry propose on a (possibly new) leader.
			c.waitMu.Lock()
			delete(c.waiters, index)
			c.waitMu.Unlock()
			start = time.Now()
		}
	}
}

// Get performs a linearizable read via the Raft log.
func (c *Cluster) Get(ctx context.Context, clientID string, requestID uint64, key string) (ApplyResult, error) {
	return c.propose(ctx, Command{
		Op: OpGet, ClientID: clientID, RequestID: requestID, Key: key,
	})
}

// Put unconditionally sets key to value.
func (c *Cluster) Put(ctx context.Context, clientID string, requestID uint64, key string, value []byte) (ApplyResult, error) {
	return c.propose(ctx, Command{
		Op: OpPut, ClientID: clientID, RequestID: requestID,
		Key: key, Value: copyBytes(value),
	})
}

// CAS sets key to value only if the current value equals expect.
func (c *Cluster) CAS(ctx context.Context, clientID string, requestID uint64, key string, expect, value []byte) (ApplyResult, error) {
	return c.propose(ctx, Command{
		Op: OpCAS, ClientID: clientID, RequestID: requestID,
		Key: key, Expect: copyBytes(expect), Value: copyBytes(value),
	})
}

// ExecuteOnce runs a mutating operation exactly once per (clientID, requestID).
// Retries with the same IDs return the original outcome without re-running fn's
// effect — the state machine dedup table enforces this after commit.
//
// fn is called only after the operation commits; use it for real side effects
// (e.g. increment a local counter) in demos/tests. In production the effect IS
// the KV mutation inside the proposed command.
func (c *Cluster) ExecuteOnce(ctx context.Context, clientID string, requestID uint64, cmd Command) (ApplyResult, error) {
	cmd.ClientID = clientID
	cmd.RequestID = requestID
	if cmd.Op != OpPut && cmd.Op != OpCAS && cmd.Op != OpCheckpoint {
		return ApplyResult{}, fmt.Errorf("kv: ExecuteOnce requires mutating op, got %d", cmd.Op)
	}
	return c.propose(ctx, cmd)
}

// Checkpoint stores opaque agent state durably in the cluster.
func (c *Cluster) Checkpoint(ctx context.Context, clientID string, requestID uint64, state []byte) (ApplyResult, error) {
	return c.propose(ctx, Command{
		Op: OpCheckpoint, ClientID: clientID, RequestID: requestID,
		Value: copyBytes(state),
	})
}

// ReadCheckpoint returns the last checkpoint for clientID (linearizable read).
func (c *Cluster) ReadCheckpoint(ctx context.Context, clientID string, requestID uint64) (ApplyResult, error) {
	return c.Get(ctx, clientID, requestID, checkpointKey(clientID))
}

// Watch subscribes to changes on key. Events arrive after commits apply locally.
func (c *Cluster) Watch(key string) chan WatchEvent {
	ch := make(chan WatchEvent, 8)
	c.machine.Watch(key, ch)
	return ch
}

// Unwatch removes a subscription created by Watch.
func (c *Cluster) Unwatch(key string, ch chan WatchEvent) {
	c.machine.Unwatch(key, ch)
}

// MaybeSnapshot compacts the Raft log if applied index >= minEntries since
// the last snapshot. Called by tests/demo; production would trigger on size.
func (c *Cluster) MaybeSnapshot() error {
	idx := c.raft.CommitIndex()
	if idx < 64 {
		return nil
	}
	raw, err := c.machine.Snapshot()
	if err != nil {
		return err
	}
	return c.raft.Snapshot(idx, raw)
}
