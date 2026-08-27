package kv

// client.go is a remote KV client that dials any node and retries on
// NotLeader until it hits the current Raft leader. Used by the traffic agent
// when nodes run in separate containers.

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// peerPenalty is how long a peer that just failed an RPC is tried last.
//
// A stopped container's endpoint is simply gone: no RST, so an RPC to it
// blocks for the caller's whole deadline and comes back DeadlineExceeded,
// which gRPC does not treat as a broken connection. Before this, every
// request re-walked the peers from scratch and re-paid that hang on every
// dead peer ahead of the leader (2026-08-26: one killed follower stalled all
// loadgen workers ~5s and cost ~40% of throughput). Two seconds is long
// enough to skip a corpse and short enough that a healed node is retried
// well within its 10s heal window.
const peerPenalty = 2 * time.Second

// Client talks to a multi-node KV cluster over gRPC.
type Client struct {
	mu    sync.Mutex
	addrs map[uint64]string
	conns map[uint64]*grpc.ClientConn
	order []uint64 // ascending node ID, so the walk is deterministic
	// badUntil demotes peers whose last RPC failed (see peerPenalty).
	badUntil map[uint64]time.Time

	// leader is the last node that answered as leader, tried first on every
	// request. 0 = unknown. Wrong guesses cost one NotLeader round trip and
	// fix themselves from the hint; a dead leader costs one deadline and is
	// then demoted like any other failed peer.
	leader atomic.Uint64
}

// NewClient dials lazily; addrs maps node ID → "host:port" for the KV API.
func NewClient(addrs map[uint64]string) *Client {
	order := make([]uint64, 0, len(addrs))
	for id := range addrs {
		order = append(order, id)
	}
	slices.Sort(order)
	return &Client{
		addrs:    addrs,
		conns:    make(map[uint64]*grpc.ClientConn),
		order:    order,
		badUntil: make(map[uint64]time.Time),
	}
}

// Close tears down all connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, id)
	}
}

func (c *Client) stub(id uint64) (kvpb.KVClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[id]; ok {
		return kvpb.NewKVClient(conn), nil
	}
	addr, ok := c.addrs[id]
	if !ok {
		return nil, fmt.Errorf("kv: unknown peer %d", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	c.conns[id] = conn
	return kvpb.NewKVClient(conn), nil
}

// Get is a linearizable read via the leader.
func (c *Client) Get(ctx context.Context, clientID string, requestID uint64, key string) (ApplyResult, error) {
	var out ApplyResult
	err := c.viaLeader(ctx, func(stub kvpb.KVClient) (notLeader bool, leaderHint uint64, err error) {
		resp, err := stub.Get(ctx, &kvpb.GetRequest{
			ClientId: clientID, RequestId: requestID, Key: key,
		})
		if err != nil {
			return false, 0, err
		}
		if resp.NotLeader {
			return true, resp.LeaderId, nil
		}
		out = ApplyResult{Found: resp.Found, Value: resp.Value, Duplicate: resp.Duplicate}
		return false, 0, nil
	})
	return out, err
}

// ExecuteOnce runs a mutating command exactly once per (clientID, requestID).
func (c *Client) ExecuteOnce(ctx context.Context, clientID string, requestID uint64, cmd Command) (ApplyResult, error) {
	if cmd.Op != OpPut && cmd.Op != OpCAS {
		return ApplyResult{}, fmt.Errorf("kv: client ExecuteOnce supports Put/CAS only")
	}
	var out ApplyResult
	err := c.viaLeader(ctx, func(stub kvpb.KVClient) (notLeader bool, leaderHint uint64, err error) {
		req := &kvpb.ExecuteOnceRequest{
			ClientId: clientID, RequestId: requestID,
			Key: cmd.Key, Value: cmd.Value,
			UseCas: cmd.Op == OpCAS, Expect: cmd.Expect,
		}
		resp, err := stub.ExecuteOnce(ctx, req)
		if err != nil {
			return false, 0, err
		}
		if resp.NotLeader {
			return true, resp.LeaderId, nil
		}
		out = ApplyResult{Found: resp.Ok, Value: resp.Value, Duplicate: resp.Duplicate}
		return false, 0, nil
	})
	return out, err
}

func (c *Client) viaLeader(ctx context.Context, fn func(kvpb.KVClient) (notLeader bool, leaderHint uint64, err error)) error {
	deadline := time.Now().Add(8 * time.Second)
	prefer := c.leader.Load()
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids := c.tryOrder(prefer)
		for _, id := range ids {
			// Only start an attempt on a live context, so a failure below is
			// this peer's own doing. Once a dead peer has eaten the deadline,
			// every later peer would fail instantly and be demoted unfairly,
			// the leader included.
			if err := ctx.Err(); err != nil {
				return err
			}
			stub, err := c.stub(id)
			if err != nil {
				continue
			}
			notLeader, hint, err := fn(stub)
			if err != nil {
				// Tear the connection down only when it is actually broken.
				// A DeadlineExceeded from a slow-but-alive node used to close
				// and re-dial the session — handshake churn against a cluster
				// that is already struggling.
				if status.Code(err) == codes.Unavailable {
					c.invalidate(id)
				}
				c.demote(id)
				continue
			}
			if notLeader {
				if hint != 0 {
					prefer = hint
				}
				continue
			}
			c.leader.Store(id)
			return nil
		}
		// A full sweep found no leader: the cluster is mid-election. Back off
		// with jitter (250-500ms, roughly an election round) instead of the
		// old 40ms hammer — during an outage every caller re-walking all
		// peers every 40ms was itself a meaningful load source.
		wait := 250*time.Millisecond + time.Duration(rand.Int63n(int64(250*time.Millisecond)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("kv: no leader available")
}

// tryOrder is: the preferred (last known leader or hinted) peer, then the
// rest ascending, with peers inside their penalty window moved to the end.
func (c *Client) tryOrder(prefer uint64) []uint64 {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]uint64, 0, len(c.order))
	var bad []uint64
	if prefer != 0 {
		if now.Before(c.badUntil[prefer]) {
			bad = append(bad, prefer)
		} else {
			out = append(out, prefer)
		}
	}
	for _, id := range c.order {
		if id == prefer {
			continue
		}
		if now.Before(c.badUntil[id]) {
			bad = append(bad, id)
		} else {
			out = append(out, id)
		}
	}
	return append(out, bad...)
}

// demote pushes a peer that just failed to the back of the walk for
// peerPenalty. It also forgets it as leader, so the next request walks
// instead of paying the same deadline on the same corpse.
func (c *Client) demote(id uint64) {
	c.mu.Lock()
	c.badUntil[id] = time.Now().Add(peerPenalty)
	c.mu.Unlock()
	c.leader.CompareAndSwap(id, 0)
}

func (c *Client) invalidate(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[id]; ok {
		_ = conn.Close()
		delete(c.conns, id)
	}
}
