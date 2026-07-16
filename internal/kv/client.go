package kv

// client.go is a remote KV client that dials any node and retries on
// NotLeader until it hits the current Raft leader. Used by the traffic agent
// when nodes run in separate containers.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/AnushSonone/kill-my-cluster/internal/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client talks to a multi-node KV cluster over gRPC.
type Client struct {
	mu    sync.Mutex
	addrs map[uint64]string
	conns map[uint64]*grpc.ClientConn
	order []uint64
}

// NewClient dials lazily; addrs maps node ID → "host:port" for the KV API.
func NewClient(addrs map[uint64]string) *Client {
	order := make([]uint64, 0, len(addrs))
	for id := range addrs {
		order = append(order, id)
	}
	return &Client{
		addrs: addrs,
		conns: make(map[uint64]*grpc.ClientConn),
		order: order,
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
	prefer := uint64(0)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		ids := c.tryOrder(prefer)
		for _, id := range ids {
			stub, err := c.stub(id)
			if err != nil {
				continue
			}
			notLeader, hint, err := fn(stub)
			if err != nil {
				c.invalidate(id)
				continue
			}
			if notLeader {
				if hint != 0 {
					prefer = hint
				}
				continue
			}
			return nil
		}
		time.Sleep(40 * time.Millisecond)
	}
	return fmt.Errorf("kv: no leader available")
}

func (c *Client) tryOrder(prefer uint64) []uint64 {
	if prefer == 0 {
		return append([]uint64(nil), c.order...)
	}
	out := []uint64{prefer}
	for _, id := range c.order {
		if id != prefer {
			out = append(out, id)
		}
	}
	return out
}

func (c *Client) invalidate(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[id]; ok {
		_ = conn.Close()
		delete(c.conns, id)
	}
}
