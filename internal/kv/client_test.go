package kv

// The client's job under a kill is to not notice. A stopped container's
// endpoint vanishes without an RST, so an RPC to it blocks for the caller's
// whole deadline. These tests model that with a blackhole listener: it
// accepts TCP and never speaks HTTP/2, which leaves gRPC in CONNECTING until
// the context expires, exactly like production on 2026-08-26.

import (
	"context"
	"net"
	"testing"
	"time"
)

// blackhole returns an address that accepts connections and never answers.
func blackhole(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() { <-done; _ = conn.Close() }()
		}
	}()
	t.Cleanup(func() { close(done); _ = l.Close() })
	return l.Addr().String()
}

func (c *testKVCluster) leaderID(t *testing.T) uint64 {
	t.Helper()
	c.waitForLeader(5 * time.Second)
	for i, cl := range c.clusters {
		if cl != nil && cl.IsLeader() {
			return uint64(i + 1)
		}
	}
	t.Fatal("no leader")
	return 0
}

func put(ctx context.Context, cl *Client, req uint64) error {
	_, err := cl.ExecuteOnce(ctx, "chaos-test", req, Command{Op: OpPut, Key: "k", Value: []byte("v")})
	return err
}

func TestClientLearnsLeader(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	want := c.leaderID(t)

	cl := NewClient(c.kvAddrs)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := put(ctx, cl, 1); err != nil {
		t.Fatal(err)
	}
	if got := cl.leader.Load(); got != want {
		t.Fatalf("client remembered leader %d, cluster leader is %d", got, want)
	}
}

func TestClientSkipsDeadPeersOnceLeaderKnown(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	leader := c.leaderID(t)

	// Two dead peers sorted ahead of the leader. Before the fix every request
	// walked into the first one and burned its whole deadline.
	addrs := map[uint64]string{1: blackhole(t), 2: blackhole(t), 3: c.kvAddrs[leader]}
	cl := NewClient(addrs)
	defer cl.Close()

	// Premise: with no leader memory the walk hangs on peer 1 and the call
	// fails. If this ever passes, the blackhole no longer models a kill.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := put(ctx, cl, 1)
	cancel()
	if err == nil {
		t.Fatal("premise broken: a walk through blackholed peers should time out")
	}
	// The failed attempt demoted peer 1 but not 2; clear that so the next
	// part tests leader memory alone.
	cl.mu.Lock()
	cl.badUntil = make(map[uint64]time.Time)
	cl.mu.Unlock()

	cl.leader.Store(3)
	start := time.Now()
	for i := uint64(2); i < 22; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := put(ctx, cl, i)
		cancel()
		if err != nil {
			t.Fatalf("request %d with leader known: %v", i, err)
		}
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("20 requests took %v; the client is still walking dead peers", el)
	}
}

func TestClientDemotesPeerThatTimedOut(t *testing.T) {
	c := newTestKVCluster(t, 3)
	defer c.stop()
	leader := c.leaderID(t)

	addrs := map[uint64]string{1: blackhole(t), 2: c.kvAddrs[leader]}
	cl := NewClient(addrs)
	defer cl.Close()

	// First request: leader unknown, peer 1 is first and dead. It fails.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := put(ctx, cl, 1)
	cancel()
	if err == nil {
		t.Fatal("premise broken: first walk should time out on the blackhole")
	}
	if cl.leader.Load() != 0 {
		t.Fatalf("no leader should be known yet, got %d", cl.leader.Load())
	}
	if got := cl.tryOrder(0); len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("dead peer should be walked last, got order %v", got)
	}

	// Second request: the corpse is demoted, so the walk reaches the leader
	// inside the same short deadline and remembers it.
	ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
	err = put(ctx, cl, 2)
	cancel()
	if err != nil {
		t.Fatalf("second request should skip the demoted peer: %v", err)
	}
	if cl.leader.Load() != 2 {
		t.Fatalf("leader should now be remembered as 2, got %d", cl.leader.Load())
	}

	// The penalty expires: peer 1 is back in its normal slot, but the leader
	// still goes first, so a healed follower is never in the request path.
	cl.mu.Lock()
	cl.badUntil[1] = time.Now().Add(-time.Second)
	cl.mu.Unlock()
	if got := cl.tryOrder(cl.leader.Load()); got[0] != 2 || got[1] != 1 {
		t.Fatalf("expired penalty should restore order behind the leader, got %v", got)
	}
}
