package raft

// transport.go is the gRPC plumbing that carries Raft RPCs between real
// processes. It has two halves:
//
//   - Server: adapts incoming gRPC calls to the Node's Handle* methods.
//   - GRPCTransport: implements the Transport interface by dialing peers and
//     forwarding the Node's outgoing calls.
//
// Keeping this glue in one file — and keeping the Node itself free of any
// gRPC types — is what lets the algorithm run under an in-memory fake
// transport in tests. The protocol logic cannot tell the difference; only
// latency and failure modes change.

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/AnushSonone/kill-my-cluster/internal/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Server exposes a Node over gRPC. It implements raftpb.RaftServer.
type Server struct {
	raftpb.UnimplementedRaftServer
	node *Node

	grpcServer *grpc.Server
	listener   net.Listener
}

// NewServer starts serving the node's Raft RPCs on addr (e.g.
// "127.0.0.1:7001"). The server runs in a background goroutine until Stop.
func NewServer(node *Node, addr string) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("raft: listen %q: %w", addr, err)
	}
	return serveOnListener(node, lis)
}

// ServeOnListener is like NewServer but reuses an already-bound listener.
func ServeOnListener(node *Node, lis net.Listener) (*Server, error) {
	return serveOnListener(node, lis)
}

// serveOnListener starts gRPC on lis.
func serveOnListener(node *Node, lis net.Listener) (*Server, error) {
	s := &Server{
		node:       node,
		grpcServer: grpc.NewServer(),
		listener:   lis,
	}
	raftpb.RegisterRaftServer(s.grpcServer, s)
	go func() {
		_ = s.grpcServer.Serve(lis)
	}()
	return s, nil
}

// Addr returns the address the server is actually listening on (useful when
// addr was ":0" and the OS picked a free port).
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Stop halts the gRPC server immediately. We use Stop rather than
// GracefulStop deliberately: this is the "kill" a demo visitor triggers, and
// an abrupt halt is exactly the failure mode Raft is built to survive.
func (s *Server) Stop() {
	s.grpcServer.Stop()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
}

// The three RPC methods below are pure adapters: unwrap the gRPC call, hand
// it to the algorithm, return the result. All protocol thinking lives in the
// Node's handlers.

func (s *Server) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	return s.node.HandleRequestVote(req), nil
}

func (s *Server) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	return s.node.HandleAppendEntries(req), nil
}

func (s *Server) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	return s.node.HandleInstallSnapshot(req), nil
}

// GRPCTransport implements Transport by dialing each peer's gRPC server.
type GRPCTransport struct {
	mu sync.Mutex
	// addrs maps peer ID → "host:port".
	addrs map[uint64]string
	// conns caches one connection per peer. gRPC connections multiplex many
	// concurrent RPCs over one HTTP/2 session and reconnect automatically
	// after failures, so one long-lived conn per peer is the right shape —
	// re-dialing per RPC would add handshake latency to every heartbeat.
	conns map[uint64]*grpc.ClientConn
}

// NewGRPCTransport builds a transport that can reach the given peers.
func NewGRPCTransport(peerAddrs map[uint64]string) *GRPCTransport {
	return &GRPCTransport{
		addrs: peerAddrs,
		conns: make(map[uint64]*grpc.ClientConn),
	}
}

// client returns (creating if needed) the cached client for a peer.
func (t *GRPCTransport) client(peer uint64) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peer]; ok {
		return raftpb.NewRaftClient(conn), nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("raft: unknown peer %d", peer)
	}
	// insecure.NewCredentials(): nodes talk plaintext on a private/localhost
	// network. The public demo never exposes these ports (only the frontend
	// goes through the Cloudflare tunnel); mTLS between nodes can be layered
	// on later without touching the algorithm.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("raft: dial peer %d at %q: %w", peer, addr, err)
	}
	t.conns[peer] = conn
	return raftpb.NewRaftClient(conn), nil
}

// InvalidatePeer drops a cached connection so the next RPC re-dials. Call
// after a peer restarts on the same address — gRPC may otherwise keep a dead
// session open until the next heartbeat fails.
func (t *GRPCTransport) InvalidatePeer(peer uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[peer]; ok {
		_ = conn.Close()
		delete(t.conns, peer)
	}
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peer uint64, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, req)
	if err != nil {
		t.InvalidatePeer(peer)
	}
	return resp, err
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peer uint64, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.AppendEntries(ctx, req)
	if err != nil {
		t.InvalidatePeer(peer)
	}
	return resp, err
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peer uint64, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.InstallSnapshot(ctx, req)
	if err != nil {
		t.InvalidatePeer(peer)
	}
	return resp, err
}

// Close tears down all cached connections.
func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[uint64]*grpc.ClientConn)
}
