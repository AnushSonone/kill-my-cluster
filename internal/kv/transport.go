package kv

// transport.go serves the client-facing KV gRPC API.

import (
	"fmt"
	"net"

	"github.com/AnushSonone/kill-my-cluster/internal/kvpb"
	"google.golang.org/grpc"
)

// KVServer hosts the client KV gRPC service.
type KVServer struct {
	grpc   *grpc.Server
	lis    net.Listener
	server *GRPCServer
}

// serveKVOnListener registers the KV service on lis (tests use this with a
// pre-bound listener for stable addresses).
func serveKVOnListener(srv *GRPCServer, lis net.Listener) (*KVServer, error) {
	gs := grpc.NewServer()
	kvpb.RegisterKVServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	return &KVServer{grpc: gs, lis: lis, server: srv}, nil
}

// NewKVServer listens on addr and serves the KV API.
func NewKVServer(cluster *Cluster, addr string) (*KVServer, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("kv: listen %q: %w", addr, err)
	}
	return serveKVOnListener(NewGRPCServer(cluster), lis)
}

// Addr returns the bound address.
func (s *KVServer) Addr() string { return s.lis.Addr().String() }

// Stop shuts down the KV gRPC server.
func (s *KVServer) Stop() {
	s.grpc.Stop()
	if s.lis != nil {
		_ = s.lis.Close()
	}
}
