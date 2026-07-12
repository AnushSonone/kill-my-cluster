package kv

// grpc.go exposes the Cluster over gRPC for remote clients.

import (
	"context"
	"io"

	"github.com/AnushSonone/kill-my-cluster/internal/kvpb"
)

// GRPCServer implements kvpb.KVServer by delegating to a local Cluster.
type GRPCServer struct {
	kvpb.UnimplementedKVServer
	cluster *Cluster
}

// NewGRPCServer wraps c for gRPC registration.
func NewGRPCServer(c *Cluster) *GRPCServer {
	return &GRPCServer{cluster: c}
}

func (s *GRPCServer) notLeaderResp(leaderID uint64) (uint64, bool) {
	return leaderID, true
}

func (s *GRPCServer) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	res, err := s.cluster.Get(ctx, req.ClientId, req.RequestId, req.Key)
	if err == ErrNotLeader {
		return &kvpb.GetResponse{NotLeader: true, LeaderId: s.cluster.LeaderID()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.GetResponse{
		Found: res.Found, Value: res.Value, Duplicate: res.Duplicate,
	}, nil
}

func (s *GRPCServer) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	res, err := s.cluster.Put(ctx, req.ClientId, req.RequestId, req.Key, req.Value)
	if err == ErrNotLeader {
		return &kvpb.PutResponse{NotLeader: true, LeaderId: s.cluster.LeaderID()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.PutResponse{PreviousValue: res.Value, Duplicate: res.Duplicate}, nil
}

func (s *GRPCServer) CAS(ctx context.Context, req *kvpb.CASRequest) (*kvpb.CASResponse, error) {
	res, err := s.cluster.CAS(ctx, req.ClientId, req.RequestId, req.Key, req.Expect, req.Value)
	if err == ErrNotLeader {
		return &kvpb.CASResponse{NotLeader: true, LeaderId: s.cluster.LeaderID()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.CASResponse{
		Swapped: res.Found, PreviousValue: res.Value, Duplicate: res.Duplicate,
	}, nil
}

func (s *GRPCServer) ExecuteOnce(ctx context.Context, req *kvpb.ExecuteOnceRequest) (*kvpb.ExecuteOnceResponse, error) {
	cmd := Command{Op: OpPut, Key: req.Key, Value: req.Value}
	if req.UseCas {
		cmd.Op = OpCAS
		cmd.Expect = req.Expect
	}
	res, err := s.cluster.ExecuteOnce(ctx, req.ClientId, req.RequestId, cmd)
	if err == ErrNotLeader {
		return &kvpb.ExecuteOnceResponse{NotLeader: true, LeaderId: s.cluster.LeaderID()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.ExecuteOnceResponse{
		Ok: res.Found, Value: res.Value, Duplicate: res.Duplicate,
	}, nil
}

func (s *GRPCServer) Checkpoint(ctx context.Context, req *kvpb.CheckpointRequest) (*kvpb.CheckpointResponse, error) {
	res, err := s.cluster.Checkpoint(ctx, req.ClientId, req.RequestId, req.State)
	if err == ErrNotLeader {
		return &kvpb.CheckpointResponse{NotLeader: true, LeaderId: s.cluster.LeaderID()}, nil
	}
	if err != nil {
		return nil, err
	}
	return &kvpb.CheckpointResponse{Ok: res.Found, Duplicate: res.Duplicate}, nil
}

// Watch streams change events for a key until the client disconnects.
func (s *GRPCServer) Watch(req *kvpb.WatchRequest, stream kvpb.KV_WatchServer) error {
	ch := s.cluster.Watch(req.Key)
	defer s.cluster.Unwatch(req.Key, ch)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&kvpb.WatchResponse{
				Key: ev.Key, Value: ev.Value, Found: ev.Found,
			}); err != nil {
				return err
			}
		}
	}
}

// DrainWatch consumes events until ctx is done (test helper).
func DrainWatch(ctx context.Context, stream kvpb.KV_WatchClient) ([]*kvpb.WatchResponse, error) {
	var out []*kvpb.WatchResponse
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, msg)
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		default:
		}
	}
}
