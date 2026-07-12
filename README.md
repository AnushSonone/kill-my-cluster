# kill-my-cluster

A Raft-based distributed key-value store that keeps running no matter how many
nodes you kill, with a live demo where anyone can try to take it down.

## Status

Under active development, built in phases:

- **Phase 1 — Storage engine (durability):** a crash-safe write-ahead log (WAL)
  with recovery and snapshotting. ← *done*
- **Phase 2 — Raft consensus (leader election, log replication, snapshots).** ← *done*
- **Phase 3 — KV store + exactly-once semantics.** ← *done*
- Later — public demo, observability (Phase 5), bank tenant (Phase 4), sharding, chaos testing.

## Layout

```
internal/storage/   Phase 1: write-ahead log + durable key-value store
internal/raft/      Phase 2: Raft consensus (election, replication, persistence)
internal/raftpb/    generated gRPC types for Raft node-to-node RPCs
internal/kv/        Phase 3: replicated KV state machine + exactly-once + client API
internal/kvpb/      generated gRPC types for client KV API
proto/raft.proto    Raft wire protocol
proto/kv.proto      Client KV API (Get/Put/CAS/ExecuteOnce/Checkpoint/Watch)
cmd/storagedemo/    Phase 1 crash-recovery demo
cmd/raftdemo/       Phase 2: 3-node Raft elect → kill leader → re-elect
cmd/kvdemo/         Phase 3: linearizable KV + exactly-once retry demo
```

## Requirements

- Go 1.22+ (developed on 1.26).
- **Phase 2+:** `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for regenerating `internal/raftpb/` after editing `proto/raft.proto`).

## Running

Build and test everything:

```bash
go test ./...
```

Phase 1 — crash recovery demo (run twice; counter resumes from disk):

```bash
go run ./cmd/storagedemo
```

Phase 2 — 3-node Raft demo (elects a leader, replicates writes, kills the leader, re-elects in ~200ms):

```bash
go run ./cmd/raftdemo
```

Phase 3 — linearizable KV + exactly-once (retried request runs the mutation once):

```bash
go run ./cmd/kvdemo
```
