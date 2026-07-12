# kill-my-cluster

A Raft-based distributed key-value store that keeps running no matter how many
nodes you kill, with a live demo where anyone can try to take it down.

## Status

Under active development, built in phases:

- **Phase 1 — Storage engine (durability):** a crash-safe write-ahead log (WAL)
  with recovery and snapshotting. ← *done*
- **Phase 2 — Raft consensus (leader election, log replication, snapshots).** ← *done*
- **Phase 3 — KV store + exactly-once semantics.** ← *done*
- **Phase 4 — Bank tenant agent.** ← *done*
- Later — observability (Phase 5), public demo, sharding, chaos testing.

## Layout

```
internal/storage/   Phase 1: write-ahead log + durable key-value store
internal/raft/      Phase 2: Raft consensus
internal/kv/        Phase 3: replicated KV + exactly-once
internal/bank/      Phase 4: bank tenant + naive twin
cmd/storagedemo/    Phase 1 demo
cmd/raftdemo/       Phase 2 demo
cmd/kvdemo/         Phase 3 demo
cmd/bankdemo/       Phase 4 demo — $1,000 conserved vs naive drift
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

Phase 4 — bank tenant ($1,000 conserved; naive twin leaks on duplicate credits):

```bash
go run ./cmd/bankdemo
```
