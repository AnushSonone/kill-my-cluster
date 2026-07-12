# kill-my-cluster

A Raft-based distributed key-value store that keeps running no matter how many
nodes you kill, with a live demo where anyone can try to take it down.

## Status

Under active development, built in phases:

- **Phase 1 — Storage engine (durability):** a crash-safe write-ahead log (WAL)
  with recovery and snapshotting. ← *done*
- Phase 2 — Raft consensus (leader election, log replication, snapshots). ← *next*
- Phase 3 — KV store + exactly-once semantics.
- Later — public "kill the cluster" demo, observability, sharding, chaos testing.

## Layout

```
internal/storage/   Phase 1: write-ahead log + durable key-value store
cmd/storagedemo/    tiny program to see crash-recovery in action
```

## Requirements

- Go 1.22+ (developed on 1.26).

## Running

Build and test the storage engine:

```bash
go test ./...
```

See crash recovery for yourself — run this, kill it (Ctrl-C), and run it again;
the counter resumes exactly where it left off because every write is durably
logged before it's acknowledged:

```bash
go run ./cmd/storagedemo
```
