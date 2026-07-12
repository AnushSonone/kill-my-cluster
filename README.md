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
- **Phase 5 — Observability (Prometheus + Grafana).** ← *done*
- **Docker cluster** — `cmd/node` + Compose (7 nodes). ← *done*
- **Demo web UI** — SvelteKit SVG cluster graph. ← *local*
- Later — game layer, public demo on Oracle, sharding, chaos testing.

## Layout

```
internal/storage/      Phase 1: WAL + durable KV
internal/raft/         Phase 2: Raft consensus
internal/kv/           Phase 3: replicated KV + remote Client
internal/bank/         Phase 4: bank tenant + naive twin
internal/metrics/      Phase 5: Prometheus collectors
cmd/node/              Production node (Docker entrypoint)
cmd/storagedemo/       Phase 1 demo
cmd/raftdemo/          Phase 2 demo
cmd/kvdemo/            Phase 3 demo
cmd/bankdemo/          Phase 4 demo
cmd/metricsdemo/       Phase 5 host-process demo
deploy/observability/  Prometheus + Grafana only (host scrape)
deploy/compose/        Full stack: 3 nodes + Prometheus + Grafana
```

## Requirements

- Go 1.22+ (developed on 1.26).
- **Phase 2+:** `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for regenerating protos).
- **Docker / Phase 5:** Docker + Docker Compose.

## Running

```bash
go test ./...
```

```bash
go run ./cmd/storagedemo   # Phase 1
go run ./cmd/raftdemo      # Phase 2
go run ./cmd/kvdemo        # Phase 3
go run ./cmd/bankdemo      # Phase 4
```

### Phase 5 — host metricsdemo + observability compose

```bash
cd deploy/observability && docker compose up -d
go run ./cmd/metricsdemo
# Grafana http://localhost:3000  (admin/admin)
```

### Docker cluster (preferred)

```bash
cd deploy/compose
docker compose up -d --build
```

- **Control plane:** http://localhost:8080 — kill / partition / restart, **auto-heal ~2s**, Reset all, SSE live state  
- **Cluster graph UI:** http://localhost:5173 — SvelteKit SVG mesh (select a node → kill/partition)  
- Grafana: http://localhost:3000 (`admin` / `admin`)  
- Prometheus: http://localhost:9090  
- **7-node** Raft group (quorum 4)

Local frontend dev (hot reload):

```bash
cd web && npm install && npm run dev
# proxies /api → http://127.0.0.1:8080
```

```bash
curl -X POST http://localhost:8080/api/nodes/1/kill      # comes back ~2s
curl -X POST http://localhost:8080/api/nodes/2/partition  # rejoins network ~2s
curl -X POST http://localhost:8080/api/reset
docker compose down
```
