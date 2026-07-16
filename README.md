# kill-my-cluster

Custom Raft KV with a public proof: **seven machines**, kill one, watch election /
catch-up and a **10s** auto-heal. Fullscreen Threlte 3D mesh + collapsible Grafana
(leader, term, commit index, applies/s).

## Status

- **Phase 1 — Storage engine (WAL + snapshots).** ← *done*
- **Phase 2 — Raft consensus.** ← *done*
- **Phase 3 — KV store + exactly-once.** ← *done*
- **Phase 4 — Observability (Prometheus + Grafana).** ← *done*
- **Docker cluster** — 7 machines + control plane + web. ← *done*
- **Demo web UI** — fullscreen Threlte 3D (+ SVG `?view=svg`). ← *done*
- Later — host on Oracle Always Free (`demo.anush.wiki`).

## Layout

```
internal/storage/      WAL + durable storage
internal/raft/         Raft consensus
internal/kv/           replicated KV + remote Client
internal/metrics/      Prometheus collectors (Raft / apply)
internal/controlplane/ kill / partition / restart + SSE
cmd/node/              Docker node entrypoint (+ optional KV heartbeat agent)
cmd/controlplane/      whitelist kill switch
cmd/*demo/             phase demos (storage / raft / kv / metrics)
deploy/observability/  Prometheus + Grafana only (host scrape)
deploy/compose/        Full stack: 7 nodes + Prom/Grafana/CP/web
web/                   SvelteKit demo UI (Threlte 3D)
```

## Requirements

- Go 1.22+ (developed on 1.26).
- **Phase 2+:** `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for regenerating protos).
- **Docker:** Docker + Docker Compose.
- **Web (local hot reload):** Node 20+.

## Running

```bash
go test ./...
```

```bash
go run ./cmd/storagedemo
go run ./cmd/raftdemo
go run ./cmd/kvdemo
```

### Host metricsdemo + observability compose

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

| Service        | URL                     | Notes                                      |
|----------------|-------------------------|--------------------------------------------|
| Demo UI        | http://localhost:5173   | Kill Machine N · 3D mesh · Grafana drawer  |
| Control plane  | http://localhost:8080   | Kill / partition / restart · heal ~10s · SSE |
| Grafana        | http://localhost:3000   | `admin` / `admin` — 4 Raft panels          |
| Prometheus     | http://localhost:9090   | Scrapes machine metrics                    |

**7-machine** Raft group (quorum 4). Node1 runs a light KV Put heartbeat
(`demo/heartbeat` @ 1.5s) so commit indexes / applies/s keep moving.

Local frontend hot reload (compose CP must be up):

```bash
cd web && npm install && npm run dev
# proxies /api → http://127.0.0.1:8080
# force SVG: http://localhost:5173/?view=svg
```

```bash
curl -X POST http://localhost:8080/api/nodes/1/kill      # heals ~10s
curl -X POST http://localhost:8080/api/nodes/2/partition  # rejoins ~10s
curl -X POST http://localhost:8080/api/reset
docker compose down
```
