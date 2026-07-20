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
- Public UI: [anush.wiki/blog/raft](https://anush.wiki/blog/raft) (wiki). No `demo.anush.wiki`.
- Hosted compose: Oracle VM + loadgen; see `migration_anush_wiki.md` and `deploy/oracle/README.md`.

## Layout

```
internal/storage/      WAL + durable storage
internal/raft/         Raft consensus
internal/kv/           replicated KV + remote Client
internal/metrics/      Prometheus collectors (Raft / apply / writes / reads)
internal/controlplane/ kill / partition / restart + SSE + presence/QPS
cmd/node/              Docker node entrypoint (+ optional KV heartbeat agent)
cmd/loadgen/           sustained Put/Get traffic (~1.5K / ~10K targets)
cmd/controlplane/      whitelist kill switch
cmd/*demo/             phase demos (storage / raft / kv / metrics)
deploy/observability/  Prometheus + Grafana only (host scrape)
deploy/compose/        Full stack: 7 nodes + Prom/Grafana/CP/web/loadgen
web/                   SvelteKit local demo UI (Threlte 3D); public UI is the wiki
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
# Grafana http://localhost:3001  (admin/admin)
```

### Docker cluster (preferred)

```bash
cd deploy/compose
docker compose up -d --build
# Oracle VM overlay (no local Svelte web, Prometheus unpublished):
# docker compose -f docker-compose.yml -f docker-compose.oracle.yml up -d --build
```

| Service        | URL                     | Notes                                      |
|----------------|-------------------------|--------------------------------------------|
| Public UI      | anush.wiki/blog/raft    | Wiki HUD + kill (proxies `/api/raft`)      |
| Local web      | http://localhost:5173   | Optional Threlte UI                        |
| Control plane  | http://localhost:8080   | Kill / partition / restart · heal ~10s · SSE |
| Grafana        | http://localhost:3001   | `admin` / `admin` — leader/term/writes/reads |
| Prometheus     | http://localhost:9090   | Scrapes machine metrics                    |
| Loadgen        | (compose service)       | Targets ~1.5K writes/s and ~10K reads/s    |

**7-machine** Raft group (quorum 4). Compose `loadgen` drives Put/Get traffic.
Public reset is disabled (`ALLOW_RESET=false`).

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
