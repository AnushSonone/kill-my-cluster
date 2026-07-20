# Migration: kill-my-cluster backend → anush.wiki/blog/raft

Public UI stays on **anush.wiki/blog/raft**. This repo is the Go cluster + control plane on Oracle.
No `demo.anush.wiki` product URL.

**Status:** Oracle Cloud VM is already provisioned. Hosting compose is in `deploy/compose/` (+ `docker-compose.oracle.yml` overlay).

## Architecture

```
Visitor → anush.wiki/blog/raft (wiki UI)
       → same-origin /api/raft/* (Next rewrite)
       → Control plane on Oracle :8080
       → Raft nodes 1–7 + loadgen + Prometheus + Grafana :3001
```

## Deploy on Oracle (after git pull)

```bash
cd deploy/compose
docker compose -f docker-compose.yml -f docker-compose.oracle.yml up -d --build
```

- Control plane: `http://<vm>:8080/healthz`
- Grafana: `http://<vm>:3001` (admin/admin)
- Prometheus: not published on the oracle overlay (CP scrapes internally)

Set Vercel `RAFT_CP_URL=http://<vm-ip>:8080` (or https if you terminate TLS).

## Design defaults (locked for v1)

| Topic | Default |
|-------|---------|
| Public actions | Kill only (reset disabled) |
| Heal | 10s |
| Presence | Open SSE connections |
| HUD | users, uptime, writes/s, reads/s, machines + kill |
| QPS targets | ~1.5K writes/s, ~10K reads/s via loadgen |
| QPS reality | Gets still go through the Raft log; Mac compose measured ~300 writes/s and ~600 reads/s. Hitting full targets needs faster propose path (ReadIndex / group-commit) or more CPU on the VM. |
| Grafana | Port 3001; secondary under wiki lab |
| Wiki proxy | `/api/raft/:path*` → `$RAFT_CP_URL/api/:path*` |
