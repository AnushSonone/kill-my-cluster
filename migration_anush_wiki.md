# Migration: kill-my-cluster backend → anush.wiki/blog/raft

Public UI stays on **anush.wiki/blog/raft**. This repo is the Go cluster + control plane on Oracle.
No `demo.anush.wiki` product URL.

**Status:** Oracle Cloud VM is already provisioned. Hosting compose is in `deploy/compose/` (+ `docker-compose.oracle.yml` overlay). Cloudflare Tunnel is the planned secure edge between Vercel and the VM (see below).

---

## Architecture (target)

```
Visitor
  → anush.wiki/blog/raft          (Vercel / Next UI)
  → same-origin /api/raft/*       (Next rewrite)
  → https://raft-api.anush.wiki   (Cloudflare edge, TLS)
  → cloudflared tunnel on Oracle  (no public :8080)
  → control plane localhost:8080
  → Raft nodes 1–7 + loadgen + Prometheus + Grafana (private)
```

Kill rate limits stay in the control plane (~1.5 kills/s global, ~2s per-IP cooldown, 10s heal). Cloudflare adds TLS, hides the VM IP, and optional WAF / bot rules. It does not replace CP kill limits.

---

## Information needed before implementing Cloudflare

Fill these in (or paste into chat). Implementation is blocked until the marked items are known.

### Account / DNS

| # | Question | Answer |
|---|----------|--------|
| 1 | Cloudflare account email (or confirm Zero Trust is under the same account that will own `anush.wiki`) | |
| 2 | Is `anush.wiki` already on Cloudflare DNS (nameservers pointed at CF)? If not, who owns DNS today (Vercel / registrar / other)? | |
| 3 | Preferred public hostname for the control plane (default: `raft-api.anush.wiki`) | |
| 4 | Optional Grafana hostname (default: none for v1; or `raft-grafana.anush.wiki` if you want public embeds) | |

### Oracle VM

| # | Question | Answer |
|---|----------|--------|
| 5 | SSH user + host (or bastion path), e.g. `opc@x.x.x.x` | |
| 6 | Confirm OS (Oracle Linux / Ubuntu) and arch (`aarch64` Ampere) | |
| 7 | Can the agent (or you) install `cloudflared` and run it as a systemd service? | |
| 8 | Current OCI security list: is `:8080` already open to `0.0.0.0/0`? (We will close it after the tunnel works.) | |

### Cloudflare Tunnel / Zero Trust

| # | Question | Answer |
|---|----------|--------|
| 9 | Prefer **Cloudflare Tunnel** (recommended) vs orange-cloud proxy to a public VM IP? | Tunnel (default) |
| 10 | Tunnel name (default: `kill-my-cluster`) | |
| 11 | Auth to create the tunnel: Cloudflare API token with Zone:DNS Edit + Account:Cloudflare Tunnel Edit, **or** you create the tunnel in the dashboard and paste the tunnel token | |
| 12 | Any Zero Trust Access policy on `raft-api`? (default: **public**, no Access login; wiki is public) | Public |

### Vercel / wiki

| # | Question | Answer |
|---|----------|--------|
| 13 | Vercel project for anush.wiki (confirm) | |
| 14 | Who will set `RAFT_CP_URL=https://raft-api.anush.wiki` in Vercel production? | |
| 15 | Keep local override `RAFT_CP_URL=http://127.0.0.1:8080` for `npm run dev`? (default: yes) | |

### Optional hardening

| # | Question | Answer |
|---|----------|--------|
| 16 | Cloudflare WAF rate limit on `POST /api/nodes/*/kill` at the edge? (default: light complement only; CP remains source of truth) | |
| 17 | Restrict tunnel ingress so only `/api/*` and `/healthz` are exposed (default: yes) | |
| 18 | Expose Grafana through Cloudflare? (default: no for v1) | |

---

## Cloudflare plan (implementation)

### Goal

Encrypt and stabilize the path from the wiki (Vercel) to the Oracle control plane without opening Raft/KV/Prom to the internet, and without a `demo.anush.wiki` product URL.

### Locked choices

| Topic | Choice |
|-------|--------|
| Edge | Cloudflare Tunnel (`cloudflared` on the VM) |
| Public hostname | `raft-api.anush.wiki` (change if Q3 says otherwise) |
| Product URL | still `anush.wiki/blog/raft` only |
| Wiki → CP | Vercel `RAFT_CP_URL=https://raft-api.anush.wiki` |
| Kill limits | control plane (unchanged) |
| OCI after cutover | deny public `:8080` / `:3001` / node ports; SSH only (or SSH via separate path) |
| Grafana | private / optional second tunnel hostname later |

### Why Tunnel (not just orange-cloud)

- No need to publish OCI `:8080` to the world.
- TLS at Cloudflare; VM speaks HTTP to `cloudflared` on localhost.
- IP of the Ampere box stays off public DNS.
- Works with Vercel server-side rewrites (origin is an HTTPS hostname Cloudflare owns).

### Phase 0 — Prerequisites

1. Collect answers in the tables above.
2. Confirm `anush.wiki` zone is in Cloudflare (or migrate DNS carefully).
3. On Oracle: cluster already runnable via:

   ```bash
   cd deploy/compose
   docker compose -f docker-compose.yml -f docker-compose.oracle.yml up -d --build
   ```

4. Local check on VM: `curl -sS http://127.0.0.1:8080/healthz` → `ok`.

### Phase 1 — Create Tunnel

1. In Cloudflare Zero Trust → Networks → Tunnels → Create → **Cloudflared**.
2. Name: `kill-my-cluster`.
3. Copy the install token (or use API).
4. On the VM, install `cloudflared` for `linux/arm64` (Ampere).
5. Run as systemd service with the token (or `cloudflared service install <token>`).
6. Confirm tunnel shows **Healthy** in the dashboard.

### Phase 2 — Public hostname → localhost CP

Configure published application routes on the tunnel:

| Public hostname | Service (VM) | Path |
|-----------------|--------------|------|
| `raft-api.anush.wiki` | `http://127.0.0.1:8080` | `*` (or restrict to `/api/*` + `/healthz`) |

DNS: Cloudflare creates a CNAME `raft-api` → `<tunnel-id>.cfargotunnel.com` (proxied).

Verify from a laptop (not the VM):

```bash
curl -sS https://raft-api.anush.wiki/healthz
curl -sS https://raft-api.anush.wiki/api/nodes | head
```

SSE check (short):

```bash
curl -sS -N -m 3 https://raft-api.anush.wiki/api/stream | head
```

### Phase 3 — Harden OCI

After Phase 2 works:

1. OCI security list / NSG: **remove** ingress for `8080`, `3001`, `9090`, `7000`, `8000`, `9100` from `0.0.0.0/0`.
2. Keep SSH (22) restricted to your IP (or bastion).
3. Confirm kill/metrics still work via `https://raft-api.anush.wiki` only.

### Phase 4 — Wire anush.wiki

1. Vercel production env: `RAFT_CP_URL=https://raft-api.anush.wiki`
2. Redeploy anush-wiki (Next rewrite `/api/raft/:path*` → `$RAFT_CP_URL/api/:path*`).
3. Local `.env.local` stays `http://127.0.0.1:8080` for compose.
4. Smoke test production:

   - open `https://anush.wiki/blog/raft`
   - HUD goes live (users / uptime / QPS)
   - kill a non-leader, then kill leader; heal ~10s
   - confirm 429 when spamming kill (CP rate limit)

5. CP `CORS_ORIGINS` can stay allowlisted for direct browser hits; primary path is same-origin via Vercel so CORS is secondary.

### Phase 5 — Optional Cloudflare edge rules

Only after Phase 4 is stable:

1. WAF custom rule or Rate limiting rule: e.g. cap `POST` to `raft-api.anush.wiki/api/nodes/*/kill` per IP (loose, above CP limits so CP still owns UX errors).
2. Bot Fight / Super Bot Fight: enable carefully; false positives on SSE are painful. Prefer off initially.
3. Do **not** put Cloudflare Access (login wall) in front of `raft-api` for v1; the demo is public.

### Phase 6 — Docs / memory

1. Update this file with the real hostname, tunnel name, and “ports closed” confirmation.
2. Update brain `projects/kill-my-cluster/STATUS.md` and `projects/anush-wiki/STATUS.md` with `RAFT_CP_URL` and tunnel status.
3. Update `deploy/oracle/README.md` with cloudflared systemd notes (token stays out of git; use env or root-only file).

### Repo artifacts to add when implementing (not secrets)

- `deploy/oracle/cloudflared/` README: install steps, example ingress YAML (no tokens).
- Optional compose profile or systemd unit template for `cloudflared`.
- `.env.example` on wiki: document production `https://raft-api.anush.wiki`.

**Never commit:** tunnel tokens, Cloudflare API keys, OCI private keys.

---

## Deploy on Oracle (cluster, before or with Tunnel)

```bash
cd deploy/compose
docker compose -f docker-compose.yml -f docker-compose.oracle.yml up -d --build
```

- Control plane (on VM loopback): `http://127.0.0.1:8080/healthz`
- Grafana (local to VM): `:3001` (admin/admin); not required on Cloudflare for v1
- Prometheus: unpublished on oracle overlay

After Tunnel: public CP is only `https://raft-api.anush.wiki`.

---

## Design defaults (product v1)

| Topic | Default |
|-------|---------|
| Public actions | Kill only (reset disabled) |
| Heal | 10s |
| Presence | Open SSE connections |
| HUD | users, uptime, writes/s, reads/s, machines + kill |
| QPS targets | ~1.5K writes/s, ~10K reads/s via loadgen |
| QPS reality | Gets still go through the Raft log; Mac compose measured ~300 writes/s and ~600 reads/s |
| Grafana | Port 3001 on VM; secondary; no CF hostname unless requested |
| Wiki proxy | `/api/raft/:path*` → `$RAFT_CP_URL/api/:path*` |
| Edge | Cloudflare Tunnel → `raft-api.anush.wiki` |
| Kill rate limit | Control plane (source of truth); CF optional complement |

---

## Out of scope

- Moving Raft into Next.js
- `demo.anush.wiki` branding
- Cloudflare Access login for visitors
- Exposing Prometheus publicly
