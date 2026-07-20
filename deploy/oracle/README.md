# Oracle host notes

VM is already provisioned (Ampere A1 recommended). This repo does not create the instance.

## Bring up the cluster

```bash
git clone https://github.com/AnushSonone/kill-my-cluster.git
cd kill-my-cluster/deploy/compose
docker compose -f docker-compose.yml -f docker-compose.oracle.yml up -d --build
```

## Firewall (OCI security list / NSG)

| Port | Service | Public? |
|------|---------|---------|
| 22 | SSH | your IP |
| 8080 | control plane | yes (or 443 via reverse proxy) |
| 3001 | Grafana | optional |
| 9090 | Prometheus | no |
| 7000/8000/9100 | Raft/KV/metrics | no |

## Wire anush.wiki

Set Vercel env `RAFT_CP_URL` to `http://<public-ip>:8080` (or `https://…` if TLS terminates on the VM).

Wiki rewrites `/api/raft/*` → `$RAFT_CP_URL/api/*`.

See root `migration_anush_wiki.md` for product defaults.
