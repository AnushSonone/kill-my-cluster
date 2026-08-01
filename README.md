# kill-my-cluster audit log

Append-only incident record for the live demo, archived off the Oracle
VM so it survives the box being lost.

Written by `internal/controlplane/audit.go` on `main`, pulled every 6h
by `.github/workflows/audit-log.yml` from the public `/api/audit`
endpoint. One JSON object per line, oldest first.

Kinds: `controlplane_started`, `quorum_lost`, `quorum_regained`,
`leader_changed`, `progress_stalled`, `progress_resumed`,
`watchdog_stepdown`.

`progress_stalled` means the commit index froze for 60s while quorum
held and a leader was in office. That is the 2026-07-28 signature, and
because the raft commit-stall watchdog acts at 30s, a line here means
the watchdog did not save it.

```bash
git show origin/audit-log:events.jsonl | jq 'select(.kind=="progress_stalled")'
```
