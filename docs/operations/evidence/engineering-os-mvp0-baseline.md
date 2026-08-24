# Engineering OS MVP 0 Live Baseline

Observed: 2026-07-27 21:56–22:00 America/New_York
Host: `gameflow-agent` (`gameflow-agent.tailc24acb.ts.net`, `100.126.253.6`)
Method: read-only non-login SSH from the Windows GameFlowData workstation.

## Summary

| Collector | Live evidence source | Baseline | Status |
|---|---|---|---|
| Gateway | `systemctl --user show/status hermes-gateway.service` | active/running since 2026-07-21 08:27 EDT; PID available | resolved |
| Kanban | read-only SQLite URI plus `hermes kanban --board gameflow stats` | DB integrity `ok`; 6 done, 1 blocked, 0 running | resolved |
| Dispatcher | gateway active state plus Kanban task/claim/heartbeat fields | dispatcher is gateway-embedded; `hermes kanban daemon --help` marks standalone daemon deprecated | resolved |
| Kanban backup | newest file metadata under `~/.hermes/backups/kanban/gameflow` | newest backup 2026-07-27 00:02 EDT; daily timer active | resolved |
| GBrain | `hermes mcp test gbrain` plus `systemctl --user` service state | MCP connected in 131 ms with tools discovered; `gbrain-gameflow.service` active | resolved |
| Git checkout | `git -C ~/projects/GameFlowData status/branch/rev-list` | `main`, clean, 0 ahead / 0 behind | resolved |
| Scheduler | `systemctl --user list-timers --all` | Kanban backup, GBrain nightly audit, state backup, cache cleanup, and weekly code refresh timers present | resolved |
| Disk | `df` and bounded `du` | root filesystem 77 GiB total, 17 GiB used (22%); `~/.hermes` 2.6 GiB; remote repo 333 MiB | resolved |
| Artifacts | bounded `du` of approved directories | model artifacts 150 MiB; MLB artifacts 48 MiB; backtest results 8 KiB; `graphify-out` absent | resolved |
| Tailscale | `tailscale status --self`, `tailscale status --json`, `tailscale serve status` | MagicDNS active; Serve supported but no route configured | resolved |

## Non-login command behavior

- `$HOME/.local/bin/hermes` exists and is executable but is not on the non-login SSH `PATH`.
- `$HOME/.hermes/hermes-agent/venv/bin/hermes` exists and is executable.
- No standalone `gbrain` executable was found in the checked conventional paths. GBrain health must therefore use the active systemd service plus the authenticated Hermes MCP transport check rather than assuming `gbrain` is on `PATH`.
- No `sqlite3` executable is installed. Kanban inspection must use Python's standard-library `sqlite3` module in read-only URI mode or the supported Hermes Kanban CLI.

## Kanban schema signals

The task table exposes `status`, `started_at`, `completed_at`, `claim_expires`, `worker_pid`, `last_heartbeat_at`, `current_run_id`, `consecutive_failures`, and `last_failure_error`. These are sufficient for running, blocked, stale-claim, and heartbeat checks without mutation.

Current task state:

- done: 6
- blocked: 1
- running: 0
- blocked pilot: `pilot: verify GameFlow read-only worker context`

## Scheduler observations

- `gameflow-kanban-backup.timer`: active; last run about 21 hours before observation.
- `gbrain-nightly-sync-audit.timer`: active.
- `gameflow-agent-state-backup.timer`: active.
- `gbrain-weekly-code-source-refresh.timer`: timer active, but its most recent service unit is failed. MVP 0 must surface this as a warning/failure rather than treating timer presence as health.

## Tailscale exposure decision support

- Stable private hostname: `gameflow-agent.tailc24acb.ts.net`.
- `tailscale serve status` returned `No serve config`.
- MVP 0 should bind Uvicorn to `127.0.0.1:8765` and use Tailscale Serve HTTPS for private exposure.
- No public listener is required or approved.

## Threshold baseline

- Disk use: 22%; warning and critical thresholds remain unset pending owner approval.
- Aggregate bounded artifact footprint: about 198 MiB plus 8 KiB of backtest results; growth threshold remains unset pending owner approval.
- Kanban backup maximum age from the plan remains 36 hours.
- Stuck task threshold from the plan remains 30 minutes.

## Known limitations

- The dashboard cannot report a total droplet or Tailscale outage while both the host and dashboard are unreachable.
- GBrain MCP transport/service health is runtime evidence; canonical page content and old handoffs are not runtime-health proof.
- The daily brief generation time is not approved, so the daily timer must not be enabled yet.
- Seven-day certification and host-reboot certification cannot be completed in the initial implementation session.

## Commands used

All commands were read-only and secret-bearing authorization values were redacted by Hermes:

- `systemctl --user status/show/list-timers`
- `tailscale status --self`, `tailscale status --json`, `tailscale serve status`
- `$HOME/.local/bin/hermes mcp test gbrain`
- `$HOME/.local/bin/hermes kanban --board gameflow stats`
- Python `sqlite3` read-only URI queries and `PRAGMA integrity_check`
- `git status`, `git branch --show-current`, `git rev-list --left-right --count`
- `df -h` and bounded `du -sh`
