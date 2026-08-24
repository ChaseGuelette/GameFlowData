# Engineering OS MVP 0 Certification Log

Status: initial implementation operational; seven-day and host-reboot gates remain open.

Started: 2026-07-27 America/New_York
Host: `gameflow-agent`
Private URL currently validated: `http://100.126.253.6:8765/` and `http://gameflow-agent:8765/` on the tailnet.
Preferred future URL: Tailscale Serve HTTPS after the tailnet owner enables Serve.

## Implemented and verified

- Isolated `ops/engineering_os` Python package installed in `/home/chase/.local/share/gameflow-engineering-os/venv`.
- Configuration installed with mode 0600 under `/home/chase/.config/gameflow-engineering-os/engineering_os.yaml`.
- Monitor-owned state stored under `/home/chase/.local/state/gameflow-engineering-os`.
- Read-only collectors cover gateway, Kanban, Kanban backup, GBrain MCP transport/service, Git checkout, systemd timers/services, disk, and bounded artifact directories.
- `gameflow-engineering-os-web.service` is enabled and active.
- `gameflow-engineering-os-collect.timer` is enabled and active at five-minute intervals.
- `gameflow-engineering-os-brief.timer` is installed but disabled pending Chase's selected daily generation time.
- Local loopback `/healthz` and dashboard root returned HTTP 200 before tailnet binding.
- Tailnet client checks from the Windows workstation returned HTTP 200 for `/healthz` and dashboard root.
- Browser navigation from the workstation rendered the live brief, eight health cards, and event history without obvious layout/rendering errors.
- Linux package tests: 32 passed.
- Windows package tests: 32 passed.
- Safe synthetic backup failure generated one `kanban.backup` failure event; restoration generated one recovery event.
- Web-service restart retained all four existing events and restored `/healthz` and `/` successfully.

## Live initial health

- Healthy: gateway, Kanban database, Kanban backup, GBrain MCP, disk, bounded artifacts.
- Warning: remote Git checkout has the newly deployed untracked MVP 0 paths until reviewed/versioned.
- Warning: `gbrain-weekly-code-source-refresh.service` has a failed latest result while its timer remains active.
- Disk use at initial baseline: 22%.
- Bounded artifact footprint at initial baseline: approximately 201,760 KiB.

## Post-review hardening

The first adversarial review correctly found that the backup collector treated SQLite `-wal`/`-shm` sidecars as candidate backups. That could make the backup appear newer than the actual `.db`, and the original read-only connection was capable of creating sidecars. The deployed collector now:

- considers only `.db`, `.sqlite`, and `.sqlite3` files;
- opens the selected backup with SQLite `mode=ro&immutable=1`;
- reports the actual newest database backup age (22.5 hours at correction time); and
- left all backup-sidecar names and mtimes unchanged during the verified post-fix collection.

The same review cycle also hardened non-zero command handling, GBrain positive-tool discovery, timer-service probe failures, JSON/query/basic-auth secret redaction, bounded active Kanban task identities, one-time web state-store initialization, operator-local timestamps and freshness, pinned package dependencies, enforced brief retention, and the disabled daily-generation owner gate. Kanban now uses an immutable snapshot only when no non-empty WAL is present; otherwise it reports `unknown` instead of showing stale data. A live collection left the production Kanban directory with no `-wal` or `-shm` sidecars before or after the read. The corrected suite passed 41 tests on both Windows and Linux.

## Tailscale exposure result

`tailscale serve` reported that Serve is not enabled for the tailnet and provided an owner activation URL. Rather than expose a public listener or block MVP 0, the deployed config binds only to the host's Tailscale IPv4 address (`100.126.253.6`) on port 8765. This is private to the tailnet. No Funnel or public bind was configured.

After Serve is enabled, switch `web.bind_host` back to `127.0.0.1`, set the approved HTTPS `public_base_url`, restart the web service, and configure `tailscale serve --bg --https=443 http://127.0.0.1:8765`.

## Open certification gates

- [ ] Chase chooses the daily brief generation time in America/New_York.
- [ ] Daily brief timer is updated/enabled only after that choice.
- [ ] Seven consecutive expected daily briefs, including weekends if approved, remain visible.
- [x] Safe simulated failure appeared prominently.
- [x] Recovery was recorded exactly once.
- [x] Existing blocked Kanban task is represented.
- [ ] Approved backup-staleness and disk-threshold fixtures are exercised; real disk thresholds remain unset.
- [x] Brief uses live collectors rather than handoff state for runtime health.
- [x] Web-service restart retained state/history.
- [ ] Approved host reboot verifies web/timer/history recovery.
- [ ] Chase confirms readability and usefulness from both desktop and phone.
- [ ] Tailscale Serve HTTPS is enabled, or Chase explicitly accepts tailnet HTTP as the ongoing MVP 0 route.

MVP 1 must not start until the remaining owner and seven-day certification gates are complete.
