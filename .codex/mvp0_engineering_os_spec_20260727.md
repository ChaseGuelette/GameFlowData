# Implementation Spec: GameFlow Engineering OS MVP 0

## Goal

Implement only MVP 0 from `docs/operations/gameflow-engineering-os-technical-implementation.md`: a separately packaged, read-only control-plane monitor with deterministic collectors, persisted briefs/events, server-rendered private dashboard, CLI, tests, and systemd/Tailscale deployment assets.

## Allowed scope

Create files only under:

- `ops/engineering_os/`
- `docs/operations/evidence/engineering-os-mvp0-certification.md` if useful

Do not modify existing product/backend/dashboard code, existing GameFlow documentation, configs, models, DB code, or unrelated dirty files.

The live baseline already exists at `docs/operations/evidence/engineering-os-mvp0-baseline.md`; read it for exact remote paths and evidence sources but do not rewrite it.

## Required architecture

- Python >=3.11 isolated package named `gameflow-engineering-os` / import package `gameflow_engineering_os`.
- Dependencies limited to FastAPI, Uvicorn, Jinja2, PyYAML, Pydantic, and test dependencies such as pytest/httpx.
- No imports from GameFlow production `src/`.
- SQLite only for this monitor's own state, health snapshots, brief history, and events.
- Every collector is read-only, bounded by timeout, catches exceptions, redacts secrets, and returns a common typed `HealthCheckResult`.
- Missing/unparseable evidence becomes `unknown`, `failed`, or `not_configured`, never healthy.
- Timestamps must be timezone-aware UTC ISO values; operator rendering uses the configured `America/New_York` timezone.

## Configuration

Create `ops/engineering_os/config/engineering_os.example.yaml` with:

- timezone
- daily brief enabled/schedule/retention
- event enable/recovery/repeat window
- web loopback host, port 8765, public URL null, stale threshold
- remote paths from the plan/baseline
- command paths (`/home/chase/.local/bin/hermes`, `/usr/bin/systemctl`, `/usr/bin/tailscale`, `/usr/bin/git`, `/usr/bin/df`, `/usr/bin/du` or safely configurable equivalents)
- collector timeout
- backup/stuck thresholds
- null disk and artifact-growth thresholds until approved
- expected systemd units/timers
- bounded artifact directories

Config loading must reject unknown statuses/bad timezone/bad schedule, permit null thresholds, and create only the monitor's state/log directories when explicitly requested by CLI/runtime—not at import time.

## Data contracts

Implement typed/Pydantic models for:

- statuses: healthy, warning, failed, unknown, not_configured
- `HealthCheckResult`
- deterministic `DailyBrief`
- persisted `HealthEvent`

Required result fields follow the technical plan: check id, status, summary, observed time, source, freshness, metrics, bounded evidence, recommended action.

## State and events

- SQLite initialization must be idempotent and transactional.
- Persist latest check snapshots and timestamped collection runs.
- Persist one dated daily brief as JSON/text; regenerating the same date should update/replace deterministically rather than create duplicates.
- Persist health events with transition type.
- healthy -> warning/failed creates one failure event.
- unchanged warning/failed is deduplicated inside repeat window.
- warning/failed -> healthy creates exactly one recovery event.
- unknown is never coerced to healthy.
- Concurrent invocation must not corrupt state; use WAL/busy timeout/transactions.
- Retention pruning applies only to monitor-owned brief/snapshot/event rows/files.

## Collectors

Implement focused modules under `collectors/`:

1. Gateway: `systemctl --user show hermes-gateway.service` properties. Active/running healthy; inactive/failed failed; errors unknown.
2. Kanban: Python sqlite3 read-only URI on configured DB; `PRAGMA integrity_check`; counts by status; detect stale running claims/heartbeats using configured threshold; never mutate DB.
3. Kanban backup: newest configured backup file age and optional read-only SQLite integrity check on the newest backup. Respect 36-hour warning threshold.
4. GBrain: check `gbrain-gameflow.service` active and run bounded `/home/chase/.local/bin/hermes mcp test gbrain`; redact Authorization/token material; transport/tool discovery determines status. Do not call or write GBrain pages.
5. Git: configured repo branch, porcelain status, upstream ahead/behind; do not fetch, checkout, or mutate. Dirty is warning; divergence is warning.
6. Scheduler: inspect configured expected systemd user timers and their activated services. Surface the baseline case where a timer is active but its most recent service unit is failed.
7. Disk: parse `df -P` for configured path. Null thresholds mean metrics are reported with `not_configured` only if no health determination is possible; 22% usage itself should remain healthy while explicitly stating thresholds are unset. Never recurse broadly.
8. Artifacts: bounded `du -sk` only on configured explicit directories; absent optional paths are recorded, not fatal. Compare total to the previous persisted measurement when available; null growth threshold means report measurement without inventing warning threshold.

Use a shared bounded subprocess runner with timeout, no shell interpolation for dynamic values, output truncation, and redaction for bearer tokens, Authorization headers, URL credentials, common secret assignments, and long token-like strings.

## Runner and brief

- Collection runner executes all collectors independently so one failure cannot prevent other results.
- Persist all results and transition events.
- Deterministic brief summarizes decisions (only blockers/owner gates), health, active Kanban work, risks/changes, and up to three suggested actions.
- Overall health precedence: failed > warning/unknown > healthy; not_configured should remain visible.
- Include source and observed/freshness data.
- Brief text follows the roadmap's compact section order.

## CLI

Console command `gfos` with:

- `gfos check --json`: collect, persist, and print JSON; non-zero only for runner/persistence failure, not merely a degraded health result.
- `gfos collect`: collect and persist.
- `gfos brief --stdout`: render latest persisted state without mutation; fail if none.
- `gfos brief --generate`: collect, generate, persist, and print the brief.
- `gfos events`: print recent events.
- `gfos status`: compact latest state.
- `gfos serve`: run Uvicorn with config bind settings.

Every command accepts `--config PATH`, defaults to `GFOS_CONFIG` then a documented path, and does not expose secrets.

## Web dashboard

FastAPI/Jinja server-rendered UI, no Node build:

- `GET /`: latest brief, current failures/recoveries, health cards, active work, decisions, risks, timestamps/freshness, prominent stale/no-data banner.
- `GET /briefs`: retained dated history.
- `GET /health/{check_id}`: bounded evidence/current state/event history.
- `GET /healthz`: service and monitor DB readiness only, with no internal details.
- `POST /refresh`: optional manual refresh, enabled by config and protected by same-origin check plus configured CSRF token stored only server-side/config. If no secure implementation is reasonable for MVP 0, disable the route by default and document CLI refresh; do not expose an unsafe write endpoint.
- Handle healthy/warning/failed/unknown/stale/empty/DB-error states.
- Responsive, readable CSS for phone/desktop; no external CDN dependency.
- Never render unredacted command output.

## Tests and TDD

Follow strict TDD per vertical behavior. Tests must cover at least:

- config validation/null thresholds
- model status validation/serialization
- subprocess timeout/malformed output/redaction
- each collector: healthy fixture plus relevant timeout/missing/malformed/permission/stale case
- runner isolation when one collector fails
- deterministic brief healthy/failed/unknown/no-decisions/truncation/freshness
- state idempotence and concurrent-safe access
- event first failure/dedup/repeat/recovery/unknown behavior
- CLI core commands
- web healthy/warning/failed/unknown/stale/empty/DB error and no secret leakage

Tests must not inspect the real local host or remote services; inject command runners/clocks/filesystem fixtures. Record RED/GREEN evidence in the worker summary if possible.

## Deployment assets

Create:

- web service
- collect oneshot + timer (e.g. every five minutes)
- brief oneshot + daily timer with a placeholder/disabled-until-approved schedule
- environment/config example
- install script or exact README commands

Requirements:

- user services, loopback bind, restart policy
- absolute venv/gfos/config paths
- state/log directories with restrictive permissions
- daily timer must not be enabled until Chase selects the time
- Tailscale Serve HTTPS command routes the private MagicDNS hostname to `http://127.0.0.1:8765`; no public Funnel
- explicit install, verify, rollback, and uninstall instructions
- services independent from Hermes gateway and customer production

## README

Document:

- package setup with `python3 -m venv` and pip editable/install flow
- config install
- local tests and all required CLI commands
- systemd installation/manual run/enable steps
- Tailscale Serve setup/status/reset
- daily timer owner gate
- safe failure/recovery simulation approach that changes only a fixture/configured synthetic check or a dedicated monitor test flag; do not stop production services
- seven-day certification and reboot checks
- limitations and rollback

## Non-goals / invariants

- No product DB, Supabase, Railway, Vercel, model, scraper, trading, training, sweep, deployment, git integration mutation, Kanban mutation, GBrain write, artifact cleanup, Telegram, or email functionality.
- Do not modify existing systemd services/timers except installing the new `gameflow-engineering-os-*` units.
- Do not enable the daily brief timer before owner schedule approval.
- No public listener or Tailscale Funnel.
- Do not claim seven-day/reboot/phone certification completed from unit tests.
- Preserve GameFlow invariants: no recalibration changes, no Railway advanced scraping, no direct DB work.

## Validation

Run from repository root:

- `python -m pytest ops/engineering_os/tests -q`
- `python -m ruff check ops/engineering_os` when Ruff is available
- `python -m compileall -q ops/engineering_os/src`
- package install in a temporary venv and `gfos --help`
- local temp-config `gfos check --json`, `gfos brief --generate`, and FastAPI `/healthz` smoke
- `git diff --check -- ops/engineering_os docs/operations/evidence/engineering-os-mvp0-baseline.md`

## Review criteria

- Only allowed new paths are touched.
- Real read-only evidence routes match the baseline.
- No secret leakage, shell injection, broad filesystem recursion, or accidental mutation.
- Tests cover degraded and stale behavior, not only happy paths.
- Systemd/Tailscale instructions are reversible and keep the site private.
- MVP 1+ functionality is not implemented early.
