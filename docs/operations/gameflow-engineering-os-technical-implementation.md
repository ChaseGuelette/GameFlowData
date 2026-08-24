# GameFlow Engineering OS Technical Implementation Plan

> **For Hermes:** Use `subagent-driven-development` to implement this plan task-by-task after Chase approves the relevant MVP. Do not implement future MVPs early. Use the GameFlow human approval gates and independently verify worker output.

**Goal:** Build a low-touch GameFlow management control plane with a private Tailscale dashboard as Chase's primary review surface, a daily executive brief, durable worker/reviewer flows, and later artifact, debt, incident, and growth governance.

**Architecture:** Start with script-first, read-only collectors plus a small server-rendered website on the existing `gameflow-agent` droplet. Expose the site only through Tailscale and reuse Kanban, GitHub, GBrain, and existing worker/reviewer profiles as authorities rather than rebuilding them. Expand the dashboard only after real managed workflows prove which additional views and actions are useful.

**Tech stack:** Python 3, FastAPI, Jinja2, HTMX or minimal browser JavaScript, Uvicorn, pytest, YAML/JSON contracts, SQLite for monitor/brief state, systemd user services/timers, Hermes Kanban, GBrain MCP/CLI, Git/GitHub, and Tailscale Serve HTTPS. A later expanded console may use dedicated Postgres, but Postgres, a Node build, Telegram, and SMTP are not MVP 0 dependencies.

**Roadmap and operator usage:** [`gameflow-engineering-os-roadmap-and-usage.md`](gameflow-engineering-os-roadmap-and-usage.md)

---

## 1. Implementation status and scope boundaries

### Implemented and live today

- MVP 0 is packaged under `ops/engineering_os` with isolated configuration, collectors, SQLite state/events, deterministic brief rendering, CLI, FastAPI/Jinja dashboard, tests, and user-systemd assets.
- The dashboard is live over the private tailnet at `http://gameflow-agent:8765/`; `/healthz` and desktop access were verified.
- `gameflow-engineering-os-web.service` and `gameflow-engineering-os-collect.timer` are enabled. The daily brief timer remains disabled pending owner approval of its schedule.
- Immediate failure/recovery behavior and service-restart persistence were verified. Seven-day, phone, and host-reboot certification remain open.
- Remote host `gameflow-agent` is reachable through Tailscale SSH.
- Hermes Kanban board exists at `/home/chase/.hermes/kanban/boards/gameflow/kanban.db`.
- Gateway-embedded Kanban dispatch, `gameflow-worker`, and `gameflow-reviewer` were validated.
- Isolated worktrees, dependent review, worker-loss recovery, and Kanban backup were validated.
- Kanban backup helper exists at `/home/chase/bin/backup-gameflow-kanban`.
- Daily Kanban backup timer exists with 14-backup retention.
- GBrain MCP is available on the remote execution plane.

### Infrastructure validated but not operationally adopted

- The board contains validation/audit tasks, not a real managed backlog.
- Existing worker/reviewer capabilities have not been used as Chase's normal development workflow.
- Technical-debt evidence exists, but it is not an approved execution queue.
- GBrain contains plans and handoffs, but those do not prove live runtime health.

### Planned

MVP 0 certification/hardening remains in progress. MVP 1 artifact/repository inventory, standardized packets, expanded manager console, incident workflow, and growth lane remain future work.

### MVP 0 review backlog

The first implementation review found several gaps. Critical false-health and read-only backup issues were corrected before deployment: non-zero command handling, backup sidecar selection/mutation, secret redaction, positive GBrain tool discovery, timer-service probe failures, immutable Kanban reads, active-task identities, local-time/freshness presentation, dependency pins, brief retention, and one-time web store initialization.

The remaining findings are deliberately deferred as bounded future work:

| Priority | Finding | Required implementation/verification |
|---|---|---|
| P1 | Artifact growth threshold is configured but not evaluated against prior persisted measurement | Compare each bounded artifact total with the preceding stored result, persist the delta, warn only when an owner-approved threshold is exceeded, and test first-run/decrease/increase cases |
| P1 | `warning -> failed` can be deduplicated when the summary is unchanged | Treat status severity changes as transitions and add exact event tests |
| P1 | Retention covers daily briefs but not collection runs or event history | Add transactional pruning with explicit retention tests while preserving latest state |
| P1 | Read-only CLI commands initialize SQLite/runtime directories | Add an existing-state, SQLite read-only mode for `brief --stdout`, `status`, and `events`; prove source paths remain unchanged |
| P1 | Listener safety relies on configuration convention | Reject wildcard/public binds and accept only loopback or explicitly approved Tailscale addresses |
| P1 | `check` and `collect` return success even when an execution failure produces `unknown` or `failed` health | Define the health-to-exit-code contract, return non-zero for execution/persistence failures, and add exact CLI regression tests |
| P2 | Kanban stale detection combines heartbeat and claim expiry | Evaluate stale heartbeat and expired claim independently; report incompatible schema as unknown |
| P2 | Collector timeout is per subprocess/query rather than a total budget | Apply one deadline across scheduler probes and bound SQLite integrity queries |
| P2 | Required degraded-state tests and operations docs are incomplete | Add malformed/timeout/permission/DB-error fixtures plus exact fresh-install, uninstall, reboot, and seven-day runbooks |
| P2 | YAML brief schedule and systemd timer can drift | Define one authority or a validated generation step before the daily timer is enabled |
| P3 | `check`/collection does not emit the planned per-run timestamped JSON evidence file | Either implement the artifact or explicitly revise Task 0.7's contract |

Do not begin MVP 1 until Chase accepts the certification evidence and the P1 items above are either fixed or explicitly waived.

### Non-negotiable boundaries

- MVP 0 is read-only except for its own state/log files and approved service installation.
- MVP 1 inventory is read-only and cannot clean or archive files.
- No production DB work is performed by the control plane in early MVPs.
- No long model job is launched by an agent.
- No worker can infer push, PR, merge, deployment, publication, or deletion authorization.
- The management system must not become a runtime dependency of customer production.

---

## 2. Deployment topology

```text
Chase
  |-- normal Hermes chat: request, explanation, approval
  |-- private Tailscale URL: daily brief, health, decisions, reviews

Windows workstation
  |-- GameFlowData development checkout
  |-- local databases and local-only scrapers
  |-- GPU/CPU-heavy training and backtests
  |-- Chase-launched long jobs

GitHub
  |-- source, branches, commits, optional PRs, CI, releases

 gameflow-agent droplet
  |-- Hermes gateway and Kanban dispatcher
  |-- gameflow-worker and gameflow-reviewer
  |-- GBrain MCP and canonical GameFlowBrain access
  |-- engineering-os collectors, renderer, event state, scheduler
  |-- private dashboard/API in MVP 0; expanded manager UI in MVP 4+

Railway / Vercel / Supabase
  |-- existing customer production
  |-- observed through read-only provider APIs/logs when explicitly added
  |-- not coupled to control-plane availability

S3-compatible object storage in MVP 3+
  |-- immutable model artifacts, archives, result bundles, off-droplet backups
```

### Private access and self-health limitation

MVP 0 runs independently of the Hermes gateway: systemd schedules collectors and the web service directly. The dashboard can therefore display a gateway failure while the site itself remains available.

The site binds to loopback and is exposed through Tailscale Serve HTTPS, or binds only to the host's tailnet interface if live discovery shows that is safer and simpler. It is never exposed on a public interface for MVP 0.

A dashboard cannot notify Chase while the droplet, Tailscale path, or dashboard service itself is unreachable. MVP 0 accepts that limitation because Telegram and email were operationally cumbersome. Mitigations are:

- systemd restart policy for the web service;
- persisted briefs and event history;
- a prominent stale-data banner;
- a `/healthz` endpoint;
- host-reboot certification;
- an optional external outage adapter only if Chase later chooses one.

---

## 3. Proposed repository layout

MVP 0–3 code remains isolated from production application imports under a new top-level operations package:

```text
ops/engineering_os/
  README.md
  pyproject.toml
  config/
    engineering_os.example.yaml
    checks.yaml
    subsystem_catalog.yaml
  src/gameflow_engineering_os/
    __init__.py
    cli.py
    config.py
    models.py
    runner.py
    render.py
    state.py
    events.py
    web/
      __init__.py
      app.py
      routes.py
      view_models.py
      templates/
        base.html
        index.html
        brief_history.html
        health_detail.html
      static/
        app.css
        app.js
    collectors/
      __init__.py
      gateway.py
      kanban.py
      kanban_backup.py
      gbrain.py
      git_repo.py
      scheduler.py
      disk.py
      artifacts.py
    workflows/
      __init__.py
      packets.py
      artifact_inventory.py
      debt_prioritization.py
  tests/
    fixtures/
    test_config.py
    test_models.py
    test_runner.py
    test_render.py
    test_state.py
    test_events.py
    test_web.py
    test_collectors_*.py
    test_artifact_inventory.py
  deploy/
    gameflow-engineering-os-web.service
    gameflow-engineering-os-brief.service
    gameflow-engineering-os-brief.timer
    gameflow-engineering-os-collect.service
    gameflow-engineering-os-collect.timer

config/engineering_os/
  proposal_packet.md
  implementation_packet.md
  review_packet.md
  artifact_manifest.schema.json
  debt_item.schema.json

docs/operations/
  gameflow-engineering-os-roadmap-and-usage.md
  gameflow-engineering-os-technical-implementation.md
```

The exact Python package name is intentionally separate from `src/` so importing GameFlow production modules is not the default. Collectors should invoke supported CLI/API boundaries or inspect explicitly approved local state.

### Extraction checkpoint

Keep this package in GameFlowData through MVP 3 for rapid iteration and code review. Before expanding the dashboard in MVP 4, decide whether to extract it into a private `GameFlowControlPlane` repository. Extract only if at least one is true:

- the control plane has an independent deployment cadence;
- it requires permissions that should not be available to GameFlowData workers;
- web-console dependencies materially pollute the product repository;
- multiple repositories/products need the same control plane;
- ownership and testing are clearer after extraction.

Do not create a new repository merely to appear organizationally mature.

---

## 4. Configuration contract

`ops/engineering_os/config/engineering_os.example.yaml` documents non-secret configuration:

```yaml
timezone: America/New_York

daily_brief:
  enabled: false  # owner gate; enable only after Chase approves the schedule
  schedule: "08:00"
  retain_days: 90

events:
  enabled: true
  record_recovery: true
  repeat_after_hours: 24

web:
  bind_host: 127.0.0.1
  bind_port: 8765
  public_base_url: null  # set to the approved Tailscale HTTPS URL
  stale_after_minutes: 20

paths:
  kanban_db: /home/chase/.hermes/kanban/boards/gameflow/kanban.db
  kanban_backups: /home/chase/.hermes/backups/kanban/gameflow
  gameflow_repo: /home/chase/projects/GameFlowData
  state_dir: /home/chase/.local/state/gameflow-engineering-os
  log_dir: /home/chase/.local/state/gameflow-engineering-os/logs

thresholds:
  backup_max_age_hours: 36
  stuck_task_minutes: 30
  disk_warning_percent: null
  disk_critical_percent: null
  artifact_growth_warning_bytes: null
```

Thresholds remain `null` until the live baseline is measured and Chase approves values. Collectors must report measurements even when event thresholds are unset.

MVP 0 requires no Telegram, email, GitHub write, or production credentials. Tailscale controls network access. If later adapters require secrets, they live in a droplet-local environment file with restrictive permissions, are never committed or printed, and are loaded explicitly by only the service that needs them.

---

## 5. Core data contracts

### 5.1 Health check result

Every collector returns the same typed shape:

```json
{
  "check_id": "kanban.dispatcher",
  "status": "healthy",
  "summary": "Dispatcher active; 0 running, 1 blocked, 0 stale claims",
  "observed_at": "2026-07-27T12:00:00Z",
  "source": "remote local state",
  "freshness_seconds": 2,
  "metrics": {
    "running": 0,
    "blocked": 1,
    "stale_claims": 0
  },
  "evidence": [],
  "recommended_action": null
}
```

Allowed status values:

- `healthy`
- `warning`
- `failed`
- `unknown`
- `not_configured`

Rules:

- Exceptions become `unknown` or `failed`, never `healthy`.
- `observed_at` is mandatory.
- `source` identifies the live evidence route.
- Secret-bearing command output is never stored in `evidence`.
- A collector timeout is bounded and represented explicitly.

### 5.2 Daily brief result

The renderer consumes collected health plus work summaries and emits both JSON and text:

```json
{
  "brief_date": "2026-07-27",
  "generated_at": "2026-07-27T12:00:05Z",
  "health": [],
  "decisions": [],
  "active_work": [],
  "risks": [],
  "suggested_actions": []
}
```

The renderer is deterministic. It persists JSON plus a server-rendered view. An LLM may later create an optional narrative, but dashboard availability and status must not depend on one.

### 5.3 Health event state

SQLite state deduplicates unchanged failures and records recovery history:

```text
check_state
- check_id primary key
- last_status
- last_summary_hash
- first_failed_at
- last_observed_at
- last_event_at
- recovered_at
```

Transitions:

- `healthy -> failed/warning`: create a dashboard event.
- `failed/warning -> same unchanged state`: do not create duplicate events inside the repeat window.
- `failed/warning -> healthy`: create one recovery event.
- `unknown`: display based on check criticality and duration, never represent as healthy.

### 5.4 Proposal packet

Stored as card body plus structured metadata:

```yaml
problem: string
evidence:
  - source: string
    observed_at: timestamp
    summary: string
business_impact: string
subsystem_ids: [string]
debt_ids: [string]
recommended_scope: string
non_goals: [string]
risks: [string]
dependencies: [string]
validation: [string]
decision_requested: approve | revise | defer | reject
```

### 5.5 Implementation packet

```yaml
approved_outcome: string
allowed_paths: [string]
forbidden_paths: [string]
subsystem_ids: [string]
debt_ids: [string]
invariants: [string]
non_goals: [string]
workspace_kind: worktree
validation_commands: [string]
requires_separate_authorization:
  - push
  - pull_request
  - merge
  - deploy
  - database_write
  - long_job
```

### 5.6 Review packet

```yaml
card_id: string
branch: string
worktree: string
commits: [string]
changed_files: [string]
validation:
  - command: string
    exit_code: integer
    summary: string
acceptance_criteria: [object]
architecture_impact: string
debt_impact: string
artifact_impact: string
security_runtime_risk: string
residual_risks: [string]
rollback: string
push_state: local_only | pushed
pr_state: none | open | closed
merge_state: unmerged | merged
review_recommendation: approve | rework | shelf
```

---

## 6. MVP 0 implementation tasks

**Implementation checkpoint — 2026-07-27:** Tasks 0.1–0.8 have an operational first implementation and initial live verification. Task 0.9 remains open for the seven-day brief window, phone validation, approved host reboot, final schedule, and owner acceptance. The review backlog in Section 1 is future hardening and must remain visible until resolved or waived.

### Task 0.1: Capture a live, read-only control-plane baseline

**Objective:** Replace handoff assumptions with current evidence before coding checks.

**Files:**

- Create: `docs/operations/evidence/engineering-os-mvp0-baseline.md`

**Steps:**

1. Inspect live service units, timers, gateway, board database, backup directory, GBrain health, repository state, and disk usage.
2. Record commands, timestamps, redacted outputs, and unknowns.
3. Confirm non-login SSH path behavior for `/home/chase/.local/bin/hermes`.
4. Identify the supported live signal for the embedded Kanban dispatcher.
5. Confirm Tailscale Serve availability, MagicDNS hostname, and the approved private URL pattern without exposing a public listener.
6. Run `git diff --check -- docs/operations/evidence/engineering-os-mvp0-baseline.md`.

**Done when:** Every planned MVP 0 collector has a named evidence source or is explicitly marked unresolved.

### Task 0.2: Create the isolated package and config loader

**Objective:** Establish a separately testable operations package with no production imports.

**Files:**

- Create: `ops/engineering_os/pyproject.toml`
- Create: `ops/engineering_os/src/gameflow_engineering_os/__init__.py`
- Create: `ops/engineering_os/src/gameflow_engineering_os/config.py`
- Create: `ops/engineering_os/src/gameflow_engineering_os/models.py`
- Create: `ops/engineering_os/config/engineering_os.example.yaml`
- Test: `ops/engineering_os/tests/test_config.py`
- Test: `ops/engineering_os/tests/test_models.py`

**TDD sequence:**

1. Write tests for missing config, unknown status, timezone parsing, and null thresholds.
2. Run the scoped tests and verify failure.
3. Implement only the config and model types needed by MVP 0.
4. Run the scoped tests and verify pass.
5. Run a compile/import check with the package's selected Python environment.

### Task 0.3: Implement bounded health collectors

**Objective:** Produce deterministic `HealthCheckResult` records without mutation.

**Files:**

- Create collectors under `ops/engineering_os/src/gameflow_engineering_os/collectors/`.
- Test each collector in `ops/engineering_os/tests/test_collectors_*.py`.

**Collector requirements:**

- Gateway: service active state and last meaningful failure.
- Kanban: task counts, stale claims, blocked/running tasks, dispatcher signal.
- Backup: newest backup age and SQLite integrity result where safe.
- GBrain: transport health, page/embed/orphan counts, and observed timestamp.
- Git: branch, dirty paths, ahead/behind, and fetch freshness without changing branches.
- Scheduler: expected timer states and last runs.
- Disk: filesystem usage plus targeted directory measurements.
- Artifacts: MVP 0 reports only bounded aggregate growth; full classification belongs to MVP 1.

**Tests:**

- healthy fixture;
- command timeout;
- malformed output;
- missing executable/path;
- stale evidence;
- permission error;
- secret redaction.

### Task 0.4: Implement deterministic brief rendering

**Objective:** Render a useful daily brief and dashboard view model from fixtures without an LLM.

**Files:**

- Create: `ops/engineering_os/src/gameflow_engineering_os/render.py`
- Test: `ops/engineering_os/tests/test_render.py`

**Required tests:**

- all healthy;
- one failed check;
- unknown check;
- blocked and stale work;
- no decisions;
- long evidence is truncated;
- timestamps and freshness are shown;
- responsive summary remains readable on desktop and phone;
- deep evidence stays behind detail views.

### Task 0.5: Implement stateful failure and recovery events

**Objective:** Record visible transitions without repeating unchanged failures.

**Files:**

- Create: `ops/engineering_os/src/gameflow_engineering_os/state.py`
- Create: `ops/engineering_os/src/gameflow_engineering_os/events.py`
- Test: `ops/engineering_os/tests/test_state.py`
- Test: `ops/engineering_os/tests/test_events.py`

**Required tests:**

- first failure creates an event;
- unchanged failure is deduplicated;
- repeat window creates at most one additional event;
- recovery creates one event;
- unknown state is never coerced to healthy;
- concurrent invocation does not corrupt state.

### Task 0.6: Implement the private dashboard MVP

**Objective:** Serve the latest brief, health state, and event history at one private URL independently of Hermes.

**Files:**

- Create web files under `ops/engineering_os/src/gameflow_engineering_os/web/`.
- Test: `ops/engineering_os/tests/test_web.py`.

**Requirements:**

- `GET /` shows latest brief, decisions, active work, risks, health cards, and freshness.
- `GET /briefs` shows dated brief history.
- `GET /health/<check_id>` shows bounded evidence and history.
- `GET /healthz` returns service/database readiness without leaking details.
- Stale collector or brief data produces a prominent banner.
- The UI is server-rendered and usable on mobile without a JavaScript build.
- No write actions are exposed in MVP 0 except a CSRF-protected manual refresh if approved.
- Browser responses never contain secrets or raw credential-bearing command output.
- Route tests cover healthy, warning, failed, unknown, stale, empty, and database-error states.

### Task 0.7: Add CLI, runner, and JSON evidence files

**Objective:** Support manual validation before scheduling.

**Files:**

- Create: `ops/engineering_os/src/gameflow_engineering_os/cli.py`
- Create: `ops/engineering_os/src/gameflow_engineering_os/runner.py`
- Test: `ops/engineering_os/tests/test_runner.py`

**Commands to support:**

```text
gfos check --json
gfos brief --stdout
gfos brief --generate
gfos collect
gfos events
gfos status
```

Every run writes a timestamped JSON result under the configured state directory. Collector or persistence failure returns non-zero.

### Task 0.8: Install systemd user services and timers

**Objective:** Run the dashboard, brief generation, and health collection independently of interactive SSH and the Hermes gateway.

**Files:**

- Create units under `ops/engineering_os/deploy/`.
- Create: `ops/engineering_os/README.md` with install/rollback commands.

**Deployment requirements:**

1. Copy/install the package into an isolated virtual environment on the droplet.
2. Install config and state directories with restrictive permissions.
3. Install the web service plus brief/collector timers.
4. Run services manually before enabling timers.
5. Bind Uvicorn to loopback and expose it only through the approved Tailscale route.
6. Verify the HTTPS URL from a second Tailscale-connected device.
7. Verify timezone and next triggers.
8. Confirm user lingering, restart, and service-survival policy.
9. Record rollback commands.
10. Do not enable the daily timer until Chase confirms generation time.

### Task 0.9: Run MVP 0 end-to-end certification

**Objective:** Prove the real user path, not just unit tests.

**Certification:**

1. Open the private HTTPS URL from Chase's desktop and phone over Tailscale.
2. Generate an on-demand healthy brief and confirm it appears.
3. Enable the daily brief and collector timers.
4. Simulate one safe collector failure without stopping customer production.
5. Confirm the failure appears prominently with fresh evidence.
6. Restore the condition and confirm one recovery event.
7. Confirm no duplicate events inside the suppression window.
8. Restart the web service and verify persisted history.
9. Reboot the host in an approved window and verify the URL, timers, and history recover.
10. Let seven expected daily briefs generate and remain browsable.
11. Record outcomes in `docs/operations/evidence/engineering-os-mvp0-certification.md`.
12. Chase decides whether MVP 0 is accepted.

---

## 7. MVP 1 technical implementation

### 7.1 Artifact and repository inventory contract

The inventory command must avoid broad recursive scans on the Windows-mounted repository. Use:

1. `git ls-files` plus sizes for tracked accounting.
2. Targeted measurements for known generated paths.
3. Targeted manifest/config reference extraction.
4. Explicit skip and permission-error reporting.

Initial targeted paths must be confirmed from the live baseline. Candidate scopes include:

- `src/models/artifacts/`
- `src/models/mlb/artifacts/`
- `backtest_results/`
- approved temporary/output directories

Every inventory row contains:

```json
{
  "path": "...",
  "kind": "tracked_code|artifact|result|cache|temporary|unknown",
  "bytes": 0,
  "modified_at": "...",
  "git_tracked": false,
  "references": [],
  "classification": "production_critical|reproducibility_critical|active|archive_candidate|regenerable|unknown",
  "classification_evidence": [],
  "proposed_action": "retain|investigate|archive|delete_candidate",
  "confidence": "high|medium|low"
}
```

No inventory classification directly authorizes action.

### 7.2 Kanban pilot graph

Create cards only after Chase approves the proposal:

```text
T1 read-only inventory worker
  -> T2 independent evidence review
  -> owner decision
```

Do not pre-create cleanup implementation. The shape of cleanup depends on T1/T2 and Chase's decision.

### 7.3 Validation

- Compare inventory totals with independent targeted size checks.
- Verify production artifact references separately.
- Confirm `git status` before and after is substantively unchanged.
- Confirm no DB, Railway, Vercel, Supabase, model run, archive, or deletion occurred.
- Display the result in the private dashboard and link it from the relevant workflow state.

---

## 8. MVP 2 technical implementation

### 8.1 Subsystem catalog

`ops/engineering_os/config/subsystem_catalog.yaml` becomes the machine-readable routing layer. Each entry includes:

```yaml
id: mlb-model-lifecycle
purpose: Configuration-driven MLB train/evaluate/promote workflow
paths:
  - src/models/mlb/lifecycle
  - configs/mlb
criticality: high
runtime: workstation-and-railway
validation:
  - pytest <scoped tests>
invariant_slugs:
  - operations/critical-invariants
debt_ids: []
owners:
  human: Chase
```

Do not populate the entire repository in one pass. Add entries for subsystems touched by the first three real tasks, then expand based on use.

### 8.2 Debt projection

Keep the adjudicated audit/register as full evidence. Add a deterministic projection that produces:

- top candidate clusters;
- evidence freshness;
- affected subsystem;
- active-card relationship;
- disposition;
- owner decision needed.

The projection may propose Kanban cards but cannot create Ready work automatically.

### 8.3 Packet validation

Before promotion to Ready, validate that an implementation packet contains:

- existing assignee profile;
- allowed scope;
- required validation;
- relevant invariants;
- non-goals;
- separate authorization boundaries;
- valid parent dependencies;
- isolated worktree for code changes.

A reviewer rejects incomplete packets rather than guessing missing scope.

---

## 9. MVP 3 technical implementation

### 9.1 Artifact registry authority

Begin with versioned manifests in Git plus immutable blobs in current locations. Introduce object storage only after the manifest and checksum path works locally.

Proposed manifest location:

```text
artifacts/manifests/<artifact-id>.json
artifacts/production/<consumer-id>.json
```

Do not migrate current production loading immediately. First add read-only parity checks proving that manifests resolve to the same artifacts production currently loads.

### 9.2 Storage migration sequence

1. Generate manifests for a bounded non-production artifact set.
2. Verify checksums and local loading.
3. Upload copies to selected object storage.
4. Download to a temporary location and verify checksum/loading.
5. Add one non-production consumer path.
6. Validate rollback to local artifact.
7. Extend to one production candidate only after approval.
8. Preserve current production packaging until deployment and rollback are proven.

### 9.3 Cleanup executor

The executor accepts an approved immutable batch manifest. It refuses paths not present in that manifest.

Required modes:

```text
plan
archive
verify-archive
quarantine
verify-post-action
delete-after-retention
```

Safety controls:

- path allowlist;
- checksum before action;
- production-manifest exclusion;
- reproducibility-retention exclusion;
- dry run by default;
- action log;
- independent post-action verification;
- no broad wildcard deletion.

---

## 10. MVP 4 manager-console expansion boundary

MVP 0 already provides a server-rendered dashboard. Do not replace it with a SPA or add a Node frontend build until MVP 3 usage evidence shows that richer client interaction is worth the operational cost. The expanded architecture is:

```text
private browser over Tailscale
  -> manager API
      -> Kanban adapter
      -> GitHub adapter
      -> GBrain adapter
      -> health/brief store
      -> artifact registry
      -> approval audit log
```

Requirements:

- Private Tailscale network; add application authentication if the approved threat model requires more than tailnet identity.
- Read adapters do not write directly into foreign databases.
- Approval commands are idempotent.
- Every status displays source and observed timestamp.
- UI outage cannot block workers, briefs, or customer production.
- No public exposure merely for convenience.
- No direct production secrets in browser payloads.

A dedicated Postgres database becomes justified when concurrent manager API, collectors, webhooks, and audit history exceed the simple SQLite state model. Migration requires a backup/restore test and does not replace the Kanban database as task authority.

---

## 11. MVP 5–7 technical boundaries

### Incident system

- Incidents have immutable IDs, severity, impact, evidence, owner, and timeline.
- Detection is not root cause.
- Fix cards link to the incident.
- Review must reproduce the original failure path when accessible.
- Resolution requires monitoring evidence.

### Growth system

- Marketing experiments are separate from engineering cards but may depend on them.
- Draft approval and publish approval are separate.
- Customer data access follows existing privacy/auth policies.
- Spending and outbound actions require explicit authorization.

### Selective autonomy policy

Each autonomous action class requires:

- narrow machine-readable eligibility policy;
- test fixtures and real dry run;
- bounded permissions;
- idempotency;
- rollback/containment;
- audit events;
- exception alerts;
- explicit owner approval and revocation procedure.

---

## 12. Verification ladder

Every MVP advances through this ladder:

1. Unit tests with success/failure fixtures.
2. Local CLI dry run.
3. Remote droplet manual run.
4. Real private-dashboard access or worker run from Chase's normal device.
5. Failure and recovery simulation.
6. Repeated scheduled operation for the stage's acceptance window.
7. Chase acceptance.

A service, timer, card, manifest, or page existing is not completion. The original user path must work.

### MVP 0 verification commands

Exact commands depend on the package setup selected in Task 0.2, but implementation must provide copy-pasteable commands for:

```text
pytest ops/engineering_os/tests -q
gfos check --json
gfos brief --stdout
gfos brief --generate
gfos collect
gfos events
curl --fail http://127.0.0.1:8765/healthz
systemctl --user status gameflow-engineering-os-brief.timer
systemctl --user status gameflow-engineering-os-collect.timer
systemctl --user status gameflow-engineering-os-web.service
journalctl --user -u gameflow-engineering-os-brief.service --since today
```

The implementation README must include the actual virtual-environment executable paths used on `gameflow-agent` rather than assuming non-login SSH finds `hermes` or `gfos` on `PATH`.

---

## 13. Rollback and failure containment

MVP 0 rollback:

1. Disable the new timers.
2. Stop active engineering-OS services.
3. Preserve logs and state for diagnosis.
4. Remove installed units only after evidence is captured.
5. Leave Hermes gateway, Kanban, GBrain, GameFlowData, and customer production untouched.

MVP 1–2 rollback:

- Archive or block experimental cards.
- Do not alter existing Kanban history.
- Remove packet enforcement only through a reviewed config change.
- Keep evidence reports.

MVP 3 rollback:

- Restore from archive/quarantine using checksums.
- Restore production manifest pointer.
- Re-run model-loading and scoped regression tests.
- Do not permanently delete until retention and restore verification pass.

---

## 14. Security and permissions

- Monitor account receives only permissions necessary for read-only checks, brief persistence, and serving the private dashboard.
- Worker profiles do not receive production secrets by default.
- Reviewers need read access to branches/diffs/tests, not merge authority.
- Tailscale exposure is private and scoped; the application listens on loopback unless an approved tailnet-only bind is used.
- Object-storage credentials are scoped by bucket/prefix and operation.
- The manager console does not expose secret-bearing provider responses.
- Production DB operations continue through the GameFlow SQL-runner/delegated pattern.
- Every mutation records actor, request, timestamp, target, approval, and outcome.

---

## 15. Open decisions and implementation gates

Before enabling daily brief scheduling, Chase must decide:

1. Daily brief generation time in America/New_York.
2. Whether to retain the verified direct tailnet route (`http://gameflow-agent:8765/`) or enable Tailscale Serve HTTPS.
3. Whether seven-day certification includes weekends; the roadmap currently assumes yes.
4. Whether a manual refresh button is allowed in the otherwise read-only MVP.

After the baseline, Chase must approve:

5. Disk warning and critical thresholds.
6. Artifact growth warning threshold.
7. Which safe condition is used for failure/recovery simulation.

Before MVP 3 storage migration:

8. Object-storage provider and cost constraints.
9. Retention periods by artifact class.
10. Whether any current production artifacts may leave Git packaging.

Before MVP 4:

11. Whether the control plane remains in GameFlowData or moves to a private repository.
12. Manager-console authentication and access model.
13. Whether a dedicated Postgres database is justified by observed usage.

---

## 16. Final acceptance definition

The engineering operating system is not considered useful merely because agents, boards, or dashboards exist. The staged program succeeds when:

- Chase can access a dependable daily brief from one bookmarked private URL;
- the latest failures, recoveries, and evidence freshness are obvious from one private dashboard;
- a normal request becomes a bounded proposal rather than immediate uncontrolled work;
- approved implementation occurs in isolation;
- independent review verifies actual evidence;
- artifacts and debt have explicit lifecycle state;
- consequential actions remain human-gated;
- the system measurably reduces the amount of architecture and operational state Chase must hold in his head.
