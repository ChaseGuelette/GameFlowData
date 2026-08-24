# GameFlow Engineering OS Roadmap and Usage Guide

**Status:** MVP 0 is implemented and operational on `gameflow-agent`; seven-day, phone, and host-reboot certification remain open, so MVP 1 is not yet promoted.

**Purpose:** Move GameFlow from ad hoc agent-assisted development to a low-touch, human-gated operating system that Chase can manage through daily briefs, concise approval packets, and verified results without routinely opening Kanban, GBrain, or SSH.

**Companion technical plan:** [`gameflow-engineering-os-technical-implementation.md`](gameflow-engineering-os-technical-implementation.md)

---

## Start here: MVP sequence, usage, and promotion gates

Do not build a large all-purpose portal or populate Kanban with every known debt item. Start with a small private dashboard that makes the daily brief and live control-plane state easy to inspect from any Tailscale-connected machine. Add workflows and pages only when the prior stage is dependable.

| Stage | What ships | How Chase uses it | Move on only when |
|---|---|---|---|
| **MVP 0 — Private dashboard and daily brief** | A small read-only website over Tailscale with the daily brief, live health, failure/recovery history, and freshness indicators | Open one bookmarked URL from a Tailscale-connected machine; act only on explicit warnings or decisions | Seven consecutive daily briefs are generated and visible; a safe simulated failure and recovery appear correctly; no manual SSH/Kanban/GBrain inspection is needed to understand current health |
| **MVP 1 — One managed work loop** | One real read-only repository/artifact inventory passes through proposal, approval, worker, review, and owner decision | Start the request in chat, then approve/edit scope and review the result from the dashboard | The task survives disconnects, creates no unauthorized changes, and Chase can make the final decision without opening Kanban |
| **MVP 2 — Repeatable engineering governance** | Standard proposal, implementation, and review packets; subsystem/debt context injection; top debt priorities | Approve bounded work, review verified branches, and defer/reject proposals | At least three real engineering tasks complete through the workflow; no worker self-report is accepted without diff/test verification; Chase finds the packets usable |
| **MVP 3 — Artifact lifecycle and controlled cleanup** | Artifact manifests, lifecycle states, retention policy, dry-run cleanup, archive/delete approval | Review cleanup batches and model lifecycle decisions | One cleanup batch is approved, executed, and verified without breaking model loading or reproducibility; production artifacts are unambiguously identified |
| **MVP 4 — Manager-console expansion** | Expand the proven dashboard into an executive inbox aggregating approvals, initiatives, reviews, health, debt, and artifacts | Use one website as the primary management surface while chat remains the intake/explanation path | The expanded console is grounded in real workflows rather than duplicating GitHub/Kanban/GBrain; all consequential actions remain human-gated |
| **MVP 5 — Bug and operations automation** | Incident intake, reproduction workflow, recurring health/debt checks, regression verification | Approve fixes and review incident packets | At least two real failures travel from detection through verified remediation; alerts are actionable and not noisy |
| **MVP 6 — Growth operating lane** | Marketing research, experiment proposals, drafts, approval, measurement, and stop/continue decisions | Manage growth experiments from the same executive inbox | Engineering control loops are stable; no content is published or customer contact made without approval |
| **MVP 7 — Selective autonomy** | Narrow auto-actions for proven low-risk classes | Review exceptions and periodic audit summaries | Each autonomous action class has a tested policy, rollback, audit trail, and explicit Chase approval; model promotion, DB changes, trading, merging, and outbound publishing remain gated |

### Current starting point

MVP 0 now runs as a separately packaged, read-only control plane under `ops/engineering_os`. The private dashboard is reachable from Tailscale-connected devices at `http://gameflow-agent:8765/` (tailnet-IP fallback: `http://100.126.253.6:8765/`). The web service and five-minute collector timer are enabled; the daily brief timer is installed but intentionally disabled pending Chase's schedule decision. Initial failure/recovery behavior, service restart persistence, desktop access, and live read-only collection were verified. See `evidence/engineering-os-mvp0-certification.md` for the exact evidence and open gates.

The remote Kanban infrastructure remains a backend queue rather than Chase's normal management surface. The 30 adjudicated technical-debt clusters have not been converted into an operating backlog, and MVP 1 has not started. GBrain and existing plans remain project knowledge rather than runtime-health proof.

### Default operating rule

- **Put important state in one private dashboard.** Do not require routine Kanban, GBrain, or SSH inspection.
- **The dashboard is Chase's primary review surface; Kanban is initially a backend queue.**
- **GBrain is project knowledge, not runtime-health proof.**
- **GitHub is code and integration authority.**
- **The droplet coordinates always-on work.**
- **The Windows workstation remains the heavy-compute, local-data, and Chase-launched job plane.**
- **Long model training, sweeps, broad DB work, production mutation, merge, and publication remain explicitly human-gated.**

---

## Daily operating experience

### Daily executive brief

MVP 0 changes the prior weekly proposal to a **daily** brief displayed at the dashboard's stable Tailscale URL. Once Chase approves and enables the daily timer, the system generates and stores one dated brief every day, including healthy days. Until then, on-demand generation is available and the dashboard clearly shows the latest persisted brief. The dashboard shows prior brief history, generation timestamp, collector freshness, and a prominent stale banner when the expected brief is missing.

The brief should be short and deterministic:

```text
GameFlow daily brief — YYYY-MM-DD

Needs your decision
- <only real approval/review/blocker items>

Health
- Control plane: healthy | warning | failed
- Kanban: <running/blocked/stuck counts>
- GBrain: <reachable + key health summary>
- Backups: <fresh/stale>
- Remote repo: <clean/dirty/diverged>
- Disk/artifacts: <usage + meaningful delta>

Active work
- <initiative/card — state — next gate>

Risks and changes
- <new or materially changed items only>

Suggested action
- <zero to three concrete decisions>
```

Rules:

- Generate and retain one daily brief, including on healthy days.
- Show current failures, material degradation, and recovery events at the top of the dashboard.
- Do not create duplicate unchanged events throughout the day.
- Show evidence timestamps so stale checks cannot appear current.
- Report unknown as unknown; never convert missing telemetry into healthy.
- Keep deep logs behind detail links unless needed for action.

### How Chase requests work

Chase uses normal Hermes chat for requests and explanation. The dashboard displays durable proposals, active work, decisions, and results:

```text
Investigate repository and model-artifact bloat. Do not delete anything. Bring me a ranked proposal.
```

The system returns a proposal packet. Nothing executes until the requested scope is approved. After execution, Chase receives a review packet and chooses:

- approve the result or next slice;
- request changes or more evidence;
- defer;
- reject;
- archive;
- authorize push/PR/merge when applicable.

### What Chase should not need to do

Routine operation must not require Chase to:

- SSH into `gameflow-agent`;
- inspect the Kanban database;
- browse GBrain;
- check whether the gateway or dispatcher is running;
- discover stuck workers;
- inspect artifact directories manually;
- remember maintenance schedules.

Those remain available as administrative fallbacks.

---

## MVP 0 — Private dashboard and daily brief

### Goal

Make silent control-plane failure visible before attempting organizational automation.

### Scope

- Read-only live inventory of the droplet control plane.
- Health collectors for gateway, Kanban dispatcher/tasks, Kanban backup age, GBrain, Git checkout, cron/timers, disk, and artifact-directory growth.
- A small server-rendered web dashboard hosted on `gameflow-agent` and exposed only through Tailscale.
- Daily executive brief with dated history.
- Stateful failure, warning, and recovery events shown in the dashboard.
- Explicit source/freshness timestamps and stale-state banners.
- On-demand refresh command and button for manual validation.

### Out of scope

- A polished multi-page management portal.
- Populating all debt findings into Kanban.
- Code cleanup or artifact deletion.
- Production DB mutation.
- Model training or sweeps.
- Automated merge, deployment, or remediation.

### Usage

1. Connect the device to Chase's tailnet.
2. Open `http://gameflow-agent:8765/`; use `http://100.126.253.6:8765/` only if MagicDNS does not resolve.
3. Read the latest brief and health cards. If there is no decision or failure, no action is required.
4. Treat a stale banner or unreachable URL as an operational signal; the dashboard cannot notify Chase while the droplet or Tailscale path itself is down.
5. Use Hermes chat for requests, explanations, and any approval. MVP 0 exposes no consequential action controls.

Current scheduling state:

- live health collection runs every five minutes;
- the web service starts automatically;
- daily brief generation remains owner-gated and disabled;
- Tailscale Serve HTTPS remains optional future hardening; the currently verified route binds directly to the tailnet address and is not public.

Administrative CLI and deployment details live in `ops/engineering_os/README.md`; routine use should not require SSH.

### Deferred hardening from implementation review

These findings are future work, not blockers to operating the current read-only pilot. Complete them before claiming full MVP 0 certification or promoting MVP 1:

1. Implement persisted artifact-growth comparison and enforce `artifact_growth_warning_bytes`.
2. Record severity changes such as `warning -> failed` as distinct events even when the summary is unchanged.
3. Apply retention to collection snapshots and event history, not only daily briefs.
4. Make read-only CLI paths such as `brief --stdout`, `status`, and `events` open existing state without initializing or mutating it.
5. Validate `web.bind_host` so only loopback or approved tailnet addresses are accepted.
6. Detect an expired Kanban claim independently of a fresh heartbeat and report missing expected schema fields as unknown.
7. Enforce one overall collector deadline, including SQLite integrity checks and multi-unit scheduler probes.
8. Complete the degraded-state test matrix and tighten fresh-install, uninstall, reboot, and seven-day certification instructions.
9. Decide how the YAML brief schedule and systemd `OnCalendar` remain synchronized before enabling the brief timer.
10. Emit the planned timestamped JSON evidence for every run and return non-zero when command, collector, or persistence failures make the result unknown or failed.

### Promotion gate to MVP 1

All must pass:

1. Seven expected daily briefs are generated, retained, and visible at the stable Tailscale URL.
2. A safe simulated failure is detected and appears prominently in the dashboard.
3. Recovery is detected and recorded exactly once.
4. A stuck/blocked test task is represented accurately.
5. Backup staleness and disk thresholds are tested without destructive actions.
6. The brief does not claim GBrain or production health from stale handoffs.
7. Service restart and host reboot recovery are tested without losing brief history.
8. Chase confirms the page is fast, readable, and useful on both desktop and phone.

---

## MVP 1 — One managed work loop

### Goal

Prove that one real outcome can be delegated and reviewed without Chase supervising the infrastructure.

### Pilot

Run a read-only repository and model-artifact inventory.

The pilot must:

- use tracked-file accounting first;
- scan only targeted generated/artifact paths;
- classify tracked code, generated results, caches, model artifacts, temporary files, and unknowns;
- identify references from production configs/manifests where available;
- compare findings with the existing adjudicated technical-debt evidence;
- produce a cleanup proposal;
- make no deletion, move, archive, commit, DB write, or production change.

### Workflow

```text
Chase request
  -> proposal packet
  -> Chase approves/edits scope
  -> read-only Kanban worker
  -> evidence artifact
  -> independent reviewer
  -> concise result packet
  -> Chase decision
```

### Usage

Chase starts work through chat and reviews durable state in the dashboard. Kanban stores task state underneath. The result packet should answer:

- What was inspected?
- What was found?
- What is production or reproducibility critical?
- What appears regenerable?
- What is unknown?
- What is the safest first cleanup slice?
- What evidence would prove that cleanup safe?

### Promotion gate to MVP 2

- Worker completes after a disconnect/reconnect scenario.
- Reviewer independently validates scope and evidence.
- No unauthorized files or systems change.
- Results are reproducible from a checked-in command or manifest.
- Chase can approve or reject the recommendation without opening Kanban.
- At least one bounded next action is sufficiently specified for implementation.

---

## MVP 2 — Repeatable engineering governance

### Goal

Turn the pilot workflow into the default path for meaningful GameFlow code work.

### Standard packets

#### Proposal packet

- Problem and user/business impact.
- Current evidence and freshness.
- Recommended scope and non-goals.
- Affected subsystem and known debt.
- Risk, dependencies, and expected cost.
- Smallest useful next action.
- Decision requested from Chase.

#### Implementation packet

- Approved outcome.
- Allowed files/subsystems.
- Relevant invariants and GBrain decisions.
- Related debt IDs.
- Required branch/worktree.
- Exact validation commands.
- Forbidden mutations and non-goals.
- Expected completion metadata.

#### Review packet

- Card, branch, worktree, and commits.
- Changed files and scoped diff summary.
- Validation commands and actual results.
- Acceptance-criteria status.
- Architecture, debt, artifact, security, and runtime impact.
- Residual risks and rollback.
- Push, PR, and merge state.
- Independent reviewer recommendation.

### Technical-debt handling

Do not create 30 active cards merely to populate the board.

- Preserve all 30 adjudicated clusters in the evidence/debt register.
- Revalidate them against current code and runtime evidence.
- Rank the top three to five by safety, recurring friction, storage cost, delegation impact, and remediation value.
- Present those priorities to Chase.
- Create executable Kanban work only after approval.
- Keep Shelf, Retain, Superseded, rejected, and Needs-Evidence items out of Ready.

### Usage

Chase manages outcomes and approvals. Workers remain scoped; the orchestrator attaches relevant system context rather than asking each worker to rediscover the repository.

### Promotion gate to MVP 3

- Three real tasks complete through proposal, implementation, independent review, and owner decision.
- Every code task uses an isolated branch/worktree.
- Applicable invariants and debt are attached before execution.
- Reviewer verifies actual diffs and tests.
- Push, PR, merge, production mutation, and long jobs remain separately authorized.
- Chase confirms the process reduces rather than increases management burden.

---

## MVP 3 — Artifact lifecycle and controlled cleanup

### Goal

Replace filename/directory inference with explicit artifact identity, status, retention, and verified cleanup.

### Lifecycle

```text
candidate
  -> evaluated
  -> flat-approved
  -> optional-kelly-approved
  -> production
  -> superseded
  -> archived
```

Rejected artifacts follow:

```text
candidate -> rejected -> retained-until -> deletion-eligible
```

### Required artifact metadata

- Immutable artifact ID.
- Model/stat/lane.
- Source commit and config hash.
- Feature manifest.
- Training, calibration, and evaluation windows.
- Evaluation references and decision state.
- Checksum and storage location.
- Production consumer.
- Retention class and date.
- Superseded-by relationship.

### Cleanup policy

1. Inventory.
2. Classify.
3. Produce dry-run batch.
4. Obtain approval.
5. Prefer quarantine/archive for uncertain files.
6. Execute only approved paths.
7. Verify model loading, checksums, tests, and reproducibility evidence.
8. Permanently remove only after the retention gate.

Age alone never proves that a model artifact is safe to delete.

### Usage

The daily brief reports only material artifact growth and cleanup decisions. Chase reviews a batch summary, not individual filesystem noise.

### Promotion gate to MVP 4

- Production artifact identity is deterministic.
- One cleanup batch completes with before/after evidence.
- Rollback/archive recovery is tested.
- Production model loading and relevant tests pass after cleanup.
- Unknown artifacts remain quarantined or unresolved rather than guessed safe.
- Real use shows which additional initiative, review, debt, and artifact views justify expanding the MVP dashboard.

---

## MVP 4 — Manager-console expansion

### Goal

Expand the private MVP dashboard only after real workflows prove what else must be aggregated.

### Initial pages

1. Executive inbox.
2. Initiatives and current gates.
3. Engineering work and review packets.
4. System health and incidents.
5. Models and artifacts.
6. Technical-debt priorities.
7. Growth experiments when MVP 6 begins.

### Authority boundaries

- Kanban owns work state.
- GitHub owns source/integration state.
- GBrain owns durable project decisions and lessons.
- Artifact registry owns artifact lifecycle metadata.
- Runtime providers own deployment/runtime state.
- The console aggregates and records approvals; it does not silently duplicate authorities.

### Usage

The website is already Chase's health and brief surface. At this stage it becomes the primary portfolio and approval surface. Chat remains the fast intake and explanation interface.

### Promotion gate to MVP 5

- Private Tailscale access and authentication are verified.
- Every displayed status includes source and freshness.
- Approval actions are idempotent and audited.
- A console outage cannot affect customer production.
- Chase uses it for real decisions and confirms that it replaces proven friction.

---

## MVP 5 — Bug and operations automation

### Goal

Make failures move through a consistent evidence and regression workflow.

### Workflow

```text
detect/report
  -> reproduce
  -> classify severity and subsystem
  -> propose bounded fix
  -> Chase approval when required
  -> isolated implementation
  -> regression verification from original path
  -> release authorization
  -> monitor
```

### Usage

Material production failures appear immediately and prominently on the dashboard, followed by a compact incident packet once evidence is available. The system must distinguish a detected symptom from a proven root cause. External push alerting remains optional rather than an MVP dependency.

### Promotion gate to MVP 6

- Two real incidents complete the workflow.
- Original failures are retested end to end.
- Alert volume is useful and deduplicated.
- Follow-up debt is registered without automatically entering Ready.
- Recovery and rollback evidence are captured.

---

## MVP 6 — Growth operating lane

### Goal

Apply the proven human-gated operating loop to marketing and product growth.

### Workflow

```text
evidence
  -> audience/problem
  -> hypothesis
  -> experiment proposal
  -> Chase approval
  -> draft/build
  -> approval to publish/run
  -> measure
  -> continue/stop decision
```

### Usage

Chase reviews hypotheses, drafts, budgets, and measured outcomes. No agent publishes, messages customers, or spends money without explicit approval.

### Promotion gate to MVP 7

- At least three growth experiments have explicit hypotheses and outcomes.
- Metrics and costs are captured.
- Draft and publish approvals are separate.
- Growth work does not bypass engineering/security review when code or customer data is involved.

---

## MVP 7 — Selective autonomy

### Goal

Automate only stable, reversible, low-risk action classes proven by earlier MVPs.

Possible future candidates:

- Delete verified caches covered by a tested policy.
- Archive superseded non-production artifacts after retention.
- Open low-risk dependency-update proposals.
- Auto-close duplicate or superseded triage items.
- Restart a non-production worker after a bounded health check.

Never default to autonomous:

- model promotion;
- production DB changes;
- live trading changes;
- broad artifact deletion;
- Git merge or deployment;
- customer messaging or publishing;
- spending or subscription changes;
- architecture migrations.

### Usage

Chase approves each autonomy policy, not every individual action. Exceptions and periodic audit summaries are pushed to the executive inbox.

---

## Governance rules across all stages

1. Proposal and integration are separate human gates.
2. Read-only discovery does not authorize implementation.
3. Board state is operational truth; plans and comments do not prove that a card exists or ran.
4. Worker self-report is not verification.
5. No global conformal recalibration offsets.
6. Q10 miscalibration must not be blindly corrected.
7. Probabilities use empirical CDF where required by GameFlow contracts.
8. Advanced stats scraping remains off Railway.
9. Main-context agents do not call Supabase MCP directly for GameFlow DB work.
10. Destructive DB-adjacent actions require independent preflight verification.
11. Chase launches long training, sweep, and broad resource-heavy jobs.
12. Missing telemetry is unknown, not healthy.
13. Push, PR, merge, deploy, publish, and delete are distinct authorizations.
14. Every automated action must have an audit trail and rollback or containment path.

---

## Decisions required before implementation

1. Daily brief generation time in America/New_York.
2. Stable Tailscale hostname/URL and whether to use Tailscale Serve HTTPS or bind directly to the tailnet interface.
3. Whether an optional external outage notifier should ever be added after the dashboard MVP; it is not required for MVP 0.
4. Initial disk and artifact-growth warning thresholds after a read-only baseline.
5. Retention destination for archived artifacts; no object-storage vendor is selected by this roadmap.

These decisions do not block writing the collectors and renderer, but they block real scheduled delivery and cleanup execution.
