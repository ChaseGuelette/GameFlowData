# GameFlow Tech-Debt Census and Agent Execution Program

> **For Hermes:** This is a planning and operating document. Do not begin broad scans, DB work, refactors, or long-running agent loops until Chase approves the relevant phase and lane. Use `gameflow-learning-mode`, `gameflow-explore`, and `gameflow-sql-runner` as applicable.

**Goal:** Build a comprehensive, evidence-backed inventory of GameFlow technical debt, classify and prioritize it, create safe remediation plans, and establish a repeatable local-plus-droplet agent workflow for resolving the queue without losing control of architecture or production safety.

**Architecture:** Treat Chase's current workstation/SSH session as the planning, review, and model-training control plane. Treat the remote droplet as a durable agent execution plane. Treat repository markdown, the tech-debt register, and GBrain as the contract and knowledge layer connecting both. Every agent loop receives a bounded written goal, produces evidence and validation, and stops at an explicit review gate.

**Tech Stack:** GameFlowData, Hermes Agent `/goal`, tmux, SSH/Tailscale, Hermes worktree mode, optional Hermes Kanban, GBrain/GameFlowBrain, pytest/Ruff/typecheck/build checks, Railway/Vercel/Supabase read-only observability, local model-training infrastructure.

---

## 1. Desired outcome

At the end of the program, Chase should have:

1. One authoritative, navigable tech-debt register.
2. Evidence-backed coverage across every major GameFlow subsystem.
3. A clear distinction between defects, structural debt, operational risk, missing capability, stale documentation, and intentionally accepted constraints.
4. A remediation strategy for every confirmed item.
5. A ranked execution backlog that distinguishes urgent safety/reliability work from opportunistic cleanup.
6. Lane-specific migration or improvement plans for large items.
7. A repeatable workflow in which planning happens interactively and bounded Hermes agents execute approved work on the droplet.
8. A verification trail showing which findings are confirmed, rejected, resolved, or still need evidence.

This program should not claim mathematical certainty that every possible debt item was found. “Comprehensive” means every defined audit lane completed its checklist, every existing issue/plan was reconciled, and unresolved coverage gaps are explicit.

---

## 2. Operating model

### 2.1 Workstation / current SSH session: control and training plane

Primary responsibilities:

- interactive planning and architecture decisions;
- reviewing and approving debt classifications;
- running GPU-heavy or data-local model training loops;
- inspecting local artifacts and backtest outputs;
- reviewing agent-produced diffs and reports;
- deciding promotion, deployment, and production actions;
- maintaining the canonical register and master roadmap.

This plane should retain human judgment. Agents may propose priorities, but Chase approves major migrations, production mutations, and resource-heavy work.

### 2.2 Remote droplet: durable agent execution plane

Primary responsibilities:

- run bounded Hermes `/goal` sessions inside persistent tmux sessions;
- perform code archaeology and lane-specific audits;
- execute approved refactors, tests, documentation work, and CI/tooling improvements;
- run independent review agents;
- produce explicit reports, diffs, test output, and blockers;
- continue work while Chase is detached from SSH.

A `/goal` is a standing goal inside one Hermes session; it is not by itself a durable supervisor or project queue. Durable operation requires tmux/systemd process persistence, and multi-item coordination should eventually use Hermes Kanban or another explicit queue.

### 2.3 Repository/GBrain: contract and knowledge plane

Use the following separation:

- `docs/understanding/tech-debt-register.md`: authoritative human-facing debt inventory.
- `.hermes/audits/tech-debt/`: evidence reports from bounded audit lanes.
- `.hermes/plans/tech-debt/`: approved remediation/migration plans.
- existing `.hermes/plans/god-class-migrations/`: structural migration plans that must be reconciled, not duplicated.
- GBrain/GameFlowBrain: durable project facts, lessons, decisions, handoffs, and retrieval.
- Hermes skills: reusable procedures and guardrails, not project status.

Planning documents are contracts between planning and execution. Agent loops should not infer large scopes from chat history alone.

---

## 3. Core principles

1. **Evidence before registration.** No debt item enters as confirmed from vibes alone.
2. **Coverage by lane, not one giant repo scan.** Broad scans create noise and miss context.
3. **Inventory before migration.** Reconcile current plans, known issues, incidents, tests, runtime warnings, and existing register entries before creating new work.
4. **Separate debt discovery from debt fixing.** Audit agents should not silently refactor what they find.
5. **One bounded goal per agent loop.** Never give `/goal` the unbounded instruction “find and fix all tech debt.”
6. **Unique output ownership.** Parallel agents write separate lane reports to avoid merge conflicts.
7. **Human approval at phase gates.** Broad DB work, production changes, heavy compute, and large migrations require Chase's approval.
8. **Behavior-preserving structural work.** Characterization tests precede extraction/refactoring.
9. **Independent verification.** Agent self-reports are not proof; review diffs and rerun focused validation.
10. **Reject false debt.** Some complexity is inherent domain complexity or an accepted constraint.
11. **Do not optimize for file size alone.** Mixed ownership and ambiguous contracts matter more than LOC.
12. **Protect production invariants.** Existing GameFlow modeling, DB, Railway, and trading invariants remain binding.

---

## 4. Debt taxonomy

Every registered item should have one primary type and optional secondary tags.

### Primary types

- **Correctness:** known or likely wrong behavior.
- **Reliability:** retries, idempotency, races, overlap, brittle recovery, partial failure.
- **Safety:** live trading, bankroll, temporal leakage, DB mutation, auth, or production blast radius.
- **Security/privacy:** secrets, auth boundaries, RLS, dependency exposure, unsafe agent permissions.
- **Architecture:** god modules, mixed responsibilities, ambiguous ownership, circular dependencies.
- **Data/schema:** table growth, indexing, migration drift, lineage, retention, deduplication, null semantics.
- **Model/MLOps:** train/serve skew, artifact identity, reproducibility, feature coverage, evaluation/promotion contracts.
- **Performance/cost:** slow queries, excess API usage, compute waste, agent-credit waste, storage bloat.
- **Testing/verification:** missing characterization, brittle tests, untested runtime contracts, weak E2E coverage.
- **Operations/observability:** unclear job state, weak logs, missing alerts, manual recovery, hidden schedules.
- **Developer experience:** fragile setup, inconsistent CLIs, environment drift, difficult local/remote workflows.
- **Documentation/knowledge:** stale or contradictory docs, undocumented boundaries, retrieval/source-routing problems.
- **Product debt:** unfinished flows, UX friction, missing analytics, unclear monetization/marketing infrastructure.
- **Dependency/platform:** stale libraries, deprecated APIs, vendor lock-in, version incompatibility.
- **Agent workflow:** unsafe autonomy, missing scopes, weak handoffs, duplicated agent work, poor state ownership.

### Statuses

- `candidate`
- `confirmed`
- `planned`
- `in-progress`
- `resolved`
- `accepted`
- `rejected`
- `blocked`

### Priority classes

Avoid false precision from arbitrary numeric scores.

- **P0 — immediate safety/correctness:** active risk to money, data integrity, security, or production correctness.
- **P1 — near-term reliability/unblocking:** recurring incidents, promotion blockers, severe workflow friction.
- **P2 — strategic maintainability:** high-value structural debt whose cost compounds with new work.
- **P3 — opportunistic cleanup:** real debt with low current risk.
- **P4 — accepted/watch:** understood constraint or low-value cleanup; monitor rather than schedule.

### Effort classes

- `XS`: bounded investigation or tiny fix.
- `S`: one focused agent session / one small change.
- `M`: several slices with a lane plan.
- `L`: multi-session migration.
- `XL`: program-level redesign requiring staged architecture decisions.

---

## 5. Required debt-entry schema

Each item in `docs/understanding/tech-debt-register.md` should include:

```markdown
### TD-###: Title

Status: candidate | confirmed | planned | in-progress | resolved | accepted | rejected | blocked
Priority: P0 | P1 | P2 | P3 | P4
Primary type: <taxonomy type>
Area: <subsystem/lane>
Effort: XS | S | M | L | XL
Confidence: high | medium | low

Evidence:
- Exact path, symbol, test, log, metric, GBrain page, incident, or user observation.

Why it matters:
- Concrete operational, product, maintenance, or comprehension impact.

Failure mode / risk if ignored:
- What can break or become more expensive.

Current workaround:
- Existing mitigation, if any.

Dependencies and interactions:
- Related debt IDs, migration plans, services, tables, or model lanes.

Solution options:
1. Lowest-risk option.
2. More complete option.
3. Accept/shelf option when reasonable.

Recommended approach:
- Preferred option and rationale.

Safe first step:
- Smallest bounded evidence or implementation action.

Validation / done condition:
- Exact evidence proving resolution or justified acceptance.

Owner decision needed:
- Confirm / shelf / reject / approve plan / approve production work.

Sources reviewed:
- Code, docs, tests, runtime, DB, GBrain.
```

Resolved and rejected entries remain in the register so future agents do not rediscover them.

---

## 6. Audit coverage lanes

Each lane gets its own bounded audit report. Reports identify findings; they do not implement fixes.

### Lane A: Existing debt and plan reconciliation

Scope:

- `docs/understanding/tech-debt-register.md`
- `operations/known-issues`
- root `ISSUES.md`
- `.hermes/plans/god-class-migrations/`
- `.hermes/plans/mlb-stat-suite-rebuild/`
- recent incident/remediation plans and handoffs

Questions:

- Which entries are current, stale, resolved, duplicated, contradicted, or missing evidence?
- Which plan-only lanes were implemented partially or completely?
- Which plans are obsolete or superseded?
- Which previously fixed issues need regression guards rather than active debt items?

Output:

- `.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md`

### Lane B: Python architecture and module boundaries

Path scopes:

- `src/models/`
- `src/models/mlb/`
- `src/backtesting/`
- `src/orchestration/`
- `src/paper_trading/`
- `src/processing/`
- `src/scrapers/`
- `src/arbitrage/`

Checks:

- large classes/functions and mixed responsibility;
- private-helper coupling and facade regrowth;
- duplicate policies/contracts;
- circular/import-time side effects;
- scattered state transitions;
- unclear service ownership;
- dead compatibility paths.

Output:

- `.hermes/audits/tech-debt/01-python-architecture.md`

### Lane C: Testing, CI, and verification quality

Path scopes:

- `tests/`
- dashboard tests/config
- CI/workflow configuration
- characterization and E2E contracts

Checks:

- untested high-risk paths;
- skipped/flaky/slow tests;
- mismatch between local and CI commands;
- missing end-to-end tests for original failure modes;
- missing parity and anti-regrowth guards;
- fixture duplication and brittle mocks;
- absence or staleness of CI/CD claims.

Output:

- `.hermes/audits/tech-debt/02-testing-ci-verification.md`

### Lane D: Database, schema, data lifecycle, and query performance

Evidence sources:

- migration files and schema wrappers;
- SQL-runner read-only summaries;
- known table growth/query incidents;
- Railway/Supabase logs and performance advisors when approved.

Checks:

- oversized tables and retention/partitioning needs;
- missing/unsafe indexes;
- migration drift and untracked manual DDL;
- pooler/role/RLS inconsistencies;
- orphaned tables/columns;
- data lineage and deduplication ambiguity;
- dangerous broad backfills;
- read/write path mismatch.

Output:

- `.hermes/audits/tech-debt/03-database-data-lifecycle.md`

Safety:

- Main context never calls Supabase MCP directly.
- Use GameFlow SQL-runner isolation.
- Read-only first.
- Probe scope and obtain approval before heavy queries, DDL, indexes, or backfills.

### Lane E: Scheduler, jobs, ingestion, and runtime operations

Path scopes:

- `src/orchestration/`
- relevant scrapers and processing entrypoints
- Railway config/logs
- `docs/understanding/railway-scheduler.md`

Checks:

- duplicate schedules and overlap behavior;
- locks, retries, timeouts, idempotency;
- job registry/timing ownership;
- stale docs versus runtime truth;
- partial failures and recovery;
- rate limits and provider dependencies;
- observability and actionable alerts;
- workstation-only versus Railway-safe workloads.

Output:

- `.hermes/audits/tech-debt/04-scheduler-ingestion-operations.md`

### Lane F: Model training, artifacts, evaluation, and promotion

Path scopes:

- `src/models/`
- `src/models/mlb/`
- `src/backtesting/`
- targeted artifact metadata
- relevant scripts and reports

Checks:

- artifact identity and manifest pinning;
- train/backtest/inference feature parity;
- deterministic/reproducible training;
- leaked/default artifact directories;
- fragmented stat-suite workflows;
- model-family controls and ablation validity;
- quote-clean/CLV/ranker gate consistency;
- promotion and rollback contracts;
- stale unsupported model/stat paths;
- compute inefficiency and repeat-work risk.

Output:

- `.hermes/audits/tech-debt/05-model-mlops-promotion.md`

This lane must retrieve Hard Facts, Critical Invariants, and relevant lessons before recommendations.

### Lane G: Trading, bankroll, and market integration safety

Path scopes:

- `src/paper_trading/`
- Kalshi/Polymarket integrations
- arbitrage path
- trading orchestration jobs

Checks:

- paper/live parity;
- duplicated selection/sizing/ledger logic;
- lifecycle state-machine gaps;
- idempotency and reconciliation;
- unsupported-stat guards;
- stale live code paths;
- bankroll/risk controls;
- alerting and manual kill switches.

Output:

- `.hermes/audits/tech-debt/06-trading-market-safety.md`

No live trading changes occur during the audit.

### Lane H: Dashboard, APIs, auth, UX, and product debt

Path scopes:

- `dashboard/src/app/`
- `dashboard/src/components/`
- `dashboard/src/lib/`
- `dashboard/src/types/`

Checks:

- god routes/components;
- auth/RLS boundary assumptions;
- duplicate fetch/mapping/view logic;
- hook correctness;
- weak loading/error/empty states;
- accessibility/mobile issues;
- missing pagination and multi-instance rate limiting;
- stale/unreachable product routes;
- Stripe/monetization incompleteness;
- analytics/marketing/product feedback gaps.

Output:

- `.hermes/audits/tech-debt/07-dashboard-product.md`

### Lane I: Infrastructure, deployment, security, and dependency health

Evidence sources:

- Railway/Vercel service configuration;
- deployment/build/runtime logs;
- dependency manifests and lockfiles;
- secret/config patterns;
- current remote workstation setup.

Checks:

- config drift across workstation/droplet/Railway/Vercel;
- manual deployment/recovery steps;
- missing health checks and rollback paths;
- overprivileged roles/tokens;
- dependency age/vulnerabilities;
- service ownership and single points of failure;
- backup/restore confidence;
- cost and resource limits.

Output:

- `.hermes/audits/tech-debt/08-infra-security-dependencies.md`

### Lane J: Hermes, GBrain, documentation, and agent workflow

Scope:

- `docs/understanding/`
- GameFlow skills and AGENTS contract
- GBrain source routing/health
- handoff/wrap-up workflow
- droplet agent execution workflow

Checks:

- direct-read/source-routing fragility;
- stale metadata warning ambiguity;
- contradictory docs and duplicate sources of truth;
- missing audit-to-register promotion flow;
- unsafe or unbounded `/goal` prompts;
- session/process persistence;
- multi-agent conflict handling;
- agent credit/model-routing efficiency;
- missing E2E verification from real agent paths.

Output:

- `.hermes/audits/tech-debt/09-agent-knowledge-workflow.md`

### Lane K: Performance, storage, and cost efficiency

Checks:

- tracked and generated repository bloat;
- artifact/result retention;
- repeated expensive queries;
- duplicate model runs;
- API/provider spend;
- agent model selection by task complexity;
- unnecessary context/tool loading;
- long-running job resource contention.

Output:

- `.hermes/audits/tech-debt/10-performance-storage-cost.md`

Use tracked-file accounting and targeted artifact directories; do not recursively scan heavy `/mnt/c` trees.

---

## 7. Audit-agent contract

Every droplet audit agent receives a prompt containing:

```text
Goal: Audit one named GameFlow tech-debt lane and write one evidence report.

Read first:
- AGENTS.md
- relevant Hermes skills
- existing tech-debt register
- named existing plans/GBrain pages

Allowed scope:
- exact paths and external read-only evidence sources

Forbidden:
- production mutation
- DB writes or broad queries
- code refactors
- unrelated file exploration
- inventing findings from file size alone
- modifying the canonical debt register

Required output per finding:
- proposed debt title/type/status/priority/effort/confidence
- exact evidence
- why it matters and failure mode
- current workaround
- solution options
- recommended safe first step
- validation/done condition
- duplicates or interactions with existing IDs/plans

Required closeout:
- paths/sources checked
- coverage gaps
- confirmed candidates
- rejected suspicions
- report path
```

Agents should use read-only goals during discovery. They write only their unique lane report.

---

## 8. Droplet execution pattern

### 8.1 One persistent session per bounded lane

Use tmux so SSH detach does not kill the Hermes session. Conceptual pattern:

```bash
tmux new-session -d -s gf-debt-01 'cd ~/GameFlowData && hermes -w'
tmux attach -t gf-debt-01
```

Inside Hermes:

```text
/goal Audit the Python architecture lane using the approved audit contract. Write only .hermes/audits/tech-debt/01-python-architecture.md. Stop after validation and summary; do not refactor code.
```

Exact paths/commands must be validated against the droplet's current checkout and Hermes profile before launching the batch.

### 8.2 Worktree discipline

- Use `hermes -w` for agents that may write files.
- Give each agent a unique branch/worktree and unique report path.
- Do not let parallel agents edit the canonical register.
- Consolidation happens in one designated integration session after review.

### 8.3 Agent lifecycle

Each loop follows:

1. Read contract and lane scope.
2. Inventory existing evidence.
3. Run bounded discovery.
4. Write lane report.
5. Self-check report schema and scope.
6. Stop at review gate.
7. Independent reviewer checks evidence and false positives.
8. Chase/integration session adjudicates findings.

### 8.4 When to use `/goal`, delegation, Kanban, or cron

- `/goal`: one continuing bounded mission in one persistent Hermes session.
- `delegate_task`: short parallel investigations whose result can die with the parent session.
- tmux-spawned Hermes: hours-long work that must survive SSH detach.
- Hermes Kanban: multiple dependent backlog tasks, workers, blockers, and resumable execution.
- cron: scheduled audits/watchdogs, not open-ended migration work.

The likely mature workflow is planning docs plus Kanban-dispatched lane tasks. `/goal` remains useful for a single lane or remediation loop.

---

## 9. Credit and compute allocation

Use expensive reasoning where it changes decisions; use cheaper/local compute for mechanical work.

### Strong API model

Use for:

- architecture synthesis;
- prioritization and tradeoff analysis;
- security/safety review;
- adjudicating conflicting audit findings;
- writing high-impact migration plans;
- reviewing promotion/trading semantics.

### Lower-cost API or local coding model

Use for:

- mechanical file inventory;
- locating tests/callsites;
- implementing an approved narrow plan;
- adding characterization tests;
- documentation formatting;
- repetitive static checks.

### Workstation compute

Use for:

- GPU model training;
- sweep/ablation/backtest loops;
- large local artifact analysis;
- tasks requiring local DB/data proximity.

### Droplet compute

Use for:

- persistent Hermes orchestration;
- source audits;
- test/lint/typecheck/build loops that fit available resources;
- documentation and migration execution;
- independent review agents.

Do not run duplicate model training merely because compute is available. Artifact identity and experiment manifests must prevent repeated or misdirected runs.

---

## 10. Program phases and approval gates

### Phase 0: Reconcile the current scaffold

Tasks:

1. Preserve the existing `docs/understanding/tech-debt-register.md` as the starting register.
2. Inventory current TD-001 through TD-006.
3. Reconcile `operations/known-issues`, `ISSUES.md`, existing migration plans, and recent handoffs.
4. Classify old plan sets as current, partial, complete, superseded, or stale.
5. Create the audit directory/index only after Chase approves this program.

Gate:

- Chase approves taxonomy, lane scope, and output locations.

### Phase 1: Run bounded discovery audits

Tasks:

1. Execute Lane A first so parallel agents do not rediscover known items.
2. Run lanes B-K in controlled parallel batches, no more than available isolated workers can safely support.
3. Require unique reports and explicit coverage gaps.
4. Run an independent review pass for each report.

Gate:

- All lanes have reports or explicit blocked reasons.
- No candidate is promoted solely from an agent assertion.

### Phase 2: Consolidate and adjudicate

Tasks:

1. Deduplicate findings across lanes.
2. Link related items and existing migration plans.
3. Classify each finding: Confirm, Shelf, Reject, Needs Evidence.
4. Assign priority and effort classes.
5. Update the canonical register in one integration session.
6. Create a coverage matrix showing each lane's evidence sources and unresolved gaps.

Gate:

- Chase reviews P0/P1 items and all proposed large migrations.

### Phase 3: Produce remediation plans

Tasks:

1. XS/S items receive a concise fix brief with exact validation.
2. M/L/XL items receive lane-specific markdown plans.
3. Existing god-class/stat-suite plans are updated rather than duplicated.
4. Plans separate investigation, behavior-preserving migration, behavior changes, and production cutover.
5. Every plan names dependencies, rollback, tests, and done conditions.

Gate:

- Chase selects the first execution wave.

### Phase 4: Execute through droplet agents

Tasks:

1. Convert approved plans into bounded goals or Kanban tasks.
2. Use isolated worktrees.
3. Require scoped diffs and focused tests.
4. Run independent spec and code-quality reviews.
5. Merge only after local/control-plane verification.
6. Update register status and evidence after each resolved item.

Gate:

- No automatic production deployment unless explicitly approved.

### Phase 5: Prevent recurrence

Tasks:

1. Add characterization and anti-regrowth tests.
2. Add CI/static checks where they provide clear value.
3. Update understanding docs and GBrain durable facts/lessons.
4. Add a periodic read-only debt radar only after the manual process proves useful.
5. Track debt introduced by new features in the same register.

Done condition:

- Debt discovery becomes part of normal project work rather than a one-time cleanup campaign.

---

## 11. Initial evidence already known

The program begins with evidence, not an empty slate:

- `docs/understanding/tech-debt-register.md` already contains TD-001 through TD-006.
- `.hermes/plans/god-class-migrations/README.md` tracks ten structural lanes; lanes 01 and 02 are marked complete, lane 03 core-complete, and lanes 04-10 remain unimplemented or deferred.
- `.hermes/plans/mlb-stat-suite-rebuild/` contains a separate multi-document rebuild program.
- `operations/known-issues` includes DB table growth, API rate limiting, pagination/mobile debt, deferred issue IDs, and GBrain retrieval concerns, but it explicitly warns that entries may be stale.
- Handoff 106 established the comprehension/debt scaffold and confirmed scheduler/documentation complexity.
- The latest resume exposed active GBrain direct-read/list source-routing fragility while source-scoped query and remote canonical markdown remained available.
- The latest model handoff showed a concrete artifact-identity failure mode: evaluation ran against the production artifact rather than the intended newly trained model directory.

These are starting points for reconciliation, not a final debt inventory.

---

## 12. Validation and quality controls

### Audit completeness

- Every lane report lists paths/evidence checked.
- Every lane report lists what it could not inspect.
- Existing plans/issues are reconciled before new IDs are assigned.
- Suspicions that failed investigation are recorded as rejected to avoid repeat work.

### Finding quality

- Every candidate has exact evidence.
- Risk is a concrete failure mode, not “hard to maintain.”
- The proposed first step is bounded and safe.
- Validation is observable and repeatable.
- Large refactors are not recommended when a smaller mitigation or acceptance decision is better.

### Agent quality

- Agent writes are restricted to unique report/plan paths.
- Each report receives an independent evidence review.
- Code changes receive scoped diff and test verification.
- Original failure modes are retested end-to-end when behavior changes.

### Production safety

- No direct Supabase MCP use from main context.
- No blind indexes, backfills, or broad data scans.
- No live trading enablement.
- No model promotion from audit-only evidence.
- No Railway advanced-stats scraping.
- No production deployment without explicit approval.

---

## 13. Immediate next planning decisions

Before starting Phase 0, Chase should approve or adjust:

1. Whether this full-project scope includes product/marketing debt alongside engineering debt. This plan includes it as a separate product lane rather than mixing it with code architecture.
2. Whether `.hermes/audits/tech-debt/` and `.hermes/plans/tech-debt/` are the desired durable paths.
3. Whether the droplet should begin with manual tmux + `/goal` sessions or whether to establish Hermes Kanban first.
4. How many parallel droplet agents are safe given CPU/RAM, API rate limits, and repository conflict risk.
5. Which model/provider tiers should be assigned to discovery, synthesis, implementation, and review.
6. Whether runtime/DB audits should be part of the first pass or deferred until code/docs reconciliation exposes targeted questions.
7. Whether `phone.txt`, currently untracked in the local repo, is unrelated and should remain outside this program.

Recommended default:

- approve the taxonomy and paths;
- run Lane A locally first;
- validate one droplet audit loop end-to-end with Lane J or Lane B;
- review the quality of that report;
- only then fan out the remaining discovery lanes.

---

## 14. Files likely to be created or updated after approval

Planning/audit artifacts:

- Create: `.hermes/audits/tech-debt/README.md`
- Create: `.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md`
- Create: `.hermes/audits/tech-debt/01-python-architecture.md`
- Create: `.hermes/audits/tech-debt/02-testing-ci-verification.md`
- Create: `.hermes/audits/tech-debt/03-database-data-lifecycle.md`
- Create: `.hermes/audits/tech-debt/04-scheduler-ingestion-operations.md`
- Create: `.hermes/audits/tech-debt/05-model-mlops-promotion.md`
- Create: `.hermes/audits/tech-debt/06-trading-market-safety.md`
- Create: `.hermes/audits/tech-debt/07-dashboard-product.md`
- Create: `.hermes/audits/tech-debt/08-infra-security-dependencies.md`
- Create: `.hermes/audits/tech-debt/09-agent-knowledge-workflow.md`
- Create: `.hermes/audits/tech-debt/10-performance-storage-cost.md`
- Update after adjudication: `docs/understanding/tech-debt-register.md`
- Create approved remediation plans under `.hermes/plans/tech-debt/`
- Update existing lane plans where they already cover a finding.

No production code changes belong to the planning/discovery phases.

---

## 15. First pilot goal

After Chase approves the program, use a small pilot before parallel fan-out:

```text
/goal Reconcile the existing GameFlow tech-debt inventory. Read AGENTS.md, docs/understanding/tech-debt-register.md, operations/known-issues through GBrain, root ISSUES.md, the god-class migration index, the MLB stat-suite rebuild index, and recent relevant handoffs. Write only .hermes/audits/tech-debt/00-existing-inventory-reconciliation.md. For each existing item or plan, classify it as current, stale, resolved, partial, superseded, duplicate, or needs evidence. Do not edit code or the canonical register. Stop after validating the report and summarizing coverage gaps.
```

Pilot success criteria:

- survives SSH detach/reattach;
- respects scope;
- produces exact evidence rather than generic advice;
- does not modify production code or unrelated files;
- clearly separates current debt from stale records;
- gives enough information for Chase to adjudicate without rereading the entire project history.
