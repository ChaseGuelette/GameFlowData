# Infrastructure, Security, and Dependency Audit

**Audit lane:** 08 — infrastructure, security, deployment, dependencies, and recovery
**Audit date:** 2026-07-18
**Mode:** Read-only repository audit; this report is the only file written
**Scope:** Tracked deployment manifests/configuration, Railway/Nixpacks/Vercel setup, GitHub workflows, Python/npm manifests and locks, source-level environment-variable contracts, backup/recovery/archive code and runbooks, deployment privilege/readiness/rollback/observability boundaries, and existing audits 00/02/04/07
**Excluded:** `.env` and credential files, secret values, live variables/services/log APIs, deployed state, package installation/update, vulnerability-service queries, deploys, DB access, source/config/plan/register edits, and auth findings already fully owned by report 07

## Executive assessment

The repository can build and deploy through Railway and Vercel, and current tracked-file scanning found no recognizable live private key, provider token, JWT, or unredacted credential URL. The strongest infrastructure debt is instead reproducibility and recovery ambiguity:

1. The Python production environment is installed from a partially pinned `requirements.txt`, while the hash-bearing `uv.lock` describes a different, much smaller pyproject dependency graph and is unused by Railway/CI.
2. The local sync utility is not a recoverable backup: a full refresh commits `TRUNCATE` before import, continues after table failures, and returns process success even with failed tables. No tracked `pg_dump`/`pg_restore`, point-in-time-recovery, restore-test, RPO, or RTO runbook was found.
3. GitHub Actions are tag-pinned rather than commit-SHA-pinned, omit an explicit token-permission boundary, and allow a third-party action to write source/test commits. Report 02 already owns the moving-ref/auto-fix correctness problem; this report adds the supply-chain and privilege dimension.
4. No tracked environment template or machine-checkable deployment variable contract exists. Current docs and startup checks omit material Railway/Vercel variables and use stale Stripe names.
5. Railway has process restart policy but no deploy-readiness contract or tracked rollback procedure. The scheduler intentionally starts with missing “required” variables, so a deployment can be process-alive while many jobs are unrunnable.
6. Duplicate Vercel configs encode two implicit project-root modes, and the repo-root mode uses `npm install` while CI uses `npm ci`.
7. Production archival retains seven days by code default while scheduler comments/docstrings say thirty days.
8. A 231 KB Railway log export is tracked under `.hermes/tmp`; no standard secret value was detected, but it records operational messages and environment-variable names and creates an avoidable future leakage path.

Existing reports remain authoritative for their lanes:

- Report 02 owns CI mutation, moving-ref validation, coverage/typecheck, and dashboard test gaps.
- Report 04 owns scheduler runtime behavior, subprocess/overlap/telemetry, CDN-only enforcement, advisory startup validation, and stale schedule docs.
- Report 07 owns dashboard route authorization, service-role use, subscription capability enforcement, Stripe webhook/checkout behavior, and dashboard runtime observability.
- This report does not duplicate those findings; it records infrastructure interactions and uncovered deploy/supply-chain/recovery boundaries.

## Method and evidence boundaries

### Sources inspected

- `AGENTS.md` and the active `gameflow-explore` guidance
- `.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md`
- `.hermes/audits/tech-debt/02-testing-ci-verification.md`
- `.hermes/audits/tech-debt/04-scheduler-ingestion-operations.md`
- `.hermes/audits/tech-debt/07-dashboard-product.md`
- `railway.toml`, `nixpacks.toml`, both Vercel configs, `.python-version`, `.gitignore`
- `.github/workflows/ci.yml`
- `pyproject.toml`, `requirements.txt`, `requirements-dev.txt`, `uv.lock`
- root and dashboard npm manifests/locks
- current Railway/scheduler/deployment docs and selected historical infrastructure notes
- `scripts/sync_local_db.py`, `scripts/run_full_archival.py`, and `src/orchestration/archive_old_props_job.py`
- source-level environment references in allowed deployment/orchestration/dashboard/docs/script paths
- tracked-file names and a redacting secret-pattern scan over current tracked text files

### Mechanical evidence

- Current tracked deployment manifests include `railway.toml`, `nixpacks.toml`, `vercel.json`, and `dashboard/vercel.json`; no Dockerfile, Compose file, Procfile, `runtime.txt`, or `.tool-versions` is tracked.
- `.python-version` specifies Python 3.11 (`.python-version:1`); CI also requests 3.11 (`.github/workflows/ci.yml:18-21,46-49`), and Nixpacks requests `python311` (`nixpacks.toml:4-5`).
- Both npm lockfiles use lockfile version 3. The root lock contains 598 package records; the dashboard lock contains 587.
- Source-level extraction found at least 29 distinct direct environment-variable names in the bounded deployment/orchestration/dashboard/docs/script paths. This is a lower bound because dynamic lookups and nonstandard wrappers were not inferred.
- A redacting current-HEAD scan opened no credential-named files and inspected 1,333 tracked text files. It found no recognizable private-key header, live provider-token shape, JWT shape, or unredacted credential URL. Placeholder/masked database URLs were found in docs/code/tests and were not treated as secrets.
- `.hermes/tmp/railway_logs_4h.jsonl` is tracked, is 231,306 bytes, and contains 1,081 JSONL rows with top-level keys `level`, `message`, and `timestamp`. Redacted term-only inspection found environment-variable names in early lines but did not print or inspect their values.
- Targeted tracked-doc/source searches found no `pg_dump`, `pg_restore`, disaster-recovery, restore-test, RPO, or RTO procedure.

### Severity vocabulary

- **Critical:** no trustworthy recovery path for destructive/data-loss scenarios, or a direct credential/privilege exposure with immediate broad impact.
- **High:** deployment or supply-chain behavior can silently change the running tree, dependencies, access boundary, or production readiness.
- **Medium:** operator/config drift can cause incorrect retention, difficult rollback, ambiguous deploy ownership, or avoidable information exposure.
- **Low:** bounded hygiene issue with a current mitigation and no demonstrated production failure.

---

## Findings

### I-01 — High: there is no verifiable backup/restore contract, and the local sync utility can produce a partial copy with a successful exit

**Exact evidence**

- `scripts/sync_local_db.py:2-24` describes a one-way Supabase-to-local sync for offline training/backtesting, not a backup or restore system.
- Its table registry is an explicit subset of domain tables (`scripts/sync_local_db.py:50-96`); it does not claim to preserve roles, grants, RLS policies, functions, triggers, extensions, storage objects, or all schemas.
- Schema reflection deliberately removes foreign-key constraints before creating local tables (`scripts/sync_local_db.py:255-284`).
- A full refresh executes and commits `TRUNCATE TABLE ... CASCADE` before the remote export is imported (`scripts/sync_local_db.py:411-415`). The data import commits later at `:417-480`, so export/import failure can leave the local table empty.
- Per-table exceptions are logged, rolled back, appended to `failed`, and processing continues (`scripts/sync_local_db.py:586-605`). The final block only logs the failed table names and does not return a nonzero exit (`:611-622`).
- The remote session uses timeouts/application naming but does not enforce `default_transaction_read_only=on` (`scripts/sync_local_db.py:212-218,561-579`). Current statements are reads, but least privilege is conventional rather than session-enforced.
- `src/orchestration/archive_old_props_job.py:50-70` atomically moves rows between two tables in the same production database. That is retention/archival, not an independent backup.
- Targeted tracked searches found no `pg_dump`, `pg_restore`, restore drill, provider-PITR procedure, RPO, or RTO documentation.

**Concrete failure mode**

An operator treats the local sync as a recovery copy, runs `--full`, and one table fails after its committed truncate. The script continues, reports failed tables only in logs, and exits zero; automation or a human can accept an incomplete local copy. In an actual production-loss event, that copy also lacks database security/schema objects and any unregistered tables, while the in-database archive is unavailable if the same database/project is lost or corrupted.

**Confidence:** High for repository behavior and absence of a tracked recovery contract; unknown whether Supabase/provider backups exist live because live services were excluded.

**Current mitigation**

- Remote export statements are currently read-only in code and database URLs are redacted before logging (`scripts/sync_local_db.py:194-218,551-553`).
- Incremental paths stage and upsert by primary key (`scripts/sync_local_db.py:442-480`).
- The dense CLV full-refresh path has an explicit large-run guard (`scripts/sync_local_db.py:536-549`).
- Production archival inserts and deletes in one database transaction per batch (`archive_old_props_job.py:50-70`).

**Existing-report interaction**

- Report 00 leaves production DB/data-lifecycle truth as a coverage gap (`00-existing-inventory-reconciliation.md:383-396`).
- Report 04 covers archive scheduling and job telemetry but does not claim the archive is a backup (`04-scheduler-ingestion-operations.md:72-118`).
- This finding does not authorize DB inspection or a restore attempt.

**Safe evidence step**

In a future approved no-network unit slice, inject fake local/remote connections into `sync_table`/`main`, force failure immediately after full-refresh truncate and on one later table, and assert rollback/state plus process exit. Separately produce a paper recovery inventory from schema migrations/provider documentation: protected assets, backup owner, retention, encryption, PITR availability, RPO/RTO, and a disposable restore-test procedure. Do not use production data or credentials.

**Done condition**

A tracked recovery runbook distinguishes retention/archive, local analytical replica, logical backup, and provider PITR; identifies every protected asset and owner; defines RPO/RTO and rollback/restore commands; records a successful disposable restore drill; and verifies row/schema/policy/function integrity. The sync utility fails nonzero on any failed table, never commits destructive local refresh before a recoverable replacement is ready, and enforces remote read-only access.

---

### I-02 — High: Railway and CI install an unhashed dependency graph that conflicts with the repository’s hash-bearing lock

**Exact evidence**

- Railway creates a venv and runs `pip install -r requirements.txt` (`nixpacks.toml:15-19`). CI installs the same file and then `requirements-dev.txt` (`.github/workflows/ci.yml:51-55`). Neither path consumes `uv.lock` or uses hashes.
- `requirements.txt:1-16` pins many direct packages, but `discord.py`, `aiohttp`, `Pillow`, `pybaseball`, `cryptography`, and `rich` use lower bounds (`requirements.txt:17-22`). Transitive dependencies are not locked or hash-verified.
- `pyproject.toml:6-17` declares a much smaller runtime set than `requirements.txt`, including SQLAlchemy `2.0.37` and python-dotenv `1.2.1` (`:13,16`). `requirements.txt` instead installs SQLAlchemy `2.0.46` and python-dotenv `1.0.1` (`requirements.txt:8,13`).
- `uv.lock` is hash-bearing but follows pyproject: the root package metadata lists only the pyproject runtime dependencies (`uv.lock:199-244`), python-dotenv resolves to `1.2.1` (`uv.lock:720-727`), and SQLAlchemy resolves to `2.0.37` (`uv.lock:809-817`). It does not represent the Railway scientific/orchestration dependency set.
- `pyproject.toml:29-31` also permits any setuptools version `>=61.0` for builds rather than locking build tooling.

**Concrete failure mode**

Two clean installs from the same commit can resolve different transitive versions, especially through the six lower-bound production requirements. A developer using `uv sync` receives a materially different environment from Railway/CI and may not install pandas/numpy/xgboost/APScheduler/Discord dependencies at all. A dependency or transitive release can therefore break the next Railway build without a source change, while the checked-in hash lock gives false confidence because deployment ignores it.

**Confidence:** High.

**Current mitigation**

- Python major/minor is consistently 3.11 across `.python-version`, CI, and Nixpacks.
- Most direct scientific dependencies are exact-pinned in `requirements.txt`.
- `uv.lock` contains registry hashes for the graph it actually represents.
- CI installs the same two requirements files used for the Python test environment, reducing but not eliminating deployment drift.

**Existing-report interaction**

- Report 02 identifies SQLAlchemy/python-dotenv version drift and states that compatibility across pyproject/requirements was not installed or tested (`02-testing-ci-verification.md:295-307`).
- This finding promotes the deploy/supply-chain consequence; it does not duplicate report 02’s coverage/typecheck findings.

**Safe evidence step**

Without installing packages, generate and review a proposed single authority mapping: production groups, dev groups, Python version, lock generator, hash enforcement, Railway command, and CI command. In a disposable future build lane, resolve once under Python 3.11, record the lock diff, and compare import/version inventories between CI and a Railway-equivalent container/build before changing production.

**Done condition**

One declared dependency source produces one complete, hash-verified production lock including scientific/orchestration dependencies; Railway and CI install it immutably; dev dependencies are a defined group; pyproject/requirements no longer disagree; build tooling is constrained; and a clean Railway-equivalent build plus CI import smoke proves the same versions from the same commit.

---

### I-03 — High: GitHub Actions’ privileged write path is tag-pinned and has no explicit least-privilege token contract

**Exact evidence**

- Every workflow action is referenced by a mutable major-version tag: `actions/checkout@v4`, `actions/setup-python@v5`, `stefanzweifel/git-auto-commit-action@v5`, and `actions/setup-node@v4` (`.github/workflows/ci.yml:16,18,30,42,46,71,73`). No action is commit-SHA-pinned.
- The workflow has no top-level or job-level `permissions:` declaration (`.github/workflows/ci.yml:1-88`). Effective `GITHUB_TOKEN` permissions therefore depend on repository/org defaults and event behavior rather than a reviewed file contract.
- The third-party auto-commit action is intentionally placed after `ruff --fix` and allowed to commit `src/**/*.py` and `tests/**/*.py` (`.github/workflows/ci.yml:23-33`).
- Python tests then check out a branch ref after that write-capable job (`.github/workflows/ci.yml:35-44`).

**Concrete failure mode**

A compromised or unexpectedly changed action tag executes in a job whose intended purpose requires repository writes. Without explicit workflow permissions, the blast radius is governed by external defaults. Independently, the current design lets generated source/test changes land outside normal review and lets later jobs validate a different tree.

**Confidence:** High for the static privilege/pinning boundary; branch protection and actual repository token defaults were not inspected.

**Current mitigation**

- The write-capable job is push-only (`.github/workflows/ci.yml:10-13`), not pull-request-triggered.
- Its file pattern is limited to Python files under `src/` and `tests/` (`:29-33`).
- GitHub-hosted runners are ephemeral, and no repository secrets are explicitly passed to the workflow in current YAML.

**Existing-report interaction**

- Report 02 TV-01 fully owns auto-mutation and moving-ref correctness (`02-testing-ci-verification.md:52-78`). This finding adds action provenance and token least privilege.
- A single remediation should address both reports: immutable event-SHA checks, no CI source mutation, SHA-pinned actions, and explicit permissions.

**Safe evidence step**

Read-only review of repository Actions settings and one prior workflow’s permission/checkout logs, without exposing token values. Produce a proposed permissions matrix: default `contents: read`; no write token for lint/test/build; any separately approved bot write path isolated to a dedicated workflow/environment with pinned action SHAs.

**Done condition**

All third-party actions are pinned to reviewed full commit SHAs with an update process; the main CI workflow declares minimal read permissions; no lint/test/build job can push; event-SHA immutability is tested; and any exceptional write automation is isolated, auditable, branch-protected, and unable to access unrelated secrets.

---

### I-04 — High: environment-variable ownership is dispersed, incomplete, and stale across deploy surfaces

**Exact evidence**

- No tracked `.env.example`, `.env.sample`, environment schema, or equivalent machine-readable template appeared in the tracked inventory. `.gitignore:6-11` correctly ignores `.env` but supplies no tracked contract.
- Railway startup labels only `DATABASE_URL`, `ODDS_API_KEY`, and `RAPIDAPI_KEY` required, plus seven optional values (`src/orchestration/scheduler.py:505-520`). Missing required variables only warn and startup continues (`:522-543`).
- The bounded source inventory found at least 29 directly referenced names, including live-trading, arbitrage, Discord, MLB/NBA trading, Stripe, Supabase service-role, subscription, and public-site variables. This exceeds the scheduler and deployment-doc inventories.
- `brain/Infrastructure/Railway-Setup.md:11-15` lists four variables and calls `DISCORD_CHANNEL_ALERTS` a webhook URL, while the newer deployment guide configures it as a channel ID (`docs/railway_deployment.md:52-57`).
- `brain/Infrastructure/Vercel-Setup.md:10-14` lists only Supabase public URL/key and Anthropic. It omits `SUPABASE_SERVICE_ROLE_KEY`, `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`, `STRIPE_PRICE_MONTHLY`, `STRIPE_PRICE_ANNUAL`, `NEXT_PUBLIC_SITE_URL`, and `SUBSCRIPTION_REQUIRED`, all referenced by current dashboard code.
- The subscription plan uses stale names `STRIPE_PRICE_ID` and `NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY` (`docs/paid_subscription_plan.md:141-147`), while current code expects separate `STRIPE_PRICE_MONTHLY` and `STRIPE_PRICE_ANNUAL` (`dashboard/src/lib/stripe.ts:15-18`).
- Supabase server clients silently substitute placeholders for missing public URL/key (`dashboard/src/lib/supabase/server.ts:7-9`), while the admin client fails closed when service-role credentials are missing (`dashboard/src/lib/supabase/admin.ts:3-9`).

**Concrete failure mode**

A new Railway/Vercel environment is configured from tracked docs and appears to build/start, but individual jobs, Stripe paths, paid access, alerts, or service-role APIs fail only when invoked. A stale Stripe variable name can produce empty price IDs. Placeholder Supabase fallback can turn a configuration error into late auth/network failures rather than a clear readiness failure. Operators cannot determine which service owns a variable, whether it is required under a gate, whether it is public/server-only, or what rotation/redeploy scope applies.

**Confidence:** High for repository drift; live deployed variables were intentionally not inspected.

**Current mitigation**

- Secret files are ignored (`.gitignore:6-11`).
- Server-only Stripe and admin Supabase helpers throw when their core secrets are absent (`dashboard/src/lib/stripe.ts:5-11`; `dashboard/src/lib/supabase/admin.ts:3-9`).
- Railway logs a startup inventory for a subset of variables (`scheduler.py:505-543`).
- Trading and arbitrage gates default off in current scheduler/job logic, reducing accidental activation risk (report 04’s environment matrix at `04-scheduler-ingestion-operations.md:83-118`).

**Existing-report interaction**

- Report 04 E-09 owns job-aware scheduler environment validation (`04-scheduler-ingestion-operations.md:381-411`).
- Report 07 owns the authorization and paid-capability behavior behind dashboard variables (`07-dashboard-product.md:60-201,411-424`).
- This finding is the cross-service contract/deployment layer; it must not weaken default-off trading gates or expose secret values.

**Safe evidence step**

Generate a values-free matrix from source references and manifests: name, service, owner, public/server-secret classification, required condition/gate, default, validation point, rotation impact, and safe local/CI placeholder policy. Review names only against Railway/Vercel dashboards later; do not export values.

**Done condition**

A tracked values-free environment schema/template is authoritative and CI-validates names, types, gates, and public/server classification; Railway/Vercel readiness uses that contract; stale names are removed from docs; missing enabled-feature variables fail before traffic/jobs; default-off trading gates remain explicit; and secret rotation/redeploy ownership is documented without storing values.

---

### I-05 — High: Railway deploy success is not equivalent to scheduler readiness, and rollback is undocumented

**Exact evidence**

- `railway.toml:4-10` selects Nixpacks, starts one scheduler process, and configures only `ON_FAILURE` with three retries. It declares no health/readiness command or deployment validation.
- Scheduler startup environment checks are advisory and continue when required variables are absent (`src/orchestration/scheduler.py:505-543`).
- After registration the scheduler logs jobs and calls `scheduler.start()` (`src/orchestration/scheduler.py:1310-1329`), but startup does not prove DB writability, model-artifact completeness, required provider access by enabled job, or Discord/telemetry behavior.
- `docs/railway_deployment.md:64-74` treats `railway up` as complete and says Railway creates cron services from `railway.toml`, although current architecture is one always-on worker and APScheduler owns schedules.
- Monitoring guidance is logs/dashboard/manual runs (`docs/railway_deployment.md:128-157`). No previous-deployment rollback, commit-to-deployment identity check, model-artifact rollback, smoke gate, or rollback verification appears in the tracked deployment docs.
- The current guide says push-triggered automatic redeploy for model commits (`docs/railway_deployment.md:120-126`), while `brain/Infrastructure/Railway-Setup.md:27-30` simultaneously says push triggers deploy and “deploys are manual git push,” leaving ownership semantics ambiguous.

**Concrete failure mode**

A build starts and remains process-alive while its database URL is read-only/wrong, an enabled job lacks a provider key, a production model artifact is incomplete, or required native imports fail only in a child process. Railway sees a running scheduler, but the first affected job fails minutes or hours later. During an incident, operators have no tracked rule for selecting/redeploying a known-good deployment and matching its code plus committed model artifacts, increasing recovery time and the chance of rolling back only one half of the release.

**Confidence:** High for static readiness/rollback absence; Railway platform settings and actual deployment history were excluded.

**Current mitigation**

- Railway restarts a failed scheduler up to three times (`railway.toml:7-10`).
- Scheduler startup logs environment presence and all registered triggers (`scheduler.py:505-543,1310-1315`).
- Jobs write `job_executions` and send Discord alerts when they actually launch; report 04 documents this (`04-scheduler-ingestion-operations.md:53-118`).
- Production model artifacts are committed, so code and model files can be versioned together (`docs/railway_deployment.md:96-126`).

**Existing-report interaction**

- Report 04 E-07/E-08/E-09 owns run identity, missed-run telemetry, and job-aware startup validation (`04-scheduler-ingestion-operations.md:318-411`).
- This finding adds deploy-level readiness and rollback. A health solution must suit a non-HTTP worker and must not imply that a liveness endpoint proves data/provider readiness.

**Safe evidence step**

Design a no-side-effect preflight command that checks imports, manifest/artifact presence, env-name requirements for enabled jobs, DB connection role/writability only through an approved isolated check, and scheduler inventory without starting jobs. Separately document a dry rollback tabletop using commit/deployment IDs and artifact manifests; do not deploy or call live services in this audit.

**Done condition**

Railway deployment has explicit liveness and readiness semantics for a worker; enabled-job preflight fails the deploy on missing core contracts; deployment logs expose commit/artifact identity; post-deploy smoke observes scheduler registration and telemetry without executing trading/scraping; and a tested runbook restores a known-good code-plus-model deployment with verification and escalation ownership.

---

### I-06 — Medium: Vercel deployment behavior depends on an undocumented project-root choice and differs from CI installation

**Exact evidence**

- Root `vercel.json:1-6` assumes repository-root deployment and manually runs `cd dashboard && npm install` / `npm run build`.
- `dashboard/vercel.json:1-4` assumes the Vercel project root is already `dashboard/` and contains only framework detection.
- README says the dashboard deploys automatically “from `/dashboard` directory” (`README.md:110-114`), while `brain/Infrastructure/Vercel-Setup.md:5-8` says root directory `dashboard/` is “configured in root `vercel.json`.” The root file does not declare a root directory; it merely changes directories in commands.
- CI uses `npm ci` against `dashboard/package-lock.json` (`.github/workflows/ci.yml:73-85`), but root-mode Vercel uses `npm install` (`vercel.json:4-5`).
- Dashboard `package.json` uses many semver ranges (`dashboard/package.json:11-34`), although the committed lockfile mitigates normal resolution drift.

**Concrete failure mode**

A maintainer changes the root config while the Vercel project is configured with `dashboard/` as its root, so the edited file is ignored; or changes the dashboard config while repo-root mode is active. A project re-import can silently switch which config is authoritative. Install behavior also differs from CI: a manifest/lock mismatch that `npm ci` rejects can be resolved or rewrite-oriented under `npm install`, so the deployed build contract is weaker than the tested one.

**Confidence:** High for static dual-mode ambiguity; current Vercel project root was not queried.

**Current mitigation**

- Both modes target the same Next.js app and dashboard lockfile.
- CI uses immutable `npm ci` and Node 20 (`.github/workflows/ci.yml:73-85`).
- Exact versions are used for Next, React, React DOM, ESLint config, and TypeScript (`dashboard/package.json:18-20,32,34`).

**Existing-report interaction**

- Report 00 leaves deployed Vercel state as a coverage gap (`00-existing-inventory-reconciliation.md:383-394`).
- Reports 02/07 own dashboard build/test/runtime behavior, not project-root configuration ownership.

**Safe evidence step**

Inspect only Vercel project settings metadata later—project root, detected config path, Node version, install/build commands, Git branch, and deployment commit—without reading variable values. Compare one preview build log to CI’s lock/install commands.

**Done condition**

One Vercel project-root mode and one config file are authoritative; docs name the exact root/config; deploy uses immutable lockfile installation and an explicit Node version matching CI; a values-free env contract is preflighted; and preview/production deployments expose the source commit and rollback target.

---

### I-07 — Medium: production props retention is seven days in code while scheduler ownership says thirty

**Exact evidence**

- The scheduled archive job defines `DEFAULT_RETENTION_DAYS = 7` and passes it unless the CLI overrides it (`src/orchestration/archive_old_props_job.py:41-43,73-93,110-114`).
- The job module header also says rows older than seven days (`archive_old_props_job.py:3-9`).
- The scheduler calls the job with no retention argument (`src/orchestration/scheduler.py:719-721`).
- The scheduler wrapper docstring says “older than 30 days” (`scheduler.py:719-721`), and the registration comment says rows `> 30 days` (`scheduler.py:963-972`).
- The newer scheduler explainer lists the archive schedule but does not state the effective retention (`docs/understanding/railway-scheduler.md:100-106`).

**Concrete failure mode**

An operator relies on scheduler comments and expects 30 days of hot `raw_player_props_combined` data, but the deployed default moves rows after seven days. Queries, investigations, or features that read only the hot table lose 23 days of expected coverage. Data remains in the archive table, reducing permanent-loss risk, but access/performance semantics differ materially from the declared policy.

**Confidence:** High.

**Current mitigation**

- Rows are moved transactionally to `raw_player_props_archive`, not dropped outright (`archive_old_props_job.py:50-70`).
- The archive job is bounded to 500,000 rows per run (`:41-43,82-85,107-128`).
- The CLI supports an explicit retention override (`:87-92`).

**Existing-report interaction**

- Report 04 inventories the 03:00 archive registration but does not adjudicate this 7-vs-30 policy drift (`04-scheduler-ingestion-operations.md:72-118`).
- Report 00 groups old table-size/retention/index claims into one DB lifecycle evidence cluster (`00-existing-inventory-reconciliation.md:178-217`). This finding establishes only the static effective default, not the live row distribution.

**Safe evidence step**

Add a future static unit test that captures the scheduled wrapper’s effective archive CLI/default and compares it to a single declared retention constant/policy. Separately use the SQL-runner pattern, after approval, to measure hot/archive date bounds before changing retention; do not infer desired policy from comments alone.

**Done condition**

One owned retention policy declares hot/archive windows and consumers; scheduler/job/docs/tests agree on the effective value; changing it requires a reviewed config change and coverage preflight; archive retrieval/restore is documented; and live date-bound verification confirms the intended policy after deployment.

---

### I-08 — Medium: a Railway production-log export is tracked in the repository

**Exact evidence**

- `.hermes/tmp/railway_logs_4h.jsonl` is a tracked 231,306-byte file with 1,081 JSONL records. Git history shows it entered the repository in commit `0562ab6`.
- Its records contain `timestamp`, `level`, and free-form `message` fields. Redacted term-only inspection found environment-variable names on `.hermes/tmp/railway_logs_4h.jsonl:5-12`; no values were printed or inspected.
- `.gitignore:23-29` ignores generic JSON except selected manifest files, but ignore rules do not protect a file already tracked.
- The redacting current-HEAD scan found no recognizable private key, provider token, JWT, or unredacted credential URL in this file. That lowers current credential confidence but does not make arbitrary production logs safe to version.

**Concrete failure mode**

A future log refresh or a new exception includes a connection string, provider response, user identifier, request payload, market/trading state, or token-bearing header. Because the artifact is already tracked, it can be updated and committed despite the generic JSON ignore. Even without secrets, detailed production schedules/failures/environment presence can aid reconnaissance and persist beyond platform log-retention controls.

**Confidence:** High for the tracked operational artifact and future exposure path; low for current secret exposure because the redacting scan found no standard secret shapes and values were not reviewed.

**Current mitigation**

- Generic `*.json` and `*.log` patterns are ignored (`.gitignore:23-36`).
- Current standard secret-pattern scan was negative.
- The file contains a bounded four-hour export rather than an ongoing live stream.

**Existing-report interaction**

- Report 04 intentionally did not inspect live Railway logs (`04-scheduler-ingestion-operations.md:457-467`). This finding concerns the tracked static export only.
- No operational conclusions in this report are inferred from the log message values.

**Safe evidence step**

Run a values-redacting secret/PII classifier over the current blob and its Git history, reporting only rule/path/line and entropy category. Determine whether the artifact is still needed as test evidence; if so, replace it in a future approved change with a minimal synthetic/redacted fixture outside a tracked temp directory.

**Done condition**

No production log dump is tracked; `.hermes/tmp/` or an equivalent operational-output path is explicitly ignored; required regression fixtures are synthetic and reviewed; secret scanning covers current diff and history as policy requires; and any exposed credential discovered during history review is rotated before history-remediation decisions.

---

### I-09 — Medium: Nixpacks native-library assembly is floating and selects arbitrary store matches

**Exact evidence**

- `nixpacks.toml:4-5` names Nix packages but does not pin a Nixpacks image/version or nixpkgs revision in the repository.
- Setup searches the entire `/nix/store`, takes the first matching `libz.so.1`, `libstdc++.so.6`, and `libgcc_s.so.1`, and copies them into `/opt/lib` (`nixpacks.toml:8-13`). Match order is not declared or version-validated.
- The venv is created with `--system-site-packages` (`nixpacks.toml:15-19`), coupling application imports to both Nix-provided and pip-installed packages.
- Historical deployment docs record repeated Nixpacks/pip/native-library failures and explain why the custom venv/library copy exists (`docs/railway_deployment.md:190-225`).
- `railway.toml` merely selects `builder = "nixpacks"` (`railway.toml:4-5`).

**Concrete failure mode**

A Nixpacks/nixpkgs update changes store contents or ordering, causing the build to copy a different ABI/library than the Python wheels expect. `--system-site-packages` can also allow a Nix package to satisfy an import instead of the intended pip graph. The same commit can therefore start failing or change native runtime behavior after builder updates.

**Confidence:** Medium-High. The nondeterministic selection is explicit; current Railway builder revision and actual store contents were not inspected.

**Current mitigation**

- The copied library path survives Nixpacks’ final `/app` copy (`nixpacks.toml:6-13,21-23`).
- Python is explicitly 3.11 and subprocesses use the same venv interpreter (`nixpacks.toml:17-18,25-26`).
- The current arrangement was created from observed native import failures, not speculative complexity (`docs/railway_deployment.md:190-225`).

**Existing-report interaction**

- Report 04 correctly treats `nixpacks.toml` as the current Railway build owner but focuses on runtime scheduling (`04-scheduler-ingestion-operations.md:53-62`).
- I-02’s dependency authority should be resolved together with this build provenance issue; replacing only pip requirements does not pin the native runtime.

**Safe evidence step**

In a disposable future Railway-equivalent build, print only builder/runtime versions, selected library realpaths/checksums, and Python package versions; compare two clean builds of the same commit. Evaluate a pinned builder/image or deterministic Nix derivation before modifying production.

**Done condition**

The builder/base/nixpkgs provenance is pinned or recorded reproducibly; native libraries are selected deterministically and ABI-smoke-tested; pip and system package precedence is explicit; two clean builds of the same commit produce the same dependency/native inventory; and rollback retains the prior build image/artifact.

---

## Security and privilege-boundary reconciliation

### Boundaries confirmed in scope

- `AGENTS.md:24` requires Python backend use of the `postgres` role and dashboard/client use of `authenticated` with RLS. This audit does not recommend silently changing those roles.
- Dashboard browser/server user clients use the public Supabase URL/anon key, while service-role creation is isolated to server code (`dashboard/src/lib/supabase/server.ts:1-31`; `dashboard/src/lib/supabase/admin.ts:1-17`).
- Report 07 already establishes that several route-local authorization checks are missing before service-role operations (`07-dashboard-product.md:60-130`). Those are critical but are not duplicated here.
- CI YAML does not explicitly receive repository/provider secrets; its build uses placeholder public Supabase values (`.github/workflows/ci.yml:83-88`).
- Railway’s single worker intentionally shares one backend database credential across many jobs. This creates broad service blast radius, but changing the role boundary would conflict with the current project invariant and requires a separately approved architecture/DB-policy review. It is recorded as a coverage gap, not a new finding.

### Security headers

`dashboard/next.config.ts:18-30` sets `X-Content-Type-Options`, `X-Frame-Options`, legacy `X-XSS-Protection`, `Referrer-Policy`, and `Permissions-Policy`. No repository-defined Content Security Policy appears there. This audit did not promote “missing CSP” to a finding because no exploit path or complete third-party/script inventory was established, and platform headers were not queried. A future report-07 security slice can characterize CSP in report-only mode before enforcement.

## Rejected suspicions / resolved items not reopened

1. **Rejected: a recognizable live secret is hardcoded in current tracked text.** The redacting scan found masked/example URLs and test/template candidates but no standard private key, provider token, JWT, or unredacted credential URL. This is heuristic current-HEAD evidence, not proof against arbitrary encodings or Git history.
2. **Rejected: service-role secrets are imported into browser modules.** In the inspected scope, `SUPABASE_SERVICE_ROLE_KEY` is referenced in server route/admin-client code. Report 07’s issue is missing server authorization, not demonstrated client-bundle exposure.
3. **Rejected: Railway currently depends on Docker manifests that are missing.** Railway explicitly uses Nixpacks (`railway.toml:4-5`) and Vercel builds Next.js. No Dockerfile is required by the tracked platform contract; the absence only limits local platform-parity options.
4. **Rejected: Python version is broadly inconsistent.** `.python-version`, CI, Ruff target, and Nixpacks all target Python 3.11 (`.python-version:1`; `.github/workflows/ci.yml:18-21,46-49`; `pyproject.toml:36-39`; `nixpacks.toml:4-5`).
5. **Rejected: dashboard dependencies have no lockfile.** `dashboard/package-lock.json` is tracked and CI uses `npm ci`. I-06 concerns Vercel config/install authority, not lockfile absence.
6. **Rejected: `uv.lock` is missing hashes.** It includes artifact hashes. I-02 is that its dependency universe differs from production and deploy paths ignore it.
7. **Rejected: the in-database props archive deletes data without a copy.** Each batch inserts to `raw_player_props_archive` and deletes the selected hot rows in one transaction (`archive_old_props_job.py:50-70`). It is still not an independent disaster-recovery backup.
8. **Not reopened: dashboard service-role authorization, paid API entitlement, Stripe webhook acknowledgment, checkout idempotency, and Ask rate limiting.** Report 07 fully owns those source findings.
9. **Not reopened: CI auto-fix/moving-ref correctness, zero coverage, no typecheck, and dashboard build-only verification.** Report 02 fully owns those findings; I-03 covers only supply-chain provenance and workflow privilege.
10. **Not promoted: root `claude-flow` alpha dependency as a production risk.** Root `package.json:1-5` uses a caret alpha range but has a lockfile, and current Railway/Vercel commands do not install the root package. Its update policy belongs to agent-tooling maintenance unless a deploy path begins consuming it.
11. **Not promoted: no SBOM or Dependabot/Renovate configuration.** These are useful controls, but no current vulnerable package was established and no external advisory query was allowed. They remain supply-chain coverage gaps.

## Coverage gaps

- No live Railway/Vercel settings, deployments, build logs, project roots, variable names/values, health checks, access controls, rollback history, or platform backup settings were queried.
- No `.env`, `.env.local`, credential file, key file, secret manager, or live variable value was opened.
- No GitHub repository settings, branch protection, environment protection, Actions token defaults, provenance attestations, or prior Actions logs were inspected.
- No package was installed, updated, imported, built, or resolved. Current lock compatibility and native ABI behavior remain unexecuted.
- No OSV/GitHub Advisory/npm audit/pip-audit query was made, so current known-vulnerability status is unknown.
- Secret scanning was heuristic and values-redacting. It did not scan deleted Git history, binary model/joblib artifacts, package tarballs, Git LFS, remote branches, forks, CI artifacts, or external deployment logs.
- Binary model artifacts were not deserialized. Pickle/joblib supply-chain trust, artifact signatures, and safe loader boundaries require a separate model-artifact security audit; loading untrusted joblib files would itself be unsafe.
- No Supabase backups/PITR, schema grants, RLS policy behavior, role passwords, extensions, storage buckets, or restore capability was inspected.
- No restore drill was performed. I-01 establishes missing repository contract and sync semantics, not that provider recovery is unavailable.
- No production table date bounds were queried, so I-07 establishes effective code default only, not actual current hot/archive coverage.
- No runtime CSP/header response was fetched. Platform-added headers and third-party script compatibility are unknown.
- No workstation Task Scheduler, Docker Desktop, local Postgres service, filesystem backup, cloud drive, or whole-machine state was inspected.
- Service ownership is implicit: Railway owns one worker and Vercel owns the dashboard, but no tracked on-call/escalation/RACI or deploy approver matrix was found. This was not promoted without organizational evidence.
- Observability vendor/platform configuration was not inspected. Repository evidence supports logs, Discord, and `job_executions`; report 04/07 own their runtime gaps.

## Prioritized safe evidence queue

1. **Recovery contract first:** inventory provider backup/PITR metadata without values, define RPO/RTO/assets/owners, and create a disposable restore drill. Unit-characterize `sync_local_db.py` failure/exit semantics before anyone treats it as backup.
2. **Single Python dependency authority:** reconcile pyproject/requirements/uv lock, then validate one immutable Python 3.11 graph in CI and a disposable Railway-equivalent build.
3. **CI provenance/privilege:** combine report 02 TV-01 with I-03—immutable event SHA, non-mutating lint, explicit read permissions, SHA-pinned actions.
4. **Values-free environment schema:** reconcile Railway/Vercel/dashboard names, classifications, defaults, gates, and owners; preserve default-off live-trading controls.
5. **Railway readiness and rollback:** define worker-appropriate preflight, commit/artifact identity, post-deploy smoke, and known-good rollback without running domain jobs.
6. **Vercel ownership:** verify project root/config metadata and converge on one config plus `npm ci`/Node parity.
7. **Retention reconciliation:** measure live hot/archive bounds through the approved SQL-runner lane, then choose and test one declared retention value.
8. **Tracked-log hygiene:** values-redacting history scan, rotate only if an actual exposure is found, then replace/remove the production log artifact in an approved change.
9. **Native build reproducibility:** record/pin Nixpacks/Nix/native-library provenance and compare two clean disposable builds.
10. **Advisory/SBOM policy:** after dependency authority is settled, run approved lock-based vulnerability/SBOM tooling without updating packages and triage findings separately.

This queue is evidence and sequencing guidance only. It does not authorize source/config/plan/register edits, credentials access, live-service calls, DB actions, package changes, deploys, or history rewriting.

## Validation record

- Report scope is limited to the requested infrastructure/security/dependency/recovery lane and interactions with reports 00/02/04/07.
- Every promoted finding includes exact path/line evidence, a concrete failure mode, confidence, current mitigation, existing-report interaction, safe evidence step, and done condition.
- Rejected suspicions and coverage gaps are recorded separately.
- No `.env`/credential values, live service, live variable, DB, package install/update, deployment, source/config/plan/register file, or whole-machine path was accessed or changed.
- Only `.hermes/audits/tech-debt/08-infra-security-dependencies.md` was written.
