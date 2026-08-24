# GameFlow Repository Archival and Reduction Plan

> **For Hermes:** Use `gameflow-implementation-worker` or `subagent-driven-development` to execute this plan one independently reviewable slice at a time. Every archive/removal slice requires manifest/readback verification before `git rm`. Do not launch model training, sweeps, broad DB work, deployments, or live-provider actions.

**Goal:** Reduce GameFlowData to the source, configuration, tests, production artifacts, and integrations required to build and verify the supported NBA/MLB models, ingest their data, send Discord alerts, and serve the core website.

**Architecture:** Treat Git as source and deployment metadata, not an experiment/artifact/knowledge archive. Preserve one narrow active product: NBA/MLB models, verification, sportsbook scrapers, orchestration, Discord transport, DB contracts, and a simplified website. Move historical binaries and evidence to a checksummed archive outside the repository; remove retired product lanes and duplicate knowledge only after dependency gates pass.

**Tech stack:** Python 3.11, pytest, Next.js/TypeScript, npm, Git, SHA-256 archive manifests, GBrain as canonical project knowledge.

---

## 1. Measured baseline

Read-only inventory on 2026-08-24:

- 1,554 tracked files.
- 229,404,307 tracked bytes (218.8 MiB).
- `src/models/artifacts/`: 69 files / 156,454,985 bytes.
- `src/models/mlb/artifacts/`: 50 files / 57,838,129 bytes.
- Retained NBA production candidates: 12 files / 21,390,763 bytes.
- Retained MLB production candidates: 20 files / 15,842,116 bytes.
- Non-production NBA/MLB artifacts: 177,060,235 bytes, or 77.2% of the tracked checkout.
- Removing only non-production artifact binaries would reduce the tracked checkout to about 49.9 MiB.
- Other duplication: `.claude/` 195 files, `.hermes/` 106 files, `docs/` 173 files, `brain/` 119 files, root `Handoffs/` 21 files.
- Git object storage is separately bloated: 307.83 MiB loose objects plus a 71.26 MiB pack. Current-tree cleanup will not remove old binaries from history.

### Reduction targets

1. Active tracked checkout at or below 60 MiB.
2. Active tracked file count at or below 900 files.
3. No experiment/run binary outside explicitly declared production artifact directories.
4. No checked-in temporary probes, caches, generated result data, local brain mirror, or session chronology.
5. Core NBA/MLB model, scraper, Discord, scheduler, DB, and website checks remain green.
6. Git-history rewriting is a separate final decision, never bundled with current-tree pruning.

---

## 2. Product boundary

### Retain as the core

- NBA training, inference, feature generation, Monte Carlo, backtesting, model audit, and production artifacts.
- MLB pitcher strikeouts and batter hits training/inference/feature/lifecycle/backtesting paths, plus any other sportsbook-backed stat that an active manifest explicitly declares.
- Model-validation contracts: empirical CDF, quote-clean replay, temporal integrity, artifact identity, flat-first certification, and required tests.
- Sportsbook and league-data scrapers needed by NBA/MLB.
- `src/db/`, applied migrations, schema reconstruction, and RLS/auth contracts.
- `src/discord_bot/alerts.py` or a smaller retained Discord transport extracted from it.
- Scheduler/job infrastructure for supported NBA/MLB ingestion, inference, resolution, and alerts.
- Website core: public landing/picks/pricing, login/signup, props dashboard, one combined history/performance surface, account/subscription, and only APIs needed by retained pages.
- `AGENTS.md`, `CLAUDE.md`, one concise README, a small operational runbook set, and GBrain pointers.

### Remove from active product

- Bot Tracker page and all browser-facing Kalshi control APIs.
- Arb Scanner page and arb verification API.
- Data Vault route and its dedicated components/helpers/types.
- Duplicate History route after its behavior is folded into Performance.
- Kalshi sports trading and UI lane, subject to the zero-exposure/closeout-code gate.
- Polymarket/Kalshi arbitrage lane unless Chase explicitly reverses the current back-to-basics direction.
- Checked-in non-production model binaries and run archives.
- Duplicate local knowledge/session trees and generated assistant state.

### Quarantine pending one explicit decision

- NCAAB source/tests/migrations/docs: detached from the active product but coherent enough not to delete accidentally.
- Standalone non-sports Kalshi or Polymarket research: default recommendation is archive, not retain.
- DFS and Ask AI: user did not request removal, so retain during this reduction unless their dependency cost proves disproportionate.
- Inactive MLB stat artifacts currently mixed into `src/models/mlb/artifacts/production/`: retain until a required-stat manifest identifies active sportsbook-backed consumers.

---

## 3. Safety rules for every slice

1. Archive outside the repository. Never move files to an in-repo `archive/` directory and call that reduction.
2. Before removal, write a manifest containing original path, byte size, SHA-256, classification, consumer/reference evidence, archive location, and restoration command.
3. Copy, hash the destination, compare hashes programmatically, and perform one readback/restoration smoke test before `git rm`.
4. Do not deserialize unknown historical model binaries merely to inventory them.
5. Keep production artifact directories until manifest-backed loaders and clean-clone smoke tests pass.
6. Preserve all applied migration history during this session. Schema squashing is a separate DB-governance task.
7. Remove scheduler registrations and imports before deleting their implementation modules.
8. Remove implementation-coupled tests in the same commit as retired implementation; retain tests for shared primitives and anti-regrowth rules.
9. No commits, push, deployment, DB mutation, provider calls, model runs, or history rewrite without separate authorization.
10. Chase launches any eventual long model verification job. This plan uses import, fixture, build, and existing scoped test checks only.

---

## 4. Execution sequence

### Task 1: Establish the reduction ledger and immutable baseline

**Objective:** Make every later removal measurable and reversible.

**Files:**
- Create: `scripts/repo_reduction_inventory.py`
- Create: `docs/operations/repository-scope.md`
- Create outside repo: `<ARCHIVE_ROOT>/gameflow-reduction-2026-08-24/manifest.json`
- Modify: `.gitignore`

**Steps:**

1. Add a tracked-file-first inventory script that reports count/bytes by approved path group and rejects tracked generated-file patterns.
2. Record the exact starting commit, branch, `git status`, file count, tracked bytes, and production artifact paths.
3. Define allowlisted active artifact roots and forbidden names (`production_old_*`, `production_bad_*`, `production_archived_*`, `nba_run_*`, experiment/ablation/validation outputs, `.zip` run bundles).
4. Fix `.gitignore` so it ignores every non-production run/backup naming family while explicitly allowing required JSON manifests/configs. The current blanket `*.json` rule must not silently drop model provenance.
5. Add a non-mutating `--check` mode suitable for CI.
6. Verify the script against current Git state before removing anything.

**Validation:**

- `python scripts/repo_reduction_inventory.py --json`
- `python scripts/repo_reduction_inventory.py --check` should fail on the current known bloat and later pass.
- `git diff --check -- .gitignore scripts/repo_reduction_inventory.py docs/operations/repository-scope.md`

**Commit boundary:** inventory/guardrails only.

---

### Task 2: Archive and remove non-production model binaries

**Objective:** Capture the largest reduction first without changing model behavior.

**Retain initially:**

- `src/models/artifacts/production/`
- `src/models/artifacts/production_playoffs/`
- `src/models/mlb/artifacts/production/`

**Archive/remove after reference classification:**

- `src/models/artifacts/production_old_*/`
- `src/models/artifacts/production_bad_*/`
- `src/models/artifacts/production_archived_*/`
- `src/models/artifacts/nba_run_*/`
- `src/models/artifacts/hybrid_pts_test/`
- `src/models/artifacts/run_20260131_112534.zip`
- Non-production `src/models/mlb/artifacts/{ip_ablation_*,validation_2025_*,baselines/,ablations/,ranker_retrains/,mlb_run_*}` entries that are tracked.

**Steps:**

1. Enumerate literal references from source/config/scripts/docs to each candidate path.
2. Classify each candidate as active rollback, frozen evidence, rejected/invalid, duplicate, or unknown.
3. Copy all non-production candidates to `<ARCHIVE_ROOT>/artifacts/` while preserving relative paths.
4. Generate and verify SHA-256 entries for every archived file.
5. Restore one NBA and one MLB artifact directory to a temporary location and verify exact hashes.
6. Replace historical literal paths in retained docs with immutable archive IDs only where the document remains active.
7. `git rm` only candidates whose archive readback passed.
8. Run production-loader and artifact-audit fixture tests without training.
9. Re-run the size inventory; expected active tracked size is approximately 49.9 MiB before other reductions.

**Validation:**

- `python -m pytest tests/test_mlb_training_artifacts.py tests/test_audit_mlb_model_artifacts.py -q`
- `python -m pytest tests/test_negbin_model.py -q`
- `python scripts/repo_reduction_inventory.py --check`
- Clean-clone or temporary-worktree production artifact presence check.

**Commit boundary:** artifact archive ledger plus current-tree binary removal only.

---

### Task 3: Declare the production artifact contract

**Objective:** Ensure reduction cannot leave a partial or ambiguous model suite.

**Files likely to change:**

- `src/models/mlb/training/artifacts.py`
- `src/models/mlb/mlb_model_suite.py`
- NBA artifact loader under `src/models/quantile_trainer.py` or a new focused manifest module.
- `src/orchestration/inference_job.py`
- `src/orchestration/mlb_inference_job.py`
- `tests/test_mlb_training_artifacts.py`
- New focused NBA/MLB production-manifest tests.

**Steps:**

1. Inventory current production files and consumers; do not infer required stats from filename presence.
2. Add or complete one versioned manifest per deployable NBA/MLB suite with required files, stat/model family, hashes, source run/commit, and forbidden artifacts.
3. Make production inference fail closed on a missing, corrupt, mixed, or undeclared required artifact.
4. Keep permissive partial loading only as an explicit diagnostic mode.
5. Remove inactive MLB production binaries only after the manifest and all retained consumers agree they are not required.
6. Add anti-regrowth coverage proving global conformal-offset files are rejected by production artifact validation.

**Validation:** temporary-directory tests for missing file, extra stale file, mixed run IDs, hash mismatch, forbidden offset, and valid suite.

**Commit boundary:** artifact contract hardening, then a separate commit for any newly proven inactive production artifact removal.

---

### Task 4: Remove duplicate knowledge and assistant-generated state

**Objective:** Reduce file-count and authority drift without losing unique project truth.

**Archive/remove candidates:**

- `brain/`
- root `Handoffs/`
- `ACTIONITEMS.md`
- `.thoughts.md`
- `CLAUDE.md.backup`
- `.hermes/tmp/` including tracked probes, outputs, caches, copied remote brain files, logs, and generated CSV/JSON evidence.
- Old `.hermes/plans/` and `.hermes/audits/` after exporting a frozen evidence bundle and retaining only the active reduction plan plus genuinely current runbooks.
- `.claude/.flow_library/`
- `.claude/glm_spec_*`
- stale `.claude/commands/`, copied skills, obsolete SQL/query files, and helpers not used by the current Hermes workflow.
- Historical `docs/development_docs/*session*.md` and duplicate session chronology.

**Retain:**

- `AGENTS.md`, current `CLAUDE.md`, root `README.md`.
- A concise `docs/operations/`, model lifecycle guide, scraper/runbook docs, and schema/deployment docs that match active code.
- The current reduction plan until execution is complete.
- Remote canonical GBrain as knowledge authority.

**Steps:**

1. Compare local brain/handoff pages with remote GBrain and isolate local-only content.
2. Promote only approved unique durable facts; do not bulk-copy stale prose.
3. Export a checksummed knowledge/evidence bundle outside the repo.
4. Remove duplicate trees rather than creating a third in-repo archive.
5. Run a broken-link scan on retained Markdown.
6. Add ignore/CI guards against tracked `.hermes/tmp`, `__pycache__`, generated result files, and new local brain mirrors.

**Validation:**

- No active docs point to deleted local brain/handoff paths.
- GBrain canonical slugs remain retrievable.
- `git ls-files` contains no `.hermes/tmp/`, `__pycache__`, or `.claude/.flow_library/` entries.

**Commit boundary:** knowledge export/removal separate from runtime code.

---

### Task 5: Remove Bot, Arb, and Data Vault website surfaces

**Objective:** Simplify the website before touching retained user-facing behavior.

**Remove:**

- `dashboard/src/app/(protected)/bot-tracker/`
- `dashboard/src/components/bot-tracker/`
- `dashboard/src/lib/hooks/useBotTracker.ts`
- `dashboard/src/lib/hooks/useTradeQueue.ts`
- `dashboard/src/types/bot-tracker.ts`
- Kalshi-only dashboard types after import verification.
- `dashboard/src/app/api/kalshi/`
- `dashboard/src/app/(protected)/arb-scanner/`
- `dashboard/src/components/arb-scanner/`
- `dashboard/src/lib/hooks/useArbScanner.ts`
- `dashboard/src/types/arb-scanner.ts`
- `dashboard/src/app/api/arb/verify/route.ts`
- `dashboard/src/app/(protected)/stats/`
- `dashboard/src/components/stats/`
- `dashboard/src/lib/stats/`
- `dashboard/src/types/stats.ts`

**Modify:**

- `dashboard/src/components/layout/Navbar.tsx`
- `dashboard/src/lib/sport-config.ts`
- `dashboard/src/lib/supabase/middleware.ts` if removed admin routes are listed there.
- Dashboard package dependencies only if post-removal import/dependency analysis proves them unused.

**Steps:**

1. Remove desktop and mobile nav links first.
2. Remove privileged Kalshi/arb API routes in the same slice as their UIs; do not leave invisible service-role mutation endpoints.
3. Remove page, component, hook, and type clusters as whole dependency units.
4. Delete the `statsVault` feature flag from the type and NBA/MLB configs instead of leaving a permanent false flag.
5. Search for route strings and imports; zero matches are required except intentional archival prose.
6. Build the dashboard before proceeding.

**Validation:**

- `npm run lint` from `dashboard/`.
- `npm run build` from `dashboard/`.
- Static checks that `/bot-tracker`, `/arb-scanner`, `/stats`, `/api/kalshi/*`, and `/api/arb/verify` are absent.
- Confirm props, account, auth, pricing, scoreboard, games, and retained APIs still compile.

**Commit boundary:** one removal commit per route family: Bot/Kalshi, Arb, Data Vault.

---

### Task 6: Combine History and Performance into one route

**Objective:** Keep all useful betting results while exposing one coherent page.

**Canonical route:** `/performance`

**Files:**

- Modify: `dashboard/src/app/(protected)/performance/page.tsx`
- Extract as needed: focused panels under `dashboard/src/components/performance/`.
- Reuse: `dashboard/src/components/history/`, `dashboard/src/lib/hooks/useHistoryData.ts`, `dashboard/src/lib/hooks/usePerformanceData.ts`.
- Remove after parity: `dashboard/src/app/(protected)/history/page.tsx`.
- Modify: `dashboard/src/components/layout/Navbar.tsx`.
- Modify: `dashboard/next.config.ts` for a permanent `/history` → `/performance` redirect.

**Behavior contract:**

- One nav item: `Performance`.
- Preserve personal bets, model history, MLB model history, DFS entries if DFS remains, editing/deletion, date/direction/status/source filters, KPI/charts, and record import/manual entry.
- Fetch only the active panel's data; failures must not render as believable zero metrics.
- Do not duplicate `My Bets` under multiple indistinguishable tabs.

**Steps:**

1. Extract current History behavior into a renderable panel without changing queries.
2. Add it to `/performance` and preserve all current filters/actions.
3. Add the redirect and remove the History nav item.
4. Verify route parity manually and with focused hook/view tests.
5. Remove the old route only after parity passes.
6. Deduplicate shared hooks/view models as a separate cleanup commit; avoid a page rewrite mixed with deletion.

**Validation:**

- Dashboard lint/build.
- Browser smoke: NBA/MLB switch, personal bets, model history, performance metrics, edit/delete, date filters, and `/history` redirect.
- Query error smoke: failures display explicit error state rather than zero/empty success.

**Commit boundary:** behavior-preserving merge first; deduplication second.

---

### Task 7: Retire Kalshi sports and arbitrage backend code

**Objective:** Remove an unavailable product lane while retaining Discord and supported sportsbook primitives.

**Mandatory precondition:** Verify no outstanding exchange exposure requires the closeout client/code. This is a read-only, separately approved provider/DB check; do not infer zero exposure from local status rows. If exposure exists or cannot be verified, first remove new-order schedules and UI, then retain only a quarantined closeout bundle until exposure is resolved.

**Removal candidates after the precondition:**

- `src/trading/kalshi/`
- Kalshi sports portions or all of `src/scrapers/kalshi/` if non-sports is not retained.
- `src/scrapers/polymarket/` if standalone Polymarket is not retained.
- `src/arbitrage/`
- `src/models/kalshi_edge.py`
- `src/paper_trading/kalshi_paper_trader.py`
- `src/paper_trading/kalshi_analysis.py`
- `src/paper_trading/arb_paper_trader.py`
- `src/orchestration/kalshi_*.py`
- `src/orchestration/arb_scan_job.py`
- Kalshi/arb-only scripts and docs.
- `tests/test_kalshi_*.py` and implementation-coupled arb tests.

**Modify:**

- `src/orchestration/scheduler.py`: remove wrappers, registrations, env checks, and job names before module deletion.
- `src/discord_bot/alerts.py`: retain prediction, job, and performance transport; delete Kalshi/arb-only renderers/senders after caller removal.
- `requirements.txt`/lockfiles: remove provider-only dependencies only after import search.
- Deployment/env documentation: remove retired variable names without reading or printing values.

**Steps:**

1. Remove new-position and high-frequency scheduler registrations.
2. Decide non-sports Kalshi/standalone Polymarket. Default for this plan is archive both.
3. Extract any provider-neutral helper with an active NBA/MLB consumer; do not refactor retired code merely to make deletion elegant.
4. Delete callers before implementations, then tests/docs/config.
5. Add anti-regrowth checks for retired namespaces/routes/jobs.
6. Verify scheduler import and dry job enumeration without running jobs.

**Validation:**

- `python -m compileall -q src`
- `python -m pytest -q` after implementation-coupled tests are removed.
- Search active source for `kalshi`, `polymarket`, `arb_scanner`, retired job names, and retired env names; remaining matches must be explicitly justified historical or generic text.
- Confirm `src/discord_bot/alerts.py` still exposes retained NBA/MLB/job alert senders.

**Commit boundary:** scheduler containment, then UI/API removal, then backend/provider removal, then dependency/docs cleanup.

---

### Task 8: Prune detached and obsolete source without weakening model verification

**Objective:** Remove code that is neither part of NBA/MLB production nor needed to recreate/verify it.

**Candidates requiring reference tests:**

- NCAAB source under `src/models/ncaab_*`, `src/scrapers/ncaab/`, `src/processing/ncaab/`, NCAAB orchestration jobs/tests/docs/migrations. Default: archive as one coherent bundle only after Chase confirms NBA/MLB-only scope.
- One-off analysis scripts under `scripts/` and `src/tools/` whose result is already captured canonically.
- `notebooks/` after exporting any unique methodology or final results.
- Legacy duplicate MLB entrypoints only where the YAML lifecycle and retained wrappers cover the same behavior.
- Obsolete social-image, old deployment, old calibration-offset, and superseded experimental code.

**Explicitly retain until tests prove otherwise:**

- MLB legacy feature-store compatibility facades currently used by the migrated boundary.
- NBA/MLB backtest, artifact, temporal, scraper, and model-validation tests.
- Applied DB migrations.
- Empirical CDF and Q10 behavior.
- Model report generation needed to verify candidates.

**Steps:**

1. Produce an import/call/config/test reference report for each candidate group.
2. Classify whole coherent lanes; do not scatter half of a lane across active and archive trees.
3. Archive candidate source/docs with a commit/tag reference and checksums.
4. Remove one lane per commit and run the scoped plus full tests.
5. Keep NCAAB if scope remains ambiguous; its small source footprint is not worth accidental loss compared with the model binaries already removed.

---

### Task 9: Remove root and generated residue

**Objective:** Leave a clean repository surface.

**Candidates:**

- `GameFlowData.egg-info/`
- tracked `.next/`
- `graphify-out/`, including generated `cache/`, `graph.json`, and `GRAPH_REPORT.md`
- `scripts/__pycache__/` and `docs/.ipynb_checkpoints/`
- tracked `tmp/` and malformed `backtest_resultsauditstmp_dryrun_selected5`
- `_test_longspec.py`
- `tmp_mcp_gbrain_probe.py`
- `phone.txt`
- `slate_test.png` unless an active test consumes it
- generated logs, `.out`, `__pycache__`, CSV/JSON result bundles, local screenshots, and test output directories
- stale root `package.json`/`package-lock.json` if no supported root Node tool remains

**Steps:**

1. Prove no active import/build/test reference.
2. Remove residue and update `.gitignore` with narrow patterns.
3. Keep the dashboard's own `dashboard/package.json` and lockfile.
4. Run the anti-regrowth inventory check.

**Commit boundary:** mechanical residue only.

---

### Task 10: Consolidate active documentation

**Objective:** Replace hundreds of chronological documents with a small operating set.

**Target retained docs:**

1. System overview/data flow.
2. NBA train/evaluate/infer runbook.
3. MLB YAML lifecycle train/evaluate/infer runbook.
4. Scraper and scheduler runbook.
5. Discord integration runbook.
6. Dashboard local/deploy runbook.
7. DB schema/migration policy.
8. Repository scope/artifact archive policy.
9. Critical invariants pointer to GBrain.

**Steps:**

1. Classify each current document as active runbook, canonical GBrain knowledge, historical evidence, duplicate, or obsolete.
2. Merge only current executable instructions into the target set.
3. Export historical evidence outside the repo with a checksum ledger and source commit.
4. Remove session logs and superseded plans from active navigation.
5. Validate all retained commands and links.

---

### Task 11: Final verification and measured closeout

**Objective:** Prove reduction without relying on visual inspection or worker self-report.

**Commands/checks:**

1. `git status --short --branch`
2. `git diff --check`
3. `python scripts/repo_reduction_inventory.py --json`
4. `python scripts/repo_reduction_inventory.py --check`
5. `python -m compileall -q src`
6. `python -m pytest -q`
7. `npm run lint` from `dashboard/`
8. `npm run build` from `dashboard/`
9. Clean-clone/worktree import and production-artifact validation.
10. Dashboard browser smoke for public/auth/props/performance/account and NBA/MLB switching.
11. Readback verification of the archive manifest and at least one restored NBA/MLB artifact.
12. Compare before/after tracked file count, tracked bytes, top-level counts, and largest files.

**Done when:**

- Active tracked size is at or below 60 MiB or every byte above the target is explicitly justified.
- Active tracked file count is at or below 900 or every retained large family is explicitly justified.
- No retired routes/APIs/jobs import successfully or appear in navigation/scheduler inventory.
- NBA/MLB source, configs, production artifacts, backtests/audits, scrapers, Discord transport, scheduler, DB migrations, and website core remain present and verified.
- Archive hashes/readback pass.
- No model training, DB mutation, provider mutation, deployment, or history rewrite occurred.

---

## 5. Optional final phase: Git-history compaction

Current-tree cleanup will reduce checkout/build context but not old clone history. The Git object database is currently hundreds of MiB because deleted binaries remain in prior commits.

Only after the reduced branch is accepted:

1. Create and verify a full bare mirror backup.
2. Confirm no active branches/tags need old binary paths.
3. Produce a `git filter-repo --analyze` report.
4. Propose exact path/pattern removals and expected reclaimed bytes.
5. Obtain explicit approval for a history rewrite and coordinated force-push.
6. Rewrite in a disposable mirror first; verify retained commits, tags, production artifacts, and a fresh clone.
7. Rotate/replace remote history only in a scheduled window.

Do not run `git gc`, `filter-branch`, BFG, `git filter-repo`, force-push, or tag deletion as part of the normal reduction slices.

---

## 6. Key risks and mitigations

- **Artifact loss:** external manifest, SHA-256 readback, restoration smoke, and no deletion before verification.
- **Partial production suite:** manifest-backed fail-closed loader before trimming production directories.
- **Hidden scheduler imports:** remove registrations/callers first; compile and enumerate jobs before deleting modules.
- **Retired UI leaves privileged APIs:** remove pages and corresponding Kalshi/arb API routes together.
- **Discord accidentally removed with trading alerts:** extract/retain shared NBA/MLB/job transport and verify exact senders.
- **Knowledge loss:** compare with remote GBrain, export local-only evidence, never create another in-repo archive.
- **Migration/schema loss:** preserve migrations this session; no live DB changes.
- **NCAAB regret:** quarantine pending explicit scope decision because its source footprint is small.
- **History still large:** report current-tree and history sizes separately; history rewrite is optional and separately authorized.
- **Huge mixed diff:** one coherent lane per commit/review; never combine binary removal, dashboard behavior changes, and scheduler deletion.

---

## 7. Decisions required before execution

Recommended defaults are shown first:

1. **Archive destination:** a checksummed directory outside the repo with a second copy or immutable object storage; do not rely on Git history alone for model binaries.
2. **Supported sports:** NBA and MLB only; quarantine NCAAB until Chase explicitly retains it.
3. **Prediction markets:** retire/archive Kalshi sports, non-sports Kalshi, Polymarket, and arb code after the exposure/closeout gate.
4. **Website:** retain DFS, Ask AI, auth, subscriptions, props, account, public pages, and one combined Performance surface; remove Bot, Arb, and Data Vault.
5. **Git history:** current-tree cleanup now; defer history rewrite until the reduced branch is accepted and backed up.

These decisions affect classification but not the safe first two actions: establish the manifest/guardrail and archive the 177 MB of non-production model artifacts.
