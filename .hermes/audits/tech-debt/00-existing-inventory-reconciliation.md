# Existing GameFlow Tech-Debt Inventory Reconciliation

**Audit lane:** Lane A — existing debt and plan reconciliation

**Audit date:** 2026-07-18

**Mode:** Read-only audit; this report is the only file written

**Scope:** Existing register entries, GBrain Known Issues, `ACTIONITEMS.md`, the root issue tracker, the ten god-class migration plans, the MLB stat-suite rebuild, and recent relevant handoffs

**Constraint:** No new TD IDs are proposed here

## Executive conclusion

The existing inventory is useful but is not yet a reliable current backlog.

- `docs/understanding/tech-debt-register.md` is the intended authoritative register, but its six entries have not been reconciled against later implementation and runtime evidence.
- TD-003 and TD-004 are materially stale as written: god-class lanes 01-02 are complete, lane 03 is core-complete with an explicitly deferred optional cleanup, and most of the MLB stat-suite rebuild implementation is committed.
- GBrain `operations/known-issues` mixes active defects, old production observations, accepted constraints, resolved product work, and workflow guidance. Several entries are now contradicted by current code.
- `ACTIONITEMS.md` is a March 2026 chronological session log, not a current action tracker. Its top nine items require individual reconciliation, while hundreds of older repeated “remaining action items” are historical duplicates.
- Root `ISSUES.md` is intentionally absent from `HEAD`: commit `bdcb381` deleted it on 2026-06-21. GBrain still claims it exists and summarizes four deferred issues from it. The missing tracker is therefore a source-of-truth contradiction, not merely a missing local file.
- God-class lanes 01 and 02 are complete; lane 03 is core-complete/accepted-deferred; lanes 04-10 remain documentation-only according to their own progress logs.
- The MLB stat-suite rebuild is substantially implemented and committed. Its status prose saying “complete locally; pending Chase review/commit” is stale. The remaining gap is plan closeout and explicit proof against all whole-program done criteria, not a restart of the rebuild.

Preliminary disposition:

- **Confirm:** current GBrain direct-read/source-routing fragility; stale scheduler documentation; duplicate 10:00 AM NBA lines trigger; in-memory Ask API rate limiting; documentation/source-of-truth drift.
- **Shelf:** optional Lane 03 facade import cleanup; god-class lanes 04-10 until their business priority is approved; lower-priority UI/mobile cleanup.
- **Reject:** the known-issues claims that `test_finds_latest_run_directory` still fails, that AI chat is not persisted, that history lacks pagination, and that the repository has no CI.
- **Needs Evidence:** live DB/table-size/index claims, current production linker recurrence, actual production env gates, Vercel/Stripe/NCAAB deployment state, and GBrain stale-page health.

## Method and evidence boundaries

### Sources checked

- `AGENTS.md`
- GameFlow structural-audit, god-class migration, and model-suite rebuild guidance from `gameflow-explore`
- `docs/understanding/tech-debt-register.md`
- `ACTIONITEMS.md`
- Root `ISSUES.md` presence and Git history
- `.hermes/plans/2026-07-14_210940-gameflow-tech-debt-census-and-agent-execution-program.md`
- `.hermes/plans/god-class-migrations/README.md`
- All ten god-class lane progress logs and done-when sections
- `.hermes/plans/mlb-stat-suite-rebuild/README.md` and the eight detailed plan documents
- Current targeted code/tests for disputed claims
- GBrain canonical `/home/chase/GameFlowBrain/Operations/Known-Issues.md`, read over SSH from `gameflow-agent`
- Recent GBrain handoffs 099-110, with focused use of 099, 100, 102-106, and 108-109
- Scoped Git history for the issue tracker, migrations, and stat-suite rebuild

### Evidence limitations

- No Supabase/production DB query was made. Table sizes, index state, live row counts, and applied migrations remain unverified.
- No Railway/Vercel environment or logs were queried. Production env-gate and deployment claims remain unverified unless a handoff explicitly records the deploy.
- Hermes did not expose GBrain MCP tools in this session. Canonical remote markdown was accessible over SSH, so this report used `/home/chase/GameFlowBrain` rather than a stale local mirror.
- Current code was sampled only to resolve direct contradictions. This was not Lane B, C, D, E, F, G, or H implementation discovery.
- `ACTIONITEMS.md` has 3,154 lines of repeated session snapshots. This audit reconciles the current top-level nine-item list and treats older repeated lists as historical duplicates; it does not pretend each repeated occurrence is an independent backlog item.

## Classification vocabulary

- **current:** evidence supports an active issue or still-applicable plan.
- **stale:** the statement or status no longer describes current state.
- **resolved:** implementation and validation evidence show the issue is closed.
- **partial:** meaningful implementation landed, but plan/done criteria or operational validation remain incomplete.
- **superseded:** a newer architecture, tracker, plan, or decision replaced the item.
- **duplicate:** the same issue is already represented by another item/plan.
- **accepted-deferred:** the issue is understood and intentionally not blocking current work.
- **needs evidence:** current state cannot be established safely from available read-only evidence.

Triage is preliminary: **Confirm / Shelf / Reject / Needs-Evidence**.

---

## 1. Canonical tech-debt register reconciliation

### TD-001 — GBrain direct-read/source-routing fragility

- **Classification:** current
- **Preliminary triage:** Confirm
- **Exact evidence:**
  - Register lines 58-77 describe source-scoped query success while direct `get_page`/`list_pages` can fail.
  - GBrain `Operations/Known-Issues.md`, section `GBrain Retrieval / Sync Issues`, independently records manual import/sync source-routing ambiguity and a locally carried source-scoped query patch.
  - The July census plan, lines 727-728, says the latest resume still exposed direct-read/list fragility while source-scoped query and remote canonical markdown remained available.
  - This audit could access remote canonical markdown over SSH, but no direct GBrain MCP tool was available to perform the requested quiet-window route comparison.
- **Contradiction:** Known Issues says query-time source isolation is fixed in a local carried GBrain patch, but that does not prove Hermes direct page/list source selection is fixed. These are related but distinct retrieval paths.
- **Coverage gap:** No fresh-session direct `get_page`/`list_pages`/query/raw-MCP comparison was run.
- **Disposition:** Keep the item. Its next evidence step should remain the quiet-window route matrix already specified by TD-001; do not merge it into generic “GBrain is down” language.

### TD-002 — GBrain stale-pages metadata hygiene ambiguity

- **Classification:** needs evidence
- **Preliminary triage:** Needs-Evidence
- **Exact evidence:**
  - Register lines 79-97 cite high `stale_pages` with clean embedding/orphan health.
  - GBrain Known Issues still cites `stale_pages = 67` on 2026-05-13 and explicitly says to audit before treating it as current retrieval risk.
- **Contradiction:** The count is more than two months old. No current health output was retrieved in this audit.
- **Coverage gap:** Current `get_health`, stale-page identities, and metadata-cause classification were not available.
- **Disposition:** Retain as an evidence task, not a confirmed active defect. Do not run metadata repair from the old count.

### TD-003 — Structural migration lanes remain partially complete

- **Classification:** stale
- **Preliminary triage:** Confirm after rewriting the status, not as currently worded
- **Exact evidence:**
  - Register lines 99-116 only says “multiple lanes” have mixed states.
  - Migration index lines 30-32 marks Lane 01 complete, Lane 02 complete, and Lane 03 core-complete.
  - Lane 01 done-when lines 1282-1289 are fully checked; its 2026-05-26 closeout records 27 focused passing tests and states remaining work is artifact interpretation, not structural migration.
  - Lane 02 lines 1237-1255 declares complete and checks every done-when criterion.
  - Lane 03 lines 951-961 checks every core criterion and explicitly defers only optional Phase 7 import cleanup.
  - Lanes 04-10 each contain only a 2026-05-19 “initial migration documentation” progress entry and unchecked prose done criteria.
  - Handoff 100 says Lane 03 is core-complete and recommends moving to another lane rather than doing Phase 7 by default.
- **Contradiction:** TD-003 groups resolved, accepted-deferred, and unstarted lanes under one “partially complete” candidate.
- **Coverage gap:** No current structural code audit was performed for lanes 04-10; their plan logs are the authoritative implementation-status evidence for this reconciliation.
- **Disposition:** Replace the umbrella ambiguity during later register adjudication with a status matrix or links to the migration index. Do not create duplicate TD IDs in this report.

### TD-004 — MLB stat-suite tooling fragmentation

- **Classification:** partial
- **Preliminary triage:** Confirm only as closeout/remaining-boundary debt
- **Exact evidence:**
  - Register lines 118-135 describes the rebuild as a candidate and recommends reviewing plan status.
  - `.hermes/plans/mlb-stat-suite-rebuild/07-implementation-sequence.md` records Slices 1-5 complete, Slice 6 core lifecycle extraction complete, and Slice 7 runbook complete.
  - Current files exist for generic operation and shared training: `scripts/run_mlb_stat_ablation.ps1`, `scripts/resume_mlb_stat_ablation_audit.ps1`, `src/models/mlb/training/profiles.py`, `feature_controls.py`, `artifacts.py`, and `base_orchestrator.py`.
  - Current focused tests exist for the generic runner, profiles, controls, artifacts, base orchestrator, and pitcher variants.
  - Git history contains committed implementation: `5abce93`, `1899a31`, `b79e7b5`, `d730eda`, `bdcb381`, and `d3a4ba5`.
  - Handoff 103 says Slices 2-5 were complete; Handoffs 108-109 show later pitcher-K evaluation/ablation activity, so the suite was not merely a dry-run scaffold.
- **Contradiction:** Plan statuses repeatedly say “complete locally; pending Chase review/commit,” but the corresponding commits are in Git history. The register’s wording understates implementation.
- **Coverage gap:** The plan has no single final closeout marker against all whole-rebuild done criteria. Strategy-module extraction was explicitly deferred, and this audit did not rerun the full 1,188-test evidence cited in the plan.
- **Disposition:** Treat the original fragmentation problem as substantially remediated. Keep only the precise remaining closeout/compatibility questions after a scoped Lane F audit; do not restart or duplicate the rebuild.

### TD-005 — Production scheduler/job complexity and stale docs

- **Classification:** current
- **Preliminary triage:** Confirm
- **Exact evidence:**
  - Register lines 137-159 cites current `scheduler.py`, stale schedule docs, and resilience tests.
  - Handoff 106 lines 45-55 explicitly upgraded TD-005 to confirmed and designated current scheduler code plus `docs/understanding/railway-scheduler.md` as schedule truth.
  - Recent handoffs 102, 104, and 105 document production scheduler/connection-pressure/lines-job incidents and hardening, confirming operational complexity remains consequential.
  - Current `src/orchestration/scheduler.py` still contains many explicit registrations and multiple env gates.
- **Contradiction:** None that resolves the debt. Recent hardening mitigates incidents but does not make old schedule prose current or replace the planned registry boundary.
- **Coverage gap:** No production schedule/log comparison was run; that belongs to Lane E.
- **Disposition:** Keep confirmed. Deduplicate it with god-class Lane 07 rather than creating a parallel scheduler migration plan.

### TD-006 — Duplicate 10 AM NBA props-only scheduler trigger

- **Classification:** current
- **Preliminary triage:** Confirm
- **Exact evidence:**
  - Register lines 161-179 identifies the collision.
  - Current `src/orchestration/scheduler.py:884-889` registers `lines_props_10am` at 10:00 ET.
  - Current `src/orchestration/scheduler.py:925-930` registers `props_every_5` for `9-23` at `*/5`, which includes 10:00 ET.
  - `src/orchestration/scheduler.py:116-120` still limits the in-process overlap lock to `lines_job.py`.
- **Contradiction:** Handoff 105 says scheduler timeout/overlap hardening was deployed, but the duplicate registration remains in current code. Hardening is mitigation, not resolution.
- **Coverage gap:** No Railway `job_executions`/log evidence was queried to prove routine skip noise.
- **Disposition:** Keep confirmed as a bounded scheduler issue. Runtime frequency/noise still needs Lane E evidence before deciding remove vs offset vs accept.

---

## 2. GBrain `operations/known-issues` reconciliation

The page is correctly labeled “active issue inventory with possible stale entries,” but its sections are not a single backlog taxonomy.

### Active Bugs

| Existing item | Classification | Triage | Evidence and contradiction |
|---|---|---|---|
| Railway Lines Scraper intermittent linker failures | needs evidence | Needs-Evidence | Known Issues cites Handoff 088 and says later last-30-day unlinked count reached zero. Handoffs 104-105 concern a later lines/injury/scheduler incident and hardening, but do not prove the incremental linker recurrence is closed. No current Railway traceback/log was inspected. |
| `test_finds_latest_run_directory` failing | resolved | Reject | `tests/test_run_backtest.py:25-29` now creates `nba_run_*` directories. Scoped execution on 2026-07-18 passed: `1 passed`. `ACTIONITEMS.md:25` and Known Issues are stale. |
| MLB paper bets disabled because total-bases/runs-scored models leaked `at_bats` | stale | Needs-Evidence | Current `mlb_inference_job.py:256-258` places paper bets when not dry-run, not `--skip-bets`, and `MLB_TRADING_ENABLED` is true. Current scheduler docs list active models as pitcher strikeouts, batter hits, and batter RBIs, not the two leaked stat lanes. Actual production env value was not inspected. Rewrite as an env/model-support verification item if still needed. |

### “Recently Fixed (Session 15)”

- **Classification:** resolved
- **Triage:** Reject as active debt; preserve as historical regression evidence
- **Items:** Railway MLB daily stats timeout, unresolved MLB bets caused by missing game stats, `pitcher_outs` column mismatch, and missing MLB Discord P&L summary.
- **Evidence:** The section itself marks them fixed. No later handoff in 099-110 claims recurrence.
- **Coverage gap:** This audit did not rerun those original production paths. Lane C/E/G should check whether regression tests exist before deleting historical context.

### Technical Debt section

| Existing item | Classification | Triage | Evidence and contradiction |
|---|---|---|---|
| `raw_player_props_combined` at 67M+; archive/partition | needs evidence | Needs-Evidence | The count and 9-14s timings are old and DB-backed. They overlap Known Issues performance bottlenecks and ACTIONITEMS index/drop discussions. No SQL was run. Preserve as one data-lifecycle item, not three duplicates. |
| In-memory `/api/ask` rate limiting | current | Confirm | `dashboard/src/app/api/ask/route.ts:8-32` defines `RATE_LIMIT`, a process-local `Map`, and `checkRateLimit`. This is exact current code evidence. It also overlaps god-class Lane 09. |
| No pagination on history/performance | partial | Confirm only for the remaining surface | Current `dashboard/src/app/(protected)/history/page.tsx:61-63` says React Query hooks fetch with pagination. No corresponding pagination marker was found in the targeted performance page. The blanket claim is stale; history is resolved, performance requires focused verification. |
| DFS/heatmap mobile horizontal scroll | needs evidence | Shelf | Old ACTIONITEMS snapshots also call this future work. No browser/mobile audit was performed. Defer to Lane H rather than treating it as confirmed from old prose. |
| AI chat not persisted across modal close | resolved | Reject | Current `dashboard/src/app/api/ask/history/route.ts:25-43` reads `chat_conversations`/`chat_messages`, and lines 69-73 support deletion. The known-issue statement is stale. |
| No CI/CD; deploys manual git push | partial | Confirm only for deployment automation | `.github/workflows/ci.yml` exists, so “No CI” is false. Whether Railway/Vercel deploy and rollback are manual was not inspected. Rewrite as “deployment/rollback automation needs evidence” if retained. |

### GBrain Retrieval / Sync Issues

| Existing item | Classification | Triage | Evidence and contradiction |
|---|---|---|---|
| High `stale_pages` | needs evidence | Needs-Evidence | Duplicate of TD-002; old count from 2026-05-13. Keep one canonical item. |
| Manual import/sync source routing suspect | current | Confirm | Supports TD-001. Distinguish import/sync routing from direct-read routing. |
| Query-time source isolation fixed only by private carried patch | accepted-deferred | Shelf | A known private maintenance constraint, not proof of an active GameFlowData code defect. Keep in GBrain operations unless upstream/rebase work is approved. |
| Hybrid natural-language query weakness | accepted-deferred | Shelf | Current workflow already requires hard-facts/lesson-list/keyword routes. This is an accepted retrieval limitation with a documented workaround. |
| Cross-source `gbrain link` gap | accepted-deferred | Shelf | Current workaround is explicit and the issue belongs to GBrain tooling, not a new GameFlowData TD item. |
| Frozen lesson evals passed 5/5 | resolved | Reject as debt | This is validation evidence/cautionary guidance, not an unresolved issue. Preserve in retrieval runbooks, not active inventory. |

### Deferred Issues copied from deleted `ISSUES.md`

All four are **superseded as tracker records** because root `ISSUES.md` was deleted, but their underlying code concerns require separate adjudication:

| Legacy issue | Classification | Triage | Reason |
|---|---|---|---|
| ISS-017 misleading ratio names | accepted-deferred | Shelf | Known artifact-compatibility constraint; Lane 03 preserves those historical names intentionally. Rename only with an approved retrain/artifact migration. |
| ISS-018 pre-game inference requires a game row | needs evidence | Needs-Evidence | Lane 03 moved context ownership but explicitly preserved behavior. A current real-client reproduction is needed before promotion to the canonical register. |
| ISS-020 `validate_features=False` disables XGBoost safety | needs evidence | Needs-Evidence | Old issue says blocked on pandas compatibility. Current dependency/runtime behavior was not inspected. |
| ISS-023 Stage 2 dedup keeps one stat/player/game | accepted-deferred | Shelf | The old rationale ties this to correlation-aware Kelly. No live/Kelly change is approved; keep as a modeling/trading design constraint pending Lane F/G evidence. |

### Performance Bottlenecks

- **DFS RPC 9-14s / authenticated 8s timeout**
  - **Classification:** duplicate / needs evidence
  - **Triage:** Needs-Evidence
  - **Evidence:** Duplicates the 67M-row item and old ACTIONITEMS query/index notes. Known Issues also records a 30s SECURITY DEFINER workaround.
  - **Disposition:** One Lane D finding should reconcile current table size, query plan, role timeout, RPC latency, retention, and safe index state. Do not preserve these as separate unconnected debt items.

---

## 3. `ACTIONITEMS.md` reconciliation

### Document-level status

- **Classification:** superseded
- **Preliminary triage:** Reject as an authoritative backlog; retain as historical session chronology
- **Exact evidence:** The document begins with “Session Summary (2026-03-25 — Session 90)” and then repeats earlier session summaries and “Remaining Action Items” through 3,154 lines. Recent canonical handoffs now extend through Handoff 110 dated 2026-07-16.
- **Contradiction:** GBrain and current plans describe substantial April-July implementation absent from the March top section.
- **Coverage gap:** Runtime/deployment state for some product and DB items cannot be inferred from Git alone.

### Current top nine items

| # | March item | Classification | Triage | Evidence |
|---|---|---|---|---|
| 1 | Retrain NBA model with position injury features | superseded | Reject as current instruction | The item predates later production models and June feature-store work. Current model choice must come from current artifacts/model handoffs and prior lessons, not this March command. |
| 2 | Deploy dashboard to Vercel and set Anthropic key | needs evidence | Needs-Evidence | Code history cannot prove current Vercel deployment/env state. Treat as product/infra verification, not an assumed outstanding deploy. |
| 3 | Monitor MLB pipeline “tomorrow” | stale | Reject | Time-bound to 2026-03-26. Handoffs 101, 107-109 document much later MLB CLV/inference/model work. Any current monitoring need requires new runtime evidence. |
| 4 | Check MLB pitcher-K probable-starter coverage | needs evidence | Needs-Evidence | Later pitcher-K models and audits exist, but no current schedule/coverage query was run. Reframe as a current coverage metric only if Lane F/E reproduces a gap. |
| 5 | Stripe integration | needs evidence | Needs-Evidence | No current deployed product-flow check was performed. Lane H should inspect current route/UI/webhook state before confirming. |
| 6 | NCAAB migrations/backfill/train | needs evidence | Shelf | This is a separate deferred product/model lane. DB migration state and current priority were not checked. |
| 7 | Clean old model backups | needs evidence | Shelf | Targeted filename search did not find the two named `production_old_2026*` paths under `src/models/artifacts`, but generated/untracked artifact layout was not broadly scanned. Do not perform cleanup from this old instruction. |
| 8 | Fix `test_finds_latest_run_directory` | resolved | Reject | Current test uses `nba_run_*`; scoped pytest passed 1/1. |
| 9 | Drop unused `idx_props_dfs_latest` | needs evidence | Needs-Evidence | DB truth was not queried. Any drop is DB-destructive and requires SQL-runner verification plus independent count/state checks. |

### Older repeated action lists

- **Classification:** duplicate / stale
- **Triage:** Reject as independent backlog items
- **Evidence:** The same Stripe, Vercel, NCAAB, model-backup, pagination, index, and MLB pipeline items recur across many session snapshots with differing status.
- **Disposition:** Use these entries only as historical leads. Promote an item to the canonical register only after current code/runtime/DB evidence. Do not count repeated occurrences as separate debt.

---

## 4. Root issue-tracker reconciliation

### `ISSUES.md`

- **Classification:** superseded
- **Preliminary triage:** Confirm the source-of-truth contradiction; do not restore the old tracker blindly
- **Exact evidence:**
  - `ISSUES.md` is absent from the working tree and `HEAD`.
  - Git history shows commit `bdcb381` (`feat: add shared MLB training artifact helpers`, 2026-06-21) deleted `ISSUES.md`.
  - The prior tracker had 43 issues and marked many fixed; GBrain retains only four deferred summaries.
  - GBrain Known Issues still ends with “See full issue tracker: `ISSUES.md` at project root,” which is false.
  - `ACTIONITEMS.md` repeatedly says “13 open issues remain in ISSUES.md,” which is also stale.
- **Contradiction:** The new July census program designates `docs/understanding/tech-debt-register.md` as authoritative, while GBrain and old action logs still route readers to the deleted tracker.
- **Coverage gap:** The deletion commit message does not explain whether every still-open issue was intentionally migrated. Only four deferred issues are represented in GBrain Known Issues.
- **Disposition:** Do not resurrect the 43-item tracker wholesale. During consolidation, use Git history to produce a one-time migration ledger: resolved historical issues stay historical; any still-current issue needs fresh evidence and then belongs in the canonical register without reusing or inventing TD IDs in this audit.

---

## 5. God-class migration reconciliation

### Summary matrix

| Lane | Plan classification | Implementation status | Preliminary triage | Exact evidence |
|---|---|---|---|---|
| 01 MLB quote-clean/backtest sweep | resolved | complete | Reject as active debt; preserve guards | Index line 30; plan lines 871-902 and 1282-1289; all done-when boxes checked; thin-runner/inventory/promotion-contract tests recorded. |
| 02 MLB feature-store boundary | resolved | complete | Reject as active debt; preserve guards | Index line 31; plan lines 1237-1255; all criteria checked; 96 focused and 133 lane-wide tests recorded. |
| 03 NBA/general feature-store boundary | accepted-deferred | core-complete | Shelf optional Phase 7 | Index line 32; plan lines 951-961; core criteria checked, callsite import cleanup explicitly optional; Handoff 100 confirms. |
| 04 Training orchestrator | current | documentation-only / not started in this lane | Shelf pending priority; Needs-Evidence for overlap | Progress lines 617-637 contain only initial documentation; done criteria lines 641-648 unchecked. MLB shared base work from the stat-suite plan overlaps part of this cross-NBA/MLB lane, so scope must be reconciled before implementation. |
| 05 Daily prediction runner | current | documentation-only / not started | Shelf pending priority | Progress lines 682-702 says documentation only; done criteria lines 706-713 remain prose. |
| 06 Paper-trading shared primitives | current | documentation-only / not started | Shelf pending trading priority | Progress lines 671-689 says documentation only/no DB or trading actions; done criteria lines 693-700 remain prose. |
| 07 Scheduler/job registry | current | documentation-only / not started | Confirm planning overlap with TD-005; Shelf implementation until approved | Progress lines 614-631 says documentation only; done criteria lines 635-641 require a declarative 45-job registry. Current scheduler incidents and stale docs make the plan relevant. |
| 08 Arbitrage matcher/scanner | current | documentation-only / not started | Shelf | Progress lines 654-672 says documentation only/no scan/actions; done criteria lines 676-682 remain prose. Arb is parked behind default-off env gates, reducing urgency. |
| 09 Dashboard Ask API route | current | documentation-only / not started | Confirm rate-limit debt; Shelf broad route migration | Progress lines 644-661 says documentation only; done criteria lines 665-671 remain prose. Current route still owns in-memory rate limiting, while chat persistence now exists, so the plan must be refreshed before execution. |
| 10 Dashboard god components/pages | current | documentation-only / not started | Shelf | Progress lines 595-613 says documentation only; done criteria lines 617-623 remain prose. Old claims about missing history pagination/chat persistence have drifted, so re-inventory before implementation. |

### Cross-plan contradictions and gaps

1. **Index vocabulary is inconsistent.** Lanes 04-10 show priorities (“Medium,” “Medium-low”) rather than implementation statuses, while lanes 01-03 show completion statuses. This invites false comparisons.
2. **Lane 04 overlaps the MLB stat-suite rebuild.** The stat-suite shared `base_orchestrator.py` and artifact helpers satisfy part of Lane 04’s MLB lifecycle/artifact goals, but Lane 04 also covers NBA and broader validation/promotion ownership. It is partial by overlap, not implemented as a lane.
3. **Lane 09 assumptions drifted.** Chat persistence exists now, while in-memory rate limiting remains. The plan should be refreshed before any implementation slice.
4. **Lane 10 assumptions drifted.** History pagination now exists; performance/mobile behavior still needs evidence.
5. **Completed lanes still require regression guards, not active debt labels.** Lane 01/02 anti-regrowth tests and Lane 03 facade guards are the ongoing control.

---

## 6. MLB stat-suite rebuild reconciliation

### Overall status

- **Classification:** partial
- **Preliminary triage:** Confirm a bounded closeout audit; reject a duplicate rebuild
- **Implementation assessment:** Substantially implemented and committed; status documentation is stale; final program closeout is missing.

### Slice-by-slice status

| Slice | Reconciled classification | Evidence |
|---|---|---|
| 0 plan set | resolved | Eight plan documents plus README exist. |
| 1 generic CLV analyzer | resolved | Plan marks complete; Handoff 099; commit `5abce93`; generic and compatibility tests documented as 36 passed. |
| 2 generic operational runner | resolved | `scripts/run_mlb_stat_ablation.ps1`, pitcher wrapper, resume audit wrapper, and static test exist; commit `1899a31`. “Pending commit” prose is stale. |
| 3 training profiles/controls | resolved | `training/profiles.py`, `feature_controls.py`, and tests exist; commit `b79e7b5`. |
| 4 pitcher-K controls | resolved | Pipeline/control changes and pitcher variant tests recorded; commit `d730eda`. |
| 5 shared artifacts | resolved | `training/artifacts.py` and tests exist; commit `bdcb381`. |
| 6 base orchestrator | partial | `training/base_orchestrator.py` and tests exist; commit `d3a4ba5`; plan explicitly calls this “core lifecycle extraction” and defers strategy modules/model-objective movement. |
| 7 baseline restoration docs/runs | partial | Runbooks are marked complete. Later Git history includes `773965d Trained new pitcher k model`, and Handoffs 108-109 record pitcher-K audit/ablation evidence, but this audit did not reconcile every artifact/command against the plan’s reproducibility criteria. |

### Whole-program done criteria

- **No cloned stat-specific large wrapper:** supported by the generic runner files; current.
- **Generic dry-run for batter and pitcher:** plan/test evidence supports it; not rerun here.
- **Generic CLV for batter/pitcher fixtures:** plan/test evidence supports it.
- **Existing batter compatibility:** compatibility wrapper/tests are documented.
- **Pitcher-K family controls:** files/tests and commits support it.
- **Shared training helper tests:** documented as passing, including a full-suite result of 1,188 tests at Slice 6.
- **Production model loading backward compatibility:** asserted by plan validation; not independently exercised here.
- **Exact baseline restoration commands/gates:** docs exist; artifact-to-command reproducibility was not independently checked.

### Contradictions

1. `07-implementation-sequence.md` says Slices 1-6 are pending review/commit, but Git history contains their commits.
2. The README still frames the rebuild as a future executive decision without a current status section.
3. Slice 7 says actual training/sweeps require approval/execution, while later commits/handoffs show pitcher training and audit work occurred.
4. TD-004 still describes genericization as recent candidate work rather than mostly-landed architecture.

### Disposition

Do not create a second stat-suite plan. A future Lane F closeout should:

1. map each whole-program done criterion to a current test/file/command;
2. decide whether deferred strategy-module extraction is accepted architecture or remaining debt;
3. verify current pitcher and batter dry-runs without long training;
4. reconcile baseline docs with the exact trained artifact and later Handoffs 108-109;
5. update the existing README/implementation sequence rather than opening a duplicate plan.

---

## 7. Recent handoff reconciliation

| Handoff | Relevance and classification |
|---|---|
| 099 — stat-suite Slice 1 | current implementation evidence; supersedes “plan-only” interpretation of CLV genericization. |
| 100 — NBA feature-store Lane 03 core complete | current completion evidence; establishes accepted deferral of optional callsite cleanup. |
| 102 — DB connection pressure/scheduler fix | current incident evidence for TD-005; production recurrence monitoring still needs evidence. |
| 103 — stat-suite Slices 2-5 | current implementation evidence, but its “ahead of origin/no push” state was superseded by commits now present in history. |
| 104 — NBA lines failure investigation | superseded by Handoff 105 for the implemented hardening, but preserves incident/root-cause context. |
| 105 — NBA lines hardening deployed | resolved remediation evidence; does not resolve the duplicate 10:00 trigger or broad scheduler architecture debt. |
| 106 — comprehension/debt scaffold | current authority for register intent and TD-005/TD-006 promotion. |
| 108 — pitcher-K dense CLV audit | current evidence that stat-suite tooling progressed into real model evaluation. |
| 109 — batter artifact correction/pitcher plan | current warning about artifact identity and later stat-suite usage; detailed model conclusions belong to Lane F. |
| 110 — remote Kanban validation | operationally recent but not evidence that any debt item or migration lane was resolved. |

No recent handoff was treated as timeless authority where current code or plan done criteria contradicted it.

---

## 8. Duplicate and supersession map

- **TD-001** duplicates/absorbs the actionable part of Known Issues manual source-routing and direct-read fragility. Keep detailed GBrain patch/workaround facts in operations docs.
- **TD-002** duplicates Known Issues `stale_pages`; retain one evidence task.
- **TD-003** is an umbrella over the god-class index and should not become ten new debt items automatically.
- **TD-004** points to the existing MLB stat-suite rebuild; no duplicate remediation plan is needed.
- **TD-005** overlaps god-class Lane 07 and stale scheduler docs; use one scheduler architecture plan plus separately scoped correctness findings.
- **TD-006** is a concrete current symptom within TD-005/Lane 07, not proof that the entire registry migration must be prioritized immediately.
- **Known Issues 67M rows**, **DFS RPC 9-14s**, **authenticated timeout**, and **ACTIONITEMS index/drop notes** are one DB/data-lifecycle evidence cluster, not four independent items.
- **Known Issues pagination/mobile/chat/rate-limit claims** overlap god-class Lanes 09-10 and Lane H; stale resolved portions must be removed before those plans are executed.
- **Deleted ISSUES.md deferred issues** are historical leads, not a parallel live tracker.

---

## 9. Coverage gaps for later lanes

These are gaps, not new debt IDs:

1. **Production DB truth:** current `raw_player_props_combined` size, RPC latency, index validity/usage, retention, role timeout, and whether `idx_props_dfs_latest` exists.
2. **Production scheduler truth:** actual duplicate-trigger skip rows, current overlap behavior, current job count versus Lane 07’s recorded 45, and whether recent connection pressure recurred.
3. **Production deployment truth:** Vercel dashboard version, Stripe completion, Railway env gates, and NCAAB migration state.
4. **GBrain live health:** current stale-page count/causes and a fresh direct/list/query/raw-MCP source-routing matrix.
5. **Issue migration completeness:** whether any of the deleted tracker’s non-deferred issues lacked regression tests or were deleted without explicit closure.
6. **God-class plan drift:** current code inventory for lanes 04-10 before implementation; especially Lane 04 overlap with shared MLB training and Lanes 09-10 dashboard changes.
7. **MLB stat-suite closeout:** current dry-runs, production-loader compatibility, strategy-module decision, and exact artifact/runbook reproducibility.
8. **Product/UI reality:** performance-page pagination, mobile table behavior, and current Stripe/Ask UI E2E behavior.
9. **Resolved production bugs:** regression-guard coverage for Known Issues “Recently Fixed” entries.
10. **Model/trading semantics:** legacy ISS-017/018/020/023 require Lane F/G evidence and relevant hard-facts/lesson retrieval before any recommendation.

---

## 10. Preliminary adjudication queue

### Confirm

- TD-001 GBrain direct-read/source-routing fragility, with distinct sub-paths documented.
- TD-005 scheduler complexity/stale docs, linked to Lane 07.
- TD-006 duplicate 10:00 NBA lines trigger.
- Current Ask API in-memory rate limiting.
- Root tracker/GBrain/ACTIONITEMS source-of-truth drift.
- Need for a bounded MLB stat-suite closeout rather than another rebuild.

### Shelf / accepted-deferred

- Lane 03 Phase 7 import cleanup.
- God-class lanes 04-06 and 08-10 until current priority and refreshed scope are approved.
- Lane 07 broad registry migration while small scheduler correctness/observability work is handled separately.
- Legacy ratio naming until an artifact-compatible retrain/migration.
- Correlation-aware Kelly/dedup redesign while live/Kelly work remains unapproved.
- Mobile layout and other low-risk UX cleanup pending Lane H.
- Private GBrain carried patch/upstreaming and hybrid-search limitations with documented workarounds.

### Reject as active debt

- `test_finds_latest_run_directory` failure.
- “AI chat is not persisted.”
- “History has no pagination.”
- “No CI.”
- God-class lanes 01 and 02 as unfinished structural debt.
- Lane 03 core migration as unfinished merely because optional import cleanup remains.
- March “monitor tomorrow” instructions and repeated historical ACTIONITEMS as a live backlog.
- GBrain frozen lesson eval success as an issue.

### Needs-Evidence

- TD-002 current stale-page health.
- Current Railway linker recurrence.
- Whether MLB paper betting is disabled in deployed env and for which supported stats.
- Current table size/query/index claims.
- Performance-page pagination/mobile behavior.
- Deployment/Stripe/NCAAB state.
- Legacy ISS-018 and ISS-020 current reproduction.
- Stat-suite final compatibility/artifact closeout.

---

## 11. Recommended reconciliation actions for the later integration phase

This audit does not edit canonical sources. When Chase approves consolidation:

1. Keep `docs/understanding/tech-debt-register.md` authoritative and update existing TD-001 through TD-006 in place; do not mint IDs from this report automatically.
2. Replace TD-003’s umbrella ambiguity with the lane status matrix and link to the existing migration index.
3. Rewrite TD-004 around the precise remaining stat-suite closeout, or mark the original fragmentation resolved if the closeout confirms all criteria.
4. Remove or move resolved Known Issues claims (`test_finds_latest_run_directory`, chat persistence, history pagination, “no CI”) to historical validation notes.
5. Fix GBrain Known Issues’ broken pointer to deleted root `ISSUES.md` and state that the canonical register superseded it.
6. Treat `ACTIONITEMS.md` as archived chronology or place an explicit non-authoritative banner at its top; do not continue appending current backlog to old session snapshots.
7. Refresh god-class plans 04, 09, and 10 before implementation because overlapping MLB/dashboard work has changed their starting state.
8. Close out the existing MLB stat-suite plan with current commit/test/artifact evidence; do not create a duplicate plan.
9. Route DB/runtime unknowns to Lane D/E read-only evidence gathering, not assumptions or destructive cleanup.
10. Require regression guards or real-client E2E evidence before declaring old production bugs permanently resolved.

## Validation performed

- Confirmed this report contains: scope/method, classification vocabulary, every TD item, GBrain issue sections, top ACTIONITEMS, root tracker, all ten god-class lanes, MLB stat-suite status, recent handoffs, contradictions, coverage gaps, triage, and no new TD IDs.
- Scoped test executed to resolve a disputed issue: `tests/test_run_backtest.py::TestFindLatestModelDir::test_finds_latest_run_directory` — **1 passed**.
- No DB query, model training, backtest, production job, deploy, config change, or code edit was performed.
