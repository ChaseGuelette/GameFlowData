# GameFlow System-Overhaul Technical-Debt Adjudication

**Adjudication date:** 2026-07-18
**Mode:** final large-context, read-only meta-review
**Output scope:** this report only
**Decision overlay:** Chase's 2026-07-18 decision makes Kalshi sports and most related app functionality decommissioning targets, not feature-hardening targets.

## Executive verdict

GameFlow does not need a single repository-wide rewrite. The evidence supports a dependency-ordered program dominated by four safety/correctness contracts and four pruning/consolidation programs:

1. **Remove forbidden behavior before refactoring it.** The executable global conformal-offset path is a critical invariant violation. Remove its activation/writer path and protect the empirical-CDF/Q10 contracts.
2. **Contain and decommission Kalshi sports before preserving its architecture.** Report 12's ordered cut supersedes Kalshi sports hardening, Lane 06 parity work, Lane 08 as currently framed, and Kalshi-specific dashboard/live-lifecycle investment. Report 10 quantifies the credit: up to 1,442 sports/lifecycle triggers per day can disappear after authoritative closeout.
3. **Close model promotion identity, evidence, and rollback contracts.** Manifest readers, required-stat completeness, sweep/CLV/ranker lineage, fail-closed promotion evidence, atomic promotion, and tracked production provenance are one root-cause family. Do not restart the MLB stat-suite rebuild or completed god-class lanes.
4. **Fix bounded runtime correctness before Lane 07 extraction.** MLB lines false success, exact schedule collisions, restart-blind retries, the duplicate NBA 10:00 trigger, and the missing CDN-only guard are smaller and safer than a scheduler rewrite.
5. **Establish database authority and access contracts before schema cleanup.** The migration chain, RLS/grants/definer functions, object ownership, retention, and recovery cannot be repaired safely from tracked files alone. File-only manifests come first; live metadata remains approval-gated.
6. **Fix paid-product boundaries before page/route decomposition.** Ask entitlement/usage, Stripe retry/idempotency, performance query fan-out, and dashboard testing are active supported-product concerns. Kalshi routes should be removed, not hardened.
7. **Prune deploy and knowledge weight before broad refactors.** Report 13 and report 10 prove that named non-production model artifacts are 168,994,105 bytes, 76.81% of the 220,023,222-byte tracked checkout; 74,754,388 bytes are extra byte-identical copies. Stale trackers, local brain mirrors, tracked temp/log residue, duplicate plan indexes, and generated residue should be removed/archive-consolidated after their stated dependency checks.
8. **Protect completed boundaries.** God-class Lanes 01 and 02 are complete; Lane 03 is core-complete with accepted optional cleanup. MLB feature-store facades, current production/playoff artifacts, applied migration history, completed migration evidence, and anti-regrowth tests are explicit retains.

The immediate overhaul is therefore **cuts + contract tests + bounded correctness fixes**, not broad god-class decomposition. Lanes 04-05 and 09-10 remain useful routing plans only after their baselines are refreshed; Lanes 06 and 08 are materially superseded by the product cut; Lane 07 must be resized after Kalshi removal.

## Scope and authority

Read completely for this adjudication:

- `AGENTS.md` and `.hermes/audits/tech-debt/README.md`;
- completed reports `00` through `10`, excluding the intentionally absent `11`, plus reports `12` and `13`;
- `docs/understanding/tech-debt-register.md`;
- `.hermes/plans/god-class-migrations/README.md` and the implementation-status evidence quoted by report 00;
- remote canonical `/home/chase/GameFlowBrain/Operations/Hard-Facts.md`, `/home/chase/GameFlowBrain/Operations/Critical-Invariants.md`, and `/home/chase/GameFlowBrain/Execution-Plan.md` over read-only SSH to `gameflow-agent`.

No broad source rescan was repeated. Exact source evidence below is inherited from the completed lane reports and was cross-checked for contradictions across reports. This review did not run SQL, provider calls, jobs, tests, builds, model loads, training, backtests, deployments, or source/config/register/plan/card/GBrain edits.

The audit directory and this report are currently untracked local artifacts. That is durability evidence, not approval or Git history.

## Relevant prior lessons and invariants

- Never deploy global conformal recalibration offsets; repeated evidence says they hurt ROI. Q10 miscalibration is edge-bearing and must not be blindly corrected.
- Promotion-capable probabilities use empirical sample CDF, `(samples > line).mean()`, never Gaussian CDF. Quoted-contract comparator normalization is a separate semantic layer.
- Full retrains are risky; production hyperparameters must be locked and validated. Selector output is not an ablation. Correlated feature families require family-level validation.
- Quote-clean replay, linked coverage/dropout, CLV, ranking, and intraday evidence are a sequence; a quote-clean flag alone is not promotion evidence.
- Raw timestamps do not prove temporal integrity; as-of behavior must hold end to end.
- Never put advanced-stat scraping on Railway. Railway `daily_stats_job` is CDN-only.
- Never run non-concurrent `CREATE INDEX` on `raw_player_props_combined`.
- Python backend access uses `postgres`; dashboard/client access uses `authenticated` with RLS. Do not collapse these roles.
- Main-context agents do not call Supabase MCP. Destructive DB-adjacent scope requires independent count verification.
- Completed compatibility facades and anti-regrowth tests are architecture controls, not clutter.

No canonical lesson specifically defined atomic model promotion/rollback or a model-manifest reader transaction. Those remain engineering contracts to prove, not invented modeling policy.

## Quantitative audit inventory

| Inventory | Count / measurement | Interpretation |
|---|---:|---|
| Completed reports reviewed | 13 | `00-10`, `12-13`; `11` is this deliverable |
| Numbered raw findings | 108 | 8 PA, 8 TV, 8 DB, 10 E, 9 MMP, 10 TMS, 13 F, 9 I, 10 AKW, 8 PSC, 15 BP |
| Canonical register entries | 6 | TD-001 through TD-006; update these before considering new IDs |
| God-class migration lanes | 10 | 2 complete, 1 core-complete/accepted-deferred, 7 documentation-only before product-cut overlay |
| Tracked Python audit surface | 249 files / 64,557 LOC | Report 01; size routed reads but was not debt by itself |
| Python test inventory | 124 test files / 1,121 test functions | Report 02; significant breadth, weak promotion/dashboard/CI contracts |
| Dashboard tests | 0 | No tracked dashboard test files or test runner |
| Scheduler registrations | 48 call sites; 45 default | Before product cuts; Lane 07's 45-site plan baseline is stale |
| Kalshi configured triggers | 1,532/day | 1,442 sports/lifecycle; 90 unresolved non-sports |
| Tracked checkout | 1,432 files / 220,023,222 bytes | Report 13/10 tracked-file accounting |
| Named non-production model artifacts | 168,994,105 bytes | 76.81% of checkout; archive after identity/rollback gates |
| Proven duplicate model copies | 74,754,388 extra bytes | 23 SHA-256 duplicate groups; identity does not itself authorize deletion |
| Root-cause clusters adjudicated below | 30 | Every cluster carries the required fields |

The 108 findings are not 108 backlog items. They reduce to 30 clusters, including explicit retains, product-cut supersessions, shelves, and evidence queues.

## Root-cause and adjudication matrix

### RC-01 — Forbidden global conformal-offset execution path

- **Disposition:** Confirm
- **Severity / confidence:** Critical / High
- **Source reports and exact evidence:** PA-01 (`01:52-81`), MMP-01 (`05:114-144`), TV-04 (`02:143-168`), BP-01 (`13:102-149`). Writers: `src/models/train_pipeline.py:705-864,1049-1052,1163-1166`; consumers: `src/models/monte_carlo.py:137-859`, `src/orchestration/inference_job.py:161-174`, `edge_refresh_job.py:229-283`, `src/backtesting/run_backtest.py:188-198`, `run_sweep.py:1031-1038`.
- **User/runtime impact:** File presence silently changes production, refresh, backtest, and sweep probability policy in direct conflict with the strongest invariant; model comparisons can attribute offset effects to model binaries.
- **Dependencies:** Inventory all consumers; retain report-only diagnostics if useful; do not alter Q10/model math.
- **Cheap cut vs refactor:** **Cheap policy cut before Lane 04.** Remove implicit activation/writing; do not extract it as supported policy.
- **Safe first evidence step:** No-DB temporary-directory characterization proving which consumers activate a synthetic offset artifact.
- **Done condition:** No production/promotion-capable path activates offsets by file presence; production validators reject the artifact; any legacy analysis mode is explicit/non-production; anti-regrowth tests cover writer and consumers.
- **Canonical-register action:** Create a new TD only after Chase confirms the cut; do not hide it inside TD-003/004. Proposed title: “Forbidden global calibration-offset path remains executable.”

### RC-02 — Probability/edge policy divergence across daily, backtest, sweep, and fallback paths

- **Disposition:** Confirm
- **Severity / confidence:** Critical / High
- **Source reports and exact evidence:** PA-02 (`01:83-112`), PA-06 (`01:205-233`), TV-04/TV-07 (`02:138-168,228-255`), Lane 05. NBA backtest Gaussian fallback: `src/backtesting/backtest_harness.py:671-866`; daily quantile fallback: `src/models/daily_runner.py:876-952`; fast sweep duplication: `src/backtesting/run_sweep.py:224-346`.
- **User/runtime impact:** A backtest can validate probabilities and selections that daily inference does not execute; the Gaussian fallback creates phantom edge and violates the empirical-CDF invariant.
- **Dependencies:** Shared deterministic fixtures; preserve completed MLB Lane 01 edge seam; explicitly decide no-sample behavior.
- **Cheap cut vs refactor:** First cut forbidden Gaussian promotion behavior and add parity tests; only then extract Lane 05 shared policy.
- **Safe first evidence step:** Table-driven no-DB fixtures for populated, empty, and missing-key samples across NBA daily/backtest/fast sweep.
- **Done condition:** One typed owner defines lookup, clipping, devig, no-sample behavior, and output fields; promotion-capable paths use empirical CDF or fail closed; optimized/canonical parity is required in CI.
- **Canonical-register action:** New TD candidate after RED evidence; link Lane 05 rather than create a second migration plan.

### RC-03 — Model-suite identity, completeness, lineage, promotion, rollback, and tracked provenance are not one contract

- **Disposition:** Confirm
- **Severity / confidence:** Critical / High
- **Source reports and exact evidence:** PA-03/04 (`01:114-173`), MMP-02/04/05/07/08 (`05:146-178,214-280,315-381`), PSC-01 (`10:75-106`), BP-02/03 (`13:153-259`). NBA resolvers: `run_backtest.py:46-61`, `run_sweep.py:838-870`; MLB permissive loader: `mlb_model_suite.py:167-303`; promotion delete-before-copy: `scripts/promote_model.py:60-105`; ignored NBA JSON: `.gitignore:23-28`.
- **User/runtime impact:** Incomplete/mixed suites can load partially; sweep/CLV/ranker evidence can bind to the wrong candidate; promotion can delete the valid target and fail; clean clones omit provenance; storage cannot be safely pruned.
- **Dependencies:** Versioned manifest reader/writer, required/optional stat contract, hashes, evidence envelope, clean-clone packaging, atomic pointer/swap, rollback, archive destination.
- **Cheap cut vs refactor:** Contract-first refactor. Do not archive binaries or restart the stat-suite rebuild before the validator/rollback seam exists.
- **Safe first evidence step:** Temporary fake-directory tests for incomplete, mixed, corrupt, stale-extra, wrong-stat, lineage-mismatch, copy-failure, and forbidden-offset candidates.
- **Done condition:** NBA/MLB deployable suites have authoritative versioned manifests and hashes; every consumer validates required stats/features; sweep→CLV→ranker lineage is immutable; promotion is explicit, staged, audited, atomic, and rollback-tested; clean clone matches deployed identity.
- **Canonical-register action:** Rewrite TD-004 to this bounded remaining consumer/promotion closeout, or mark TD-004 resolved and create one new artifact-lifecycle TD. Do not preserve the old “stat-suite fragmentation” wording.

### RC-04 — Promotion metadata fails open when linked-coverage/dropout evidence was not run

- **Disposition:** Confirm
- **Severity / confidence:** Critical / High
- **Source reports and exact evidence:** MMP-03 (`05:180-212`), TV-04. `promotion_contracts.py:29-60` sets `promotion_grade` from quote-clean mode before coverage proof; `run_mlb_stat_ablation.ps1:253-269` skips dropout; `run_mlb_quote_clean_audit_suite.py:322-376` can classify absent evidence as adequate/PASS.
- **User/runtime impact:** Discovery output can be machine-labeled promotion-grade or approve sizing/feature expansion without linked coverage/dropout proof.
- **Dependencies:** None on model retraining; semantics only. Preserve quote-clean/CLV tools and completed Lane 01 structure.
- **Cheap cut vs refactor:** Cheap fail-closed metadata correction with fixture tests.
- **Safe first evidence step:** Fixture matrix for dropout PASS, FAIL, WARN, and not-run with passing CLV/ranker rows.
- **Done condition:** Not-run is machine-readable incomplete/not promotable; abbreviated discovery cannot emit sizing/feature approval; every prerequisite is explicit and tested.
- **Canonical-register action:** New TD candidate only if not absorbed into rewritten TD-004 artifact/evidence closeout.

### RC-05 — Feature-contract enforcement and stochastic reproducibility are permissive

- **Disposition:** Confirm
- **Severity / confidence:** High / High for permissive behavior; Medium for production frequency
- **Source reports and exact evidence:** MMP-06/09 (`05:282-313,383-415`). Missing selected MLB features are zero-filled in `mlb_monte_carlo.py:134-145,244-250` and `mlb_model_suite.py:124-164`; forced family members may be absent; pitcher/copula RNG is stateful while batter paths use keyed seeds.
- **User/runtime impact:** Source drift can silently become zero features; declared feature-family experiments can differ from effective features; prediction order can change pitcher samples near decision thresholds.
- **Dependencies:** RC-03 manifests; do not reopen completed feature-store boundaries or change model family/math.
- **Cheap cut vs refactor:** Add enforcement/coverage and keyed-seed contracts at consumers before broader Lane 04 extraction.
- **Safe first evidence step:** No-DB missing/renamed/default-only feature matrix plus order-invariance fixture for pitcher/copula predictors.
- **Done condition:** Required features fail closed unless an approved default policy is declared; effective family membership/coverage is persisted; all stochastic paths use versioned keyed seed policy and are order-invariant.
- **Canonical-register action:** New TD candidate, possibly one artifact/feature-compatibility sub-item under rewritten TD-004.

### RC-06 — Broad training-orchestrator migration remains unprioritized after partial shared MLB extraction

- **Disposition:** Shelf
- **Severity / confidence:** High structural consequence / High
- **Source reports and exact evidence:** PA-05 (`01:175-203`), Lane 04, report 00 (`00:278-293`). Concrete orchestrators still mix stages and call private feature-store methods, but the MLB stat-suite base/artifact work is real and Lane 04 is documentation-only.
- **User/runtime impact:** Changes remain coordinated across large workflows, but no current incident proves a full extraction outranks RC-01/03/04/05.
- **Dependencies:** Refresh Lane 04 against current shared modules; reconcile its Phase 6 wording with the never-offset decision; close artifact contracts first.
- **Cheap cut vs refactor:** Prefer RC-01/03/04/05 contract slices. Defer the broad facade shrink.
- **Safe first evidence step:** No-DB stage-call characterization and private-method inventory, only when Chase selects Lane 04.
- **Done condition:** If approved, thin facades with named stage owners and parity tests; otherwise record accepted architecture after contract risks are closed.
- **Canonical-register action:** Update TD-003 lane matrix; do not create a new TD or plan now.

### RC-07 — Legacy NBA/MLB backtest ownership duplicates canonical policy

- **Disposition:** Shelf
- **Severity / confidence:** Medium / High
- **Source reports and exact evidence:** PA-06, BP-10 (`13:535-578`), completed Lane 01. `run_mlb_backtest.py` is explicit legacy/debug-only; the harness still supplies shared constants/tests; NBA sweep private coupling is active and higher-risk.
- **User/runtime impact:** Two paths can drift, but the MLB legacy path is guarded and not silently promotion-capable.
- **Dependencies:** RC-02 shared policy; move `STAT_ACTUALS`; prove canonical one-config diagnostic equivalence.
- **Cheap cut vs refactor:** Consolidate after RC-02. Do not “repair” the legacy harness in place.
- **Safe first evidence step:** Static import/operator-use inventory and fixture parity through canonical one-config mode.
- **Done condition:** Required shared contracts move; legacy entrypoint/harness reach zero consumers and are removed with anti-regrowth guard.
- **Canonical-register action:** No new ID; link as accepted follow-up under Lane 01/RC-02 if approved.

### RC-08 — Scheduler ingestion correctness: false success, exact collisions, duplicate trigger, and restart-blind retries

- **Disposition:** Confirm
- **Severity / confidence:** Critical overall / High
- **Source reports and exact evidence:** E-01/02/05 (`04:121-186,255-285`), TD-006, PSC-05 (`10:208-239`). `mlb_lines_job.py:65-101,218-269` ignores failed child booleans; MLB collisions occur at 12:00/13:00/17:00/18:00; NBA has duplicate 10:00 registration; retry wrappers ignore persistent success.
- **User/runtime impact:** Stale/partial MLB data can be recorded as success; concurrent jobs duplicate API/DB/linker load; harmless NBA skip noise obscures operations; restarts repeat successful pipelines.
- **Dependencies:** Static schedule/resource fixture; mocked child/retry tests. Recalculate inventory after Kalshi cut.
- **Cheap cut vs refactor:** Cheap bounded fixes before Lane 07.
- **Safe first evidence step:** Mocked `mlb_lines_job` nonzero-exit RED test, cron/resource collision expansion, and persistent-success retry fixture.
- **Done condition:** Required child failures exit nonzero; no unsafe resource collisions; one intended NBA 10:00 path; restart-aware retry policy; outcomes remain attributable.
- **Canonical-register action:** Keep TD-006 but update it to confirmed until fixed. Update TD-005 with this bounded correctness child cluster; no duplicate IDs per symptom.

### RC-09 — Scheduler process ownership, run identity, misfire, gate, and schedule authority are fragmented

- **Disposition:** Confirm
- **Severity / confidence:** High / Medium-High
- **Source reports and exact evidence:** E-03/E-07/E-08/E-09/E-10 (`04:188-220,318-444`), I-04/I-05, TD-005. Direct-child timeouts do not own descendants; `job_executions` stores script rather than schedule/mode; APScheduler defaults/events are implicit; env validation is advisory; old docs claim Railway cron/old schedules.
- **User/runtime impact:** Descendants may outlive timeout, failures cannot be attributed to mode, missed runs leave no row, deployments can be alive but unrunnable, operators debug wrong schedules.
- **Dependencies:** Kalshi removal and RC-08 first; preserve code-plus-current explainer as interim truth; DB telemetry schema change is separately gated.
- **Cheap cut vs refactor:** Add structured run context and process-tree tests first; resize Lane 07 after cuts rather than implementing the stale 48/45-job design.
- **Safe first evidence step:** Harmless child/grandchild timeout fixture, pure execution-context tests, static job/env inventory, isolated APScheduler event fixture.
- **Done condition:** Full process trees are owned; every run/skip/misfire/disabled state has schedule ID/mode/attempt; concurrency/misfire policy is explicit; enabled-but-unrunnable jobs fail readiness; one generated schedule authority exists.
- **Canonical-register action:** Rewrite TD-005 with the post-cut scope and link refreshed Lane 07; keep one architecture item.

### RC-10 — Railway CDN-only invariant lacks an executable guard

- **Disposition:** Confirm
- **Severity / confidence:** High / High
- **Source reports and exact evidence:** E-04 (`04:222-253`), TV-04. Current compliant command: `daily_stats_job.py:391-395`; scraper branch: `nba_unified_scraper.py:931-932,972-978`; no scheduler/job characterization test.
- **User/runtime impact:** A future extraction could silently call blocked `stats.nba.com` from Railway and break daily stats.
- **Dependencies:** None; preserve current behavior.
- **Cheap cut vs refactor:** Very cheap anti-regrowth test before Lane 07.
- **Safe first evidence step:** Mock command capture asserting `--cdn-only` and no advanced endpoints in that branch.
- **Done condition:** CI fails if the flag disappears or CDN-only can reach `stats.nba.com`; registry metadata marks advanced stats local-only.
- **Canonical-register action:** Add as evidence/acceptance criterion inside TD-005, not a new TD.

### RC-11 — Settlement after upstream pipeline failure has undefined safety semantics

- **Disposition:** Needs Evidence
- **Severity / confidence:** Medium / Medium
- **Source reports and exact evidence:** E-06 (`04:287-316`). NBA/MLB critical failures set failure but settlement still runs; resolvers may safely skip missing data, but that was not proven.
- **User/runtime impact:** Stale/partial source state may produce delayed, no-op, or incorrect settlement.
- **Dependencies:** Fixture characterization of resolver preconditions; exclude retired Kalshi exchange settlement, which belongs to RC-27.
- **Cheap cut vs refactor:** Evidence only; do not change policy from static suspicion.
- **Safe first evidence step:** Mock each critical failure and resolver call; document which failure classes permit settlement.
- **Done condition:** Tested policy skips unsafe settlement and proves source-date/finality guards for allowed continuation.
- **Canonical-register action:** No ID until a failing/unsafe path is reproduced.

### RC-12 — Database migration/schema authority is fragmented and non-reproducible

- **Disposition:** Confirm
- **Severity / confidence:** Critical / High
- **Source reports and exact evidence:** DB-01/08 (`03:57-97,381-419`). `database/schema.sql` repeats table/index DDL; three SQL roots/manual channels exist; duplicate migration number 003; divergent `get_sportsbook_lines`; current objects lack one tracked owner; no generated type/replay gate.
- **User/runtime impact:** Clean bootstrap/recovery/decommission can omit or regress objects, RPCs, policies, or indexes; drift appears at runtime.
- **Dependencies:** File-only DDL classification manifest; then approval-gated live object fingerprints; disposable database replay.
- **Cheap cut vs refactor:** Authority manifest first; no live DDL or historical rewrite.
- **Safe first evidence step:** Classify each tracked DDL as deployable, historical/manual, superseded, or diagnostic.
- **Done condition:** One immutable ordered chain replays on a disposable DB; schema/RPC/RLS/grant/index contracts pass; generated snapshot/types are deterministic and non-deployable; live drift is reported.
- **Canonical-register action:** New TD candidate after Chase confirms DB authority as active debt.

### RC-13 — Dashboard/service-role/RLS/definer authorization is not one enforceable contract

- **Disposition:** Confirm
- **Severity / confidence:** Critical / High
- **Source reports and exact evidence:** DB-02 (`03:101-147`), F-01/F-02 (`07:62-109`). Generic evidence includes `rebuild_user_daily_log` definer caller-supplied user and missing safe search paths; Kalshi/arb routes are under-authorized service-role mutations.
- **User/runtime impact:** Browser/authenticated users may receive unintended privileged effects; definer functions may cross user/object boundaries.
- **Dependencies:** Split product cut from retained contract: remove Kalshi/arb routes under RC-27; statically classify all retained table/RPC callsites; live metadata later.
- **Cheap cut vs refactor:** Remove retired privileged routes; add shared auth/access matrix for retained APIs before broad dashboard refactor.
- **Safe first evidence step:** Mocked route auth matrix plus static `.from()`/`.rpc()` access-owner inventory; later read-only grants/policies/functions metadata.
- **Done condition:** Retired privileged routes are absent; retained APIs authorize server-side; each table/RPC has role/RLS owner; definer functions have safe path, least grants, and caller checks; CI rejects unclassified access.
- **Canonical-register action:** New retained-product security TD candidate. Do not register Kalshi hardening separately; its route issue is superseded by removal.

### RC-14 — High-volume write identity, provenance, and finality contracts are weak

- **Disposition:** Confirm
- **Severity / confidence:** High / High for source behavior; live scale needs evidence
- **Source reports and exact evidence:** DB-03/05 (`03:151-198,245-285`), PA-07 adjacency, report 04 idempotency gap. Raw NBA/MLB prop writers append without conflict handling; “dedupe” index is non-unique/incomplete; predictions/samples/bets lack stable relational lineage; derived logs lack rebuild identity.
- **User/runtime impact:** Retries inflate storage and produce nondeterministic latest rows; derivatives can outlive sources; cleanup cannot identify authority.
- **Dependencies:** Pure payload replay/identity fixtures; live duplicate/orphan/index evidence later; never add non-concurrent large-table index.
- **Cheap cut vs refactor:** Define future-write identities and provenance first; no broad dedupe/index cleanup.
- **Safe first evidence step:** Replay identical provider payload twice through fake writers and document expected identity/finality per dataset.
- **Done condition:** Scheduled writers have tested idempotency and tracked keys where appropriate; provenance IDs/derivatives are traceable; game IDs normalize before persistence; destructive cleanup remains independently counted and approved.
- **Canonical-register action:** New DB lifecycle TD candidate; keep one cluster rather than separate raw-prop, queue, and lineage IDs.

### RC-15 — Retention, archival, and recovery are contradictory and unverified

- **Disposition:** Confirm
- **Severity / confidence:** High / High for code contradiction; live backlog/recovery unknown
- **Source reports and exact evidence:** DB-04/07 (`03:202-241,342-377`), I-01/I-07 (`08:68-106,301-334`), PSC-06 (`10:241-272`). Effective hot retention is 7 days while scheduler says 30; archive insert is positional with missing tracked DDL; 500k/day cap; no terminal archive owner; local sync can truncate then succeed partially; no tracked restore contract.
- **User/runtime impact:** Consumers can lose expected hot coverage, archive schema can drift, backlog can grow, and partial analytical copies can be mistaken for backups.
- **Dependencies:** Human retention/RPO/RTO/owner decisions; file-only lifecycle registry; approval-gated date/size/inflow/schema/PITR evidence.
- **Cheap cut vs refactor:** Reconcile policy prose and fail-closed tooling tests first; do not choose 7 vs 30 or purge from comments.
- **Safe first evidence step:** Pure tests for explicit retention/named columns/max scope/failure exit; paper recovery inventory; later read-only SQL/provider metadata.
- **Done condition:** One lifecycle registry defines hot/archive/terminal retention and consumers; archive DDL/key is tracked; capacity has measured headroom/alerts; destructive tools are bounded/approved; disposable restore proves schema/security/data integrity.
- **Canonical-register action:** New TD candidate combining old 67M/DFS/archive/backup leads; do not create separate IDs from old counts.

### RC-16 — CI mutates code, validates weak/ambiguous contracts, and has privileged supply-chain drift

- **Disposition:** Confirm
- **Severity / confidence:** High / High
- **Source reports and exact evidence:** TV-01/02 (`02:52-107`), I-02/I-03 (`08:110-180`). Push CI runs unpinned/latest Ruff with `--fix`, third-party auto-commit action, branch-ref checkout, zero coverage floor, no Pyright, duplicated pytest/coverage config, mutable action tags/no explicit permissions; production lock authority disagrees.
- **User/runtime impact:** Green checks may describe a different tree; unreviewed source changes can land; clean installs can differ by commit-independent resolution; configuration edits can be inert.
- **Dependencies:** One Python dependency/config authority; GitHub settings metadata can be reviewed read-only; do not combine package updates with policy change.
- **Cheap cut vs refactor:** Non-mutating pinned CI and explicit permissions are cheap; dependency lock convergence is a separate verified build slice.
- **Safe first evidence step:** Inspect prior checkout SHAs/permissions; introspect selected pytest/coverage config; propose one dependency authority without installing.
- **Done condition:** CI validates event SHA, never pushes, pins action SHAs/tool versions, uses least permissions, runs one pytest/coverage config plus typecheck/ratchet, and Railway/CI consume one hash-verified Python 3.11 graph.
- **Canonical-register action:** New CI/supply-chain TD candidate; one entry can cite both TV and I evidence.

### RC-17 — Deployment/config readiness and build provenance are ambiguous

- **Disposition:** Confirm
- **Severity / confidence:** High / High for files; live settings need evidence
- **Source reports and exact evidence:** I-04/05/06/09 (`08:184-297,374-407`), BP-07. At least 29 env names lack a values-free schema; scheduler readiness is advisory; Railway rollback absent; two Vercel root modes; Nixpacks chooses first store library and uses system site packages.
- **User/runtime impact:** Builds can start but enabled paths fail later; operators cannot identify config owner/rollback target; same commit can resolve different native/deploy behavior.
- **Dependencies:** Kalshi env retirement after closeout; live settings names/metadata only, never values; RC-03 artifact identity.
- **Cheap cut vs refactor:** Values-free schema and Vercel authority choice before larger deployment work; native reproducibility requires disposable builds.
- **Safe first evidence step:** Generate env-name matrix; inspect Railway/Vercel project/build metadata without values; record native/dependency inventory in a disposable build.
- **Done condition:** One env schema and Vercel config; enabled features fail readiness early; commit/artifact identity and rollback are explicit; builder/native/package provenance is reproducible.
- **Canonical-register action:** New infra TD candidate, or split only if Chase wants separate runtime readiness and dependency-build ownership.

### RC-18 — Paid dashboard capability and Stripe/Ask state are not durable or idempotent

- **Disposition:** Confirm
- **Severity / confidence:** High / High
- **Source reports and exact evidence:** F-03/04/05/06 (`07:113-203`), PSC-04 (`10:175-206`), report 00 in-memory Ask confirmation. API routes bypass page subscription; Stripe webhook acknowledges DB failure; checkout customer/session creation lacks durable idempotency; Ask uses process-local quota charged before validation.
- **User/runtime impact:** Unsubscribed users can consume paid LLM work; paying/cancelled users can have stale entitlement; duplicate Stripe customers/sessions can appear; spend caps reset across Vercel instances.
- **Dependencies:** Shared capability matrix; mocked Stripe/provider/repos; no live Stripe calls.
- **Cheap cut vs refactor:** Entitlement-before-cost and webhook retry semantics before Lane 09/page refactor.
- **Safe first evidence step:** Mock anonymous/unsubscribed/subscribed Ask, invalid/provider-failed requests, webhook DB errors, and concurrent checkout.
- **Done condition:** Paid APIs enforce server-side entitlement; usage is atomic cross-instance with intentional charge semantics; webhook failures return retryable non-2xx; checkout is idempotent; one typed product catalog owns price/display mapping.
- **Canonical-register action:** New dashboard monetization/usage TD candidate; do not fold into generic Lane 09 size debt.

### RC-19 — Dashboard data ownership causes unbounded duplicate reads and false-empty UI

- **Disposition:** Confirm
- **Severity / confidence:** High / High for source; runtime rows/latency unknown
- **Source reports and exact evidence:** F-07/09/10 (`07:207-299`), PSC-03 (`10:140-173`). Performance mounts at least eight Supabase requests, duplicates resolved-bet reads, fetches inactive unbounded histories, caches payloads; AnalysisModal/dashboard duplicate query/mapping and turn errors into empty/zero/fallback states.
- **User/runtime impact:** Request/egress/browser cost grows with history; outages look like valid zero bankroll/no predictions/no lines.
- **Dependencies:** Dashboard test harness; mocked request-count/KPI/query-error fixtures; no production query needed to establish source behavior.
- **Cheap cut vs refactor:** Disable inactive hooks, unify duplicate reads, and distinguish errors before JSX/god-component decomposition.
- **Safe first evidence step:** Mock each active tab and failure; assert call count, bounded inputs, KPI parity, and error-vs-empty rendering.
- **Done condition:** Only active domains fetch; one bounded repository/query contract owns each dataset; duplicate reads dedupe; errors are visible/retriable; KPI/view-model parity tests exist.
- **Canonical-register action:** New dashboard performance/data-contract TD candidate linked to refreshed Lane 10.

### RC-20 — Dashboard verification, route observability, and accessibility lack an executable baseline

- **Disposition:** Confirm
- **Severity / confidence:** High overall / High
- **Source reports and exact evidence:** TV-03 (`02:109-136`), F-11/12/13 (`07:303-371`). Zero dashboard tests; build-only CI; mutation/read errors are swallowed; privileged actions lack correlation; modal/tab/disclosure semantics are incomplete.
- **User/runtime impact:** Auth, billing, API shape, KPI, interaction, and error changes compile green; users see false success; keyboard/screen-reader behavior is fragile.
- **Dependencies:** Kalshi-specific test investment is superseded by removal; retained-product harness first.
- **Cheap cut vs refactor:** One shared no-network unit/route/component harness; add removal/anti-regrowth tests for retired surfaces, not behavior suites.
- **Safe first evidence step:** Prove one pure test and one mocked route test, then add retained auth/billing/Ask/data contracts.
- **Done condition:** CI runs lint/typecheck/unit/route/component tests; critical retained routes have auth/error contracts; mutations surface outcomes/correlation; bounded accessibility checks cover changed UI.
- **Canonical-register action:** New verification TD candidate; link Lanes 09/10 Phase 0.

### RC-21 — GBrain roadmap and authority pointers route correct tools to stale or conflicting truth

- **Disposition:** Confirm
- **Severity / confidence:** High / High
- **Source reports and exact evidence:** AKW-01/02/09/10 (`09:95-161,366-429`), BP-04/05/06/07. Remote `Execution-Plan.md` still presents March/old Kalshi work as current; migration/index pages point to checked-in `brain/`; ACTIONITEMS is plausible “Roadmap”; Known Issues points to deleted/missing trackers; old/new plan/deployment authorities overlap.
- **User/runtime impact:** Resume/planning agents can duplicate completed work, recommend decommissioned Kalshi growth, use stale local brain, or pick obsolete plans/schedules.
- **Dependencies:** This report's approved decisions; compare local-only knowledge before archive; remote GBrain writes remain out of scope here.
- **Cheap cut vs refactor:** Consolidate authorities and archive stale mirrors/trackers; do not repair parallel sources.
- **Safe first evidence step:** Read-only link/pointer/source-qualified checksum inventory and proposed current-phase/supersession diff.
- **Done condition:** One current execution plan names the audit/decommission gates; active pages/skills point only to remote `gameflow`; legacy trackers/mirrors are archival; one god-class index and one schedule/deploy authority remain.
- **Canonical-register action:** Update TD-001 only for retrieval routing, not roadmap drift. New knowledge-authority TD candidate may be warranted; link AKW/BP evidence.

### RC-22 — Audit evidence durability and canonical-register schema cannot preserve adjudication provenance

- **Disposition:** Confirm
- **Severity / confidence:** High / High
- **Source reports and exact evidence:** AKW-03/04 (`09:165-227`), report 00, audit README. Audit/program files are untracked; current register has six items and a lighter schema than the audit's confidence/source/validation/dependency requirements.
- **User/runtime impact:** Reviewers can see accepted-looking register entries without source evidence; untracked artifacts can disappear; merged/rejected candidates become undiscoverable and duplicate work regrows.
- **Dependencies:** Chase chooses lifecycle model and authorizes tracking/register edits later.
- **Cheap cut vs refactor:** Durability/schema decision before any bulk register promotion or Kanban creation.
- **Safe first evidence step:** Scoped tracked/untracked inventory and paper loss-analysis mapping three clusters into current register fields.
- **Done condition:** Reports are durably retrievable from one reviewed revision/store; accepted entries cite immutable evidence, confidence, validation, dependencies, adjudication date, and supersession; rejected/merged items stay traceable without becoming backlog.
- **Canonical-register action:** Update register template/process first; no new TD ID for the meta-process unless Chase wants the process itself tracked.

### RC-23 — Lesson/fact validation and GBrain freshness semantics can overstate current authority

- **Disposition:** Needs Evidence
- **Severity / confidence:** High / High for documented drift; current service state is time-sensitive
- **Source reports and exact evidence:** AKW-05/06/08 (`09:231-362`), TD-001/TD-002. Lesson protocol differs by entrypoint; an active lesson is past review date; all Hard Facts remain `needs-chase-validation`; remote source/cycle freshness failed in report 09 while embeddings/orphans were healthy.
- **User/runtime impact:** Agents can elevate unvalidated/stale facts, omit lessons depending on entrypoint, or confuse freshness failure with broken embeddings.
- **Dependencies:** Chase fact validation; read-only current health/timer/source retrieval matrix; do not auto-run sync/cycle.
- **Cheap cut vs refactor:** Validate/label before tooling changes.
- **Safe first evidence step:** Fact-by-fact validation matrix, expired-lesson inventory, cross-entrypoint retrieval fixture, current read-only source freshness/timer evidence.
- **Done condition:** Validation state is preserved in retrieval; active lessons are current or warned; all entrypoints enforce the same protocol; freshness SLA/exceptions are explicit; embeddings/orphans remain healthy.
- **Canonical-register action:** Keep TD-001 confirmed for direct-route fragility; keep TD-002 Needs-Evidence until current health identities/causes are measured. Do not register old counts.

### RC-24 — DB engine/configuration is captured at import time

- **Disposition:** Confirm
- **Severity / confidence:** Medium / High
- **Source reports and exact evidence:** PA-08 (`01:264-290`). `src/db/client.py:11-18,69-110` loads dotenv/captures URL/creates default engine during import; tests require reimport to vary config.
- **User/runtime impact:** Test/CLI bootstrap order can retain stale configuration; imports create global state and bypass explicit local/default selection.
- **Dependencies:** Direct `engine` importer inventory; preserve postgres-role invariant; no DB connection needed.
- **Cheap cut vs refactor:** Small isolated infrastructure refactor, separate from model/DB schema lanes.
- **Safe first evidence step:** Import-isolation tests and direct-engine callsite inventory.
- **Done condition:** Explicit lazy factory/config owns timing, caches, disposal, local/default selection; import creates no engine; consumers do not bypass factory.
- **Canonical-register action:** New low/medium-priority TD candidate only if Chase wants it tracked after higher-risk work.

### RC-25 — NBA linker local/incremental policy lacks shared characterization

- **Disposition:** Needs Evidence
- **Severity / confidence:** Medium / High structural evidence; current correctness impact unproven
- **Source reports and exact evidence:** PA-07 (`01:235-262`). `nba_linker_local.py:312-766` and `:1043-1401` combine two modes and all adapters; incremental mode lacks focused tests.
- **User/runtime impact:** Matching/normalization/retry semantics may drift; partial batch failure is hard to prove safe.
- **Dependencies:** Pure fixtures and mocked incremental cursor/update boundaries; production recurrence remains a separate approved evidence check.
- **Cheap cut vs refactor:** Characterize before extraction; do not launch a god-class lane from LOC.
- **Safe first evidence step:** Local/incremental-shaped parity fixtures for normalization, precedence, closest game, cursor/update boundary.
- **Done condition:** Shared pure policy and adapter boundaries pass parity/idempotency/retry tests.
- **Canonical-register action:** No new ID until parity failure or operational recurrence is reproduced.

### RC-26 — Test-plan commands and deterministic fixture policy have drifted

- **Disposition:** Confirm
- **Severity / confidence:** Medium / High
- **Source reports and exact evidence:** TV-05/06/07/08 (`02:170-283`). 59 of 103 plan-referenced test paths are absent; completed lanes cite renamed paths; real clock sleeps and duplicate/random parity fixtures weaken determinism; optional capability status is ambiguous.
- **User/runtime impact:** Implementers cannot reproduce baseline/RED commands and can report false coverage; scheduler-loaded CI can flake.
- **Dependencies:** Refresh only selected/current lanes; do not create all aspirational tests merely to satisfy docs.
- **Cheap cut vs refactor:** Cheap docs/test-support hygiene after critical contract tests.
- **Safe first evidence step:** File-existence preflight and test collection for completed-lane successors; fake-clock/shared-golden-fixture proposal.
- **Done condition:** Current commands reference real files; future files are marked expected RED; completed guards map to successors; parity fixtures deterministic; optional capabilities have explicit required/optional jobs.
- **Canonical-register action:** No standalone TD required; acceptance criteria under TD-003 and CI RC-16.

### RC-27 — Kalshi sports runtime, UI, live lifecycle, and feature-hardening program

- **Disposition:** Superseded by product cut
- **Severity / confidence:** Critical removal priority / High
- **Source reports and exact evidence:** Report 12 in full; TMS-01-05/08/09, F-01/F-02, E scheduler inventory, DB-06, PSC-02. Ten always-registered jobs, five Kalshi APIs, bot tracker, sports scraper/edge/paper/live services, and 22 focused tests remain. Up to 1,442 sports/lifecycle triggers/day are removable after closeout.
- **User/runtime impact:** Under-authorized controls, uncertain partial/unknown/finality state, scheduled/no-op cost, accidental new exposure, and stale agents/docs all remain while an unavailable product looks active.
- **Dependencies:** Stop new exposure first; separately approved authoritative provider/DB exposure reconciliation; retention/accounting decision; non-sports split; remove callers before lifecycle/schema.
- **Cheap cut vs refactor:** **Contain/remove/archive. Do not implement exactly-once, partial-fill expansion, paper/live parity, Kalshi observability, or Kalshi-specific MLOps.**
- **Safe first evidence step:** Static classification of sports-remove/closeout-only/non-sports schedules/routes/callers; request Chase approval for a separate read-only exposure closeout.
- **Done condition:** Zero sports schedules/routes/order-submit callsites/active go-live instructions; authoritative zero exposure and finality established; required records archived read-only; credentials/config/schema retired only after dependencies; optional anti-regrowth guard enforces decision.
- **Canonical-register action:** Do not create Kalshi hardening TDs. Add a product-decommission decision/reference, or one temporary decommission program entry if the register supports product cuts; mark Lane 06 Kalshi scope and old Kalshi plans superseded/archive.

### RC-28 — Non-sports Kalshi, standalone Polymarket, and paired arbitrage have no approved product owner

- **Disposition:** Needs Evidence
- **Severity / confidence:** High latent / High for coupling
- **Source reports and exact evidence:** Report 12 (`56-66,246-264,389-395`), TMS-06/07, DB-06, Lane 08. Current arb contracts pair Kalshi/Polymarket; near/fuzzy matches can be non-equivalent; “pure” uses indicative prices; Polymarket scraper imports Kalshi linking; no live two-leg executor exists.
- **User/runtime impact:** Retaining current UI/scanner can imply executable/guaranteed arb without semantic/executable proof; blind Kalshi removal can delete potentially independent Polymarket value.
- **Dependencies:** Chase product choice; if retained, demonstrate a supported consumer and separate read-only provider capabilities before architecture work.
- **Cheap cut vs refactor:** Default is archive/remove paired product. Refactor only after explicit Polymarket-only or read-only cross-market scope.
- **Safe first evidence step:** Product decision plus static consumer/dependency inventory; contract-equivalence fixtures only if retained.
- **Done condition:** Either all prediction-market integration is retired, or a newly scoped supported owner has provider-neutral/read-only contracts, no live claims, tested equivalence, and independent consumers.
- **Canonical-register action:** No TD until product disposition. Mark Lane 08 as superseded as written; a retained replacement would need a new plan, not refresh-in-place.

### RC-29 — `batter_hrr`, NCAAB, root Claude-flow tooling, and `run_now.py` lack product/owner decisions

- **Disposition:** Needs Evidence
- **Severity / confidence:** Medium / High for detachment, Low-Medium for disposition
- **Source reports and exact evidence:** Report 12 `batter_hrr` decision (`242-245`); BP-11/12/13 (`13:582-681`). `batter_hrr` is Kalshi-only/no sportsbook line; NCAAB has coherent code/migrations/tests but no scheduler; root Claude-flow is outside deploy path but may serve helpers; `run_now.py` is a human CLI with no tracked caller.
- **User/runtime impact:** Unsupported product plumbing and dependencies may persist or be accidentally deleted despite real manual/research use.
- **Dependencies:** Chase decisions; applied migrations always retained; manual/external usage cannot be inferred from imports.
- **Cheap cut vs refactor:** Decide first. If retired, remove executable product claims/code/tests together; if retained, name owner/review date/manual contract.
- **Safe first evidence step:** Chase confirmation plus static active-hook/manual-runbook/artifact consumer inventory; no job execution.
- **Done condition:** Each lane is explicitly planned/parked/research-only/retired with owner, supported consumers, review date, and removal/retention boundary.
- **Canonical-register action:** No TD IDs before product/owner decisions.

### RC-30 — Completed compatibility boundaries, current production targets, migrations, and anti-regrowth evidence

- **Disposition:** Retain/Protect
- **Severity / confidence:** Critical protection / High
- **Source reports and exact evidence:** BP-14/15 (`13:685-721`), report 00 lane matrix, PA rejected suspicions. At least 14 consumers use MLB feature-store facades; Lane 02 inventory tests enforce the boundary; production/playoff paths are active; migration history is irreversible evidence.
- **User/runtime impact:** Premature deletion breaks model/training/inference consumers, rollback, schema history, and the controls that keep completed migrations complete.
- **Dependencies:** Future removal only after zero-import/parity/manifest/rollback gates stated by reports 05/13.
- **Cheap cut vs refactor:** Retain. Names such as `legacy` or old-looking production targets are not evidence.
- **Safe first evidence step:** None required for retention; keep import/anti-regrowth/manifest inventory current.
- **Done condition:** Boundaries remain tested; any future replacement proves zero consumers and equivalent behavior before deletion; migrations remain immutable history.
- **Canonical-register action:** Mark TD-003 lanes 01-02 resolved and Lane 03 accepted/core-complete; do not list retained facades/artifacts/migrations as active debt.

## God-class lane status and disposition matrix

| Lane | Current implementation status | Audit disposition | Product-cut/pruning effect | Next allowed evidence gate |
|---|---|---|---|---|
| 01 MLB quote-clean/backtest sweep | Complete | Retain/Protect; reject as active debt | BP-10 legacy consolidation may follow RC-02, but do not reopen Lane 01 | Preserve inventory/promotion guards; fix RC-04 semantics in existing seam |
| 02 MLB feature-store boundary | Complete | Retain/Protect | Facades/legacy implementations are tested compatibility boundaries | Reconsider only after zero direct consumers and parity proof |
| 03 NBA/general feature-store | Core complete; optional Phase 7 deferred | Shelf optional cleanup / Retain core | No pruning credit justifies facade churn | Keep anti-regrowth; accepted-deferred callsite cleanup |
| 04 Training orchestrator | Documentation-only; partial overlap from MLB stat-suite | Shelf broad migration | RC-01/03/04/05 contract cuts precede decomposition | Refresh plan, calibration wording, current shared modules, stale test paths |
| 05 Daily prediction runner | Documentation-only | Confirm shared probability contract; Shelf broad migration | Kalshi importer removals reduce callsite scope | RC-02 parity/forbidden fallback RED tests first |
| 06 Paper-trading shared primitives | Documentation-only | Superseded by product cut for Kalshi; Shelf generic lane | Retain sportsbook/MLB/DFS/user primitives; archive Kalshi parity scope | New supported-consumer scope required before generic work |
| 07 Scheduler/job registry | Documentation-only; stale 45-job baseline | Confirm relevance, Shelf broad extraction until cuts/fixes | Remove up to 1,442 Kalshi triggers/day; recalc baseline | RC-08/10 RED tests and product cut, then refresh plan |
| 08 Arbitrage matcher/scanner | Documentation-only | Superseded as written / Needs Evidence | Current paired Kalshi/Polymarket product has no approved owner | Chase decision on Polymarket/non-sports/arb before any new plan |
| 09 Dashboard Ask API | Documentation-only | Confirm bounded retained-product needs; Shelf broad route extraction | Kalshi route work removed from dashboard hardening; chat persistence retained | RC-18 entitlement/usage and RC-20 harness first; refresh assumptions |
| 10 Dashboard god pages/components | Documentation-only | Confirm bounded data/test needs; Shelf JSX-wide refactor | Bot tracker/arb surfaces remove/archive rather than decompose | RC-19/20 harness, request-count, error-state fixtures first |

## Kalshi finding disposition summary

| Raw finding family | Final disposition | Required treatment |
|---|---|---|
| TMS-01 exactly-once submission; TMS-04 execution-time exposure | Superseded by product cut | Stop/remove new submission and approval paths; use uncertainty only in closeout assumptions |
| TMS-02 partial fills; TMS-03 unknown→cancelled; TMS-08 internal settlement | Superseded by product cut, preserve closeout evidence | Do not trust local terminal statuses; retain lifecycle/data only until authoritative zero exposure/finality |
| TMS-05 stale market/edge provenance | Superseded by product cut | Remove sports refresh/edge/proposal/paper/alert paths; no provenance investment |
| TMS-09 paper/live parity | Superseded by product cut | Archive behavior evidence; retain only demonstrated sportsbook/paper primitives |
| TMS-10 comparator wording | Archive evidence | Preserve quoted-contract semantics lesson; no core CDF/model change during removal |
| F-01/F-02 privileged arb/Kalshi routes | Remove now | Remove/disable routes and controls; do not harden retired capability in place |
| E/PSC Kalshi schedules and polling | Remove now / closeout-dependent | New exposure and product jobs first; fill/cancel/settlement only after closeout; non-sports separately decided |
| DB-06 schema/data | Needs product/retention evidence | No first-slice DB deletion; separate sports/non-sports/Poly/arb/history and preserve accounting/incident evidence |
| Lane 06/08 and Kalshi plans/tests/docs | Superseded/archive | No missing-test completion or refactor; keep until corresponding code closeout, then archive/delete implementation-coupled tests |
| Provider-neutral primitives | Retain only with demonstrated consumers | Scheduler/auth/Discord/model/paper primitives stay only where NBA/MLB/DFS/user/other supported consumers exist |

## Broader Remove / Archive / Consolidate / Retain decisions

### Remove after characterization

- BP-01 executable global calibration offsets: remove, not refactor.
- BP-08 generated/test/probe residue: root `.next` traces, egg-info, unrelated `_test_longspec.py`, malformed dry-run output, one-off MCP probe; screenshot only after provenance check.
- Kalshi sports schedules/routes/new-order/product surfaces per report 12 Phase 1; closeout services later.
- `cryptography` only after all Kalshi client/non-sports decisions and callers are gone.
- BP-10 legacy MLB single-config path after RC-02/shared imports/tests migrate.

### Archive after dependency/rollback checks

- BP-02 168,994,105 bytes of named non-production model artifacts, with immutable checksums/readback/owner/retention.
- BP-03 undeclared MLB production stat binaries after required-stat manifest and `batter_hrr`/consumer decisions.
- BP-04 checked-in `brain/` and root `Handoffs/` after source-qualified local-only salvage; do not create a third in-repo archive.
- BP-05 `ACTIONITEMS.md`, `.thoughts.md`, and `CLAUDE.md.backup` after link/salvage check.
- BP-09 `.hermes/tmp` logs/probes/mirrors after promoting any truly reusable script and reviewing restricted evidence.
- Kalshi incidents, plans, expected-behavior tests, P&L/orders/fills/fees, and sanitized closeout evidence.

### Consolidate

- BP-06 old five-lane god-class index/pre-lane plans into the ten-lane index with a supersession ledger.
- BP-07 old Railway schedule prose into one current/generated schedule authority; select one Vercel root/config after values-free live metadata.
- DB DDL/function roots into one classified migration ledger without rewriting historical application claims.
- Audit/register lifecycle into one durable evidence→adjudication→accepted-register process.

### Retain/Protect

- Current NBA production and active `production_playoffs` artifacts until manifest/atomic rollback replacement.
- Manifest-declared MLB production required-stat subset until archive decisions pass.
- Applied migrations, even for retired product lanes; schema removal uses new forward migrations only.
- Completed Lane 01/02 and core-complete Lane 03 plans/progress/anti-regrowth evidence.
- Lane 02 MLB feature-store facades and legacy implementations.
- Sportsbook NBA/MLB prediction, backtest, CLV/ranker, paper/user/DFS primitives and critical invariants.
- Generic scheduler telemetry, DB client, dashboard auth/billing/Ask, and Discord transport where supported consumers remain.

### Needs evidence/decision

- NCAAB, root Claude-flow, `run_now.py`, `batter_hrr`, non-sports Kalshi, standalone Polymarket, and any new cross-market research owner.
- PSC-08 process/import overhead; measure only after trigger cuts.
- Notebooks/archive/older development docs not reviewed by report 13; age/location is insufficient.

## Contradictions resolved

1. **Kalshi “complete/live” roadmap vs 2026-07-18 decision:** product decision wins. Old readiness/go-live/scale-up material becomes superseded archival evidence.
2. **Hard Facts/Execution Plan vs current topology/priority:** invariant content is applied conservatively; volatile topology and roadmap statements do not override current remote route or Chase's product decision.
3. **TD-003 “multiple partial lanes”:** replaced by the ten-row matrix. Lanes 01-02 resolved, Lane 03 accepted/core-complete, 04-10 documentation-only before overlays.
4. **TD-004 “stat-suite fragmentation”:** substantially remediated. Remaining debt is consumer manifest/evidence/promotion closure, not another rebuild.
5. **Lane 04 calibration Phase 6 preserving offsets:** canonical never-deploy invariant wins; extraction must not preserve automatic offset generation/activation as supported promotion behavior.
6. **History pagination/chat persistence/no CI:** blanket claims rejected. History pagination and chat persistence exist; CI exists. Remaining concerns are performance-page bounds, persistence errors, and weak CI contracts.
7. **Railway schedule docs vs source:** current `scheduler.py` plus `docs/understanding/railway-scheduler.md` are interim truth. Old Railway cron/7-job schedules are stale.
8. **Archive retention 7 vs 30 days:** code currently executes 7 days because scheduler passes no override. Desired policy remains a human/data-evidence decision.
9. **GBrain stale/doctor vs content health:** old stale-page counts are not proof of stale embeddings. Report 09 observed full embedding/zero orphan health but stale source/cycle freshness; these are distinct.
10. **Empirical `>` invariant vs Kalshi `N+ >=`:** quoted contract normalization is distinct from model sample-CDF policy. Preserve the historical lesson; do not change core probability behavior during decommission.
11. **“Legacy” feature-store files as dead:** rejected. They are active, tested implementations behind thin facades.
12. **All duplicate model hashes are deletable:** rejected. Byte identity does not replace provenance, references, manifest, or rollback.
13. **All 1,532 Kalshi triggers removable immediately:** rejected. 90/day are unresolved non-sports; lifecycle jobs require exposure closeout before removal.
14. **Report 12/13 concurrency notes:** repaired here. Report 12 is authority for Kalshi; report 13 is authority for broader non-Kalshi pruning; report 03/10 now supplement their earlier coverage gaps.

## Approval-gated DB/runtime evidence queue

These are evidence requests, not authorization. Use isolated read-only SQL/provider/platform lanes; return concise summaries. Any destructive scope later requires independent verification.

| Gate | Evidence requested | Why / dependent clusters | Prohibited in evidence pass |
|---|---|---|---|
| K-01 Kalshi exposure closeout | Authoritative provider positions, resting/partial/unknown orders, fills, settlements; bounded local queue/order correlation | RC-27; determines lifecycle removal | No new orders, public dashboard controls, cancellation/settlement without separate human approval |
| K-02 Kalshi/Poly/arb data inventory | Counts/sizes by sport/NULL/status/time, object dependencies, retention classes | RC-27/28, DB-06 | No table/index/RPC/policy drop or broad raw export |
| D-01 Live schema authority | Migration history, object definitions/owners, functions, policies, grants, indexes, exposed schemas | RC-12/13 | No DDL, baseline write, or policy change |
| D-02 Raw quote identity | Bounded duplicate aggregates, game-ID formats, actual unique constraints, relevant query plans/index validity | RC-14 | No dedupe/delete; no non-concurrent large-table index |
| D-03 Retention/archive | Hot/archive date bounds, null timestamps, eligible inflow, sizes, archive DDL/key/consumers | RC-15 | No archive/purge/retention change |
| D-04 Provenance/orphans | Prediction/sample/bet/derived-log orphan and lineage aggregates | RC-14 | No cleanup; destructive counts must be independently repeated later |
| R-01 Recovery | Supabase backup/PITR metadata, retention, protected assets, last restore evidence, RPO/RTO owner | RC-15 | No production restore or credential export |
| O-01 Scheduler runtime | Registered IDs/deployed revision, 10:00 skips, MLB collision outcomes, restart retry recurrence, process timeout evidence | RC-08/09 | No jobs/scrapes/provider calls as smoke tests |
| O-02 Deploy metadata | Railway/Vercel root/config/build commands/source commit/rollback target and env **names only** | RC-17 | No secret values or deploy |
| G-01 GBrain health | Current direct/list/query route matrix, source freshness/timer status, exact stale-page identities/causes if any | TD-001/002, RC-23 | No sync/embed/cycle/metadata repair |
| P-01 Dashboard runtime | Only after source fixes are scoped: preview query counts/latency, Stripe test-mode webhook state, safe real-client E2E | RC-18/19/20 | No production billing/admin/trading mutation |

## Dependency-ordered overhaul phases

### Immediate — no-regret containment and RED evidence

1. Approve the Kalshi sports decommission boundary and whether K-01 read-only closeout is authorized.
2. Contain new Kalshi sports exposure and privileged routes in the first future implementation slice; no DB deletion.
3. Add RC-01 forbidden-offset characterization/anti-regrowth tests and remove implicit activation/writer path in a separately approved slice.
4. Add RC-08 MLB false-success, collision, duplicate-trigger, retry tests/fixes plus RC-10 CDN-only guard.
5. Establish dashboard test harness and fix paid entitlement/webhook failure semantics before broad route/page work.
6. Make audit evidence durable and choose the register lifecycle/schema before canonical promotion.
7. Remove only BP-08 high-confidence generated residue and begin BP-09 ownership classification after approval.

### Near — contract closures and cheap consolidation

1. Implement RC-03/04 manifest reader, required-stat, lineage, fail-closed evidence, clean-clone provenance, atomic promotion/rollback contracts.
2. Add RC-02 daily/backtest/sweep probability parity and remove forbidden Gaussian promotion fallback.
3. Execute Kalshi sports surface/backend removal after K-01 zero-exposure gate; decide non-sports/Polymarket/arb and `batter_hrr`.
4. Build file-only DB DDL/access/lifecycle manifests, then request D-01 through D-04 read-only evidence.
5. Fix CI immutability/permissions/config authority; design one dependency lock authority.
6. Apply RC-19 active-tab/bounded-query/error-state fixes and RC-18 durable Ask usage/Stripe idempotency.
7. Consolidate stale knowledge/plan/schedule authorities and update remote execution plan only after Chase approves this adjudication.

### Later — architecture only after cuts/contracts

1. Refresh and, if prioritized, execute Lane 04 against current shared MLB modules and closed artifact contracts.
2. Refresh Lane 05 after probability policy is canonical; extract remaining line/BL/slate responsibilities only where evidence supports value.
3. Recompute scheduler inventory after Kalshi removal and RC-08 fixes; decide whether Lane 07 still merits broad registry extraction.
4. Refresh Lanes 09/10 only for supported retained product surfaces; remove retired bot/arb UI rather than decompose it.
5. Implement DB forward migrations only after disposable replay and approved live evidence; preserve applied history and concurrent-only large-table index rule.
6. Archive 168,994,105 bytes of non-production artifacts only after manifest/rollback/archive checksum/readback gates.
7. Revisit BP-10 legacy MLB harness, RC-24 DB client, RC-25 linker, and PSC-08 startup cost according to measured evidence and business priority.

## Canonical register proposal

Do not edit the register until Chase approves this adjudication and its schema can preserve evidence/provenance.

### Update existing IDs first

| Existing ID | Proposed action | Adjudicated content |
|---|---|---|
| TD-001 | Keep; status `confirmed` if Chase accepts evidence | Narrow to GBrain direct/list/source-route fragility. Distinguish transport, direct-read routing, query fallback, and current remote source. Do not absorb general roadmap drift. |
| TD-002 | Keep `candidate` / Needs-Evidence | Remove old count as current fact. Require current stale identities/metadata cause and clean embedding/orphan interpretation before action. |
| TD-003 | Rewrite; status matrix rather than umbrella candidate | Lanes 01-02 resolved/protected; Lane 03 accepted/core-complete; 04/05/07/09/10 documentation-only with bounded confirmed prerequisites; 06/08 superseded as written by product cut. |
| TD-004 | Rewrite or resolve original | Original stat-suite fragmentation is substantially remediated. Retain only RC-03/04/05 consumer identity/evidence/promotion/feature closeout, preferably with one existing plan rather than duplicate rebuild. |
| TD-005 | Keep confirmed; refresh scope | Post-Kalshi scheduler architecture plus RC-08/09/10. Credit removal of up to 1,442 triggers/day and perform correctness fixes before registry extraction. |
| TD-006 | Keep confirmed until fixed | Exact duplicate 10:00 NBA props trigger; bounded symptom under TD-005, with explicit one-trigger/no-routine-skip done condition. |

### Proposed new IDs only after existing updates and Chase approval

Prioritized candidates, not assigned numbers here:

1. Forbidden global calibration-offset path (RC-01).
2. Promotion-capable probability/edge parity and Gaussian fallback (RC-02), unless combined with updated TD-004.
3. Model manifest/evidence/promotion/rollback contract (RC-03/04/05), unless TD-004 is rewritten to own it.
4. Database migration/schema authority (RC-12).
5. Retained dashboard/API authorization and DB access matrix (RC-13).
6. Data identity/provenance/idempotency (RC-14).
7. Retention/archive/recovery contract (RC-15).
8. CI/dependency/supply-chain immutability (RC-16).
9. Deployment/env/readiness provenance (RC-17).
10. Paid-product entitlement/Stripe/Ask durability (RC-18).
11. Dashboard bounded data/error ownership and verification baseline (RC-19/20), either one entry or two only if owners differ.
12. Knowledge authority/audit durability (RC-21/22), if Chase wants workflow debt in the codebase register rather than GBrain operations.

Do **not** propose IDs for Kalshi feature-hardening, Lane 01/02, Lane 03 optional imports, retained facades/artifacts/migrations, NCAAB/Polymarket/root tooling before product decisions, or hypotheses such as import cost.

## Decisions requiring Chase

1. Approve the final Kalshi sports removal boundary and a separately controlled read-only provider/DB exposure closeout (K-01).
2. Choose non-sports Kalshi: retire, read-only research, or product candidate.
3. Choose Polymarket: retire, or retain standalone analytics after Kalshi decoupling.
4. Choose arbitrage: archive paired work, or commission a new non-live research scope; current Lane 08 is superseded.
5. Decide `batter_hrr`: remove as Kalshi-only production plumbing or retain explicitly research-only.
6. Set retention/location/owner for live orders/fills/fees, paper data, market/orderbooks, arb/Poly data, incidents, and final closeout evidence.
7. Decide whether a static historical bot/P&L report is wanted after live controls disappear.
8. Authorize provider credential revocation/removal only after exposure closeout.
9. Decide whether CI should enforce “no Kalshi sports schedules/routes/order-submit callsites.”
10. Select audit/register lifecycle: tracked candidate reports plus accepted-only register is recommended.
11. Confirm which proposed new register clusters are worth tracking after TD-001 through TD-006 are rewritten.
12. Choose artifact archive destination/retention/rollback owner before removing model binaries.
13. Confirm desired hot/archive retention policy only after D-03 evidence; do not infer 7 or 30 from current prose.
14. Set RPO/RTO and approve a disposable restore-drill design.
15. Decide NCAAB planned/parked/retired, root Claude-flow owner, and `run_now.py` manual ownership.
16. Decide whether Lane 04, 05, or post-cut Lane 07 is the next structural investment after immediate contract/safety work.

## Rejected and shelved items

### Rejected as active debt

- `test_finds_latest_run_directory` currently failing.
- AI chat is not persisted.
- History page has no pagination.
- Repository has no CI.
- Lanes 01/02 or Lane 03 core are unfinished.
- Large `legacy_*_feature_store.py` files are dead by name/size.
- Current `production_playoffs` is an old backup.
- All duplicate hashes or old artifacts are immediately deletable.
- Current Railway daily stats calls `stats.nba.com`.
- Local sync/same-project archive is a backup.
- Local Kalshi terminal statuses prove exchange finality.
- All tests broadly call live networks or are broadly skipped.
- Every missing planned test is a regression.
- Source code, rather than model binaries, is the main repository-size cause.
- Import/startup overhead is already material.

### Shelved / accepted-deferred

- Lane 03 optional facade import cleanup.
- Broad Lane 04 orchestrator decomposition until artifact/policy contracts close and Chase prioritizes it.
- Broad Lane 05 decomposition beyond RC-02 contract work.
- Broad Lane 07 registry migration until Kalshi cuts and bounded fixes resize it.
- Broad Lane 09/10 decomposition beyond retained-product security/cost/test prerequisites.
- Legacy MLB single-config removal until shared imports/tests move.
- Private GBrain patch upstreaming/hybrid-search limitations with working fallbacks.
- Ratio renames requiring artifact migration.
- Correlation-aware Kelly/dedup redesign and any live/Kelly activation.
- General mobile/visual cleanup except when touched by a characterized retained-product slice.

### Needs evidence, not backlog yet

- Current `stale_pages` identities/causes, provider backup/PITR, table/index sizes/plans, archive backlog, production linker recurrence, performance runtime latency, deployed env gates, and process import cost.
- NCAAB, root Claude-flow, `run_now.py`, non-sports Kalshi, Polymarket, arb, and `batter_hrr` dispositions.

## Verification record

- Read `AGENTS.md`, audit README, reports `00-10` and `12-13`, the canonical register, and god-class migration index/status evidence.
- Read remote canonical Hard Facts, Critical Invariants, and Execution Plan over read-only SSH from `gameflow-agent`.
- Counted 108 numbered raw findings and reconciled them into 30 root-cause clusters.
- Every cluster above includes disposition, severity, confidence, exact report/source evidence, impact, dependencies, cheap-cut/refactor classification, safe first evidence step, done condition, and canonical-register action.
- Applied report 12 decommission cuts and report 13 pruning before recommending refactors.
- Preserved report 12 as Kalshi authority and report 13 as broader pruning authority; repaired their parallel-completion coverage notes without rerunning source scans.
- No implementation, SQL, provider/live check, card, plan, register, GBrain, deploy, source, config, package, model, or test state was changed.
- Final validation is limited to required-field/content checks and `git diff --check` scoped to this report.
