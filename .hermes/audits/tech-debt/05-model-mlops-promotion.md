# Model / MLOps Artifact and Promotion Architecture Audit

**Audit date:** 2026-07-18

**Mode:** read-only source, artifact-metadata, plan, report, and remote canonical-knowledge audit; this report is the only file written

**Scope:** model training/evaluation artifact lifecycle; manifests; model selection; sweep/backtest/ranker/CLV routing; promotion evidence; rollback; reproducibility; lineage; feature contracts; experiment isolation; and NBA/MLB production loading. Targeted paths were `src/models/`, `src/models/mlb/`, `src/backtesting/`, `src/backtesting/mlb/`, `src/orchestration/{inference_job,mlb_inference_job,edge_refresh_job}.py`, relevant scripts/tests, production artifact metadata, audits 00/01/02/06, the MLB stat-suite rebuild, and god-class training/promotion plans.

**Prohibited and not performed:** no training, backtest, sweep, CLV run, database/API call, production load, deployment, promotion, rollback, binary deserialization, or source/config/plan/register edit.

## Executive verdict

GameFlow has meaningful MLOps safety scaffolding: `_incomplete` run directories, MLB run/config/training/feature/model metadata writers, stat profiles, quote-clean promotion labels, a read-only MLB artifact functionality audit, CLV/ranker gates, fixed bootstrap seeds, and completed MLB sweep/feature-store structural migrations. The stat-suite rebuild is substantially implemented and must not be restarted.

The promotion chain is nevertheless not closed end to end. The highest-risk gaps are:

1. NBA production and backtest loaders automatically activate global calibration offsets when a file is present, contrary to the strongest canonical invariant.
2. MLB manifests are writer-only; NBA has no equivalent authoritative manifest; both loaders infer a usable model from directory shape and filenames and can return partial model sets.
3. MLB quote-clean metadata can say `promotion_grade=true` while dense linked-coverage proof is missing, and the standard stat runner skips dropout audit in a way that can classify dense-table adequacy as `yes`.
4. Sweep, CLV-suite, and ranker artifacts are not cryptographically or structurally bound to one model manifest, source commit, input artifact set, and command/config. Correct files can therefore be routed into an incorrect evidence chain.
5. NBA promotion deletes the current target before copying the candidate, defaults to the lexically latest run, has no evidence gate, and has no first-class rollback record or atomic swap.
6. Production feature/model compatibility is permissive: missing selected features are zero-filled and requested missing MLB stats are skipped, while the separate artifact audit is not an enforced production-loader gate.
7. NBA provenance JSON is locally present but globally ignored by Git, contradicting the durable-fact expectation that production artifacts are committed and making deployed model identity weaker than the working tree suggests.
8. Pitcher/copula Monte Carlo remains stateful and order-sensitive even though batter predictors already have a stable per-player/game/stat seed pattern.

No finding below is a recommendation to change model architecture, calibration, feature families, promotion outcome, or betting policy. Safe next steps are deterministic characterization and contract evidence only.

## Retrieval trace and canonical evidence

The canonical brain was read over read-only SSH from `chase@gameflow-agent:/home/chase/GameFlowBrain`; no local brain mirror or DB-backed GBrain call was used.

Required canonical reads completed before interpretation:

- `Operations/Hard-Facts.md`
- `Operations/Critical-Invariants.md`
- lesson-tag page inventory under `Lessons/`
- keyword search over remote lesson pages for artifact, calibration, feature-family, baseline, CLV, promotion, rollback, lineage, reproducibility, experiment, and production-loading terms
- focused canonical reads of `Models/Calibration-Guide.md`, `Decisions/Calibration-Decisions.md`, and `Decisions/Model-Architecture-Decisions.md`

Lesson-tag pages listed over SSH:

1. `Lessons/Cheap-Baseline-Before-Architecture.md`
2. `Lessons/Correlated-Feature-Family-Validation.md`
3. `Lessons/Empirical-CDF-For-Probabilities.md`
4. `Lessons/Feature-Selector-Is-Not-An-Ablation.md`
5. `Lessons/Implausibly-Profitable-Backtests-Are-Methodology-Red-Flags.md`
6. `Lessons/Large-Odds-Table-Audits-Must-Be-Keyed-And-Chunked.md`
7. `Lessons/Q10-Miscalibration-Is-Edge.md`
8. `Lessons/Quote-Clean-CLV-Before-Feature-Work.md`
9. `Lessons/Raw-Timestamps-Do-Not-Guarantee-Temporal-Integrity.md`

No lesson-tag page specifically defining an artifact promotion transaction, model-manifest reader contract, or rollback protocol surfaced. That absence is a coverage result, not permission to invent model-selection policy.

## Relevant prior lessons and invariants

These rules constrain every interpretation and safe evidence step in this report:

- **Never deploy global conformal offsets.** `Operations/Hard-Facts.md:39-40`, `Operations/Critical-Invariants.md:12`, `Models/Calibration-Guide.md:15-20`, and `Decisions/Calibration-Decisions.md:13-31` record four confirmations that better calibration degraded ROI. This makes implicit offset activation a correctness defect, not optional cleanup.
- **Q10 behavior is edge-bearing.** `Lessons/Q10-Miscalibration-Is-Edge.md:18-32` requires separating cosmetic calibration metrics from betting outcomes and prohibits blindly smoothing Q10.
- **Use empirical sample CDF, not Gaussian approximation.** `Lessons/Empirical-CDF-For-Probabilities.md:18-32` and `Operations/Critical-Invariants.md:20` require `(samples > line).mean()` unless a validated component explicitly owns an analytic PMF/CDF.
- **Full retrains are risky and production hyperparameters should be locked.** `Operations/Critical-Invariants.md:32-34`, `Models/Calibration-Guide.md:41-46`, and `Decisions/Calibration-Decisions.md:33-36` prohibit treating a fresh run or latest directory as promotion proof.
- **Selector output is not ablation or promotion evidence.** `Lessons/Feature-Selector-Is-Not-An-Ablation.md:18-32` requires force-include/force-exclude and downstream evidence.
- **Correlated families must be evaluated as families.** `Lessons/Correlated-Feature-Family-Validation.md:18-32` prevents per-feature manifest/selector presence from becoming a causal conclusion.
- **Try a cheap controlled baseline before richer architecture.** `Lessons/Cheap-Baseline-Before-Architecture.md:18-32` limits future architecture work; this audit does not recommend a new model family.
- **Quote-clean replay and CLV precede feature work or promotion.** `Lessons/Quote-Clean-CLV-Before-Feature-Work.md:19-30,75-106` says legacy aggregation is hypothesis-only, positive mean CLV without ranking is insufficient, and intraday stability is a production gate.
- **Raw timestamps alone do not prove temporal integrity.** `Lessons/Raw-Timestamps-Do-Not-Guarantee-Temporal-Integrity.md:19-40` requires end-to-end as-of enforcement and retraining after leakage fixes.
- **Implausible replay quality is a methodology alarm.** `Lessons/Implausibly-Profitable-Backtests-Are-Methodology-Red-Flags.md:19-44` blocks promotion interpretation until overlap, dropout, quote timing, and independent windows are reconciled.
- **Structured hard-fact status is conservative.** `Operations/Hard-Facts.md:28-33` labels facts pending validation unless Chase confirms them. GF-F012 nevertheless records the intended architecture that production artifacts are committed under `src/models/artifacts/production/` (`:50`). Current Git evidence partially contradicts that expectation for NBA metadata and is reported below rather than silently choosing one source.

## Existing report and plan reconciliation

### Audit 00 — inventory reconciliation

Audit 00 correctly classifies the MLB stat-suite rebuild as substantially implemented and committed, with only bounded closeout/consumer-validation questions (`00-existing-inventory-reconciliation.md:115-128,299-346`). This report does not create a second stat-suite plan. Findings MMP-02, MMP-04, and MMP-06 are the consumer/lineage closeout that Audit 00 left for Lane F.

Audit 00 also distinguishes god-class Lane 04 as documentation-only while crediting the shared MLB base/artifact work as overlap (`:278-293`). That remains accurate.

### Audit 01 — Python architecture

This audit confirms and extends, without duplicating:

- PA-01 global-offset activation (`01-python-architecture.md:52-81`) into production-loading and promotion consequences (MMP-01).
- PA-03/PA-04 artifact resolver and writer-only manifest gaps (`:114-173`) into current tracked production contents and evidence lineage (MMP-02/MMP-07).
- PA-05 concrete training ownership (`:175-203`) into stat-suite/Lane 04 interaction; no new orchestrator migration is proposed.
- PA-06 NBA fast-sweep parity (`:205-233`) remains a coverage dependency, not re-audited as a separate finding here.

### Audit 02 — testing and CI

Audit 02 correctly identifies absent promotion-critical parity/anti-regrowth gates (`02-testing-ci-verification.md:138-168`) and stale plan commands (`:170-199`). Current tests prove MLB manifest writing and quote-clean labels, but not manifest consumption, lineage binding, atomic promotion, rollback, or required-stat enforcement. MMP-02 through MMP-05 specify those missing contracts.

### Audit 06 — trading and market safety

Audit 06 requires paper evidence not to substitute for executable order/fill/finality evidence and blocks live/Kelly inference (`06-trading-market-safety.md:26-48`). Its TMS-05 stale market/edge provenance and TMS-09 paper/live parity findings (`:191-213,287-309`) reinforce that model evidence needs source/run identity through selection and execution. This report does not reinterpret betting performance or enable live trading.

### MLB stat-suite rebuild

The rebuild's implementation sequence records generic CLV, generic operational runners, profiles/controls, shared artifact helpers, and lifecycle-only base extraction as complete, with model strategies intentionally deferred (`.hermes/plans/mlb-stat-suite-rebuild/07-implementation-sequence.md:23-177`). Its rollback rule is code-slice-oriented—independent reverts, compatibility wrappers, no production artifact changes (`:194-199`)—not a production model rollback protocol.

The current shared modules are real:

- profiles and supported experiment defaults: `src/models/mlb/training/profiles.py:102-193`
- force-family/feature controls: `src/models/mlb/training/feature_controls.py:26-171`
- run lifecycle and manifest writers: `src/models/mlb/training/artifacts.py:11-102`
- lifecycle-only base: `src/models/mlb/training/base_orchestrator.py:27-86`

Do not restart Slices 1-6. The unresolved seam is enforcing their metadata at consumers and evidence boundaries.

### God-class Lane 04

Lane 04 remains documentation-only (`.hermes/plans/god-class-migrations/04-training-orchestrator-migration.md:617-648`). Its intended single artifact owner, atomic finalization, explicit split policy, and promotion metadata (`:189-213,641-648`) remain relevant. However, its calibration Phase 6 text allows current offset generation (`:436-452`), which must be reconciled with MMP-01 and the canonical never-deploy rule before implementation. Structural extraction must not preserve an unsafe default as supported promotion behavior.

## Findings

### MMP-01 — CRITICAL — NBA production, refresh, and backtest behavior is switched by presence of a forbidden global-offset file

**Exact evidence**

- Training writes combined offsets during the normal orchestration path and calibrate-only can write them into a selected model directory: `src/models/train_pipeline.py:74-144,705-864,1049-1052,1163-1166` (confirmed in Audit 01 PA-01).
- `src/orchestration/inference_job.py:161-174` loads `combined_calibration_offsets` from the chosen production/run directory and passes them into `MonteCarloPredictor` with no experiment-mode gate.
- `src/orchestration/edge_refresh_job.py:229-283` independently resolves a model, loads the same offsets at `:270-276`, and caches the resulting predictor process-wide.
- NBA backtest/sweep also auto-load the artifact (`src/backtesting/run_backtest.py:188-198`; `src/backtesting/run_sweep.py:1031-1038`, cited by Audit 01).
- Canonical evidence explicitly rejects this behavior: remote `Models/Calibration-Guide.md:15-20`, `Decisions/Calibration-Decisions.md:13-31`, `Lessons/Q10-Miscalibration-Is-Edge.md:18-32`, and `Operations/Critical-Invariants.md:12`.

**Failure mode**

A model directory gains `combined_calibration_offsets.json` through normal training, calibrate-only, manual copy, or promotion. Inference, edge refresh, backtest, and sweep then silently warp samples because the file exists. The same model binaries can therefore execute two probability/calibration policies without an explicit run mode, and an evaluation can appear reproducible by directory name while violating the never-deploy invariant.

**Confidence:** High.

**Current mitigation**

The currently enumerated NBA production directory did not contain `combined_calibration_offsets.json`; canonical docs warn strongly against deployment; the path is visible in logs when offsets load. Absence today is not a guard against a future copy.

**Plan interaction**

This is the first safety precondition for Lane 04 calibration/artifact extraction and Audit 02 TV-04. It must not reopen completed MLB lanes or alter Q10/model math.

**Safe evidence step**

Add no-DB temporary-directory characterization that places an offset file beside otherwise valid dummy artifacts and inventories every constructor that activates it. Specify report-only/unsupported behavior before changing source. Do not retrain or compare ROI.

**Done condition**

File presence cannot activate global offsets in any production, refresh, promotion-capable backtest, or sweep path; a typed explicitly non-production analysis mode is required if legacy diagnostics remain; production artifact validation rejects the file; and static/runtime anti-regrowth tests cover all consumers.

### MMP-02 — CRITICAL — Model identity and completeness are filename-driven; MLB manifests are writer-only and both suites can load partially

**Exact evidence**

- MLB training writes `model_manifest.json` with stat/model/profile/git/pointer fields but no hashes (`src/models/mlb/training/artifacts.py:74-102`); concrete pitcher/batter pipelines call it (`src/models/mlb/mlb_train_pipeline.py:801-808`; `src/models/mlb/mlb_batter_train_pipeline.py:1044-1052`).
- `MLBModelSuite.from_directory` says it loads whatever exists and gracefully skips missing models (`src/models/mlb/mlb_model_suite.py:178-188`), chooses copula/single/binomial/negbin by directory shape (`:193-293`), catches load failures, and returns the surviving subset (`:297-303`). It never reads `model_manifest.json` or `suite_manifest.json`.
- The suite calls `suite_manifest.json` optional (`mlb_model_suite.py:7-18`) and only writes one on explicit `write_manifest()` (`:351-367`). No production/sweep consumer call surfaced.
- MLB resolver accepts any existing `production/` directory before checking contents (`src/backtesting/mlb/sweep_bootstrap.py:35-62`) and then loads the permissive suite (`:81-85`).
- NBA `PlayerPropsModelPipeline.load_all` independently loads each present model/config and returns the partial pipeline without a completeness assertion (`src/models/quantile_trainer.py:496-521`). NBA backtest resolvers disagree on latest-directory completeness, as documented in Audit 01 PA-03.
- Current targeted production inventory contains no `model_manifest.json` or `suite_manifest.json` in either NBA or MLB production. MLB production contains legacy mixed stat artifacts, including unsupported/optional files; NBA production contains model binaries/config but no authoritative run manifest.
- Existing tests validate manifest writing only (`tests/test_mlb_training_artifacts.py:59-84`; pitcher/batter variant tests) and quote-label ownership, not reader enforcement.

**Failure mode**

A directory can mix files from different runs, omit a requested stat, contain a corrupt model, or retain stale unsupported artifacts. The loader logs an error and proceeds with a partial/different suite. Directory name and file shape—not one immutable declared identity—decide what production, sweep, or audit executes.

**Confidence:** High.

**Current mitigation**

`_incomplete` finalization protects newly trained run directories; `scripts/audit_mlb_model_artifacts.py:162-238` can fail requested missing stats and warn about extras; model classes have internal feature metadata. That audit is separate and does not make manifests authoritative.

**Plan interaction**

This is the consumer half of completed stat-suite Slices 5-6 and the unresolved artifact/promotion portion of Lane 04. It does not justify another writer or runner rebuild.

**Safe evidence step**

Use temporary directories and fake loaders to characterize manifest/file disagreement, stale extras, corrupt required stats, absent requested stats, and mixed run IDs across NBA/MLB. No real binary load is needed.

**Done condition**

One versioned manifest per deployable suite declares required stats, model types, source run IDs/commit, feature-contract versions, and hashes for every artifact; all promotion/backtest/inference consumers validate it before construction; partial loading is explicit diagnostic mode only; and reader/writer parity tests reject stale, mixed, corrupt, or missing artifacts.

### MMP-03 — CRITICAL — Quote-clean and audit metadata can overstate promotion readiness when linked coverage/dropout evidence is absent

**Exact evidence**

- `build_promotion_contract_metadata` sets `promotion_grade = quote_clean.enabled` before evaluating dense linked-coverage proof (`src/backtesting/mlb/promotion_contracts.py:29-47`). Missing coverage adds a warning but leaves `promotion_grade=true` (`:38-60`).
- The test explicitly locks this in: dense CLV without an audit note still asserts `promotion_grade is True` (`tests/test_mlb_promotion_contracts.py:41-53`).
- `scripts/run_mlb_stat_ablation.ps1:253-269` always passes `--skip-dropout-audit` in its audit stage.
- With no dropout decision, `populate_validation_decisions` falls through to `dense_table_adequate = "yes"` (`scripts/run_mlb_quote_clean_audit_suite.py:322-330`). It can then allow flat staking, edge sizing, and feature expansion from CLV/ranker values (`:332-360`).
- `determine_gate_status` only blocks dropout when a summary exists and says FAIL; a not-run audit is not a failure (`run_mlb_quote_clean_audit_suite.py:363-376`).
- Suite metadata records `dropout_returncode=None` and no explicit `dropout_skipped`/required-gate status (`:625-639`).
- Canonical lesson evidence says quote-clean plus CLV timing/coverage/ranking/intraday evidence is a sequence, not a single flag (`Lessons/Quote-Clean-CLV-Before-Feature-Work.md:75-106`; `Lessons/Raw-Timestamps-Do-Not-Guarantee-Temporal-Integrity.md:33-40`).

**Failure mode**

A stat runner intentionally skips the expensive dropout phase. Mean CLV and ranker CIs pass on selected bets. The suite can label dense adequacy `yes`, edge sizing/feature expansion `yes`, and overall PASS even though prediction-level quote dropout, linked coverage, and timing population were never certified. Separately, the sweep artifact already says `promotion_grade=true` with only a warning.

**Confidence:** High.

**Current mitigation**

The runner and skill guidance verbally describe the abbreviated audit as CLV-only/finalist discovery; `dropout_returncode` is serialized; warnings can be manually inspected; live/Kelly remains blocked by canonical practice. Machine-readable fields contradict that cautious posture.

**Plan interaction**

This is a semantic closeout defect in completed Lane 01/stat-suite tooling, not a reason to reopen its module decomposition. Audit 02 TV-04 should add the gate contract. No betting-policy threshold is changed here.

**Safe evidence step**

Create a no-DB fixture suite with passing CLV/ranker rows and three dropout states: PASS, FAIL, and not run. Snapshot the current metadata/status and define the required fail-closed or `incomplete_evidence` result for not-run.

**Done condition**

`promotion_grade` means all declared prerequisites passed, not merely quote-clean mode; missing dense linked-coverage/dropout evidence is machine-readable `incomplete`/not promotable; abbreviated discovery runs cannot emit sizing/feature-expansion approval; and tests cover PASS/FAIL/WARN/not-run independently.

### MMP-04 — HIGH — Sweep → CLV → ranker evidence is path-coupled, not lineage-bound to one model and input set

**Exact evidence**

- MLB sweep resolution returns `model_path` in runtime (`src/backtesting/mlb/sweep_bootstrap.py:24-32,81-96`), but `save_results` does not accept or serialize it (`src/backtesting/mlb/sweep_results.py:117-143`). Sweep metadata contains dates/counts/timing and quote promotion metadata, not model path, manifest ID, git hash, artifact hashes, stat suite, seed, or command (`:130-143,205-208`).
- Per-config `metrics.json` records config and metrics but not model/input identity (`sweep_results.py:186-203`).
- The audit suite requires a separate `--model-dir` (`scripts/run_mlb_quote_clean_audit_suite.py:486-519`) but its final metadata omits that model dir (`:630-639`). When dropout is skipped, that model argument is not used to regenerate predictions at all (`:544-565`).
- CLV items are discovered by filesystem path/name (`run_mlb_quote_clean_audit_suite.py:73-83,528-532`); the suite writes those paths into item rows but no content hash (`:29-70,379-395`).
- Ranker accepts independent `--clv-matches-csv` and optional `--candidate-edges-csv` paths (`scripts/analyze_mlb_clv_ranking_diagnostics.py:1202-1212`) and writes outputs/recommendation without an input manifest/hash (`:1187-1199`).
- The generic runner discovers the post-training model by most recent filesystem `LastWriteTime` (`scripts/run_mlb_stat_ablation.ps1:185-192`), then routes separately named sweep/audit/ranker directories (`:194-303`).
- Training metadata captures a Git hash but not dirty-tree state, dependency lock hash, data snapshot/query identity, artifact hashes, or parent production model (`src/models/mlb/mlb_train_pipeline.py:766-798`; `mlb_batter_train_pipeline.py:1008-1027`).

**Failure mode**

A resumed audit can point at the right-named sweep but a newer model directory; a ranker can consume CLV rows from one config and candidate edges from another; outputs can be moved/copied while preserving plausible labels; or the same run label can refer to changed untracked inputs. The numerical reports remain internally valid but no longer prove the intended candidate lineage.

**Confidence:** High.

**Current mitigation**

Run labels embed stat/mode/time; wrappers keep model, sweep, audit, and ranker roots together; training writes timestamps/Git hash/config; CLV/ranker use deterministic bootstrap defaults. Human reports often record explicit paths. These are conventions, not enforced joins.

**Plan interaction**

This is the exact Lane F closeout requested by Audit 00 and overlaps Lane 04's promotion metadata owner. Preserve completed stat-generic tools; add a shared evidence-envelope contract rather than stat-specific forks.

**Safe evidence step**

Using tiny CSV/JSON fixtures only, deliberately mismatch model ID, sweep ID, CLV source hash, and candidate-edge source hash. Define which stage must reject each mismatch before designing storage.

**Done condition**

Every training run has an immutable run ID/manifest; sweep metadata records that ID plus manifest/artifact hashes, full effective config, code/dependency identity, and seed; CLV/audit records sweep/config/input hashes; ranker records and verifies CLV/candidate hashes; copied or mismatched artifacts fail loud; and a single command can print the complete lineage chain without DB access.

### MMP-05 — HIGH — NBA promotion is destructive, latest-by-default, evidence-unaware, and has no first-class model rollback transaction

**Exact evidence**

- `scripts/promote_model.py:36-57` lists lexically sorted completed directories; `promote(..., run_name=None)` uses the last one by default (`:60-84`).
- Validation checks six filenames only (`:26-33,86-92`); it does not inspect calibration/backtest/CLV evidence, source commit, run config, forbidden offsets, model hashes, or feature compatibility.
- Promotion removes the existing target with `shutil.rmtree` before copying (`:94-101`). A failed copy leaves no valid target.
- The only source marker is plaintext `.source` written after copy (`:103-105`). There is no previous-manifest pointer, backup, transaction journal, atomic rename, post-copy load audit, or rollback command.
- The command recommends committing the copied directory (`:107-112`), but current `.gitignore:23-28` globally ignores JSON metadata and only exempts a few root JSON names.
- The stat-suite rebuild rollback section is code-slice rollback and explicitly avoids production artifacts (`.hermes/plans/mlb-stat-suite-rebuild/07-implementation-sequence.md:194-199`); it does not mitigate model promotion.
- No corresponding MLB production promotion/rollback command surfaced in the bounded `scripts/` search; MLB inference simply prefers any `production/` directory (`src/orchestration/mlb_inference_job.py:126-153`).

**Failure mode**

An operator omits a run name and promotes the newest completed but not approved run. The existing production directory is deleted; copy fails or the candidate later fails loading; no deterministic previous target exists to restore. Even a successful copy can commit binaries without full provenance JSON, and no evidence record proves why that candidate was selected.

**Confidence:** High for NBA; high that no MLB promotion command surfaced in audited scripts, but a manual external procedure remains possible.

**Current mitigation**

Required-file checks, `_incomplete` filtering, Git history for tracked binaries, explicit `--name` targets, and manual commit review reduce risk. Git can be used manually to recover tracked artifacts, but this is not an atomic runtime rollback and does not recover ignored metadata.

**Plan interaction**

Lane 04 should own the candidate/evidence manifest contract, while deployment/runtime work should own atomic pointer/swap and rollback. Do not fold model math or retraining into this remediation.

**Safe evidence step**

Filesystem-only fault-injection with tiny sentinel files: copy failure after target deletion, invalid candidate, forbidden-offset candidate, and post-copy loader failure. Record current loss/recovery behavior without touching actual production directories.

**Done condition**

Promotion requires an explicit candidate run/evidence manifest; validates complete hashes/features/forbidden files; stages into a new directory; performs a no-DB load/contract audit; switches atomically; records previous/current immutable IDs and approver/evidence; and a tested rollback command atomically restores the prior suite. NBA and MLB use the same transaction semantics with sport-specific validators.

### MMP-06 — HIGH — Feature contracts do not fail closed at train/serve and artifact boundaries

**Exact evidence**

- Centralized MLB feature contracts exist (`src/models/mlb/features/contracts.py:15-177,283-317,444-466`) and loaders pass `as_of_time` through (`batter_inference_loader.py:10-36`; `pitcher_inference_loader.py:10-28`).
- Forced family features missing from the current dataframe are intentionally ignored (`src/models/mlb/training/feature_controls.py:126-132,148-170`); only exact forced features fail loud (`:149-151`). This can make a named family experiment narrower than declared.
- Pitcher MC constructs model input with `{feature: features.get(feature, 0)}` and fills coercion failures with zero (`src/models/mlb/mlb_monte_carlo.py:244-250`). Batch behavior does the same (`:134-145`). Batter predictors use the same defaulting pattern in `src/models/mlb/mlb_model_suite.py:124-141,159-164`; other MLB predictor classes follow model feature names with zero/default fill.
- New training writes selected-feature manifests, but production consumers do not read them (MMP-02).
- Production MLB metadata predates shared manifests. The targeted `batter_hits_negbin_meta.json:4-26` lists 22 features, while `batter_hits_best_hyperparams.json:1-41` contains tuning config but no train seasons/calibration cutoff; the artifact audit only warns when those metadata fields are absent (`scripts/audit_mlb_model_artifacts.py:212-221`).
- A temporal contract can require `as_of_time` only when callers set `promotion_grade=True` (`src/models/mlb/features/temporal_contracts.py:17-32`); the searched call path in `prop_line_feature_source.py` invokes `resolve_as_of_policy(as_of_time)` without that flag.

**Failure mode**

A declared family can train with missing members; a production source rename/outage can silently turn selected features into zero; a stale suite can load against a changed feature-store contract; and promotion/backtest can continue without proving historical variation or train/serve equivalence. Selector/calibration results then describe a different effective experiment than the run label.

**Confidence:** High for permissive behavior; Medium-High for production frequency because no DB/source-coverage query or production load was allowed.

**Current mitigation**

Central contract lists, public feature loaders, locked rejected features, exact force-feature validation, feature manifests on new runs, temporal as-of support, and the standalone artifact audit materially help. Default values are sometimes intentional compatibility behavior.

**Plan interaction**

Completed feature-store Lanes 02/03 should not be reopened. This belongs at the training-profile/evidence/production-loader boundary and Lane 04 stage contracts. Prior lessons require family-level interpretation and historical source variation before any model conclusion.

**Safe evidence step**

No-DB contract matrix: for each selected production feature, compare manifest, model metadata, training dataframe schema fixture, backtest fixture, and inference fixture. Inject one missing, renamed, nonnumeric, and default-only family member and record whether each stage warns, fails, or silently fills.

**Done condition**

Each manifest versions the feature contract and declared family expansion; required selected features fail closed unless an explicitly approved default policy exists; train/calibration/backtest/inference schema and non-default coverage summaries are persisted; family experiments record requested versus present members; and loader tests enforce parity without requiring training.

### MMP-07 — HIGH — Production loading can silently narrow the requested stat surface; the separate functionality audit is not an enforced gate

**Exact evidence**

- MLB inference defaults requested stats to every key in `MLB_STATS` (`src/orchestration/mlb_inference_job.py:168-175`) after loading a permissive suite (`:159-162`).
- Daily batter prediction filters requested stats to `suite.has_stat` and logs/skips when none are available (`src/models/mlb/mlb_daily_runner.py:521-535`). Pitcher execution similarly depends on whether the predictor loaded (`:142-154`).
- The inference job only fails when all predictions are zero despite scheduled games or when all sample arrays are zero (`mlb_inference_job.py:181-210`). It does not assert that every required requested stat produced predictions.
- Sweep prediction cache filters requested pitcher/batter stats to `suite.has_stat` (`src/backtesting/mlb/prediction_cache.py:88-108`) and catches per-player/feature/prediction errors as warnings (`:124-167,192-233`).
- The standalone audit has the correct fail-closed requested-stat check (`scripts/audit_mlb_model_artifacts.py:162-188`) and warns about extra/unsupported loaded stats (`:190-210`), but neither inference nor sweep invokes it.
- Current MLB production inventory includes legacy `batter_total_bases`, `batter_runs`, HR, and `batter_hrr` artifacts in addition to pitcher K/hits; the audit script explicitly classifies several as not live or optional (`audit_mlb_model_artifacts.py:24-30`).
- NBA `load_all` can similarly return a pipeline with only present components (`src/models/quantile_trainer.py:496-521`), while production inference does not assert all configured NBA stats loaded before running (`src/orchestration/inference_job.py:161-174`).

**Failure mode**

One required model is corrupt/missing while another loads. The job produces/stores a nonzero partial slate and reports success; sweeps evaluate only the surviving stat; stale extra artifacts broaden apparent suite capability. Monitoring sees healthy aggregate counts rather than a missing required stat lane.

**Confidence:** High.

**Current mitigation**

`audit_mlb_model_artifacts.py` fails requested missing stats; inference fails zero-total outputs and verifies persisted row counts; explicit `--stats` exists; unsupported trading stats have downstream whitelists. Aggregate health checks do not prove stat completeness.

**Plan interaction**

This completes the artifact-functionality gate described by the stat-suite skill and Audit 00. It is a loader precondition, not approval to support additional stats or promote current MLB artifacts.

**Safe evidence step**

Fake-suite tests with required stats `{pitcher_strikeouts,batter_hits}` where one loader fails, plus stale optional extras. Exercise resolver → suite → runner preflight only; do not load real binaries or query games.

**Done condition**

Each production/sweep request declares required and optional stats; required stats must load and pass feature-contract validation before any prediction; optional stats are opt-in and fail closed; per-stat output health is recorded; and inference/sweep use the same validator as the standalone audit.

### MMP-08 — HIGH — NBA production provenance metadata is not committed with the tracked binaries

**Exact evidence**

- Canonical Hard Fact GF-F012 says production artifacts are committed under `src/models/artifacts/production/` and training runs are ignored (remote `Operations/Hard-Facts.md:50`).
- `.gitignore:23-28` ignores all `*.json` except package/Vercel/TypeScript root exceptions. Artifact-directory negation at `:77-82` re-includes directories, but does not override the earlier JSON file ignore for NBA files.
- Targeted `git ls-files` showed only five NBA production binaries/configs: `minutes_model.joblib`, `pts_rate_model.joblib`, `reb_rate_model.joblib`, `ast_rate_model.joblib`, and `feature_config.joblib`.
- The working tree production directory also contains `run_config.json`, calibration reports, selected features, best hyperparameters, copula parameters, and correlation analysis, but those files are not in the tracked inventory.
- Current local `src/models/artifacts/production/run_config.json:2-11` carries train seasons, calibration season/cutoff, timestamp, and tolerances—the exact provenance missing from a clean clone.
- `scripts/promote_model.py:103-112` creates `.source` and instructs `git add` of the directory, but neither `.source` nor ignored JSON is guaranteed to stage without force/ignore exceptions.
- MLB production has an explicit recursive exception (`.gitignore:80-82`) and tracked JSON model metadata, but its current legacy suite still lacks new manifests/training metadata (MMP-02).

**Failure mode**

A developer sees rich local metadata and assumes deployment/Git rollback preserves it. A clean clone or Railway build receives only NBA binaries/config joblib files; provenance, calibration report, selected-feature JSON, and source marker disappear. Binary history can identify a commit but not reliably reconstruct the approved training/evidence chain.

**Confidence:** High.

**Current mitigation**

Feature names are embedded in `feature_config.joblib`; Git tracks model binaries and their commit history; local JSON exists; older documentation records promotion commits. This is insufficient for human-readable, independently verifiable provenance.

**Plan interaction**

This is a repository/artifact packaging prerequisite for Lane 04 and model rollback. It should not be solved by broadly unignoring all JSON or checking in training-run directories.

**Safe evidence step**

Use `git check-ignore -v` and a temporary clean-index/clone inventory in a future approved evidence slice; compare required production manifest files to tracked files without loading models.

**Done condition**

Narrow ignore exceptions track the versioned production manifest/provenance files only; manifest hashes cover binaries/config; a clean clone has the same deployable identity and validator result as the promotion workspace; generated training runs remain ignored; and tests verify packaging inventory.

### MMP-09 — MEDIUM — Pitcher and copula Monte Carlo reproducibility depends on prediction order and process history

**Exact evidence**

- A stable per-player/game/stat seed helper already exists (`src/models/mlb/mlb_monte_carlo.py:31-35`).
- Batter NegBin/binomial/compound predictors use it for each prediction (`mlb_monte_carlo.py:530-536,621-626,717-723`), making those samples order-invariant.
- Pitcher quantile predictor instead creates one stateful RNG at construction (`:59-70`) and advances it for every inverse-CDF sample (`:181-197`).
- Pitcher copula likewise creates a stateful RNG (`:341-345`) and advances it per prediction (`:376-378`).
- MLB suite also has a stateful binary predictor RNG (`src/models/mlb/mlb_model_suite.py:77-100,138-142`), although current HR production viability is separately blocked.
- Backtest prediction ordering comes from schedule/feature dataframe iteration and catches/skips failures (`src/backtesting/mlb/prediction_cache.py:54-70,113-167,170-234`). A skipped or reordered earlier player changes later pitcher samples.
- CLV bootstrap is reproducible with fixed seed 17 (`scripts/analyze_mlb_clv.py:431-456`), and ranker exposes a default seed 42 (`scripts/analyze_mlb_clv_ranking_diagnostics.py:1202-1212`); the problem is not all random computation.

**Failure mode**

The same pitcher/game/features can receive different MC samples when another game/player is inserted, skipped, reordered, or predicted earlier in a long-lived process. Edge thresholds near a boundary can therefore change between sweep, daily inference, resumed runs, or retries even with the same nominal seed and model.

**Confidence:** High for sample order dependence; Medium for material decision frequency because no replay/backtest was run.

**Current mitigation**

Base seeds are fixed; schedule SQL orders games by game time in daily runner (`src/models/mlb/mlb_daily_runner.py:199-225`); batter predictors already use stable keyed seeds; large sample counts reduce but do not eliminate threshold variance.

**Plan interaction**

This is a reproducibility contract adjacent to Lane 01 prediction caching and Lane 04 artifact/run metadata. It does not recommend a distributional-model change; preserve empirical-CDF probability semantics.

**Safe evidence step**

Pure fake-pipeline test: predict pitcher A then B, reset and predict B then A, and compare A's samples/probability. Repeat with an injected skipped player and copula fake. No DB, training, or backtest.

**Done condition**

All stochastic prediction paths derive an explicit stable seed from base seed + model/stat/player/game (and versioned sampler policy); ordering/skips/process reuse do not change a prediction; effective seed policy is recorded in sweep/inference metadata; and parity tests cover single/batch/sweep/daily adapters.

## Rejected suspicions and confirmed mitigations

1. **Rejected: the MLB stat-suite still lacks generic runners, profile controls, or shared writers.** Those exist in `scripts/run_mlb_stat_ablation.ps1`, `scripts/resume_mlb_stat_ablation_audit.ps1`, and `src/models/mlb/training/{profiles,feature_controls,artifacts,base_orchestrator}.py`; Audit 00 and the implementation sequence show they are committed. Remaining debt starts at consumer enforcement and lineage.
2. **Rejected: `run_mlb_sweep.py` is still the pre-migration god module.** Completed Lane 01 extracted config/bootstrap/cache/edge/results/line/promotion seams and has inventory tests. Findings target the semantics carried by those seams, not their file decomposition.
3. **Rejected: there is no artifact functionality audit.** `scripts/audit_mlb_model_artifacts.py` is a useful fail-closed read-only gate for requested stats. The finding is that production/sweep do not enforce it and it does not validate authoritative manifests/hashes.
4. **Rejected: all model/evaluation randomness is unseeded.** CLV uses seed 17, ranker defaults to 42, and batter MC uses stable keyed seeds. MMP-09 is specifically pitcher/copula/binary order dependence.
5. **Rejected: all production artifacts are untracked.** NBA model binaries/config joblib and MLB production files are tracked. The narrower confirmed gap is NBA JSON/source provenance and absent authoritative manifests.
6. **Rejected: MLB production loading always fails open with zero models.** `mlb_inference_job.py:195-210` fails when scheduled games yield zero aggregate predictions or zero samples. MMP-07 concerns partial required-stat loss, which aggregate checks do not catch.
7. **Rejected: feature-store temporal contracts are absent.** As-of requests and temporal contract types exist, and completed Lane 02 established focused loaders/sources. The unresolved issue is promotion-grade enforcement and artifact/feature compatibility, not a return to legacy feature-store ownership.
8. **Rejected: quote-clean/CLV/ranker gates are missing entirely.** They exist and encode meaningful lessons. MMP-03/MMP-04 concern misleading not-run semantics and missing lineage binding.
9. **Not promoted: current production model quality is bad because metadata is old or incomplete.** No model was loaded or evaluated. Metadata age/absence is an identity and reproducibility problem, not performance evidence.
10. **Not promoted: missing family members always indicate a broken experiment.** Optional/sparse feature sources and intentional defaults exist. The defect is that requested versus effective family membership and default coverage are not persisted/enforced strongly enough for interpretation.

## Coverage gaps

- No model binary was deserialized, so current NBA/MLB production load success, embedded feature names, model corruption, and numerical parity remain unverified.
- No Railway filesystem/image/log/env was inspected. The deployed artifact inventory may differ from this working tree; current production model IDs are unknown.
- No DB/API call was made, so source coverage, feature non-default variation, prediction counts, quote/dropout coverage, CLV rows, and live/paper runtime state remain unverified.
- No training, backtest, sweep, CLV, ranker, artifact audit, inference, or promotion command was run by instruction. Test references are static source evidence, not pass claims.
- Generated/ignored MLB run directories and `backtest_results/` were not broadly inventoried. This audit addresses architecture and routing, not which recent candidate won.
- Dependency/environment identity is not currently captured in reviewed manifests; the audit did not compare lockfiles to artifact creation environments.
- Dirty-worktree state at training time is not recorded; `git_hash` alone cannot prove source reproducibility.
- Training data row IDs/snapshot/query hashes are not persisted in reviewed metadata, and no DB snapshot mechanism was assessed.
- No external/manual MLB promotion procedure was found in the bounded scripts/docs search; its existence outside audited paths remains possible.
- Git branch protection, Railway deploy rollback, release tags, and artifact-store retention were outside source-only scope.
- NBA playoff/alternate target promotion via `--name` was not traced through every scheduler/consumer.
- Current production JSON files may be local residue from previous runs. Their presence was used only to compare working-tree versus tracked provenance, not as proof of deployed identity.
- Canonical lesson search surfaced no atomic artifact promotion/rollback lesson. A future durable lesson should be created only after an approved implementation and real fault-injection validation, not from this audit alone.

## Prioritized safe evidence sequence

This is characterization order, not authorization to edit or promote:

1. **MMP-01:** temporary-directory offset activation/forbidden-artifact inventory across all NBA consumers.
2. **MMP-03:** fixture-only quote-clean/dropout PASS/FAIL/WARN/not-run state matrix.
3. **MMP-02/MMP-07:** fake-suite manifest/file/required-stat consumer tests for complete, partial, mixed, stale, and corrupt layouts.
4. **MMP-04:** fixture lineage mismatch tests across sweep, CLV suite, and ranker.
5. **MMP-05:** temporary-directory promotion fault injection and rollback-state characterization.
6. **MMP-08:** clean tracked-artifact packaging inventory and ignore-rule proof.
7. **MMP-06:** selected-feature/family train-backtest-inference contract matrix with missing/default-only fixtures.
8. **MMP-09:** order-invariance tests for pitcher/copula stochastic predictions.
9. Only after the contracts exist should Chase decide whether Lane 04 implementation, promotion tooling, or packaging is prioritized. No retrain/backtest is needed for the first evidence slices.

## Audit completion criteria

This report is complete when it:

- includes remote canonical Hard Facts, Critical Invariants, lesson-tag inventory, and relevant keyword/page evidence before interpretation;
- applies prior lessons/invariants to every architecture/promotion interpretation;
- covers artifact lifecycle, manifests, model selection, sweep/backtest/ranker/CLV routing, promotion evidence, rollback, reproducibility, lineage, feature contracts, experiment isolation, and NBA/MLB loading;
- reconciles audits 00/01/02/06, the MLB stat-suite rebuild, and god-class Lane 04 without editing them;
- gives each finding exact path/symbol/line evidence, failure mode, confidence, current mitigation, plan interaction, safe evidence step, and done condition;
- records rejected suspicions and coverage gaps;
- changes only `.hermes/audits/tech-debt/05-model-mlops-promotion.md`;
- passes report-content validation and `git diff --check` scoped to this file.
