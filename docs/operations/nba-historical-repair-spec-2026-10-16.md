# NBA Historical Training/Replay Repair Specification

## Kick off immediately

- You: let the already-running [NBA-only refresh](nba-sync-inventory-2026-10-16.md) finish; do not launch another sync. Current tables are not yet accepted as a refreshed mirror.
- Other chat: S1 is delivered in its isolated worktree; preserve it and stop further edits/integration. Chase approved the additional `_run_date` method. See the [coordinator review](nba-s1-coordinator-review-2026-10-16.md).
- This chat: own S2 read-only source/timestamp research, policy recommendations, evidence reconciliation and review of the S1 result. This does not include concurrent edits to S1's files.
- You: review the S1 coordinator packet and [wiki](../understanding/nba-rebuild.md) while independent review proceeds. Do not relaunch S1 or training.

S1 does not depend on choosing the historical decision time. S2 research can proceed in parallel. S3-S5 remain gated by source evidence and explicit policy approval. Starting S1 does not authorize the rest of this document.

### Original S1 handoff — already delivered; do not relaunch

```text
Work in C:\Users\Chase\Projects\GameFlowData. Read AGENTS.md and load the relevant GameFlow skills. Read docs/operations/nba-historical-repair-spec-2026-10-16.md, then implement S1 only in an isolated worktree/branch. Do not make source edits in the coordinator's checkout.

Allowed source edits:
- src/models/features/nba/date_range_loader.py: DateRangeFeatureLoader.load
- src/backtesting/backtest_harness.py: BacktestHarness.run and the subsequently owner-approved BacktestHarness._run_date

Make failed query chunks and unexpected per-date processing exceptions fail the run instead of returning accepted partial results. Preserve successful output shapes, legitimate empty slates and expected no-quote/no-bet exclusions. Do not alter feature values, model math, odds selection, public arguments or source policies.

Do not create or edit unit tests. Run relevant existing tests, scoped Ruff and bounded real-path/failure-injection checks required by S1. No DB writes, migrations, scraping, sync, long training/replay, commit, push, merge or deployment. Do not read/print credential files. Stop and ask if another source file needs edits or if validation needs broader resources.

Return a review packet: worktree/branch, exact changed files, scoped diff summary, actual validation commands/results, any unverified paths and remaining risks. Do not claim completion from lint alone. The coordinator reviews before integration.
```

### Read while work proceeds

1. This document's S1 requirements and acceptance checks: what the worker must deliver.
2. [NBA rebuild wiki](../understanding/nba-rebuild.md): why the gates exist and what each stage does.
3. [Reviewed historical-readiness report](nba-historical-readiness-2026-10-16.md): evidence, corrections, unresolved decisions and agent status.

> For Hermes: use the GameFlow implementation-worker lane for approved source changes; independently review scoped diffs and real-path results. Do not create new unit tests.

Goal: make the first meaningful NBA retrain and its replay trustworthy without waiting for unrelated product work.
Architecture: retain minutes x rate, stable feature facade and lagged producers; repair completeness first, then use one explicit observation/feature contract for historical training and evaluation.
Tech stack: existing Python, pandas, SQLAlchemy/Postgres, and project model/CLI interfaces; no new infrastructure.

Status: S1 is implemented but uncommitted in `repair/nba-s1-fail-closed`, not integrated. Coordinator independently reran 18 existing tests, Ruff and scoped diff checks successfully; independent reviewer reran both disposable probes with all 12 controlled checks passing. Real-data/artifact acceptance waits for sync completion. S2-S5 retain explicit source/policy gates. This documentation update authorizes no integration, DB mutation or training.
Evidence: [reviewed three-agent report](nba-historical-readiness-2026-10-16.md).

S2 source-only progress: [clock review](nba-s2-source-clock-review-2026-10-16.md). Official odds docs support a conditional conservative historical-request bound, not exact snapshot identity; injury-cycle claims do not yet establish historical availability. This narrows the evidence questions but approves no S2-S5 policy or source edit.

## 1. Reconciled evidence and non-goals

Authority warning: all local sample counts and missing-table observations below describe the stale mirror at probe time. They are not current Supabase state or evidence that source freshness is ready for training. Source-code findings remain valid independently; use the remote inventory and subsequent scoped freshness checks for canonical data conclusions.

- The 12 missing source/rolling keys in the 22025 playoff sample are all zero-minute DNPs. All 202 sampled appearances meeting the current five-minute target threshold have advanced/basic-rolling/advanced-rolling rows. Do not backfill those DNPs to clear a false training blocker.
- These are concentrated samples, not full-cohort certification. Historical source coverage and non-default feature variation across the intended training/calibration/OOS windows remain to be established.
- Current local metadata has typed `snapshot_time`, `market_last_update`, `bookmaker_last_update` and `commence_time` in both odds families. It does not prove historical timestamp coverage or scrape semantics.
- `rapidapi_injuries` exposes `report_date` and textual `report_time` in the restricted metadata inventory; no usable observation history has been established. Parse/publication semantics and source retention must be checked, not guessed from an ID.
- `public.games` and `public.nba_game_schedule` were absent locally. This is not production-state evidence. Historical commence truth needs an identified source; do not infer a midnight start from a game date or apply the schedule migration as part of this spec.
- Current basic incremental rolling masks prior performances below eight minutes; the advanced path shifts without that mask. Row existence alone is not evidence of a rolling-contribution violation.

Non-goals: model-family redesign, frontend, roster deployment, Engineering OS, new feature families, arbitrary late-season exclusions, blanket feature removal, full scraping/sync/backfills, DDL/index changes, BL/Kelly optimization, deployment or model promotion.

## 2. Ownership and sequencing

This chat: S2 read-only research and final contract/policy reconciliation, bounded SQL-runner checks when scoped, worker specs, independent review and wiki updates. Continue independent research while Chase reads; stop at material policy/implementation gates rather than waiting for every document to be read.
Other chat/worktree: implement only the specifically approved slice below; one writer per slice, no shared-checkout parallel writers.
You: approve implementation/policy choices and launch long training/sweep jobs.

Sequence: S1 can proceed independently after approval. S2 establishes supported observation policy. S3 and S4 follow S2 and must agree on the shared contract. S5 follows repaired sources/contracts and a bounded end-to-end smoke. No full retrain merely to refresh an artifact timestamp.

## 3. S1 — fail closed on incomplete feature/replay execution

### Allowed edits

- `src/models/features/nba/date_range_loader.py`: `DateRangeFeatureLoader.load`, especially the failed-chunk branch currently at lines 330-367.
- `src/backtesting/backtest_harness.py`: `BacktestHarness.run` and the owner-approved `_run_date` extension. Remove the nested batch-inference catch that otherwise converts a predictor error into a harmless-looking empty outcome; preserve legitimate empty/no-quote/no-bet paths.
- Documentation/evidence for this slice only. No edits to tests, odds selectors, SQL source policy, feature values or model math.

### Required behavior

1. If any date-range query chunk fails, raise a clear `RuntimeError` before concatenating or returning successful chunks. Report requested range and failed chunk/date ranges; do not include credentials or full connection exceptions.
2. Preserve successful return shape `dict[date, DataFrame]`, existing `chunk_size` behavior and legitimate empty/no-game requests. Do not classify an empty valid slate as a query failure.
3. At the per-date replay stage, unexpected processing exceptions must invalidate the run rather than log and continue to a partial metrics result. Include the failed date in the error and propagate failure before simulation/result acceptance.
4. Do not transform a model-declared no-bet/no-quote outcome into an exception. Expected exclusions need explicit reason/counts; distinguish them from execution failures.
5. FeatureStore's current facade directly returns the loader result (`src/models/feature_store.py:112-120`) and should propagate exceptions without edits. Feature prefetch in `BacktestHarness.run:169` is outside its per-date catch; preserve propagation.
6. Do not change public arguments, data windows, model artifacts or stat policies.

### Verification required before acceptance

- Scoped Ruff on the two edited Python files.
- Existing `tests/test_backtest_harness.py` and `tests/test_nba_feature_store_inventory.py`; no new unit tests or edits that weaken assertions.
- A disposable, bounded smoke through the actual loader/facade and harness: successful data; one failed middle query chunk; one unexpected per-date inference failure; legitimate empty slate. Use temporary controlled adapters only to trigger failures, not fabricated success evidence.
- Record which checks use controlled failure injection versus real local data. Verify no accepted partial predictions/metrics are emitted after an injected execution failure. This is execution validation, not model-quality evidence.
- Confirm the normal replay CLI exits non-zero for a propagated fatal error. Do not launch an expensive replay solely for this; prepare an owner-run command if bounded execution cannot be guaranteed.
- Review `git diff -- <the two allowed files>` and `git diff --check -- <the two allowed files>`; no commit/push.

Stop if another wrapper suppresses the error and requires edits outside this scope; report its exact path/symbol for scope approval.

## 4. S2 — freeze observation policy and prove source availability

### No behavior-changing source edits yet

Use the SQL runner, indexed exact game/player/date windows, bounded statement/lock timeouts and read-only transactions. Inspect existing NBA source-ingestion code before interpreting timestamps. No raw-table-wide aggregates or historical rescrape merely because coverage is unknown.

Required evidence:

- Representative regular-season opening/middle/end windows for each proposed season; playoffs/play-in classified separately. Do not use a latest-games sample as cohort certification.
- Keyed timestamp null/variation/lag coverage for props and game lines, same-snapshot pair identity and side orientation. Local schema identifies immutable `staging_id` keys; IDs can tie-break equal timestamps but cannot substitute for availability timestamps.
- Source-backed meaning/timezone/version retention for injury `report_time`; whether retained records distinguish pre-decision from later same-day reports.
- Authoritative historical commence mapping and the count/reasons of records that cannot be assigned safely.
- Coverage/non-default variation of required feature families after actual historical feature assembly, not merely presence of raw table rows.

Owner policy decisions to freeze:

1. Exact decision time: game-relative T-minus or an explicit America/New_York slate rule with early-game handling. No implicit 18:30 default and no season-wide fixed UTC clock.
2. Quote availability predicates and stale/missing timestamp policy. Require selected observation at/before decision and strictly before commence; if market-update time is populated and validated, it must also obey the cutoffs. Do not treat an unvalidated insertion time as historical availability.
3. Missing injury/commence behavior: explicit blocked/unknown state versus a separately approved reduced-feature diagnostic run. No silent latest/zero fallback or automatic feature deletion.
4. Training/calibration/protected-OOS windows and deterministic cohort inclusion, including explicit season-boundary policy. No arbitrary last-X-games exclusion.

Exit: a versioned source/field/policy matrix with actual resolved values. Until then, S3-S5 are gated—not executable exact-command handoffs.

## 5. S3 — historical feature parity and provenance

### Allowed scope after S2 and separate approval

- Existing owners: `src/models/features/nba/training_loader.py`, `date_range_loader.py`, `injury_context.py`, `contracts.py`, and `src/models/train_pipeline.py`.
- Proposed NEW focused owners: `src/models/features/nba/observation_policy.py` and `quote_selection.py`. These are proposed paths, not existing APIs.
- Expand to the public request/facade interfaces only after tracing actual definitions/usages and presenting the needed paths; do not invent an internal call signature.

Required behavior:

- One versioned, timezone-aware per-game decision contract, used by train/calibration and historical replay.
- Replace numeric `MAX(line)`/unrestricted latest-row selection with deterministic eligible per-market selection. Game spreads/totals need not occupy one physical row or share a cross-market snapshot; validate source orientation before applying the team-directional spread transform.
- Player-prop Over/Under selection uses the same player/game/market/book/line and validated snapshot identity. Do not pair independently ranked latest outcomes.
- Share validated injury as-of selection and explicit completeness policy. A same-day date or descending ID is not observation-time proof.
- Match ordered required feature membership, four same-position injury fields, defaults, spread sign, missingness/readiness and season policy across historical train/range paths. Freeze the canonical defaults explicitly; do not silently declare every incumbent training default correct.
- Keep target eligibility (currently `min >= 5`) distinct from prior-game rolling contribution (`>=8`). Preserve producer shifts; do not blindly change all `<= game_date` joins to `<` and double-lag valid data.
- Record exact source/policy/window/feature-contract identity in the training artifact; `_incomplete` remains incomplete on failed readiness. Old loadable artifacts are not retroactively clean after code repairs.

Acceptance: same sampled player/game/policy produces equal feature values, order, defaults, spread direction, as-of provenance and completeness through both loaders. Run existing relevant checks, scoped lint and bounded real local feature assembly. No new unit tests. Stop at any missing historical observation dependency.

### Separate rolling-producer decision

`src/processing/populate_average_stats.py` and `populate_average_stats_incremental.py` are a separate proposed slice, not permission to recompute tables. Confirm both full/incremental advanced producers and normalize the invariant only after documenting affected historical features. Any derived-data rebuild and remote-to-local sync remain separately approved; do not train against unreconciled old/new derived versions.

## 6. S4 — NBA quote-clean replay

### Allowed scope after S2/S3 and separate approval

- `src/backtesting/backtest_harness.py`: BOTH `_get_lines_for_date` and `_prefetch_all_lines`.
- `src/backtesting/run_backtest.py`: explicit model/policy validation and accepted-output metadata, only as required by the approved interface.
- The approved shared quote/policy owners from S3.

Both single-date and prefetched replay must call the same decision/commence/pair-selection contract and produce identical keyed rows/provenance. Preserve existing `--model-dir`, `--local`, `--start`, `--end` interfaces. Reject incomplete artifacts or incompatible policy/features for decision-grade evaluation; do not auto-select a different run.

Save prediction/quote/rejection counts and explicit no-op reasons, exact artifact/config/window identity, selected source times, game/market/book/line and direction. A missing quote is data coverage, not evidence that a model has no edge.

Use `src/backtesting/mlb/line_selection.py` and the canonical MLB lifecycle/sweep as verified reference contracts, not the legacy MLB harness or MLB model shape. Do not import sport-specific MLB identifiers into NBA.

Acceptance: direct/prefetch equality on a bounded real historical window; no selected quote after decision or at/after tip; atomic paired outcomes; dropout/rejection accounting; non-zero execution failure path; saved audit metadata.

## 7. S5 — first meaningful training run and evaluation

Prepare, do not launch, a frozen baseline run after the readiness/contract gates. Existing trainer controls include `--local`, `--train-seasons`, `--cal-season`, and exclusive `--cal-end-date`. Supply real resolved values only after S2; placeholders are not runnable commands.

Chase launches long training. Verify the produced exact artifact through the actual loader, then run protected OOS diagnostics and quote-clean replay. Training and calibration data must not overlap protected OOS.

Start raw/no-BL and flat-stake. The inspected NBA replay CLI exposes Kelly controls, but no flat-stake option was observed in its parser: do not invent `--flat`, interpret Kelly results as flat, or hand off a falsely runnable flat command. Inspect the simulator's real sizing API and scope any required adapter/CLI addition separately.

Required evaluation: model diagnostics/calibration without blindly fixing Q10; profit/ROI, volume, Sharpe, drawdown, side splits and quote/dropout/timing evidence. BL search is finalist-only; Kelly/ranking is optional separate review. Forward paper/customer publication remain later gates, not prerequisites for producing a meaningful historical baseline.

## 8. Non-negotiable boundaries

No new unit tests; preserve existing tests and use real-path smoke/data verification. No Gaussian-CDF probability replacement, global conformal offsets, quantile-order regression or architecture redesign. No stats.nba.com scraping on Railway. No DB mutation, long job, commit/push, deployment or promotion without separate authorization. Missing evidence is unknown, never healthy.
