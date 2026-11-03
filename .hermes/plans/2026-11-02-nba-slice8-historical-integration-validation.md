# NBA Coding Slice 8 — Historical Integration and Validation Plan

> **For Hermes:** This is an umbrella scope with separate owner gates and reviewable implementation sub-slices. Do not launch one broad writer. Use the GameFlow SQL-runner for every live DB query, the implementation-worker lane for each approved code sub-slice, and independent specification/quality review before integration.

**Goal:** Wire the owner-approved historical decision/source/cohort policy through NBA training and replay, prove feature and quote parity on a stable bounded dataset, run the complete artifact/sweep/audit/lifecycle path, and freeze one meaningful baseline configuration for Chase to launch.

**Architecture:** Preserve the existing NBA minutes × rate model, feature facade, empirical-sample probability path, artifact contract, shared-input sweep, evidence audit, and lifecycle runner. Slice 8 supplies the missing approved source-policy adapters and consumer integration: one versioned per-game decision context, one source-specific observation interpretation layer, one shared feature/injury contract, and one shared quote-pair selector used by direct and prefetched replay. Readiness and execution evidence are persisted separately from model-quality results; missing source evidence remains blocked rather than defaulted into a passing run.

**Tech stack:** Existing Python, pandas, SQLAlchemy, Pydantic/PyYAML where already used, the Slice 4 observation/quote primitives, Slice 1–7 contracts, existing pytest/Ruff suites, local Postgres, and isolated read-only SQL-runner probes. No new infrastructure.

---

## Current status and launch block

Scope prepared November 2, 2026.

### Owner authorization — remaining implementation

Chase reports Slice 7 fully implemented and explicitly authorizes the remaining Slice 8 implementation. This supersedes the earlier blanket no-writer wording for 8B–8D code preparation, not the source/policy/real-data acceptance gates below. First verify the completed Slice 7 source and review state; main remains `bf7a3e9` at this checkpoint, so predecessor integration is not yet established.

Implement sequentially in an isolated Slice 8 worktree after bounded API tracing and a precise per-section writable scope. Preserve default/production behavior; historical policy must be explicit and unresolved inputs must fail closed. Do not mark the 8A matrix approved or invent decision time, provenance, injury, carryover, cohort/split or artifact values from this general code authorization. Prepare code against explicit contracts where feasible; stop for an owner decision where real semantics cannot be encoded without one.

No new unit tests or fixture edits; use affected existing suites and only a few short actual-path probes, not a new scratch framework. Write educational documentation concurrently in disjoint files. Keep S1/reconciliation/source writers protected. Commit, merge, push, archive access, DB mutation, long runs and baseline execution remain separately gated. Full Slice 8 acceptance still requires actual bounded data evidence and Chase-launched baseline execution; code delivery alone is not completion.

Status update November 2, 2026:

- Slice 6 is accepted, integrated, and pushed on main at `bf7a3e9`.
- Slice 7 is implemented in its isolated lane and is under review.
- Slice 8 remains **not implementation-ready as one broad lane**, but Slice 7 review is not a blocker to the read-only 8A evidence/policy work.

### Concurrency boundary while Slice 7 is under review

The allowed work is now split explicitly:

- **May proceed concurrently:** 8A read-only source evidence, bounded SQL-runner receipts, policy-option documentation, bounded-window discovery, and preparation of the owner decision packet. This work does not consume or modify the Slice 7 lifecycle contract.
- **May be re-traced/scoped, but not launched from this umbrella plan alone:** 8B and 8C final API/allowlist tracing. Their implementation still requires an approved 8A policy, resolved historical S1 ownership, stable source evidence for the selected sample, and a fresh collision check against active worktrees.
- **Must wait for accepted Slice 7 contract:** 8D lifecycle integration at Gate D4, lifecycle resume/invalidation assertions, and 8E frozen lifecycle YAML/launch handoff. Re-read the final reviewed Slice 7 schema, stage states, command adapters, and output identities before specifying these seams.
- **Never parallelize:** changes to Slice 7-owned lifecycle files, review fixes, or its documentation from the Slice 8 lane.

The remaining broad Slice 8 blockers are:

1. Accepted and integrated Slice 7 lifecycle contract for 8D Gate D4 and 8E only.
2. Historical S1 real-data/artifact acceptance and its separate integration decision before 8B/8C behavior changes.
3. Stable source/mirror evidence after reconciliation.
4. Owner approval of the policy values listed below.
5. Identification of an actual bounded date window with feature, game/player linkage, commence, injury, and quote coverage.

Current durable data finding:

- The November 2 bounded post-sync audit found current combined props for only 1 of 19 selected diagnostic games.
- Six selected games lacked `game_id_map` entries.
- The local mirror does not contain `raw_player_props_archive`; the remote archive is approximately 171M estimated rows and has no indexes.
- Quote clocks, injury history, final cohort, and decision-time policy remain unresolved.
- This is historical-readiness blockage, not evidence that the completed current-table transfer failed.

Do not launch source integration, archive recovery, training, replay, or a baseline merely because Slice 7 finishes. Finishing 8A is also evidence/policy readiness, not authorization for 8B–8E.

## Scope decomposition

Slice 8 must be delivered as these separately approved gates:

- **8A — Read-only source evidence and policy freeze**
- **8B — Shared historical feature/injury policy integration**
- **8C — Quote-clean direct/prefetch replay integration**
- **8D — Bounded real-data acceptance workflow**
- **8E — Frozen lifecycle baseline and Chase launch handoff**

A later gate may not start because an earlier plan exists. It starts only after the earlier gate’s evidence and owner decision are recorded.

## Global protected boundaries

Never alter in Slice 8 without a new, narrower approval:

- NBA model architecture or quantile model implementation.
- Monte Carlo, empirical CDF, copula, BL, calibration, Q10 behavior, or quantile ordering.
- `BacktestHarness._calculate_edges()` probability/math behavior.
- Existing flat-stake/chosen-side/one-bet-per-player mechanics from Slice 3.
- `BetSimulator.evaluate_predictions()`, `place_bet()`, or `_selected_quote()` unless a traced metadata compatibility defect requires a separate repair.
- Slice 5 sweep numerical owners or output schema.
- Slice 6 evidence semantics.
- Slice 7 lifecycle semantics.
- Advanced rolling-stat producers or derived-table rebuilds.
- Production inference/scheduler/Railway behavior.
- Database schema, indexes, migrations, remote data, archive recovery, relinking, or broad sync.
- Existing tests or fixtures. Current restart instructions allow existing suites and disposable probes, not new permanent unit tests, unless Chase separately authorizes them.

No source lane may overwrite or absorb active reconciliation/linker work. Recheck all worktrees and writers before each sub-slice.

---

# 8A — Read-only source evidence and policy freeze

## Evidence checkpoint — November 2, 2026

The bounded SELECT-only evidence pass and educational documentation are complete:

- `docs/operations/nba-s8-readiness-receipt-2026-11-02.md`
- `docs/operations/nba-s8-source-policy-matrix-2026-11-02.md`
- `docs/understanding/nba-historical-decision-policy.md`

The exact 19-game remote/local receipt is complete without archive access, mutation, tests, source edits, training or replay. It establishes identity parity but blocks cohort-wide quote, mapping, injury-edition and feature-source readiness. The policy matrix is awaiting Chase's explicit approval; no `configs/nba/historical_policy_v1.yaml` exists or is authorized yet.

## Goal

Produce one owner-approved, versioned policy contract backed by bounded current evidence. This is a documentation/SQL-runner lane, not behavior-changing implementation.

## Proposed outputs

Created as reviewable evidence:

- `docs/operations/nba-s8-source-policy-matrix-2026-11-02.md`
- `docs/operations/nba-s8-readiness-receipt-2026-11-02.md`

Create only after every required value or explicit blocked state is approved:

- `configs/nba/historical_policy_v1.yaml`

Do not modify the currently dirty `docs/operations/nba-core-product-restart-plan.md` from this lane. Add links later through its current owner or a coordinated narrow patch.

## Required SQL-runner evidence

All DB queries are SELECT-only, read-only, timeout-bounded, keyed, index-aware, and isolated from main context.

For an explicitly selected bounded set of games/dates, verify:

1. Game schedule identity, season type, commence time, home/away teams, and mapping completeness.
2. Player/team/game linkage for feature and quote rows.
3. Historical versus live quote provenance classes.
4. `snapshot_time`, market/book update times, insertion time, requested historical time, and commence ordering.
5. Null, duplicate, identical-repeat, and conflicting same-identity quote cases.
6. Compatible Over/Under identity availability by player/game/market/book/line/snapshot.
7. Injury `report_time` values, timezone evidence, retained editions, same-player status changes, insertion-vs-report order, and omitted-player ambiguity.
8. Required feature-family coverage and non-default variation through actual assembled features, not raw-row presence alone.
9. Opening/middle/end regular-season samples for candidate seasons; playoffs and play-in classified separately.
10. Exact current-table versus retained-archive coverage for selected IDs only. Do not scan the unindexed archive by date.

Before any archive query, inspect `EXPLAIN` and obtain owner approval for the exact bounded physical/ID probe. No archive copy or index is authorized.

## Owner decisions that must be explicit

The policy file cannot be created with placeholders. Chase must approve:

### Decision time

Choose one versioned, timezone-aware rule:

- game-relative T-minus; or
- explicit `America/New_York` slate times with defined early-game fallback/skip behavior.

No implicit 18:30 value and no fixed 23:30 UTC season-wide rule.

### Quote source interpretation

For each provenance class, record:

- source namespace/interpreter ID;
- availability clock and timezone;
- required cutoff evidence;
- whether historical requested time is accepted as a conservative upper bound;
- whether market/book update clocks are required;
- null-clock behavior;
- stale-quote threshold;
- duplicate/conflicting quote behavior;
- snapshot compatibility key construction;
- rejected/unknown behavior.

Live player-prop invocation-start time must not inherit the historical-request-time interpretation.

### Injury behavior

Record:

- parseable report-time format/timezone;
- edition/version interpretation;
- status eligibility cutoff;
- omitted-player meaning, if established;
- missing/ambiguous history behavior.

Allowed outcomes are blocked/unknown or a separately named diagnostic-only reduced-feature policy. Silent zero/latest fallback is not allowed for decision-grade historical runs.

### Cohort and split

Freeze:

- included seasons/date bounds;
- regular season/play-in/playoff treatment;
- late-season and abnormal-rotation rules;
- traded-player/team-transition handling;
- season-boundary/cold-start policy;
- target minimum minutes (currently five) versus prior-game rolling contribution rule (eight);
- training window;
- calibration window and exclusive cutoff;
- protected OOS window;
- independent validation window, if distinct.

No arbitrary last-X-games exclusion.

### Feature-family season/carryover behavior

For every required family, choose one:

- current-season only;
- prior-season carryover;
- carryover with explicit decay/indicator;
- neutral default with provenance;
- prediction-blocking when unavailable.

The same behavior must apply across training, date, date-range, replay, and scheduled inference where the same contract is intended. Slice 8 must not silently change current production inference; historical and production policy differences must be explicit and versioned.

### Missing commence/source behavior

Choose blocked/unknown versus a named diagnostic-only fallback. Never invent commence time from game date.

## 8A acceptance

- Every required field has an actual value or explicit blocked state.
- Evidence routes, query bounds, dates, and source freshness are recorded.
- Unknowns remain unknown.
- No policy was chosen by comparing retrospective ROI.
- No DB mutation, archive copy, rescrape, relink, or source edit occurred.
- Chase approves the exact versioned matrix.

---

# 8B — Shared historical feature and injury integration

## Goal

Make training, single-date, and date-range historical feature construction consume the same approved decision/source/cohort contract and produce matching ordered values, defaults, provenance, and completeness.

## Dependency gates

- 8A approved policy exists with no placeholders for the selected bounded acceptance window.
- Historical S1 fail-closed source behavior is accepted and its integration ownership is resolved.
- Slice 4 primitives remain byte/source verified on the chosen base.
- Required source rows exist for a bounded real acceptance sample.
- Slice 7 completion is not an 8B source dependency, but active-worktree collisions must be rechecked and Slice 7-owned lifecycle files remain protected.

## Proposed writable allowlist

Exact final scope must be re-traced after preceding integrations. Expected owners:

- Create `src/models/features/nba/historical_policy.py`
- Create `src/models/features/nba/historical_observation_adapters.py`
- Modify `src/models/features/nba/requests.py`
- Modify `src/models/features/nba/training_loader.py`
- Modify `src/models/features/nba/date_batch_loader.py`
- Modify `src/models/features/nba/date_range_loader.py`
- Modify `src/models/features/nba/line_sources.py`
- Modify `src/models/features/nba/injury_context.py`
- Modify `src/models/feature_store.py` only for explicit request/policy propagation
- Modify `src/models/train_pipeline.py` only for frozen policy/config identity and readiness propagation

Read/reuse without changing:

- `src/models/features/nba/observation_policy.py`
- `src/models/features/nba/quote_selection.py`
- `src/models/features/nba/contracts.py`
- `src/models/nba_artifacts.py`

Do not approve all listed modifications automatically. The implementation spec for 8B must narrow this list after tracing the final APIs and active worktrees.

## Design contract

### Historical policy object

`historical_policy.py` should represent only approved values from 8A and expose a canonical serializable identity. It must include:

- schema/policy version and stable policy ID;
- decision-time rule;
- cohort/split rule;
- quote source interpreter IDs;
- injury source interpreter ID;
- missing-source/commence behavior;
- per-feature-family season/carryover policy;
- target and rolling-history minutes rules;
- train/calibration/OOS bounds where applicable.

No policy default may silently make a run decision-grade. A missing policy is allowed only in preserved legacy/non-decision-grade compatibility paths.

### Source interpretation adapters

`historical_observation_adapters.py` converts actual source rows into the existing Slice 4 primitives:

- `ClockEvidence`
- `AvailabilityInterpretation`
- `ObservationDecision`

Adapters own source-specific meaning. The pure primitive must remain policy-independent.

Each adapter must:

- require an explicit source namespace/provenance class;
- preserve raw clocks and record IDs;
- return `UNKNOWN_SOURCE`, `MISSING_HISTORY`, or `AMBIGUOUS_HISTORY` when evidence cannot prove availability;
- never use insertion time as availability unless 8A explicitly approves that exact provenance class;
- never localize naive timestamps by guessing;
- preserve cutoff evidence checked against both decision and commence.

### Request propagation

Historical feature requests must carry or resolve one exact policy identity. Date/date-range/training paths cannot each invent their own cutoff.

Backward-compatible legacy calls may remain available, but they must be labeled non-decision-grade and must not be reachable from the Slice 7/8 baseline lifecycle.

### Feature-line behavior

Current `line_sources.py` uses date-level cutoffs, numeric `MAX()` aggregation, and zero defaults. Decision-grade historical behavior must instead:

- use the approved per-game decision context;
- select only eligible observations;
- preserve exact source/provenance and rejection reasons;
- apply the approved feature-family missingness policy;
- preserve existing feature names/order and spread orientation;
- distinguish model-input feature lines from downstream executable bet quotes.

Do not remove line features or change the artifact feature schema in this slice unless a separate artifact/architecture decision approves it.

### Injury behavior

Current injury paths select same-date rows and frequently use descending ID plus zero defaults. Decision-grade historical behavior must:

- use the approved injury adapter and decision cutoff;
- preserve report/source evidence and completeness;
- avoid latest-ID-as-edition assumptions;
- treat ambiguous/missing history according to the explicit policy;
- keep the four same-position injury fields and all existing ordered feature keys;
- keep prior-player rolling joins strictly prior to target game;
- keep target eligibility and rolling contribution rules distinct.

### Loader parity and fail-closed behavior

For the same sampled player/game/policy, training, date, and date-range paths must agree on:

- ordered feature membership;
- values;
- defaults/missingness;
- spread direction;
- injury and line provenance;
- decision and commence context;
- completeness/readiness;
- season/carryover policy.

Do not mechanically change every rolling `<= game_date` predicate to `< game_date`; those tables may already be shifted. Any producer semantics change is a separate owner-gated slice.

Chunk/query failures must be structured and propagated for decision-grade execution. They must not silently become indistinguishable empty dates. Legitimate empty slate/no-target populations remain explicit successful no-op outcomes.

## 8B validation

No new permanent tests unless separately approved. Use:

- Existing feature inventory/contract/transform/mode/source/injury suites.
- Scoped Ruff and compile checks.
- Disposable controlled probes for malformed/missing/ambiguous observation evidence.
- A bounded real local parity probe for explicitly selected player/game/date records.
- Before/after feature-frame identity comparison for unaffected legacy paths.
- Artifact ordered-feature compatibility check.

Required existing suites include, after confirming exact current filenames:

```text
tests/test_nba_feature_store_inventory.py
tests/test_nba_feature_contracts.py
tests/test_nba_feature_transforms.py
tests/test_nba_inference_loader.py
tests/test_nba_line_sources.py
tests/test_nba_source_boundaries.py
tests/test_nba_mode_loaders.py
tests/test_nba_injury_context.py
```

8B acceptance proves feature policy/parity mechanics on the bounded sample. It does not prove quote-clean betting replay or model quality.

---

# 8C — Quote-clean direct and prefetched replay integration

## Goal

Make direct and prefetched NBA replay consume one approved decision/commence/source interpretation and one coherent paired-quote contract, with exact rejection/dropout evidence.

## Dependency gates

- 8B accepted and integrated.
- Slice 3 replay behavior preserved.
- Slice 6 audit contract available for saved evidence.
- Actual bounded quote rows selected by 8A exist.
- Slice 7 completion is not an 8C harness dependency, but 8C must not change lifecycle files or claim end-to-end lifecycle acceptance before the reviewed Slice 7 contract is frozen.

## Proposed writable allowlist

- Create `src/backtesting/nba/historical_quote_adapter.py`
- Modify `src/backtesting/backtest_harness.py` only in:
  - `_get_lines_for_date`
  - `_prefetch_all_lines`
  - the narrow acquisition/policy threading seams in `run` and `_run_date`
- Modify `src/backtesting/run_backtest.py` only for explicit policy/config validation and accepted-output metadata
- Modify `src/models/features/nba/requests.py` only if 8B did not complete the required context shape
- Narrow documentation additions in a new Slice 8 evidence page

Protected methods:

- `_calculate_edges()` numerical behavior
- `_filter_best_bets()` side/selection behavior unless the adapter output shape requires a separately reviewed compatibility seam
- combo sample derivation
- simulator placement/settlement/sizing
- Slice 5 sweep owners

## Shared quote adapter

`historical_quote_adapter.py` maps DB rows to the existing Slice 4 contracts:

- `QuoteInput`
- `ObservationDecision`
- `QuoteSelectionResult`

It must preserve opaque IDs exactly and own the one explicit, reviewed normalization required for legacy padded/unpadded game IDs. Direct and prefetch paths must call the same normalization seam; do not retain one SQL-only `LPAD` behavior and one exact-ID behavior.

Every raw row must retain:

- source namespace and immutable source record ID;
- game/player/market/period/variant/book/line;
- side and price;
- snapshot compatibility key;
- raw clock evidence;
- interpreted availability/provenance;
- rejection reasons.

Pairing requires compatible Over/Under sides from the same player/game/market/book/line/source/snapshot identity. Independently ranking the latest Over and Under is prohibited.

### Query behavior

Both direct and prefetch acquisition must:

- fetch a bounded candidate set using indexed game/date/market/book predicates;
- include all source clock and provenance fields needed by the approved interpreter;
- include authoritative commence evidence;
- avoid applying an unreviewed fixed 23:30 UTC cutoff in SQL;
- perform the same reviewed eligibility/pairing contract;
- produce identical keyed eligible rows, selected rows, and ledger/rejection evidence for the same games.

SQL should bound candidate volume but must not discard evidence needed to classify ambiguity/conflicts.

### Failure and no-op behavior

- Query/loader/inference exceptions propagate as execution failure for decision-grade runs.
- Missing quotes produce a measured no-eligible-quote/dropout outcome, not model-no-edge evidence.
- Unknown clocks/commence/source produce blocked/unknown policy results.
- Legitimate no-game/no-feature/no-prediction cases remain explicit no-op categories.
- No partial success-shaped aggregate is published after a fatal date/chunk failure.

### Saved evidence

Persist through replay/sweep outputs:

- expected/acquired dates and outcomes;
- raw candidate count;
- eligible pair count;
- selected prediction/bet count;
- rejection/dropout reason counts and row-level evidence within bounded output policy;
- selected decision/commence/source clocks;
- source record IDs and policy/interpreter IDs;
- pair/snapshot compatibility identity;
- exact artifact/config/window/policy identity;
- direct/prefetch parity receipt.

8C must use the existing Slice 5 schema-extension rules and accepted Slice 6 evidence adapter. Do not create an incompatible second evidence format.

## 8C validation

Use existing replay/harness/simulator suites and disposable probes. Do not add permanent tests without approval.

Required controlled cases:

1. Quote immediately before decision: eligible.
2. Quote exactly at/after decision: rejected.
3. Quote exactly at/after commence: rejected.
4. Missing/naive/ambiguous clock: blocked/rejected per policy.
5. Compatible paired sides: accepted.
6. Missing counterpart: rejected.
7. Identical duplicate: deduplicated with provenance.
8. Conflicting duplicate/top rank: ambiguous, not silently selected.
9. Padded/unpadded game IDs: one canonical reviewed mapping.
10. Direct and prefetch paths return equal rows/ledger for the same bounded input.
11. Query/chunk failure exits non-zero and produces no complete result.
12. Missing quote is distinct from below-edge and from model failure.

Run bounded real local parity on the approved 8A window after controlled cases pass.

---

# 8D — Bounded real-data acceptance workflow

## Goal

Exercise the integrated real path in increasing, owner-approved bounds before any long training or baseline run.

## Proposed durable outputs

Create after 8B/8C approval:

- `scripts/verify_nba_historical_readiness.py`
- `docs/operations/nba-s8-real-data-acceptance-2026-11-02.md`
- machine-readable JSON/CSV receipts under an explicitly supplied output directory

The script is a verifier/orchestrator, not a data repair tool. Default behavior is dry-run/read-only. It must not contain DB credentials or inline SQL that bypasses project owners.

## CLI modes

Proposed shape:

```text
python scripts/verify_nba_historical_readiness.py --policy <approved yaml> --artifact <exact bundle> --start YYYY-MM-DD --end YYYY-MM-DD --local --dry-run
python scripts/verify_nba_historical_readiness.py --policy <approved yaml> --artifact <exact bundle> --start YYYY-MM-DD --end YYYY-MM-DD --local --max-games N --max-players N --output-dir <new dir>
```

Exact flags require implementation review. The verifier must:

- require an exact nonlegacy manifested artifact;
- require the approved policy identity;
- require explicit dates and local DB target;
- enforce small caps;
- refuse production/remote mutation;
- emit expected/acquired/skipped/blocked/failed counts;
- write incomplete-first and publish a complete receipt only after verification;
- call actual feature/model/quote owners rather than reimplementing them.

## Verification ladder

### Gate D0 — DB-free/static

- Existing scoped tests.
- Artifact validation without deserialization.
- Slice 5 sweep dry-run.
- Slice 6 audit help/controlled bundle.
- Slice 7 lifecycle dry-run/status.
- No engine/model/output creation from planning/status paths.

### Gate D1 — read-only source readiness

Use SQL-runner receipts from 8A and independently verify counts before any DB-adjacent action. No broad scans.

### Gate D2 — one-day real feature/model/quote smoke

On one approved date:

- assemble actual features;
- verify ordered feature identity;
- load exact artifact;
- run canonical no-global-offset prediction for capped players;
- require finite empirical samples for requested stats;
- select quotes through approved policy;
- emit rejection reasons;
- run one raw/no-BL flat-stake cell.

Pass proves bounded mechanics, not profitability.

### Gate D3 — three-to-seven-day real replay

- one shared acquisition across the explicit window;
- complete date accounting;
- direct/prefetch feature and quote parity;
- one raw/no-BL flat cell first;
- actual/void/settlement checks;
- optional single BL comparison only after raw parity;
- unchanged shared-input identities across cells;
- no Gaussian/0.5 fallback;
- no partial aggregate after failure.

### Gate D4 — complete bounded lifecycle

After Slices 6–7 are integrated:

- attach exact artifact for first acceptance; do not train;
- run bounded sweep;
- run evidence audit;
- run report-only lifecycle decision;
- mutate disposable copied evidence to prove audit failure and downstream resume invalidation;
- change policy/artifact/source receipt and prove invalidation.

Expected posture remains historically bounded and live-blocked until owner review.

## 8D acceptance

- A durable command reproduces each bounded gate.
- Actual outputs identify source freshness and policy/artifact identities.
- Controlled and real-data evidence are labeled separately.
- All missing source/quote/timing populations are measurable, not silently dropped.
- No DB writes, source repair, rescrape, or long jobs occurred.

---

# 8E — Frozen baseline and Chase launch handoff

## Goal

Freeze one exact meaningful baseline only after 8A–8D pass. Chase launches it; agents prepare and dry-run only.

## Baseline configuration

Create a reviewed lifecycle YAML under a new, exact path such as:

- `configs/nba/lifecycle/baseline_<approved-window>.yaml`

The final file must contain actual values for:

- exact source/repository identity;
- exact approved policy identity;
- exact manifested artifact attachment for replay acceptance, or exact training output root for a later approved training run;
- train seasons;
- calibration season and exclusive cutoff;
- protected OOS start/end;
- independent validation window if required;
- stats and bookmakers;
- sample count and seed;
- target/rolling minimum rules;
- flat stake and bankroll;
- raw/no-BL control;
- only a small preregistered comparison grid, if approved;
- exact local DB target;
- exact new output root;
- readiness-receipt identity.

No placeholder, “latest,” inferred date, or artifact discovery is allowed.

## Launch sequence

1. Agent validates config, source identity, artifact/policy identity, and output collisions with dry-run.
2. Agent gives Chase one PowerShell-safe launch command.
3. Chase launches training if the approved purpose requires a new artifact.
4. Exact produced artifact is validated and loaded through the canonical path.
5. Chase launches protected-OOS lifecycle replay/audit.
6. Agent reviews persisted evidence and returns Confirm/Shelf/Exclude plus explicit deploy posture.

## Initial evaluation rules

- Start raw/no-BL and flat-stake.
- Preserve empirical CDF and Q10 edge behavior.
- Report calibration/model diagnostics without global offset correction.
- Report bets, volume, hit rate, profit, ROI, Sharpe, max drawdown, side/stat/book concentration, timing measurability, dropout/rejection reasons, and date/game dependence.
- Treat small volume as exploratory; no headline metric overrides integrity/timing failure.
- BL search is finalist-only.
- Kelly/ranker/staking remains separate and optional.
- Historical acceptance does not authorize customer publication, paper/live promotion, deployment, or production DB changes.

## 8E acceptance

- Chase launched the exact frozen command.
- The run is complete, identity-bound, and reproducible.
- Train/calibration data do not overlap protected OOS.
- Feature/quote/source policy identities match the approved contract.
- Audit/lifecycle evidence is complete and reviewed.
- Owner records the historical acceptance decision separately from publication/promotion.

---

# Proposed implementation order and ownership

1. Continue Slice 7 review in its existing isolated lane; do not modify or duplicate its work from Slice 8.
2. In parallel, complete 8A read-only evidence collection, bounded-window discovery, and the owner policy-decision packet.
3. Complete historical S1 real-data acceptance and owner integration decision.
4. After 8A/S1/source gates close, re-trace and separately approve 8B as one reviewed feature/injury slice.
5. After 8B integration, re-trace and separately approve 8C as one reviewed replay/quote slice.
6. After Slice 7 is accepted and integrated, reconcile its final lifecycle contract into the 8D Gate D4 and 8E specifications.
7. Implement and dry-run the 8D verifier after 8B/8C integration.
8. Run bounded real gates with Chase approval.
9. Freeze 8E config/commands.
10. Chase launches the baseline.

Do not parallelize 8B and 8C against unfinished APIs. Do not let the 8D verifier become a second feature loader, predictor, quote selector, simulator, or auditor.

# Validation commands after implementation

Exact commands must be refreshed against the integrated base. Expected existing suites:

```text
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -m pytest tests/test_nba_feature_store_inventory.py tests/test_nba_feature_contracts.py tests/test_nba_feature_transforms.py tests/test_nba_inference_loader.py tests/test_nba_line_sources.py tests/test_nba_source_boundaries.py tests/test_nba_mode_loaders.py tests/test_nba_injury_context.py -q
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -m pytest tests/test_run_backtest.py tests/test_backtest_harness.py tests/test_bet_simulator.py tests/test_run_sweep.py tests/test_train_pipeline.py -q
```

Each sub-slice also requires:

- scoped Ruff on edited Python files;
- scoped compile checks;
- `git diff --check` on the exact allowlist;
- controlled failure probes;
- bounded real local consumer-path validation where approved;
- exact scoped diff review including full review of new untracked files.

No new permanent tests or fixture edits are included in this scope. If existing tests encode an incompatible historical behavior, report the exact assertion and request approval rather than weakening it.

# Acceptance definition for Slice 8

Slice 8 is complete only when:

- Source/mirror readiness is sufficient for the frozen bounded/full windows.
- Chase approved the versioned decision, quote, injury, cohort, split, carryover, and missing-source policies.
- Training/date/date-range loaders produce equivalent ordered historical features and provenance.
- Direct/prefetch replay uses one coherent temporally eligible quote-pair contract and matches on bounded real data.
- Fatal execution failures cannot publish partial success.
- The bounded feature → artifact → prediction → quote → sweep → audit → lifecycle path succeeds with exact identities.
- A frozen meaningful baseline command exists and Chase launched it.
- Protected-OOS evidence is reviewed with raw/no-BL flat-first risk and timing/dropout evidence.
- Historical acceptance and publication/promotion decisions remain separate.

Slice 8 is not complete merely because code merges, a lifecycle directory exists, or one profitable backtest finishes.

# Relevant prior lessons and invariants

- `operations/critical-invariants` and `operations/hard-facts`: empirical CDF, no global conformal offsets, preserve Q10 behavior and quantile order, temporal integrity, SQL isolation, and owner-launched long jobs.
- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: source columns are not enough; prove consumer query and artifact behavior.
- `lessons/large-odds-table-audits-must-be-keyed-and-chunked`: inspect keys/indexes, bound exact inputs, and treat timeout as unknown rather than zero.
- `lessons/quote-clean-clv-before-feature-work`: timing/source/dropout evidence precedes model-quality interpretation.
- `lessons/implausibly-profitable-backtests-are-methodology-red-flags`: integrity and overlap diagnostics outrank headline ROI.
- `lessons/cheap-baseline-before-architecture`: preserve minutes × rate and validate the smallest raw/no-BL flat path before BL, ranker, Kelly, or redesign.
- Feature selection is not an ablation or promotion gate.
- Engineering OS remains paused and is not a Slice 8 dependency.

# Ready-for-next-decision

The 8A evidence packet and wiki are complete. No 8B–8E implementation writer should launch yet. The next actionable decision is Chase's review of `docs/operations/nba-s8-source-policy-matrix-2026-11-02.md`; unresolved fields remain explicitly blocked. Lifecycle integration remains gated on the final accepted Slice 7 contract.