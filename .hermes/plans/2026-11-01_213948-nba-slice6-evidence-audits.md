# NBA coding slice 6 — evidence audits and evaluation parity implementation plan

> **For Hermes:** Use the GameFlow implementation-worker lane and subagent-driven-development review gates after Chase approves this concrete scope. Independent specification review must pass before quality acceptance. This is a planning-only handoff, not permission to launch a writer.

**Goal:** Audit saved NBA shared-input sweep evidence without querying a database, loading models, regenerating predictions or treating diagnostic evidence as promotion-grade.

**Architecture:** Add a version-aware, artifact-only reader/verifier and CLI in `src/backtesting/nba/evidence.py`. Verify the existing slice-5 output contract independently, then derive descriptive concentration, saved-evidence coverage and available quote-ordering diagnostics. Reuse canonical byte/hash helpers with a data-only saved-plan adapter; do not introduce another replay, simulator, probability engine or MLB SQL adapter.

**Tech stack:** Existing Python standard library and pandas, plus existing pure NBA artifact/config helpers. No installation or infrastructure.

## Status and decision requested

### Owner authorization and verified launch base — November 1, 2026

Chase's subsequent request, "complete slice 6", authorizes implementation of this reviewed scope and its controlled offline validation/review. The old planning-only/waiting text below is historical and superseded for this lane. Commit, merge, push, real historical runs and deployment remain separately gated.

Before launch, main advanced to `3aa73a5fbed0ebd7a3974f1e0f76793a58890c27`; merge `a44b09a` actually integrated the predecessor source. Tracked package/validator files and config/runner/validator hashes were independently verified against the reviewed bytes below. Successor worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-evidence-audits`, branch `repair/nba-evidence-audits`, created at that exact main SHA. No inherited source copies or model data are required. Separate S1/reconciliation writers remain protected. Coordinator owns documentation; implementation worker may write only `src/backtesting/nba/evidence.py` and disposable scratch probes/receipts.

The Spark availability canary was rejected with a deterministic unsupported-model HTTP 400, without implementation effects. The existing `gpt-6.1-sol-900k` / `openai-codex` route passed a tool-free canary and is selected with explicit per-launch overrides; no profile/provider configuration was changed.

### Confirmation-check scope correction — November 2, 2026

Chase explicitly approved the narrow lazy-import initializer fix via the clarification form. `src/backtesting/__init__.py` is additionally writable solely to preserve the same five public exports through lazy loading; no harness/simulator/metrics/model changes are authorized. This supersedes its prior protected status. Validate import-effect isolation plus original export object identity and unknown-attribute behavior.

Source tracing also corrects two saved-format assumptions without changing producer behavior: `BetSimulator.evaluate_predictions` calls `_selected_quote` and assigns `bet.bookmaker=bet.selected_bookmaker`. Its selected metadata projection prefers a non-null side-specific value, then same-side selected metadata, then its existing default (row bookmaker or bet decision time where applicable). The auditor must verify that exact recorded transformation, not reject a valid blank-selected-to-populated-bet fallback. This supersedes literal selected-to-bet copying/base-bookmaker-null assumptions below; missing unobserved clocks remain blocked. Positive saved flat stakes may be below the configured flat amount because the unchanged simulator caps stakes at available bankroll; reject above-flat/nonpositive stakes, but do not claim an independently proven reason for a smaller stake without replaying bankroll/settlement.

The old worker acceptance counts are provisional and superseded by confirmation runs against these corrections. Interrupted specification review produced no verdict; final spec review and subsequent quality review are still required.

### Owner-approved simpler output checks — November 2, 2026

Chase requested "Please complete the simpler version" after explicitly accepting detection rather than race prevention. This supersedes the subsequent SPEC-1 no-write-before-detection requirement and the attempted Windows native locking implementation. Remove the native directory-lock/handle/rename machinery. Use ordinary path containment/identity checkpoints, the existing exclusive incomplete-directory/non-replacing publication helpers, report readback and final source fingerprints.

Detected `output_path_changed` or `source_changed_during_audit` must be explicit, return nonzero and withhold a successfully completed publication. Keep failed output under the incomplete name; do not write a corrective receipt into a path now redirected into protected evidence. Existing-output collisions and initially unsafe destinations remain rejected. Document that observations do not lock the filesystem: a concurrent replacement can cause writes before detection, and a transient change between checkpoints may escape detection. This local owner-run tool is not a hostile concurrent-mutation security boundary. No platform-specific lock requirement is retained.

The other independent review findings remain acceptance criteria: preserve measured timing FAIL over missing-observation BLOCKED; validate inert typed payloads against the actual producer grammar; verify reconstructable component commitments; enforce base/selected bet bookmaker equality. Historical/model/promotion claims, source/test protections and the separate spec-before-quality review gates are unchanged.

### Controlled implementation acceptance — November 2, 2026

The simpler scope is complete: independent **SPEC PASS**, then **QUALITY APPROVED**, both read back against `evidence.py` SHA-256 `99ef8ddebe3ef52d91b064771df465db2b37f92d73b57c40ce5f86bc4aa1dfea` and initializer SHA-256 `f3d23431d073d7061bbe6b09cd4ebfec80c7e726f1e2d61bfb0fe7fd3e1d539d`. Final scoped regression suite: 68 passed. Original 37 API/nine CLI checks, the 81-case non-export matrix and seven simpler-export checks pass; protected source/test hashes verified unchanged. See `docs/operations/nba-evidence-slice6-2026-11-01.md` for exact receipts and limitations. No source commit, integration, push, deployment, real-data/model acceptance or successor execution is implied. Earlier planning/checkpoint text below is historical.

- Scope prepared November 1, 2026, EST. Implementation has NOT started.
- Independent corrected-plan re-review: **READY FOR OWNER APPROVAL**. All five initial scoping findings are closed in the normative v1 contract below. Review receipt: `C:/Users/Chase/AppData/Local/hermes/cache/delegation/live/deleg_2795b7f2/task-0.log`. This is plan readiness, not implementation acceptance or authorization.
- Requested decision: approve the exact source/documentation allowlist and behavioral choices below, then choose an integration base or explicitly authorize isolated development against the reviewed predecessor snapshot.
- Main currently points to `3ad12033aea05c59e1bb229abeb13b893ed5107c`; it is clean relative to the cached origin/main ref. No fetch was performed.
- Despite its subject, `3ad1203` changes seven documentation files only. Neither `src/backtesting/nba/` nor `src/models/nba_artifacts.py` is tracked/present in main. Do not call slice 5 source-integrated.
- Slice 5 is reviewed and controlled-path verified according to its delivery record, but its source is uncommitted in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-shared-input-sweep`, branch `repair/nba-shared-input-sweep`, HEAD `10d9c2258f9a26eb3ff5ef424cbeb66cd3093ff3`.
- Slice-5 config and runner bytes still match their final reviewed delivery hashes (recorded below). This source availability supports a concrete successor scope, not automatic permission to copy or integrate it.
- Main's slice-3 integration is already complete. Historical S1 remains separately owned/deferred. Sync/reconciliation has a separate writer; none of those lanes is part of this implementation.

## Launch gates

1. Chase approves this concrete scope. The request to prepare slice 6 does not authorize implementation yet.
2. Choose one explicit base:
   - Preferred: the necessary final slice-5 package and `nba_artifacts.py` are owner-integrated into the successor's base; record exact source identity and reconciliation receipt.
   - Alternative, only with explicit approval: create an isolated successor worktree and copy only required reviewed predecessor files as inherited read-only prerequisites. Preserve original bytes, origin, HEAD, SHA-256 and scoped diffs; check for drift during/after copying. This is development, not predecessor integration acceptance.
3. Revalidate registered worktrees, active writers, scoped status/diffs and exact untracked files. No writer in another owner's checkout. Never overwrite whole older replay/training files into main.
4. Resolve the final source contract against that base. If the emitted schema changes from this plan, update the adapter/acceptance contract before launch; do not silently support a guessed schema.
5. Establish the successor's own existing-test baseline and DB/network/model/output tripwires. Predecessor green counts are not slice-6 evidence.

Do not require sync completion or historical S1 integration for controlled artifact-only work. Real historical audits and model acceptance remain separately gated. Do not schedule an automatic waiting worker.

## Observed ownership/worktree boundaries

Read-only live inspection found:

- `repair/nba-cli-preflight`: modified replay/training CLI and existing fixtures, untracked `training_preflight.py` / `nba_artifacts.py`. Separate predecessor ownership.
- `repair/nba-shared-input-sweep`: modified inherited harness/simulator, untracked four-file NBA sweep package and artifact validator, plus docs. Do not classify all those files as slice-5 authored changes.
- `repair/nba-s1-fail-closed`: changes to harness and NBA date-range loader. Protected.
- `nba-reconciliation` detached worktree: untracked reconciliation script/runbook. Protected.
- Flat-replay and observation-primitives worktrees were clean; no extra commits were listed by `main..HEAD` for registered branches at inspection.
- Four existing stashes were inspected by name only; none is an implicit integration backlog.

## Proposed writable allowlist after approval

Source:

- Create `src/backtesting/nba/evidence.py` — reader, typed results, version-1 artifact adapter, descriptive analyzers, parser and `main(argv=None)`; executable with `python -m src.backtesting.nba.evidence`.

Coordinator-owned documentation:

- Create `docs/understanding/nba-evidence-audits.md`.
- Create `docs/operations/nba-evidence-slice6-2026-11-01.md` for approved decisions and actual evidence.
- Narrow status/link additions only in `docs/operations/nba-coding-readiness-plan-2026-11-01.md` and `docs/understanding/nba-training-evaluation-slices.md`.

This planning turn writes only this plan under `.hermes/plans/`. No project source/docs have been edited by this lane.

Read/reuse, do not modify:

- Predecessor `src/backtesting/nba/sweep_config.py`: read-only version-1 format/identity reference. Do not construct `SweepConfig` or call its validators while reading archived evidence: those resolve original paths.
- `src/models/nba_artifacts.py`: `canonical_sha256`, `sha256_file`, and existing exclusive-output helpers where applicable. Importing this module is stdlib-only in inspected source.
- Predecessor `sweep_runner.py` and `run_nba_sweep.py` as format references, not runtime acquisition/evaluation dependencies.
- `src/backtesting/bet_simulator.py` and `performance_metrics.py` for saved-field/metric semantics, not a second execution.

Protected: all predecessor source, harness `run`/`_run_date`, quote/observation selectors and consumers, SQL, feature loaders, train/inference/calibration/math, sync/reconciliation, MLB scripts/source, all existing tests and fixtures, production artifacts, lifecycle slice 7. No permanent tests or smoke scripts. Stop for scope expansion if a producer change is required.

## Verified predecessor output contract

Inspected source: slice-5 `sweep_runner.py:37-91,508-625,627-718`, `sweep_config.py:122-210,237-322`.

Root:

- `sweep_manifest.json`, schema version 1; `state=complete` required for accepted input.
- `sweep_summary.csv`, exactly one row per planned cell.
- `cells/<cell_id>/config.json`, `metrics.json`, `predictions.csv`, `bets.csv`, `all_bookmaker_edges.csv`.

Manifest records:

- `plan`: fixed config, artifact inspection, predictor settings, ordered features, dependencies/source receipt, source policy, cells/count, acquisition settings/identity and request identity.
- `completed_cells`: unique cell IDs, input/schema/effective-config identities and exact five-file receipts. CSV receipts include SHA-256, row count, ordered columns; JSON receipts include SHA-256.
- `input_identity`, expected/acquired dates, one observed date outcome/count record per expected calendar day, output schema contract, input-component digests and sample inventory.
- `outputs['sweep_summary.csv']` receipt, encoding declaration and diagnostic readiness.
- `historical_readiness=unverified`, `decision_grade=false`; unresolved timing/source policy/loader completeness/query ordering.

Important semantics:

- `predictions.csv` contains SELECTED predictions, not the entire acquisition population. `all_bookmaker_edges.csv` is the pre-selection candidate frame, including no-quote placeholders and sample rejection evidence. Do not use candidate row count as a player/opportunity denominator.
- Candidate quote rows can legitimately multiply a `(player_id, game_id, stat)` opportunity by bookmakers/lines. Exact keys must not be padded, coerced to integers or silently deduplicated.
- Expected dates include every calendar day. `no_game` is the incumbent game-date loader's observation, not independent proof that no games existed.
- Required CSV columns plus sorted observed extensions are recorded in `output_schemas`; collisions use declared `prediction__` / `quote__` namespaces. Unknown extensions must remain evidence, not executable objects.
- Nested CSV mapping/list/array values are typed JSON from `typed_value`; nonfinite JSON values have explicit tags and CSV NaNs are blank. Do not `eval`, deserialize pickle/joblib or reverse arbitrary objects.
- Input digests commit to raw frames/sample bytes/RNG state that are NOT fully persisted. A saved-output audit cannot reconstruct `input_identity`, rerun empirical probabilities or certify original acquisition completeness. It can check format, cross-reference equality and output commitments only. Hashes are consistency commitments, not signatures or proof of honest execution.

## Existing-analyzer inventory and reuse decision

Bounded file-only inventory inspected `scripts/analyze_mlb_clv.py` and `scripts/audit_mlb_quote_clean_dropout.py`.

- MLB CLV supports an artifact path with `--bets-csv` AND `--snapshots-csv`, otherwise queries MLB tables. Its markets, excluded books and decision/report semantics remain MLB-bound.
- MLB dropout CLI regenerates predictions and queries MLB data even when given saved sweep/bet paths. It is NOT an NBA artifact-only adapter.
- Pure helper patterns include CLV quote ordering/matching, saved-column validation, dropout buckets and timing-violation counting. Do not import their CLIs or copy their whole implementations.
- Slice 5 does not persist closing-snapshot histories or a fully observed historical opportunity universe. Therefore this bounded slice will NOT implement CLV matching, +15/+30/+60 stability or source-verified quote-clean dropout. Those capabilities explicitly report BLOCKED/UNSUPPORTED with required evidence. Do not synthesize closes, bet timestamps or dropout classifications to make them pass.
- Concentration does not need an MLB adapter: implement transparent grouped counts/stake shares directly from saved NBA bets. No policy thresholds or automatic betting recommendations.

## Behavioral contract for approval

### 1. Artifact-only read boundary

Proposed library APIs (new, not claimed to exist): `inspect_sweep(path)`, `audit_sweep(path, checks=...)`, `build_arg_parser()`, `main(argv=None)`.

- Accept one explicit saved sweep directory. Do not discover latest runs, glob model artifacts or follow model paths from receipts.
- Inspect only the root manifest and allowlisted relative output paths. Reject absolute/traversal cell IDs, duplicate IDs/JSON keys/CSV headers, symlink/junction escapes and missing/non-regular required files. Extra entries inside `cells/` or an individual cell FAIL layout validation; unrelated regular root files are listed as ignored without reading them and do not fail. Root links/special files are not followed and are listed as ignored; required paths must still be regular contained files.
- Detect `_incomplete`, failed/incomplete state, active cell/failure residue, unknown schema version, malformed/nonfinite control metadata and unsupported encoding. Such inputs cannot yield accepted integrity.
- Strict JSON parsing; identifiers read lexically with explicit numeric/date conversion only for declared fields. A string ID with leading zeroes remains that string. Boolean/readiness metadata is typed, never Python truthiness of a CSV string.
- No engine, network, subprocess child dispatch, model deserialization, predictor, simulator or artifact revalidation. Reading/validating a bundle does not resolve or access recorded original source/model paths. Historical relocated bundles remain inspectable.
- Import only pure canonical hash/file helpers from `nba_artifacts.py`. Do not import/construct `SweepConfig`, call `validate_plan`/`build_plan`, or import `sweep_runner`: inspected constructors/validators resolve original paths. Implement a data-only version-1 saved-plan adapter in `evidence.py`; validate explicit field types/domains, declared list uniqueness, derived market-stat/cell-grid membership and recorded JSON commitments without source/model access. This independent format validator is not a replacement runtime preflight or replay owner. Ordered combo definitions may be imported only from the existing pure `src/config/combo_config.py` after its import boundary is verified.
- First-version fixed resource limits: 16 MiB per JSON file, 1 GiB per CSV file, 1,000,000 data rows per CSV and 64 cells. No silent override or truncation; exceeding a limit is BLOCKED/resource_limit. Stream byte/hash/CSV accounting and analyze one cell at a time; no broad generated-directory scan. These are parser containment limits, not approved historical run sizes.

### 2. Independent output integrity

- Recompute actual file byte hashes, ordered headers and row counts; compare receipts AND version-1 minimum schema, not just a receipt derived from the same malformed file. Frozen v1 adapter field requirements are format-validation rules, not duplicate producer/simulator math. Preserve the producer's declared deterministic extension schema.
- Recompute request/acquisition/cell/schema/effective-config JSON commitments with existing canonical helpers wherever reconstructable from saved values. Validate planned/completed/summary cell-set equality with uniqueness; never accept subsets or duplicate-to-count tricks.
- Require exact version-1 effective-config keys and the provenance categories in the normative adapter contract below. Bind fixed/cell settings, input/request/schema identities, flat sizing, allowed directions, full BL settings and readiness across plan, config, metrics-embedded config, receipts and summary. Do not call the producer's `_effective_config` with fabricated shared/runtime objects or claim that consistent saved metadata proves those settings actually reached execution.
- Confirm summary counts equal actual candidate/selected/bet rows and `metrics['betting']['total_bets']`; compare saved betting/risk/calibration fields to `metrics.json` and its `metrics_json` summary representation. Handle explicit nonfinite tags deliberately; missing is not zero. Do not claim recomputed profitability, Sharpe, calibration or correctness of settlement.
- Reconcile exact inclusive expected date set, unique observed records, acquired-date subset, allowed date outcomes and nonnegative integer counts. Cross-check those counts with candidate/selected evidence only where the persisted semantics support the comparison; raw acquisition prediction count is not selected count.
- Verify selected/bet key/side/book/line/price correspondence by the occurrence-consuming multiset contract below, without rerunning selection or choosing a new tie-break. Never collapse repeated source rows or allow one occurrence to justify multiple descendants.
- When ambiguity prevents a proof, report an explicit BLOCKED relationship rather than forge exact linkage. Contradictory rows or impossible counts FAIL.
- Hash the exact bytes parsed. Record resolved contained path, file identity where available, byte size, mtime and SHA-256 for each allowlisted input; re-stat/re-hash all of them and recheck directory membership before report finalization. Any observed replacement/change yields BLOCKED/source_changed_during_audit. This establishes observed read stability only, not a lock, immutable snapshot or proof that no transient mutation occurred. The auditor never writes input files.

### 3. Descriptive concentration

For each cell, produce groups by side, stat, bookmaker, game and player, using exact saved identities and an explicit missing bucket.

- Report bet count, sum of finite valid stakes, count share and stake share, with the cell denominator stored beside each result. Zero denominator => null share plus empty reason, not passing diversification.
- Reject nonfinite/nonpositive actionable stakes as invalid evidence; do not discard them to improve a summary.
- No best-cell ranking, concentration threshold, bankroll recommendation, BL/Kelly tuning or promotion. Grouped profit may only be labeled saved-result descriptive evidence, not a new gate; not required for this first scope.

### 4. Saved-evidence coverage and exclusions

- Report separate populations: manifest acquisition counts, distinct saved candidate opportunities, candidate quote rows, selected rows and placed bets. Explain differing denominators in every summary.
- Summarize date reasons (`no_game`, `no_features`, `filtered`, `no_predictions`, `no_quotes`, `acquired`) and saved `sample_status`, `quote_status`, combo rejection reasons. Blank/unsupported reasons are unknown, not inferred no-edge or bad-source labels.
- Show observed candidate-to-selected and selected-to-bet counts/ratios only where keys/multiplicity support them. Lost-at-stage reasons not persisted remain `unmeasurable`.
- Do not relabel this as a full quote-clean dropout audit. It does not measure absent raw props, excluded books, missing history or unobserved opportunities from the original database.

### 5. Available timing checks and unsupported capabilities

- Inspect selected quote timestamp and selected decision timestamp only where both are explicit, parseable and timezone-aware. Use paired outcome evidence when actually supplied; unknown source-clock mapping remains a separate readiness blocker.
- `selected_snapshot_time <= selected_decision_time` is a saved-row ordering check, not source-availability certification. Source-specific update clocks remain recorded/unknown rather than mapped by assumption.
- Require explicit aware commence evidence before checking `decision_time < commence_time` and selected quote `< commence_time`. A `game_time` string is not automatically verified commence. No timezone localization, fixed ET default, T-minus cutoff, source age limit or synthetic date/time join.
- Output row-level violations, unmeasurable reasons and evaluated/missing counts; no 0/0 PASS. A violated applicable ordering FAILS; missing required observations BLOCKS timing; valid controlled rows pass mechanics only, with historical readiness unchanged.
- `clv` and `intraday_stability`: UNSUPPORTED by this evidence version; `source_verified_dropout`, `historical_readiness`, `promotion`: BLOCKED. List required external evidence; do not invoke MLB adapters or SQL.

### 6. CLI, results and output safety

Proposed CLI:

`python -m src.backtesting.nba.evidence --sweep-dir PATH [--checks integrity concentration coverage timing] [--output-dir PATH]`

- `--help` is effect-free; no output directory means JSON to stdout only. Default checks are integrity, concentration, coverage and timing. Integrity is mandatory before accepted descriptive analysis; omission from an explicit list does not bypass it.
- Status dimensions: integrity PASS/FAIL/BLOCKED; per-check PASS/FAIL/BLOCKED/UNSUPPORTED/NOT_REQUESTED; report construction complete/failed; historical readiness unverified; decision grade ALWAYS false. UNSUPPORTED must never be rendered PASS.
- Exit 0 only when all requested checks PASS; exit 1 for invalid evidence/violations/execution error; exit 2 when requested checks are blocked or unsupported without an overriding FAIL. Optional capabilities not requested do not make an integrity-only audit fail, but remain visible as unsupported/blocked. This distinction lets future lifecycle code consume explicit stage results without mistaking a completed report for historical acceptance.
- With explicit output: create a new exclusive incomplete audit directory, then finalize only after required report files verify. Outputs: `audit_manifest.json`, `audit_summary.json`, `audit_summary.md`, `concentration.csv`, `coverage.csv`, `timing_violations.csv`, `unmeasurable.csv`; schema-correct headers even for empty/not-requested results. Record requested checks, source file hashes, audit schema/source identity and category results.
- Completed report may faithfully contain FAIL/BLOCKED; `report_state=complete` means report serialization/readback succeeded, not audit PASS. Partial I/O failure must retain incomplete/failed state and a nonzero exit.
- Audit destinations and incomplete siblings must be outside the resolved input sweep and recorded model location. Only when `--output-dir` is explicitly supplied, path-only resolution of its destination/aliases and the recorded model location is permitted solely for output containment; never open model/source files or require the original model to exist. This narrow safety exception is not used for evidence validation. Reject an incompatible/ambiguous recorded path flavor as BLOCKED/output_containment_unknown rather than writing. Stdout-only audit remains portable and performs no original-path resolution. Never modify source manifests/cells or overwrite results.
- Sanitize exceptions; do not echo arbitrary source metadata, DB URLs or credentials. No automatic child process execution; absent/failed child receipts cannot count as successful stages. Source sweep must already be complete.

## Normative version-1 adapter details

These rules resolve the initial independent plan review's five blocking findings. They take precedence over an informal reading of earlier scope bullets.

### Saved configuration field provenance

Exact fixed-config key set:

`model_dir, start, end, stats, bookmakers, n_samples, seed, min_minutes_avg, starting_bankroll, flat_bet_size, edge_thresholds, output_dir, allowed_bets, bl_taus, bl_z_max, bl_max_weight, max_cells, local, diagnostic_history, market_stats`.

Exact cell key set before adding identity:

`edge_threshold, tau, z_max, max_weight, raw_control`.

Exact effective-config key set is the union of those two sets and:

`cell_id, bl_config, stat_config, bl_sizing_blender, max_bet_pct, kelly_fraction, kelly_used, use_bl_for_sizing, sizing_mode, min_odds, max_odds, request_identity, input_identity, empty_reason, historical_readiness, decision_grade, unresolved, output_schema_identity`.

Key categories:

1. Independently derivable from saved plan/cell: every fixed-config key (including ordered list contents), every cell key, `cell_id='cell_'+canonical_sha256(cell-without-cell_id)`, request/acquisition commitments using the producer's inspected JSON composition, and schema commitment from saved `output_schemas`. The saved-plan adapter validates exact plan key set from `sweep_config.py:260-262`; it does not consult live paths, defaults or installed dependencies. Missing/extra/changed required keys FAIL v1 conformance even when receipts were recomputed.
2. Frozen v1 format constants from inspected producer/defaults: `stat_config=null`, `bl_sizing_blender=null`, `max_bet_pct=null`, `kelly_fraction=0.125`, `kelly_used=false`, `use_bl_for_sizing=false`, `sizing_mode='flat'`, `min_odds=-200`, `max_odds=200`, `historical_readiness='unverified'`, `decision_grade=false`, and ordered `unresolved=['historical timing','source policy','loader completeness','query tie ordering']`. Raw `bl_config=null`; BL config has exact keys `tau,max_weight,min_prob,max_prob,z_max`, values from the cell for tau/weight/z and frozen v1 `min_prob=0.01,max_prob=0.99`. A future producer changing these requires explicit adapter/version review; do not silently use whatever current runtime defaults say.
3. Saved cross-file consistency only: `input_identity` and sample/component/source/artifact content commitments that cannot be rebuilt without omitted inputs. Require valid digest shapes and exact agreement wherever repeated; report `input_content_reconstruction=BLOCKED/not_persisted` and `execution_setting_application=BLOCKED/no_runtime_receipt_proof` as non-requested capability limits, not evidence contradictions. Full metadata rewrite with recomputed hashes cannot be detected as fraud by this unsigned format; never claim otherwise.
4. `empty_reason` is derivable as `'no_bets'` exactly when actual bets CSV has zero data rows, otherwise null. Runtime effectiveness, settlement correctness, model quality and historical availability are NOT independently verified by categories 1/2. A conforming saved bundle may pass requested output integrity while retaining all category-3/historical capability blocks.

Compare typed JSON structures exactly through canonical encoding, not Python's bool/int equivalence. Use saved numeric token types for commitment recomputation; don't normalize a parsed plan before hashing. Domain validation is separate from commitment checking. BL/flat constants above are schema-version validation, not betting policy recommendations.

### Occurrence-consuming correspondence

- CSV parsing preserves original cell strings. Identifier comparison is exact lexical equality for `player_id,game_id,stat,bookmaker,selected_bookmaker` and source/policy tokens: no strip, case folding, float conversion, zero-padding or treating the literal strings `None`/`nan` as missing. Only an actually empty CSV cell is the missing sentinel.
- `game_date` must already be exact valid `YYYY-MM-DD`; do not coerce datetime strings to dates. For declared line/odds/price fields only, parse finite `Decimal` from the complete token; compare exact numeric value, so `20` and `20.0` are equal without tolerance/rounding. Preserve original tokens in evidence. Actionable line/price cannot be blank/nonfinite. Timestamp matching uses either both missing or explicit aware timestamps representing the same instant, retaining lexical originals. Missing versus populated is not equal; naive timestamp availability is BLOCKED, not localized.
- Candidate-to-selected core signature: `(game_date,player_id,game_id,stat,bookmaker,line,over_odds,under_odds)`, with numeric comparison only in the last three fields. Require all copied `over_*` / `under_*` timestamp/bookmaker provenance and `quote_decision_policy` fields to agree when present in the declared v1 schemas; missing values match only missing values. Do not require preselection `selected_*` fields to equal rewritten chosen-side fields. Verify selected `selected_side` is `over`/`under`, `selected_line` equals its own line, and `selected_price` equals its own chosen-side odds.
- For selected chosen-side metadata, use the inspected harness's metadata projection only: if a matching candidate already has the same literal `selected_side`, its saved `selected_*` projection is preserved; otherwise selected snapshot/market-update/book-update/bookmaker come from that candidate's chosen-side fields, with decision time from chosen-side decision when the column exists, otherwise `bet_decision_time`. Existing-but-blank values stay blank; no value-level fallback. This validates field copying, not which side/book should win. Different possible candidate projections that cannot be distinguished from persisted evidence BLOCK the affected provenance proof; no tie-break is invented.
- Selected-to-bet core signature: `(game_date,player_id,game_id,stat,selected_side,selected_line,selected_price)` on selected rows, compared with `(game_date,player_id,game_id,stat,side,line,odds)` on bets. Bet `selected_side/line/price` must also agree with its core side/line/odds. Compare copied `selected_*` and side-specific quote provenance/decision-policy fields to the selected parent using the null/timestamp rules above.
- Inspected `BetSimulator.place_bet` leaves base `bookmaker` at its dataclass default even when `selected_bookmaker` is carried; a blank base bet bookmaker is not a contradiction. Do not require that unwritten field to equal the candidate book. Descriptive book grouping uses explicit nonblank `selected_bookmaker`, otherwise nonblank base `bookmaker`, otherwise the missing bucket; this is an output-label rule, not quote routing. Any conflicting populated bookmaker fields are reported, never silently overwritten.
- Required relationship is multiset containment at both transitions, not equality: selected occurrences are contained in candidate occurrences; bet occurrences are contained in selected occurrences. Unused candidates/selected rows are normal. Use an occurrence-consuming matching algorithm that can establish a complete injective match (not greedy matching that fails despite another valid assignment); no parent occurrence can justify two children. Keep duplicate candidate occurrences distinct. Require at most one selected and one bet occurrence per `(game_date,player_id,game_id)`, matching the inspected one-bet-per-player/game owner.
- No possible core match or excess multiplicity => FAIL/correspondence. Missing provenance permitting a core match => structural containment may PASS, but provenance availability remains BLOCKED. Do not fabricate a unique source-row identity from a non-unique matching set. Coverage ratios use these explicitly recorded matches and separate raw-row/distinct-opportunity counts.

### Normative status / exit matrix

| Observation | Relevant result | Exit if no higher-priority FAIL |
|---|---|---:|
| Complete supported v1, requested checks all evaluable/conforming | Requested checks PASS | 0 |
| Missing required file, duplicate JSON key/header/cell, traversal/escape, nonregular required path, hash/schema/count/config/correspondence contradiction, invalid typed control | Integrity FAIL | 1 |
| Producer explicitly `failed`, or complete state with failure/active-cell residue | Integrity FAIL/producer_failed | 1 |
| Declared v1 incomplete state or `_incomplete` input directory, without a contradictory complete claim | Integrity BLOCKED/producer_incomplete | 2 |
| Unknown schema/encoding version | Integrity BLOCKED/unsupported_format | 2 |
| Extra cell directory or extra entry inside declared cell | Integrity FAIL/layout | 1 |
| Unrelated root entry | List ignored name/type only; no effect on requested checks | unchanged |
| Missing/naive/ambiguous timing, zero evaluable timing population, unavailable row provenance | Affected requested check BLOCKED | 2 |
| Applicable timing ordering violation | Timing FAIL, even if other rows are unmeasurable | 1 |
| Omitted raw samples/source frames or absent closing histories | Capability BLOCKED/UNSUPPORTED; no fabricated reconstruction | 2 only if requested |
| Resource ceiling exceeded or observed input replacement/change | Integrity BLOCKED/resource_limit or source_changed_during_audit | 2 |
| Permission/read failure, output collision/write/readback error, invalid CLI request | Execution/report FAIL with sanitized reason | 1 |
| Ambiguous original model path prevents requested output containment | Report BLOCKED/output_containment_unknown; no output created | 2 |

Precedence: any observed FAIL wins over BLOCKED/UNSUPPORTED; otherwise any requested BLOCKED/UNSUPPORTED yields 2; otherwise requested PASS yields 0. Mandatory integrity must PASS before descriptive checks can be accepted. When integrity is not PASS, do not emit trusted concentration/coverage/timing aggregates; mark dependent checks BLOCKED/integrity_not_pass and output only failure evidence/schema-correct empty tables. Unknown format is not parsed speculatively. A report may serialize its FAIL/BLOCKED conclusion successfully, but remains non-decision-grade. Child-stage/source success is never inferred from report completion.

## Implementation tasks after approval

### Task 1: Freeze version-1 reader and statuses

Create the allowlisted module, explicit data/result structures, strict reader and parser. First run disposable failing cases from Hermes scratch against the real library/CLI. Add no tests to the repo. Validate help/import and stdout-only execution with runtime-effect tripwires.

### Task 2: Verify saved commitments and relationships

Implement allowlisted layout/hash/header/count checks, pure plan identities and cross-file relationships above. Validate malformed/omitted/extra cells, duplicate keys/headers, changed hashes, rewritten receipts, mismatched effective settings and moving evidence. Distinguish unverifiable input commitments from output commitments that can actually be recomputed.

### Task 3: Add descriptive saved-output diagnostics

Implement concentration and coverage with explicit denominators and unknown/missing values. Add applicable timing checks without synthesizing clocks. Keep unsupported source/CLV/horizon diagnostics explicit. Exercise populated and schema-correct empty evidence through the same public API.

### Task 4: Add safe report export and documentation

Implement stdout-only default and exclusive explicit output. Explain selected-prediction versus acquisition populations, report status versus data acceptance and all unsupported capabilities. Add only approved links/status changes to the overview/readiness pages; merge additions into concurrent docs rather than overwrite snapshots.

### Task 5: Independent review and coordinator rerun

Specification review against the exact plan first; correct in-scope defects and rerun affected real-path checks. Then code-quality review; close its findings, repeat source/fixture-scope and dependency-hash checks and coordinator validation. Review full new files because `git diff` omits untracked source. Commit/push/merge remain separately authorized.

## Validation commands and probe matrix

Confirm environment and named test files in the eventual worktree first. Existing tests were located in main, and native `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe` was verified runnable. No baseline suite has been run for this planning-only task.

PowerShell-safe commands from the approved successor worktree:

- `$env:PYTHON_DOTENV_DISABLED='1'; & 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m pytest tests/test_run_sweep.py tests/test_run_backtest.py tests/test_bet_simulator.py tests/test_backtest_harness.py -q`
- `& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m ruff check src/backtesting/nba/evidence.py`
- `& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m compileall -q src/backtesting/nba/evidence.py`
- `& 'C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe' -m src.backtesting.nba.evidence --help`
- `git diff --check -- src/backtesting/nba/evidence.py docs/operations/nba-evidence-slice6-2026-11-01.md docs/understanding/nba-evidence-audits.md docs/operations/nba-coding-readiness-plan-2026-11-01.md docs/understanding/nba-training-evaluation-slices.md`

MLB test inventory exists (`tests/test_analyze_mlb_batter_hits_clv.py`, `tests/test_audit_mlb_quote_clean_dropout.py`) but is not a substitute for NBA acceptance. Run only if an explicitly approved pure helper import actually depends on those owners; this plan does not modify them.

Disposable probes under Hermes scratch must exercise:

1. A genuine controlled bundle exported through the actual pinned slice-5 `run_sweep(..., runtime_factory=...)`, with in-memory typed collaborators, DB/network/model-loading tripwires and disposable model evidence. No fabricated upstream API responses. Do not acquire real data or load trained models. Reuse predecessor probe collaborators only after verifying source and scope.
2. Valid raw and BL cells, multiple dates, legitimate no-feature/no-quote/no-bet outputs; independent header/denominator/identity assertions, not only matching producer receipts.
3. Missing/corrupt required files, failed/incomplete/unknown-version inputs, duplicate cells, omitted middle/final date, mismatched summary/metrics/config, extra effective config field and altered allowed direction/BL/stake/input identity. Recompute malicious self-consistent receipts where useful to expose schema/relationship gaps.
4. String IDs/leading zeroes, duplicate JSON keys/headers, NaN/Infinity metadata, tagged numerical evidence, malformed nested CSV JSON, null/zero denominators and unsupported extensions.
5. Multiple books/lines per opportunity without double-counting acquisition population; actual selected/candidate/bet mismatch; tied candidates without a new tie-break.
6. Valid aware clocks, after-decision quote, decision at/after commence, missing/naive/ambiguous timestamps and absent closing snapshots. Missing evidence never yields zero-violation PASS; nice profit cannot override invalid execution/timing.
7. Library AND CLI exit/status parity. Integrity-only PASS must retain `decision_grade=false`; default missing timing => exit 2; explicit contradiction => exit 1. No output flag => no files created.
8. Traversal/symlink/native Windows junction escapes, output/input/model nesting, final/incomplete collisions, output-write failure and observed source mutation/replacement. Verify the auditor makes no source writes and before/after observations agree; do not claim to detect every transient mutation. No completed report over partial output writes. Resource-limit refusal must not masquerade as a truncated passing audit.

Record real commands/results; no expected counts presented as execution. Existing test incompatibilities require reporting, not fixture edits. No historical model/data/ROI certification from this matrix.

## Dependency fingerprint observed during scoping

Origin: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-shared-input-sweep`, HEAD `10d9c2258f9a26eb3ff5ef424cbeb66cd3093ff3`. Values below were computed from actual bytes with the native interpreter. Refresh before launch and acceptance; these are not permission to copy.

| File | Bytes | SHA-256 |
|---|---:|---|
| `src/backtesting/nba/__init__.py` | 84 | `55c93137d37ff5c1ad0df8de4c175d410b0a509b13506408b45519164b9092c5` |
| `src/backtesting/nba/sweep_config.py` | 16158 | `5c7be28fc84949b82c0ce0822a9fdf802595cc30bdf4b2a9b540e8746b02a76c` |
| `src/backtesting/nba/sweep_runner.py` | 41678 | `47c319f4eabcaa3aa928860c4eb63e84db7d4cc2e27a9c5611229ac6008e5ba4` |
| `src/backtesting/nba/run_nba_sweep.py` | 6073 | `ff05f432dda56def1c0a894178a01af3720d3f567f64fc9e0b7d0a2b82901383` |
| `src/models/nba_artifacts.py` | 39372 | `cca24e3f86c7e6af4834a8abb064f83313fbbca9624960bcfb2693006539ff64` |
| `src/backtesting/backtest_harness.py` | 46020 | `e80ff8770a1270982109ee92d1ba51caea3b5695e517bc48883b25ab16a59c5e` |
| `src/backtesting/bet_simulator.py` | 22139 | `dad00bae683c50551ed248a2cc724925c345b4cc08f8a98f57cb9b4ffcc2aaa3` |

For snapshot-mode acceptance, also fingerprint the full transitive `SOURCE_FILES` inventory declared in final `sweep_config.py`, not just this initial planning subset.

## Relevant prior lessons/invariants

Retrieved canonical `operations/hard-facts`, `operations/critical-invariants`, lesson-tag inventory and keyword search, then temporal-integrity/quote-clean lessons. Apply principles, not stale MLB status claims:

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: file consistency and timestamp columns do not prove historical as-of correctness. Explicit source observations and end-to-end query evidence remain necessary.
- `lessons/quote-clean-clv-before-feature-work`: missing valid closes/horizon data cannot be replaced by attractive ROI or synthetic ordering.
- Empirical CDF/Q10/no-global-offset invariants: this slice does not recalculate probabilities or alter models/calibration.
- DB isolation and owner-launched long jobs: no SQL, backfill, fit, historical sweep, source repair or deployment.
- Engineering OS remains paused; no frontend, manager console or lifecycle orchestration dependency.

## Ready-for-implementation definition

The scope is reviewable when source contracts, limits, allowlist and validation are explicit and independent plan review has no unresolved blocking ambiguity. Launch readiness additionally requires owner scope approval and an integrated or explicitly authorized snapshot base. These gates must not be blurred.

Do not say implemented, source-integrated, historical-data verified, model-approved or ready to promote from this planning document.
