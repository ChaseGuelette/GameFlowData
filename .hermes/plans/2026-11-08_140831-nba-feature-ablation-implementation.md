# Approved NBA feature-family ablation implementation

## Authorization / execution state

Chase explicitly selected **general NBA feature-family controls**, not previous-day injuries, when asked to disambiguate, and requested implementation. Implement the source/setup and verify offline; no training, sweeps, external data acquisition, database writes, deployment, commits or pushes. Injuries stay excluded in the proposed discovery examples. Existing defaults remain backward-compatible.

Base: `963b426df006c41bf7431d96c719a2eacbde60da`.
Isolated writer root: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-feature-ablations` (detached, initially clean).
Baseline: 108 existing tests pass under the offline guard in both main and the isolated root; zero network/DB/model-load/model-fit attempts.

Worker availability: configured Spark canary rejected by subscription (HTTP 400); OpenRouter Codex canary rejected for billing (402); OpenCode has no configured credentials. Use the standard delegated coding worker without changing provider configuration.

## Allowed implementation files

New:
- `src/models/nba/feature_controls.py`
- `docs/operations/nba-feature-ablation-setup.md`
- Optional concise example configs under `src/models/configs/nba_feature_ablations/` (four discovery configs, not a new runner).

Modify:
- `src/models/train_pipeline.py`
- `src/models/nba_artifacts.py` (only feature-control/default evidence validation)
- `src/models/nba/lifecycle/config.py`
- `src/models/nba/lifecycle/adapters.py`
- `src/models/nba/lifecycle/runner.py`

Protected: all tests/fixtures, `src/processing/feature_selection.py`, contracts/feature loaders, all MLB code, injury source/recovery, scheduler/jobs, production artifact/config directories, all backtest implementation files (especially concurrently dirty `run_nba_sweep.py` and `sweep_runner.py`), secrets and unrelated docs. No new unit tests. Use existing tests and a few controlled populated-boundary probes under the reusable guard. Ask coordinator before expanding implementation scope.

## Behavioral requirements

### 1. NBA-local explicit registry and pure controls

Implement a small NBA-specific registry, not a reuse of the coarse review taxonomy and not an import of MLB profiles. Exact disjoint families:

- `game_market`: `line_spread`, `line_total`.
- `prop_market`: `prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes`.
- `opponent_matchup`: `opp_avg_def_rtg_l5`, `opp_avg_fg3a_l5`, `opp_avg_fg3_pct_l5`, `opp_pos_off_rtg_allowed_l5`, `opp_pos_off_rtg_allowed_l15`, `opp_pos_reb_allowed_l5`, `opp_pos_reb_per100_allowed_l5`, `opp_pos_reb_allowed_l15`, `opp_pos_ast_allowed_l5`, `opp_pos_ast_per100_allowed_l5`, `opp_pos_ast_allowed_l15`, `opp_pos_threes_allowed_l5`, `opp_pos_threes_per100_allowed_l5`, `opp_pos_threes_allowed_l15`.
- `short_form`: `player_avg_pts_l3`, `player_avg_reb_l3`, `player_avg_ast_l3`, `player_avg_fg3m_l3`, `player_pts_l3_l15_ratio`, `player_reb_l3_l15_ratio`, `player_ast_l3_l15_ratio`, `player_fg3m_l3_l15_ratio`, `player_std_pts_l5`, `player_std_reb_l5`, `player_std_ast_l5`, `player_std_fg3m_l5`.
- `minutes_role`: `player_avg_min_l3`, `player_min_l3_l5_ratio`, `player_min_l3_l15_ratio`, `player_avg_min_szn`, `player_min_std_l5`, `player_min_floor_l5`, `player_games_started_l5`, `player_starter_prob`.
- `schedule`: `rest_days`, `is_back_to_back`, `games_in_last_7_days`.

Counts: 2/4/14/12/8/3. `opp_avg_pace_l5` is NOT part of opponent_matchup; minutes L3 belongs only to minutes_role. Existing misleading ratio names must not be renamed/recomputed. No injury inclusion family in this registry.

Support requested family and exact-feature include/exclude lists. Prefer a frozen `FeatureControlSpec` and pure resolve/assert helpers, with deterministic JSON-compatible receipts. Preserve tokens exactly; reject invalid/unknown family names and invalid/duplicate requests rather than silently repairing spelling. A family and its explicitly mentioned member on the same side may deduplicate after resolution; cross-side resolved overlap is an error.

Numeric candidates continue to originate from the existing `get_candidate_columns(df,target)`; do not broaden candidate discovery, change imputation, or permit controls to resurrect IDs, targets, clocks, dates, names or position strings. Exact structural-target/metadata requests must fail even if accidentally present in a numeric candidate list. Requested exact features must be actual supported numeric candidates at runtime. Dry-run must label data-dependent availability unresolved rather than pretend to have loaded data.

For requested families, enforce full declared-member availability in each actual component candidate frame; report and reject missing/non-numeric/protected members instead of silently claiming an incomplete family ablation. Requested excludes must not silently be no-ops against missing columns. Default/no-controls behavior stays unchanged.

`exclude_injury_features` is an independent existing hard exclusion. General include controls cannot override it. Reject any forced injury inclusion when it is active.

### 2. Full training integration

New independent CLI options:
- `--include-feature-families` (one or more exact registry names)
- `--exclude-feature-families`
- `--include-features` (one or more exact feature identifiers)
- `--exclude-features`

Leave the existing partial-only `--force-features` contract unchanged. New controls support coherent full-suite ordinary training first; reject nonempty new controls with partial retraining, calibration-only or strict historical modes rather than ignoring them. Preserve legacy Namespace-based callers through optional defaults / `_namespace_value`.

Filter candidates BEFORE each minutes/PTS/REB/AST feature selector. Force includes must survive in EVERY effective quantile list for EVERY trained component; apply/verify required members after selector results. Never satisfy an include only by permitting it as a candidate. Exclusions apply across all components, including cross-stat prop-line columns in the minutes model.

Assert resolved include/exclude and existing no-injury obligations before fitting and again against the pipeline/effective lists at finalization. Do not mutate module-global feature lists. Do not alter hyperparameters, selectors' tolerance, tuning/search behavior, cohort splits or source SQL.

### 3. Honest artifact evidence

Persist requested controls, registry version, exact resolved family members and per-component candidate/required/excluded/effective per-quantile evidence. Use `run_config.json` plus manifest training metadata/linked evidence consistently; retain `selected_features.json` as the actual selected configuration consumed by the model.

Keep requested settings separate from data-derived resolution so lifecycle expected-settings comparison remains explicit. Validate that manifest and run config agree and that selected/effective lists actually satisfy the declared controls. Reject a claimed nondefault control when evidence is absent or inconsistent. Legacy artifacts without control fields normalize only to empty controls/default false, not to a caller's requested nondefault values. Preserve current feature-file/hash/finalization checks.

Record exact baseline/reference artifact identity in setup documentation/example comments rather than abusing the partial-retrain parent field or building a new lineage system. Baseline path: `src/models/artifacts/no_injury_baseline/nba_run_20261108_122129`; do not load its binaries or modify its files.

### 4. Existing lifecycle threading

Add defaulted strict training fields for the four control lists plus `exclude_injury_features` in lifecycle Training config. Validate conflicts/registry and forbid unsupported historical combinations at config/CLI boundaries. No new control fields in evaluation: this is retraining, not prediction-time zeroing or `allowed_bets` filtering.

`build_training_command` must pass exactly the same flags used by direct CLI and dry-run. Settings automatically enter resolved config identity/resume hashes; ensure the new controls module participates in lifecycle source fingerprints.

Repair `inspect_artifact`'s training-settings comparison to account for no-injury and new controls without dropping unknown/mismatched settings. Cross-check saved run config and effective features for non-historical artifacts too. Attach mode still forbids training settings; independently validate the attached artifact's own declared controls and expose them in receipts. Do not silently attach a differently controlled artifact in a training lifecycle. Preserve actual calibration-cutoff and fixed-hyperparameter checks.

### 5. Usable setup, not an unexecuted framework

Prepare one separate lifecycle config/run per variant or concise direct-CLI recipes using the current strict schema:
1. Game-line-free control: exclude game_market.
2. Against that common control: exclude game_market + short_form.
3. Exclude game_market + opponent_matchup.
4. Exclude game_market + prop_market.

Every discovery example keeps `exclude_injury_features=true`, fixed incumbent supported-stat parameters, fit 22023/22024, calibration 22025, selector tolerance and evaluation settings identical. Use an explicit exploratory/discovery evaluation window consistent with the existing first-sweep report; do not label reused 22026 dates untouched or promotion-certified. Leave sweep code unchanged and do not invent CLI options for the concurrent sweep branch. Explain replay/edge odds remain available when prop_market is excluded from model inputs. Include one forced-include example demonstrating that required features survive selection.

No new matrix runner, new ranker, automatic training, new data source, or PDF/injury-policy integration. If exact raw+BL cell plumbing is outside current lifecycle support, document the existing limitation rather than touching protected sweeps; training control delivery must still be runnable and verified.

## Validation budget and criteria

Baseline metadata clarification: the completed reference manifest has `feature_tolerance=0.005`, `tune_hyperparams=false`, `tuning_trials=50`, and `calibration.end_date_exclusive=null`. Do not invent a new calibration cutoff or weaken the lifecycle's actual-cutoff gate to make an example config green. Prefer fully runnable direct-training recipes preserving the existing baseline settings; document that lifecycle execution still requires its existing explicit-cutoff prerequisites. Lifecycle config/argv/control-identity threading can be validated on an explicitly controlled config without claiming that the existing reference artifact is lifecycle-ready. Its `run_config.json` does not duplicate every selector/tuning field; use the manifest for those settings.

Use parent venv executable from main even when testing the isolated root:
`C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`

Reusable guard:
`C:/Users/Chase/AppData/Local/hermes/skills/gameflow/gameflow-implementation-worker/scripts/run_nba_offline_checks.py ROOT pytest tests/test_train_pipeline.py tests/test_nba_feature_contracts.py`

For controlled probes: same guard `ROOT probe ABSOLUTE_SCRIPT`. Scratch probes/receipts only under a unique directory beneath `C:/Users/Chase/AppData/Local/hermes/cache/scratch/`; do not add test files or read credentials. No actual XGBoost fit, model binary load, network or DB access. Guard does not automatically cover child processes: stub lifecycle child launch or explicitly wrap any child.

Required populated checks (small, not exhaustive framework):
- Registry counts/disjointness; unknown names, cross-side conflicts, structural includes, missing requested family member, injury-lock conflict, and empty candidate errors.
- Actual TrainingOrchestrator feature-selection path with numeric frames and selector stub: exclusion before selector; includes present in all minutes/PTS/REB/AST quantiles after a selector deliberately drops them; no controls preserves baseline behavior.
- Run-config/manifest effective-list conformance with no real fitting; tampered or missing nondefault evidence fails; legacy empty defaults still accepted.
- Actual direct CLI `--dry-run` for all four discovery variants and include example: no DB/model/output-artifact side effects.
- Lifecycle config load -> training argv -> direct parser/dry-run agreement; controls affect identity; mismatched artifact settings fail. Inspect attach and non-historical path, not only historical branches.
- Existing 108-test baseline stays green; scoped Ruff, compile and diff check on changed source. Existing source/fixture failures must be classified, not hidden or repaired by unauthorized test edits.

## Review and integration

One implementation writer in the isolated root. Return exact modified/new paths, validation output and failures; do not commit/push. Parent independently reviews scoped diff/new files, closes spec then quality review, and rechecks main's allowlisted bytes before integration. Apply only this lane's changes; preserve concurrent dirty sweep files and owner docs. If those source files changed since base, reconcile only the affected overlap rather than overwriting whole files.

Relevant lessons: whole-family forced treatment is not selector importance; correlated families can substitute; fixed hyperparameters and flat-stake comparisons isolate feature treatment. Preserve empirical sample probabilities, Q10 edge behavior and no global conformal offsets.

## Delivery / final verification

Implemented and integrated into the main checkout, uncommitted. All seven reviewed source/setup files matched the isolated worktree byte-for-byte on integration; protected concurrent sweep files were unchanged. Independent spec and quality reviews passed after fixing legacy no-injury artifact compatibility, protected clock fields, the common-control documentation, absent-excluded-member evidence and callable unsupported-mode gates.

Main-checkout verification: 108 existing tests passed; populated selection/artifact/lifecycle probe passed, including all four variant dry-runs and one forced-include dry-run; real completed baseline remains `verified_manifest`; negative probes reject missing excluded-member evidence, protected metadata, direct partial and direct historical controls. All guarded paths reported zero DB/network/model-load/model-fit attempts. Scoped Ruff, syntax and diff checks passed. No tests were added or edited, and no model/data jobs or publication were performed.

Operator guide: `docs/operations/nba-feature-ablation-setup.md`. Direct recipes preserve the reference's null calibration cutoff; the pre-existing explicit lifecycle cutoff prerequisite was not weakened. Main integration receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-feature-ablations-parent/integration.json`.
