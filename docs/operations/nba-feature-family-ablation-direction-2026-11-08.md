# NBA feature-family ablations — November 8, 2026

## First wave completed and reviewed

All four owner-launched models and their sweeps are complete. Use [the four-ablation review](nba-four-ablation-review-2026-11-08.md) for current results and proposed next work. Keep prop inputs; retain the game-line-free control and no-short-form lean challenger; matchup removal is not a general improvement. A later untouched comparison or one combined short-form/matchup exclusion is proposed, not launched. The original first-wave design below is historical and must not trigger duplicate training or reimplementation of delivered controls.

## First-sweep evidence update

Read [the first sweep review](nba-first-sweep-review-2026-11-08.md) before launching this proposal. Raw settings all lost money; a BL candidate is exploratory over 12 betting dates, with overs and AST weak. The updated recommended order is the game-line-free control, then separate short-form, opponent-matchup and prop-market exclusions against that common control. These are hypotheses, not proven harmful families. Preserve fixed parameters and compare the same raw controls plus one fixed BL cell; do not optimize a new grid per variant. This update supersedes only the earlier proposed priority order, not the family definitions or approval gates below.

## Kick off immediately

General training-time family controls are delivered and the first wave is analyzed. The retained implementation/design discussion below records the original proposal; it is not a current implementation queue or authorization for additional runs.

- This chat: completed direct artifact/sweep analysis. Next gate: Chase selects later-window validation or the single optional combined-removal experiment from the latest review.
- You: retain the completed baseline and choose the experiment scope; launch approved long training/replay jobs yourself.
- Other chat: no additional writer needed yet. Preserve separately owned injury and Railway work.
- Read first: the four-ablation review, `nba-feature-ablation-setup.md`, then exact completed artifact metadata. No broad readiness audit or new seasonal-sampling gate is proposed.

## Verified baseline

The first successful no-injury run has finalized at `src/models/artifacts/no_injury_baseline/nba_run_20261108_122129`.

- `artifact_manifest.json` records `completion.complete=true` and a passed canonical no-global-offset prediction sanity check. This is saved evidence, not a new model-load/fit or profitability check by this discussion.
- `run_config.json`: fit `22023`/`22024`, calibrate `22025`, `exclude_injury_features=true`; historical source verification and decision-grade status remain false.
- Programmatic JSON comparison found saved hyperparameters equal to `src/models/configs/nba_no_injury_baseline_hyperparams.json`.
- No injury-family features occur in any saved selected list.
- Selected-feature unions across quantiles: minutes 57, PTS rate 58, REB rate 57, AST rate 55. Per-quantile lists differ; these are not per-model fixed counts.
- All four components use all four `prop_line_*` columns somewhere across their quantiles, including `prop_line_threes`. They also all use `line_spread` and `line_total`. Excluding market information only from the corresponding rate model would not be a whole-family ablation.

## Architecture: reuse versus add

There is no justification for rebuilding the lifecycle or backtester.

| Boundary | Verified current behavior | Proposed narrow addition |
|---|---|---|
| Training | Full training selects numeric candidates separately for minutes/PTS/REB/AST and per quantile. Injury exclusion is explicitly implemented. | General family/exact-feature include and exclude controls, compatible with permanent injury exclusion. |
| Family definitions | `NBA_FEATURE_FAMILIES` is a coarse review taxonomy, not a training ablation registry. | Exact, non-overlapping experiment-family lists; report requested, available and effective members. |
| Partial retraining | `--force-features` is partial-only and appends features to every component's configuration, even when only some components are retrained. | Do not use this as the family runner. Start with coherent full-suite retraining; separately repair/scope partial semantics if later useful. |
| Artifact identity | Selected lists are persisted and checked against the fitted pipeline at finalization. | Persist requested/resolved controls, per-component/per-quantile sets, fixed selector settings and parent/reference identity; assert exclusion/inclusion before fit and finalization. |
| Lifecycle | Existing train/attach → artifact identity → sweep → audit → decision stages, with resume/revalidation. Strict training config currently has no feature controls or no-injury field. | Thread controls through config/command/identity checks. Use one distinct config/run per variant, not a new matrix framework. |
| Replay | Exact manifested model path; raw flat-stake cells and optional BL grids already exist. | Reuse unchanged. Feature exclusion belongs in training, not prediction-time zeroing or `--allowed-bets`. |

The lifecycle's `ranker` stage currently returns `not_requested`; do not describe it as implemented MLB ranker parity. Current sweeps remain diagnostic/non-decision-grade. Neither limitation requires making a ranker a prerequisite for flat-stake discovery.

The MLB `FeatureControlSpec` and resolver supply the right include/exclude/conflict/required-feature pattern, but are coupled to `MLBTrainingProfile` and MLB structural exclusions. Adapt the small pattern for NBA; do not import it blindly or silently change MLB behavior. NBA must retain its numeric-target/ID/clock exclusions and surface missing requested members rather than silently treating an incomplete family as tested.

## Recommended first experiments

Start subtractively because the completed baseline already includes broad feature families. These names are proposed registry names, NOT implemented CLI tokens. Counts are full proposed family membership, not the number removed from every quantile.

| Order | Family and operation | Exact scope | Question |
|---|---|---|---|
| Control | Exclude `game_market` | 2: `line_spread`, `line_total` | How much does the baseline depend on the game-line path whose historical as-of timing is still unverified? This is a confound-control experiment, not proof that game context is useless. |
| 1 | Exclude `prop_market` | 4: `prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes` | Are we gaining useful market anchoring or suppressing independent signal? Remove the whole family from every affected component; keep executable odds for replay/edge calculation. |
| 2 | Exclude `opponent_matchup` | 14: all 11 `opp_pos_*` contract members plus `opp_avg_def_rtg_l5`, `opp_avg_fg3a_l5`, `opp_avg_fg3_pct_l5` | Does recent position/matchup context add value, or add noise/redundancy beyond player history? Keep opponent pace separate. |
| 3 | Exclude `short_form` | 12: L3 PTS/REB/AST/FG3M means, their four momentum ratios and four L5 stat standard deviations | Do short windows help current form, or overreact to noisy recent games? Preserve slower player-history anchors and minutes-role features. |

The ordinary training loader's game spread/total query still aggregates `MAX()` without an as-of predicate. In contrast, the adjacent compact player-prop query explicitly filters `t60_v1` and decision-time conditions. Do not equate these two source paths or reopen completed prop-line work.

Recommended comparison structure: retain the finished model as reference; create the game-line-free control first. Until game-line timing is verified, use that same control as the parent/reference for the three substantive family exclusions, rather than allowing the suspect game-line path to influence all conclusions. This adds a bounded model variant, not a new historical-data audit. Removing game lines alone does not certify all other chronology/source evidence.

Second wave, conditional on the first results:

- `minutes_role`: 8 members — `player_avg_min_l3`, `player_min_l3_l5_ratio`, `player_min_l3_l15_ratio`, `player_avg_min_szn`, `player_min_std_l5`, `player_min_floor_l5`, `player_games_started_l5`, `player_starter_prob`. Seven are selected somewhere in each current component; `player_starter_prob` is not selected. Preserve L5/L15 minutes anchors. Treat role and stability subgroups separately only after the coarse family result warrants it; do not default-force a previously rejected starter-probability feature.
- `schedule`: 3 members — `rest_days`, `is_back_to_back`, `games_in_last_7_days`. Keep this small, interpretable group separate from pace and minutes role.
- Shelf injuries, new external data sources, travel and new architecture. Existing injury exclusion stays in every discovery run. There is no evidence here that a new source is needed.

## Comparison policy — proposed, not a new owner freeze

Feature ablations should change the feature treatment, not simultaneously change fitting or betting policy.

1. Keep the approved fit/calibration seasons, exact fixed hyperparameters, selector tolerance, chronological-fold policy, cohort and source snapshot stable. No per-variant tuning during discovery; otherwise the comparison mixes feature effects with tuner search. Use the same deterministic seeds where supported.
2. Retrain with the family removed before selection, with an assertion that none re-enters saved effective features. Keep the selector policy identical for other candidates and report substitutions; this measures the retrained pipeline's incremental family value, not a fixed-weight causal effect. Do not remove model inputs only at inference or fill them with zero.
3. For later inclusion experiments, require the resolved family to survive selection in the requested component/quantile scope. Inspect expected versus effective lists; never infer an ablation from a selector report alone.
4. Evaluate exact artifacts on identical accepted quote/date windows using raw/no-BL probabilities and fixed flat stakes first. Keep seed/sample count, books, quote cutoff, minimum-minutes eligibility and edge grid fixed. BL tuning is a finalist stage, not a family-discovery variable.
5. Compare profit, ROI, daily Sharpe, percentage drawdown and volume; show PTS/REB/AST and over/under splits, common bets, removed bets, new bets, missing quotes and dropout. Report predictive/calibration diagnostics as supporting evidence, not a license to correct Q10 or deploy global offsets. Mean CLV supports interpretation; edge-ranking is relevant to optional sizing, not a prerequisite for fixed-dollar discovery.
6. A `22026` interval used to choose a family becomes a discovery/selection window. Reserve later observations for independent confirmation; do not call the same reused window an untouched final test. The existing `22025` calibration population cannot supply an independent result for this artifact.[2]
7. Keep small samples exploratory, including fewer than 100 bets per evaluated configuration. Avoid manufacturing volume by optimizing a new policy for every family. A loss after family removal suggests retaining it; a repeatable win makes removal a candidate; a near-tie favors simpler only after checking uncertainty/coverage. Combinations and targeted forced-inclusion follow the first-family results, not a combinatorial initial sweep.

## Relevant prior lessons and evidence

GBrain lessons `lessons/feature-selector-is-not-an-ablation`, `lessons/correlated-feature-family-validation` and `lessons/cheap-baseline-before-architecture` motivate whole-family, fixed-policy experiments. Correlated features can substitute for one another in importance diagnostics; low individual importance is not proof the group is useless.[3]

Keep empirical CDF probabilities, no global conformal offsets, owner-launched long jobs and separate promotion gates. All source inspection was read-only; no ablation training, backtest, database query or code change was performed.

Current-code evidence:

- `src/models/train_pipeline.py:323-350,694-733,1340-1350,1404-1442` — partial force behavior, full candidate selection and CLI constraints.
- `src/processing/feature_selection.py:641-677` — numeric candidate discovery, metadata/target exclusions.
- `src/models/features/nba/contracts.py:10-250` — stable members and coarse taxonomy.
- `src/models/features/nba/training_loader.py:355-448,528-553` — actual generated feature meanings and distinct game/prop line paths. REB/AST/FG3M `l3_l15_ratio` names actually use L3/L5; preserve identifiers while documenting semantics.
- `src/models/nba/lifecycle/config.py:74-149`; `runner.py:498-550` — current strict config and reusable orchestration.
- `src/backtesting/nba/run_nba_sweep.py:55-81,105-129` — exact artifact, raw/BL controls and diagnostic posture.
- `src/models/mlb/training/feature_controls.py:26-31,118-171` — reusable pattern, not a drop-in cross-sport helper.
- Baseline `run_config.json`, `selected_features.json`, `best_hyperparams.json`, `artifact_manifest.json` — exact local evidence; no binary model loading.

Sources:
[2] https://scikit-learn.org/stable/common_pitfalls.html
[3] https://scikit-learn.org/stable/modules/permutation_importance.html
