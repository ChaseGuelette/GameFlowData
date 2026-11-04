
# Layer 3 — Feature builders and feature store

- Revision: 2026-11-03 / v1. Status: proposed feature and dataset contract; no model-ready corpus is claimed.
- Goal: construct the same pregame inputs for training, replay and daily inference, with explicit historical availability and missingness.
- Architecture: one `NcaabFeatureService`, immutable schema registry, bounded history reads, pure transforms and a separately fitted team-state provider.

[Previous: database](02-database-and-identity-linking.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Next: training](04-training-and-artifact-management.md)

## 1. Ownership and files

| Proposed file | Main classes/functions | Reads / produces |
|---|---|---|
| `src/ncaab/features/schema.py` | `FeatureSpec`, `FeatureSchema`, `get_schema`, `validate_feature_contract` | Ordered names, types, units, source families, availability and null policy |
| `src/ncaab/features/service.py` | `NcaabFeatureService`, `HistoryIndex` | Layer-2 facts + explicit context -> `FeatureBatch` |
| `src/ncaab/features/rolling.py` | `estimate_possessions`, `team_summary`, `rest_features` | Prior raw counts -> aggregate rates and counts |
| `src/ncaab/features/matchups.py` | `MatchupFeatureBuilder` | Explicit experimental offense-defense interactions |
| `src/ncaab/features/market.py` | `MarketFeatureBuilder` | A layer-5 reference `QuoteSelection` -> allowed aware-only values |
| `src/ncaab/features/roster.py` | `RosterFeatureBuilder`, `RosterTensor` | Prior player history + eligible membership -> masked player/role arrays |
| `src/ncaab/features/team_state.py` | `TeamStateProvider` protocol, `CachedTeamStateProvider` | Frozen layer-4 state parameters + prior games -> means/uncertainty |
| `src/ncaab/features/export.py` | `TrainingDatasetBuilder`, `DatasetExporter`, `FrozenFeatureProvider` | Features and separate labels/quotes/schedules -> immutable dataset bundle; exact-context replay reads |

The retained NBA contract pattern in `src/models/features/nba/contracts.py` is a reference for ordered lists/family ownership. Do not import its minutes/rate feature lists. A selector is a layer-4 training policy, not a replacement for this layer.

## 2. Interfaces and immutable context

```python
class NcaabFeatureService:
    def build(self, game_ids: Sequence[int], context: AsOfContext,
              *, schema_id: str, state_provider: TeamStateProvider | None = None) -> FeatureBatch: ...

class TeamStateProvider(Protocol):
    def state_before(self, game: GameSchedule, context: AsOfContext) -> TeamStatePair: ...

class TrainingDatasetBuilder:
    def build(self, split: SplitSpec, *, schema_id: str,
              decision_policy: DecisionPolicy, mode: str) -> DatasetManifest: ...
```

`AsOfContext` is always supplied by the caller, never replaced with `datetime.now()` inside a historical transform. It identifies actual cutoff, intended decision slot, clock/mode policy, cohort and run. An aware build additionally requires the same eligible reference-market policy used at serving time.

`FeatureBatch` has ordered rows keyed by `(game_id, decision_at, schema_id)`, ordered numeric names/values, per-row availability grade and blocked reasons, source revision IDs/hashes, feature fingerprint and optional roster tensor. `NcaabFeatureService` and `FrozenFeatureProvider` implement the same `build` contract: the latter requires an exact saved context/schema/fingerprint match and never silently recomputes missing contexts from a current DB. Targets and future outcome columns are not part of `values`. Persisted metadata belongs in `ncaab_feature_builds`; value arrays are immutable files.

## 3. Base schema: `team_box_v1`

One row represents one canonical home-away game. Never create two mirrored rows and then count them as independent validation games. Home/away is canonical and neutral court is explicit.

For each prefix `home_` and `away_`, emit these ordered fields:

1. `season_games` — eligible completed games this season before the decision.
2. `recent_games` — observations in the current-season last-10-game window.
3. `off_eff_l10` — aggregate points per 100 estimated possessions.
4. `def_eff_l10` — aggregate opponent points per 100 estimated possessions.
5. `tempo_l10` — estimated possessions per 40 regulation-equivalent minutes where duration is supported.
6. `efg_l10` — aggregate effective field-goal percentage.
7. `tov_rate_l10` — aggregate verified total turnovers per estimated possession.
8. `orb_rate_l10` — aggregate offensive rebounds divided by offensive rebounds plus opponent defensive rebounds.
9. `ftr_l10` — aggregate free-throw attempts per field-goal attempt.
10. `three_rate_l10` — aggregate three-point attempts per field-goal attempt.
11. `ft_pct_l10` — aggregate free throws made per attempt.
12. `hours_since_last_start` — time since the previous eligible completed game's start, not claimed recovery time since its end.
13. `prev_season_off_eff` — previous season's eligible aggregate offensive efficiency.
14. `prev_season_def_eff` — previous season's eligible aggregate defensive efficiency.
15. `prev_season_tempo` — previous season's eligible pace estimate.

Append `neutral_site`. The resulting independent baseline has 31 ordered numeric fields. Aware schema `team_box_market_v1` appends exactly four fields: `reference_home_spread`, `reference_total`, `reference_spread_home_novig`, `reference_total_over_novig`. It has 35 fields. The schema registry must generate/assert these counts rather than maintain unrelated duplicate lists.

IDs, game dates, raw result scores, final rankings, current player bios, opponent future performance, closing prices and model targets are never auto-selected because they happen to be numeric.

### Calculations and units

For a past game, estimate each team's possessions from `FGA - ORB + total_turnovers + 0.475 * FTA`; use the average of the two teams' estimates as the shared game possession estimate. This is an explicitly versioned college-basketball approximation, not measured play-by-play possessions. Reject nonpositive/impossible inputs; do not clip a corrupted estimate into validity.

Efficiency uses summed points divided by summed shared possessions, times 100. eFG uses `(FGM + 0.5 * 3PM) / FGA`. Rates are ratios of summed numerators/denominators, not unweighted averages of percentages. Zero denominators produce a documented missing value rather than infinity or an invented zero.

Tempo uses total estimated possessions divided by total game minutes, times 40. Game duration comes from verified regulation/OT period information under the men's ruleset; do not infer OT from player minutes. Missing duration makes tempo missing under its schema rule; it does not erase otherwise valid efficiency facts.

The current-season rolling window does not silently cross an offseason. Previous-season fields provide a separately visible prior. Current short windows retain their true count. Source score/count revisions and formula version are part of the fingerprint.

## 4. Missingness, cold starts and cohort

Initial prediction cohort: verified Division-I-versus-Division-I, non-exhibition full games, including neutral sites and supported postseason games. Retain mixed-division source records, but exclude them from baseline feature aggregates and labels unless a later explicit cohort variant changes the policy. Classification is season-specific, not inferred from today's conference.

- Genuine no-prior-game history is a cold start, not a pipeline failure. `season_games`/`recent_games` are zero and unsupported numeric rates are null; XGBoost may handle these declared missing values.
- An expected prior final game inside the approved source window whose required box is missing is incomplete history, not a cold start. Block affected rows or exclude them with counted reasons. History outside the acquired warm-up boundary is separately labeled `history_boundary`; an allowed previous-season null is not a claim that the team did not play.
- Unknown target neutral-site/classification/identity is a block, not a default home-court assumption.
- Player-box failures need not block a team-only schema; they can block a roster schema.
- Neural/scaled models use training-fitted transforms plus explicit masks. Never fit imputation/scaling on the evaluation window.

A `FeatureSpec` declares whether a null is allowed and why. Missing columns, unexpected dtypes, nonfinite values and undeclared defaults are hard schema errors. Do not reproduce the retained trainer behavior that silently intersects requested features with columns present.

## 5. Historical availability algorithm

1. Resolve the schedule/version and competition classification under the selected policy.
2. Read bounded prior team/game facts in a consistent database snapshot.
3. Apply `TimePolicy` eligibility **before** rolling calculations or joins.
4. Exclude the target game and all later outcomes. In strict mode, require supported knowledge clocks. In reconstructed mode, use only earlier ET game dates unless actual same-day observation evidence is present.
5. Select the latest eligible source revision within the pinned dataset policy; never the globally latest correction regardless of time.
6. Build numeric features; attach source revision IDs, null reasons and grade.
7. For aware models only, obtain eligible reference quotes and append market features.
8. Hash the ordered schema, values, source identities, context and transform versions; release DB connections before model computation.

For vectorized operations, group/sort and prior-only windows must be demonstrated with adversarial tests; `shift(1)` alone does not repair a join that already included future revisions. An input file downloaded today is not retroactively a strict pregame observation.

## 6. Market-independent/aware isolation

The independent service path does not invoke `MarketFeatureBuilder` and has no market-derived values in its schema/source lineage. Price changes cannot change its features or predictions for the same basketball context.

For a fair initial comparison, create a common eligible game/decision cohort with available reference spread and total groups, train both variants on those same rows, then remove market columns from the independent input. Cohort eligibility is recorded as metadata, not smuggled into a numeric feature. Separately report the independent model's ability to predict games without quotes.

Reference selection is implemented once in layer 5 and used here; it is not the best-price execution-book selection. A configured ordered reference-book list is required. Missing required reference groups blocks the aware row; do not fill them with a closing line, another date's line or zero. Implied paired probabilities are conditional on no push; retain this meaning in names and metadata.

## 7. Matchup and dynamic-state extensions

`MatchupFeatureBuilder` owns named experimental families, not arbitrary unchecked products. Initial families to scope are turnover-pressure, rebounding, shooting profile, foul tendency and pace interaction. Opponent defensive metrics must be built from prior opponent counts; they are not interchangeable with a team's offensive rates.

Each family defines required inputs, formula, units, null policy and ordered output list. First evaluate a whole family with force-include/force-exclude under fixed hyperparameters; selector rejection is not proof that it lacks betting value.

A `bayes_state_v1` extension adds twelve fields: each `home_`/`away_` prefix followed by `state_offense_mean`, `state_defense_mean`, `state_tempo_mean`, `state_offense_sd`, `state_defense_sd`, `state_tempo_sd`, in that order. Its independent schema `team_box_state_v1` has 43 fields and aware schema `team_box_market_state_v1` has 47. Offense/defense means are on the original points-per-100-possessions scale against a neutral league-average opponent; tempo is possessions per 40 minutes, and SDs use the corresponding units. An internally log-parameterized model must transform moments to those units rather than returning latent log values under these names. It remains market-independent and can feed either predictor variant. Layer 4 owns fitting and versioning global/state parameters; this layer only performs prior-only filtering or reads verified state outputs. Means/SD summaries fed to trees are not a complete Bayesian predictive distribution.

Historical generated features need prequential or cross-fitted state parameters whose information cutoff precedes the row. Fitting state hyperparameters to the whole outer training period and using smoothed past states would leak into those training examples. If early rows lack a valid fitted state provider, record/drop that warm-up cohort and compare the baseline on the same remaining rows.

## 8. Roster tensor extension

`RosterFeatureBuilder` creates an ordered-for-storage but permutation-invariant-in-model player set for each team. Candidate players come from eligible membership observations and prior team appearances, not the target game's final participant list or a roster reconstructed from later appearances.

Roster schema `rotation_history_v1` has this exact player-column order: `expected_minutes_l5`, `minutes_std_l5`, `team_games_observed_l5`, `player_appearances_l5`, `points_per_minute_l5`, `assists_per_minute_l5`, `rebounds_per_minute_l5`, `turnovers_per_minute_l5`, `three_attempts_per_minute_l5`, `free_throw_attempts_per_minute_l5`, `efg_l5`, `hours_since_last_appearance`. Rates use sums over eligible observed team games; minutes variability uses population standard deviation (`ddof=0`) with the observation count retained. Mutable current height, age, experience and current-team fields are excluded until historically valid evidence exists.

Role estimates use the previous five eligible team games, treating an explicitly reported DNP as zero role minutes but an unreported/missing player row as uncertain. Minutes estimates and masks are model inputs, not assertions that we know the future rotation. Normalize pooling weights over the represented player set and report represented-history/completeness grade.

Return `player_features` with twelve raw columns, a per-feature missing mask, `player_mask`, expected-minute weights, team/global context and provenance. The neural preprocessing layer fits imputation/scaling only on visible training players and appends the twelve missing indicators to the encoder input; padding never participates in fitting or pooling. Batch padding is masked; initial safety cap is 64 players per team, with an explicit block rather than silent truncation. Empty rosters or ambiguous membership cannot become an all-zero healthy tensor. `observed_players_only` reconstruction is labeled and does not claim full pregame injury/eligibility knowledge.

A roster encoder can start without PBP. Later NCAA lineup/possession features require separate crosswalk and reconstruction checks; no same-game lineup stint becomes a pregame predictor.

## 9. Export/cache and correction behavior

`DatasetExporter` writes a manifest-last bundle containing `features.parquet`, `labels.parquet`, `schedules.parquet`, `market_snapshots.parquet`, `quotes.parquet`, row/context metadata, source revision inventory, schema/config hashes, dropout report and optional roster arrays. Quote exports preserve both decision and closing observations, but readers always apply the appropriate cutoff; closing rows never enter features. These additional typed snapshots let `FileResearchStore` and `FrozenFeatureProvider` support offline replay without an implicit DB registration or network fetch. Labels include game/result version and observation cutoff but never enter the feature matrix. Training opens the bundle through digest verification.

Cache identity includes game, decision context, schema, transform versions, state artifact if present and source revisions. A late source correction changes the key; do not silently reuse old cached features or overwrite a frozen historical evaluation dataset. Incremental updates must invalidate affected recent-window and previous-season aggregates, not just append rows newer than a maximum date.

Dataset construction should prefetch bounded team/date windows, not query the DB once per feature/player. Keep source snapshots short-lived and stream output in chunks. Persist a manifest only after every file/digest and row-count reconciliation succeeds; `ncaab_feature_builds.status` remains incomplete otherwise.

## 10. Implementation and validation

Implement schema/time policy first, then pure rolling transforms, base service, reference-market integration, immutable export, and optional state/roster families. Do not start with automatic feature selection.

Proposed files: `tests/ncaab/test_feature_schema.py`, `test_feature_time_policy.py`, `test_rolling_features.py`, `test_market_feature_isolation.py`, `test_dataset_export.py`, `test_roster_features.py`, `test_team_state_features.py`.

Required fixtures include future game/result injections; later corrections; same-day games; season transitions; missing expected games versus genuine cold starts; neutral venues; known DNP versus unknown participation; market mutation leaving blind inputs unchanged; feature order/dtype tampering; and historical/live equivalent facts yielding identical features under equivalent context.

Planned command, not yet available:

```powershell
.\venv\Scripts\python.exe -m src.ncaab.cli build-dataset --config configs/ncaab/research.yaml --fold discovery --variant gbt_independent_v1 --dry-run
```

Execution is owner-launched. Acceptance requires source/context/row counts, labels separated from predictors, no future dependencies, verified file digests, correct blocked-row accounting and identical shared-consumer results—not merely a nonempty dataframe.
