# NBA Derived-Feature Readiness Audit — 2026-09-24

**Lane:** A2 derived features  
**Audit time:** 2026-09-24T11:52:16-04:00  
**Repository commit inspected:** `0c5b02f`  
**Mode:** read-only code, test, and bounded SELECT audit; no code/data/model/deployment/index mutations  
**Verdict:** **FAIL — the stable contract constants are correct, ordered, and fully produced by training and single-player loaders, but the active modes are not semantically equivalent. Single-date/date-range batches omit four contract features; date-range defaults and spread direction differ; scheduled inference can carry prior-season values; and player discovery is not viable or temporally safe at season open or around roster changes.** Local and remote derived tables are populated through the end of the 2025–26 data, but that does not establish 2026–27 live-data readiness.

## Scope

Primary code:

- `src/models/feature_store.py`
- `src/models/features/nba/contracts.py`
- `src/models/features/nba/{training_loader,inference_loader,date_batch_loader,date_range_loader}.py`
- focused NBA source/transform modules under `src/models/features/nba/`
- `src/processing/populate_average_stats.py`
- `src/processing/populate_average_stats_incremental.py`
- `src/models/daily_runner.py`
- focused NBA feature tests and source-backed docs

The separate A4 temporal audit owns the full quote/injury decision-time adjudication. This report repeats only temporal issues that directly alter feature or mode semantics.

## Relevant prior lessons and invariants

- Rolling features must use prior games only (`shift(1)`).
- NBA rolling-stat minimum minutes is eight: short appearances are excluded from rolling values while schedule counters still count appearances.
- Training, date, date-range, and single-player modes must produce aligned names and semantics.
- Exact artifact identity outranks an abstract contract when evaluating an old model, but the stable contract remains the required boundary for future loaders/models.
- Existing production artifacts are historical templates, not 2026–27 promotion candidates.

Sources: `AGENTS.md:15-28,47-56`; `docs/operations/nba-core-product-restart-plan.md:17-24,83-111`; GBrain `operations/critical-invariants`, `models/nba-model`.

## Readiness table

| Surface | Code-path readiness | Local data evidence | Remote data evidence | Verdict |
|---|---|---|---|---|
| Stable contract membership/order | Counts and order are centralized and re-exported | Import check passed | Not data-dependent | **READY** |
| Training mode | Produces all 74 stable columns on bounded playoff smoke | 1,594 rows; no contract columns missing | Not re-run remotely | **CODE READY WITH SEMANTIC BLOCKERS** |
| Single-player historical/scheduled mode | Produces all 74 stable columns | One historical and one scheduled smoke passed | Not re-run remotely | **STRUCTURALLY READY; COLD-START/SEASON BLOCKED** |
| Single-date mode | Omits all four same-position injury columns | 195-row and 19-row smokes reproduced omission | Not re-run remotely | **NOT CONTRACT READY** |
| Date-range mode/backtest feed | Omits same-position columns; default and spread semantics differ; partial chunk failures are tolerated | Same defects reproduced on one-day range smokes | Not re-run remotely | **NOT READY** |
| Prior-game basic rolling values | Season-grouped `shift(1)` and eight-minute mask | Recent rows populated/non-default | Same bounded aggregates matched | **READY** |
| Advanced rolling values | Season-grouped `shift(1)`, but no eight-minute mask | Five recent rows were all-zero/default family; broader table populated | Same bounded aggregates matched | **NOT SEMANTICALLY READY** |
| Team/opponent derived context | Populated source tables and strict-before queries in single-player mode | Team family nonzero; positional table through 2026-05-25 | Same dates/counts | **DATA PRESENT; STALE FOR FUTURE SEASON** |
| Line-context feature family | Loader paths exist, but temporal contract is unsafe (A4) | Game-line features were all zero in tested training/date slates; prop lines varied in 2025 playoffs | Not separately profiled here | **NOT READY** |
| Injury context | Single-player/training can generate values; batch modes are incomplete | Injury rows through 2026-06-12; nonzero team/same-position values in historical smoke | Same bounded aggregates matched | **PARTIAL** |
| Season-open/player roster discovery | Uses recent derived rows, not a roster source; target-date upper bound absent | Retrospective opener query selected future rows; real-time opener has no prior-30-day current rows | Code-path failure applies remotely | **NOT READY** |

# Contract verification

## Counts

Executed import/count check against `src/models/features/nba/contracts.py`:

| Contract | Count |
|---|---:|
| Minutes | 33 |
| Points rate | 31 |
| Rebounds rate | 30 |
| Assists rate | 30 |
| Threes rate | 32 |
| Unique across all lists | 74 |

These match `contracts.py:10-220`, `docs/nba_feature_catalog.md:9-20`, and the restart-plan target at `docs/operations/nba-core-product-restart-plan.md:92-100`.

## Exact current order

```text
minutes:
player_avg_min_l5, player_avg_min_l15, player_avg_usg_pct_l5, team_avg_pace_l5, opp_avg_pace_l5, line_spread, line_total, is_home, rest_days, is_back_to_back, games_in_last_7_days, player_avg_min_l3, player_min_l3_l5_ratio, player_min_l3_l15_ratio, player_avg_min_szn, player_min_std_l5, player_min_floor_l5, player_games_started_l5, player_starter_prob, team_out_count, team_out_min_sum, team_out_pts_sum, team_out_reb_sum, team_out_ast_sum, team_out_usg_sum, opp_out_count, opp_out_min_sum, player_is_questionable, player_is_probable, team_out_same_pos_count, team_out_same_pos_min_sum, team_out_same_pos_usg_sum, team_out_same_pos_starter_sum

points rate:
player_avg_pts_l5, player_avg_pts_l15, player_avg_usg_pct_l5, player_avg_ts_pct_l15, team_avg_pace_l5, opp_avg_def_rtg_l5, opp_pos_off_rtg_allowed_l5, opp_pos_off_rtg_allowed_l15, is_home, prop_line_pts, rest_days, is_back_to_back, games_in_last_7_days, player_avg_pts_l3, player_pts_l3_l15_ratio, player_std_pts_l5, team_out_count, team_out_min_sum, team_out_pts_sum, team_out_reb_sum, team_out_ast_sum, team_out_usg_sum, opp_out_count, opp_out_min_sum, player_starter_prob, player_min_l3_l5_ratio, player_min_l3_l15_ratio, team_out_same_pos_count, team_out_same_pos_min_sum, team_out_same_pos_usg_sum, team_out_same_pos_starter_sum

rebounds rate:
player_avg_reb_l5, player_avg_reb_pct_l5, opp_pos_reb_allowed_l5, opp_pos_reb_per100_allowed_l5, opp_pos_reb_allowed_l15, team_avg_pace_l5, opp_avg_pace_l5, is_home, prop_line_reb, rest_days, is_back_to_back, games_in_last_7_days, player_avg_reb_l3, player_reb_l3_l15_ratio, player_std_reb_l5, team_out_count, team_out_min_sum, team_out_pts_sum, team_out_reb_sum, team_out_ast_sum, team_out_usg_sum, opp_out_count, opp_out_min_sum, player_starter_prob, player_min_l3_l5_ratio, player_min_l3_l15_ratio, team_out_same_pos_count, team_out_same_pos_min_sum, team_out_same_pos_usg_sum, team_out_same_pos_starter_sum

assists rate:
player_avg_ast_l5, player_avg_ast_pct_l5, player_avg_usg_pct_l5, opp_pos_ast_allowed_l5, opp_pos_ast_per100_allowed_l5, opp_pos_ast_allowed_l15, team_avg_pace_l5, is_home, prop_line_ast, rest_days, is_back_to_back, games_in_last_7_days, player_avg_ast_l3, player_ast_l3_l15_ratio, player_std_ast_l5, team_out_count, team_out_min_sum, team_out_pts_sum, team_out_reb_sum, team_out_ast_sum, team_out_usg_sum, opp_out_count, opp_out_min_sum, player_starter_prob, player_min_l3_l5_ratio, player_min_l3_l15_ratio, team_out_same_pos_count, team_out_same_pos_min_sum, team_out_same_pos_usg_sum, team_out_same_pos_starter_sum

threes rate:
player_avg_fg3m_l5, player_avg_fg3a_l5, opp_pos_threes_allowed_l5, opp_pos_threes_per100_allowed_l5, opp_pos_threes_allowed_l15, team_avg_fg3a_l5, team_avg_fg3_pct_l5, opp_avg_fg3a_l5, opp_avg_fg3_pct_l5, team_avg_pace_l5, is_home, prop_line_threes, rest_days, is_back_to_back, games_in_last_7_days, player_avg_fg3m_l3, player_fg3m_l3_l15_ratio, player_std_fg3m_l5, player_min_l3_l5_ratio, player_min_l3_l15_ratio, team_out_count, team_out_min_sum, team_out_pts_sum, team_out_reb_sum, team_out_ast_sum, team_out_usg_sum, opp_out_count, opp_out_min_sum, team_out_same_pos_count, team_out_same_pos_min_sum, team_out_same_pos_usg_sum, team_out_same_pos_starter_sum
```

The old committed production artifact does not define the new stable lists. Its `selected_features.json` contains broader per-quantile historical selections, and the production directory has minutes/points/rebounds/assists models but no threes model. Treat it only as an incumbent runtime template (`src/models/artifacts/production/run_config.json:2-9`).

# Findings

## P0 — Blockers

### P0-1. Season-open and roster-change player discovery is neither cold-start capable nor target-date safe

`DailyPredictionRunner._get_players_for_games()` discovers expected players solely from `player_average_game_stats` rows for the scheduled teams with `avg_min_l5 >= 10` and `game_date >= target_date - 30 days` (`src/models/daily_runner.py:635-672`). It does not use a current roster table and has no `game_date <= target_date` upper bound.

Consequences:

- At a real season opener after the summer break, no current row exists inside the prior 30 days, so the path cannot discover established players, rookies, or new signees before they first play.
- On historical reruns/shadow checks, future rows can select eligibility and team identity for an earlier slate.
- A traded player remains attached to the last derived-stat team until a game for the new team creates a new row. Before that first appearance the player can be omitted from the new team or associated with the old team.

Bounded local opener probe for 2025-10-21 found four scheduled teams and 76 retrospective candidates, but every candidate row came from the future: candidate dates ranged from 2025-12-31 through 2026-05-30. This proves the missing upper bound; those rows would not have existed in real time.

**Required outcome:** roster/schedule truth must identify candidates as of the target/decision time; prior derived values may be joined afterward. Add a strict target-time upper bound and an explicit cold-start/rookie/trade policy.

### P0-2. Single-date and date-range modes violate the stable feature contract

Both batch loaders omit:

- `team_out_same_pos_count`
- `team_out_same_pos_min_sum`
- `team_out_same_pos_usg_sum`
- `team_out_same_pos_starter_sum`

The fields are present in every stable contract (`contracts.py:47-50,87-90,126-129,165-168,205-208`), produced by training (`training_loader.py:83-121`) and single-player inference (`injury_context.py:165-200,316-360`), but absent from the SELECT/output of `date_batch_loader.py:22-341` and `date_range_loader.py:61-367`.

Local smokes reproduced the defect:

- 2026-03-18: 195 date rows and 195 range rows, each missing the four fields.
- 2026-06-13: 19 date rows and 19 range rows, each missing the same fields.
- Training smoke: 1,594 rows for postseason season `42024`, zero missing stable columns; all four same-position features had nonzero observations.
- Single-player historical and scheduled smokes: zero missing stable columns.

Current committed artifacts happen not to select these new fields, so this does not prove the old production artifact crashes. It does prove that the advertised 74-feature facade and future default-contract backtests are not mode-compatible. Prediction uses exact per-quantile dataframe indexing (`src/models/quantile_trainer.py:264-280`), so an artifact that selects these fields will fail rather than silently align them.

### P0-3. Season-boundary behavior leaks prior-season rolling state into scheduled inference

Rolling population itself correctly resets by `(player_id, season_id)` (`src/processing/populate_average_stats.py:209-242,276-330`). The single-player source queries then discard that boundary: player, team, and opponent sources select the latest row before `as_of_date` without a `season_id` predicate (`player_sources.py:14-45`; `team_sources.py:16-23`; `opponent_sources.py:23-35`).

Before a player's first current-season derived row exists, scheduled inference therefore takes the previous season's last row rather than an explicit cold-start default or approved carryover feature.

Bounded local 2025–26 opener check:

- 37 players had at least five minutes on 2025-10-21.
- 36 had a prior derived row.
- All 36 prior rows were from another season.
- 34 carried a nonzero `avg_min_l5` into the opener candidate state.

This conflicts with the source-level season reset and makes train/serve opener semantics dependent on job timing. A carryover policy could be valid, but it must be explicit, consistently modeled in training, and separately identified; the current accidental latest-row fallback is not ready.

### P0-4. Date-range/backtest semantics differ from training, single-date, and serving

`DateRangeFeatureLoader` is not a semantic equivalent of the other modes:

1. It defaults team/opponent context to zero (`date_range_loader.py:79-85`), while training/date/single-player use priors such as pace 99.5, defensive rating 112.0, attempts 34.0, and percentage 0.36 (`training_loader.py:207-216`; `date_batch_loader.py:48-57`; `team_sources.py:8-13`).
2. It returns raw `COALESCE(lines.spread, 0)` (`date_range_loader.py:97`) rather than the player's team-directional spread. Training/date negate the source spread for home rows (`training_loader.py:235-239`; `date_batch_loader.py:76-80`), and single-player inference performs the same transformation (`inference_loader.py:49-50`).
3. It catches chunk exceptions, logs them, and returns the remaining dates (`date_range_loader.py:317-355`). A backtest can therefore finish on a silently incomplete date set.

The NBA backtest harness prefetches this mode as its primary feature feed (`src/backtesting/backtest_harness.py:163-192`). Consequently, passing tests or successful completion do not establish train/backtest/serve parity.

## P1 — High-priority gaps

### P1-1. The eight-minute rule is applied to basic rolling values but not advanced rolling values

Basic rolling values use shifted minutes `>= 8` before L3/L5/L15/season calculations, including variability, floor, and starter sums (`populate_average_stats.py:219-242,291-317`; incremental equivalent `populate_average_stats_incremental.py:254-299`). Schedule counters still count all appearances (`populate_average_stats.py:320-330`; incremental `:301-317`). This is the intended rule.

Advanced player rolling values do not apply the minutes mask. They shift every advanced row and roll it directly (`populate_average_stats.py:504-530`; incremental `populate_average_stats_incremental.py:326-345`). Therefore `player_avg_usg_pct_l5`, `player_avg_ts_pct_l15`, `player_avg_reb_pct_l5`, and `player_avg_ast_pct_l5` can include short appearances that the basic family excludes.

This is internally consistent across consumers because they read the same tables, but it violates the documented family-level eight-minute invariant and can create mixed-window semantics inside one model row.

### P1-2. Scheduled inference hard-codes the prior season identifier

Scheduled context always returns `season_id = "22025"` (`src/models/features/nba/inference_loader.py:67-82`). The 2026–27 regular-season identifier should be `22026` when that season begins. `season_id` is not in the 74 stable model inputs, so this is not currently a direct model-value defect, but it is stale metadata and can contaminate persistence, diagnostics, and future season-aware logic.

### P1-3. Line-context features are present in code but not live-data ready

Bounded local smokes showed:

- `42024` training smoke (1,594 rows): `line_spread` and `line_total` were zero for every row; prop lines had 983–1,189 nonzero rows depending on stat.
- 2026-03-18 date/range smoke (195 rows): all game-line and prop-line contract columns were zero.
- 2026-06-13 date/range smoke (19 rows): all game-line and prop-line contract columns were zero.

The separate A4 audit also found that training and inference line-feature queries do not enforce one reproducible decision-time contract. Thus the family is simultaneously sparse/defaulted in tested local slates and temporally unsafe where rows exist. Code-path presence is not live-data readiness.

### P1-4. Future 2026–27 data readiness is unproven despite healthy historical derived tables

Bounded SELECT results were materially identical locally and remotely except one unbounded remote count timed out before the narrower query succeeded:

| Table | Local max | Remote max | Notes |
|---|---|---|---|
| `player_game_stats` | 2026-06-13 | 2026-06-13 | 454,467 rows both |
| `player_average_game_stats` | 2026-06-13 | bounded recent query reached 2026-06-13 | remote whole-table count timed out at 15s |
| `player_average_advanced_stats` | 2026-06-13 | 2026-06-13 | 442,087 rows both |
| `team_average_game_stats` | 2026-06-13 | 2026-06-13 | 41,854 rows both |
| `team_allowed_by_position` | 2026-05-25 | 2026-05-25 | behind Finals/end-of-season source dates |
| `player_position_history` | 2026-09-23 | 2026-09-24 | remote one day ahead |
| `rapidapi_injuries` | 2026-06-12 | 2026-06-12 | no preseason/current reports yet |

Recent-table variation was generally real, not globally defaulted: player role values and team context varied; opponent-position rows had only 8/691 all-zero raw/per-100 rows; advanced family had 5/2,434 all-zero rows. These results establish historical population, not opening-night readiness.

## P2 — Test and maintenance gaps

### P2-1. Focused tests certify delegation and helpers, not cross-mode contract parity

All 112 focused NBA feature tests passed, but no test requires every mode to emit all contract columns with equal defaults and directional semantics. `tests/test_nba_mode_loaders.py:28-45` checks request plumbing and deprecated neutral fields only. `tests/test_nba_feature_contracts.py:21-55` validates list ownership/duplicates/families, not loader outputs.

Required regression matrix:

- exact ordered contract membership per mode;
- same-position injury columns in date/range modes;
- identical missing-source defaults;
- home/away spread direction;
- opener/cross-season behavior;
- rookie and pre-first-game traded-player inclusion;
- date-range failure must fail closed or return explicit completeness metadata.

### P2-2. Configuration semantics are partially duplicated or unused

`FeatureConfig.min_games_l5 = 3` is not enforced in the inspected loaders (`feature_store.py:51-57`). `min_minutes_for_rate` controls target construction in training (`training_loader.py:166-169`), while model training separately hard-codes ten minutes (`quantile_trainer.py:453-458`). They currently agree at the default, but changing the config would create divergence.

# Proven-safe components

- Stable feature lists are centralized, duplicate-free, family-owned, and compatibility re-exported (`contracts.py:9-273`; `feature_store.py:10-48`).
- Basic player rolling construction is season-scoped, prior-game only, and excludes sub-eight-minute appearances from rolling values while retaining them in schedule counters (`populate_average_stats.py:209-242,276-330`).
- Incremental basic rolling uses the same prior-game/eight-minute behavior and fetches only the target season (`populate_average_stats_incremental.py:115-237,244-319`).
- Training and single-player modes can currently emit all 74 stable columns in bounded historical smokes.
- Single-player position, player, team, opponent, and injury source queries use strict-before date predicates for derived stats (`context_sources.py:9-30`; `player_sources.py:14-45`; `team_sources.py:16-23`; `opponent_sources.py:23-35`; `injury_context.py:85-200`). The season/cold-start and decision-time caveats remain.
- Traded players' player-level season rolling values naturally follow the player across teams once the new team appearance exists; current-team context and injury context then use the caller's scheduled team/opponent. The blocker is discovery before that first new-team derived row.

# Commands and results

```text
python -c "from src.models.features.nba.contracts import ..."
# minutes=33, pts=31, reb=30, ast=30, threes=32, unique=74; exact order printed above

python -m pytest -q \
  tests/test_nba_feature_contracts.py \
  tests/test_nba_feature_store_inventory.py \
  tests/test_nba_feature_transforms.py \
  tests/test_nba_line_sources.py \
  tests/test_nba_injury_context.py \
  tests/test_nba_inference_loader.py \
  tests/test_nba_mode_loaders.py
# 112 passed, 1 warning in 1.68s

# Bounded SELECT-only local/remote freshness and 60-day variation probe
# Historical table dates/counts summarized above; one remote whole-table count timed out,
# then the bounded recent-window query succeeded.

# Local mode smoke: 2026-03-18 and 2026-06-13
# Date/range modes omitted the same four contract columns; single-player modes did not.

# Local bounded training smoke: get_training_dataset(['42024'])
# 1,594 rows, 94 columns, zero stable-contract columns missing.
```

# Repair order

1. **P0:** Replace derived-row player discovery with target-time roster/schedule truth; add an upper bound and explicit rookie/trade/pre-first-game handling.
2. **P0:** Define one season-opening policy: strict current-season defaults, approved prior-season carryover features, or a hybrid with explicit indicators. Implement identically in training/backtest/serve.
3. **P0:** Make date and date-range loaders emit the complete contract, including same-position injury features.
4. **P0:** Reconcile date-range defaults and team-directional spread semantics with training/date/single-player modes; fail closed on incomplete chunks.
5. **P1:** Apply the eight-minute mask to advanced player rolling values or revise the invariant explicitly and evaluate the resulting contract.
6. **P1:** Derive scheduled `season_id` from schedule/context rather than a literal.
7. **P1:** Repair and temporally certify line-context sources before treating line features as production-ready.
8. **P2:** Add a parameterized cross-mode parity suite and opener/trade/rookie fixtures.

# Unknowns and limitations

- No 2026–27 games, rolling rows, injuries, or executable prop slates exist yet; live new-season feature variation cannot be certified on 2026-09-24.
- Production scheduler execution/logs were not audited in this lane. Code readiness does not prove daily derived refresh automation.
- The bounded smoke did not load every old artifact through a full prediction run; restart policy treats those artifacts as reference templates.
- Same-position contract omissions may not break the current old production artifact because its selected feature lists predate those fields. They remain a stable-facade and future-artifact blocker.
- Quote/injury observation-time integrity and saved-decision reproducibility are adjudicated in `docs/operations/audits/nba-temporal-integrity-audit-2026-09-24.md`.
- Dirty-tree changes were preserved. This report is the only repository file created by this lane.
