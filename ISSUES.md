# Known Issues

Comprehensive audit of the core pipeline files. Issues are organized by severity.

---

## CRITICAL

### ISS-001: Minutes model silently ignores tuned hyperparameters

- **File:** `src/models/quantile_trainer.py:374`
- **Status:** Fixed
- **Impact:** All hyperparameter tuning work for the minutes model has zero effect

When hyperparameter tuning is enabled, `train_minutes_model` creates a local `config` from the tuned hyperparams (line 339), but then constructs the model suite with `self.config` (the default config) instead:

```python
# Line 339 - tuned config created correctly
config = QuantileModelConfig.from_dict(hyperparams)

# Line 374 - BUG: uses self.config instead of config
self.minutes_model = QuantileModelSuite(self.config)  # should be config
```

The rate models at line 445 correctly use `QuantileModelSuite(config)`. The log misleadingly says "Using tuned hyperparams..." but the default config is used.

**Fix:** Change `self.config` to `config` on line 374.

---

## HIGH

### ISS-002: `_run_date` returns `None` but callers expect a tuple

- **File:** `src/backtesting/backtest_harness.py:275, 287, 297, 305`
- **Status:** Fixed
- **Impact:** Every date with no data is logged as an error, masks real bugs, silently drops dates

Three early-exit paths return `None` instead of `(None, None)`. The caller at line 150 unpacks `date_preds, date_all_edges = self._run_date(...)`, which raises `TypeError: cannot unpack non-iterable NoneType object`. This is caught by a broad `except Exception` (line 155), so it doesn't crash — but every date with no data is logged as a processing error rather than a normal "no data" condition.

**Fix:** Change `return None` to `return None, pd.DataFrame()` on lines 275, 287, 297, 305.

---

### ISS-003: Non-BL edge calculation uses vigged implied probabilities

- **File:** `src/backtesting/backtest_harness.py:690-694`
- **Status:** Fixed
- **Impact:** Non-BL baseline systematically underestimates edges by ~2-3%, producing fewer bets and smaller Kelly stakes

The non-BL path computes edges against vig-inflated implied probabilities:

```python
merged["implied_over"] = merged["over_odds"].apply(odds_to_prob)
# odds_to_prob(-110) = 110/210 = 0.5238 (vigged, not fair 0.50)
```

The BL path correctly devigs. This makes the no-BL sweep configs artificially worse compared to BL configs, which use devigged market probabilities.

**Fix:** Apply multiplicative devigging (or Shin's method) to implied probabilities in the non-BL path, matching the BL path's approach.

---

### ISS-004: Injury LATERAL JOIN cross-product in feature store

- **File:** `src/models/feature_store.py:470-487, 748-765, 1490-1507`
- **Status:** Fixed
- **Impact:** Performance degradation; potential for picking non-most-recent injury stats

The teammate injury subquery joins `player_average_game_stats` and `player_average_advanced_stats` for each injured player, creating an N×M cross product before `DISTINCT ON` collapses it. For an injured player with 80 games of history in each table, this generates 6,400 intermediate rows per injured player. The `ORDER BY` across both tables can also pick non-most-recent rows when the two tables have different latest dates.

**Fix:** Split into two separate LATERAL subqueries (one for each table) or pre-join the tables in a CTE.

---

### ISS-005: Training `min > 0` vs inference `min >= 5` threshold mismatch

- **File:** `src/models/feature_store.py:1151 vs 520, 796`
- **Status:** Fixed
- **Impact:** Model trained on a population it never scores at inference time, degrading prediction accuracy

Training data includes players with as few as 1 minute (`min > 0`), while inference/backtesting excludes players under 5 minutes (`min >= 5`). Players with 1-4 minutes have very noisy stat rates that skew the model.

**Fix:** Change training query to `AND pgs.min >= 5` to match inference behavior.

---

### ISS-006: Early stopping is configured but never applied in XGBoost training

- **File:** `src/models/quantile_trainer.py:35, 147-153`
- **Status:** Fixed
- **Impact:** Models likely overfit; training takes longer than necessary

`early_stopping_rounds=50` is defined in the config but never passed to `model.fit()` or the `XGBRegressor` constructor. The `eval_set` is specified but serves no purpose without early stopping — the model trains for all 1000 estimators regardless of validation performance.

**Fix:** Pass `early_stopping_rounds=self.config.early_stopping_rounds` to `model.fit()` or the constructor.

---

## MEDIUM

### ISS-007: Combined calibration evaluates the wrong inference path

- **File:** `src/models/train_pipeline.py:401, 627`
- **Status:** Fixed
- **Impact:** Calibration report does not reflect actual production behavior when copula params are used

The calibration evaluation creates `MonteCarloPredictor` without copula params, so it evaluates the legacy independent-sampling path. Production inference uses the copula path. Additionally, copula params are computed (step 5d) after combined calibration (step 5b), so they wouldn't be available even if the code tried to pass them.

**Fix:** Reorder steps so copula params are computed before combined calibration, and pass them to the MonteCarloPredictor.

---

### ISS-008: Spread feature loses team-directional information

- **File:** `src/models/feature_store.py:434-442`
- **Status:** Fixed
- **Impact:** Feature signal is diluted; model must learn to interact `line_spread` with `is_home`

The betting lines LATERAL JOIN uses `MAX(CASE WHEN market_key = 'spreads' THEN line END)` across both home and away sides. This always returns the positive (away) spread value. All players in a game see the same positive spread regardless of which team they are on.

**Fix:** Filter by the player's team when selecting the spread, or add both home and away spreads as separate features.

---

### ISS-009: COALESCE to 0 for team stats where 0 is far from reality

- **File:** `src/models/feature_store.py` (pervasive across all SQL queries)
- **Status:** Fixed
- **Impact:** Extreme outlier feature values for early-season games and rookies

Features like `avg_pace_l5` (typical: 95-110), `avg_def_rtg_l5` (typical: 105-115), `avg_usg_pct_l5` (typical: 0.15-0.35) default to 0 when data is missing. Zero is far outside the normal range and distorts model predictions. The rest_days default of 3 and games_last_7d default of 2 are reasonable in contrast.

**Fix:** Use league-average defaults instead of 0 (e.g., `COALESCE(avg_pace_l5, 99.5)`, `COALESCE(avg_def_rtg_l5, 112.0)`).

---

### ISS-010: Chunk query failures silently swallowed in date range feature fetch

- **File:** `src/models/feature_store.py:813-815`
- **Status:** Fixed
- **Impact:** Silent data loss in backtesting; could produce misleading results

If a chunk query fails (e.g., DB timeout), the error is logged but those dates are silently dropped from the result. The backtest evaluates on an incomplete set of dates with no warning.

**Fix:** Track failed chunks and either raise an error or emit a warning with the count of missing dates.

**Resolution:** Failed chunks are now tracked in `failed_chunks` list. After processing, a `WARNING` log is emitted with the count and indices of failed chunks. See `get_features_for_date_range()` lines 618-897.

---

### ISS-011: Inference path advanced stats JOIN inconsistency with bulk paths

- **File:** `src/models/feature_store.py:1281-1286 vs 395-401`
- **Status:** Fixed
- **Impact:** Train/serve skew for advanced stat features

The single-player inference path joins `player_average_advanced_stats` by exact `game_id`, while the bulk training/backtesting paths use a LATERAL JOIN finding the most recent row by date. If the tables have gaps, inference returns 0 for advanced stats while training returns the most recent available value.

**Fix:** Change the inference path to use a date-based lookup matching the LATERAL pattern: `WHERE paas.player_id = :player_id AND paas.game_date < :as_of_date ORDER BY game_date DESC LIMIT 1`.

---

### ISS-012: Blowout factor applied per-stat inside the loop (when enabled)

- **File:** `src/models/monte_carlo.py:311-312, 427-428`
- **Status:** Open (dormant — `enabled=False` by default)
- **Impact:** When enabled, creates physically impossible per-stat differences in minutes

When blowout simulation is enabled, `_apply_blowout_factor` generates a different random blowout mask for each stat. This means a player could be "blown out" for their points prediction but not rebounds — which is physically impossible since minutes are shared.

**Fix:** Move blowout application outside the stat loop. Compute `minutes_samples` (with blowout) once before the stat loop and reuse it.

---

### ISS-013: Mutable default dictionaries shared by reference

- **File:** `src/models/monte_carlo.py:190-196`
- **Status:** Fixed
- **Impact:** Latent time-bomb — if any code mutates these dicts, it corrupts the global default for all future instances

```python
self.variance_inflation = variance_inflation or DEFAULT_VARIANCE_INFLATION
```

This assigns a reference to the module-level dict, not a copy. No current code mutates these after construction, but the pattern is dangerous.

**Fix:** Use `.copy()`: `self.variance_inflation = dict(variance_inflation or DEFAULT_VARIANCE_INFLATION)`.

**Resolution:** All four config assignments now use `dict()` to create copies. See `MonteCarloPredictor.__init__()` lines 197-207.

---

### ISS-014: MC probability hard-capped at [0.01, 0.99] due to uniform clipping

- **File:** `src/models/monte_carlo.py:653, 705`
- **Status:** Open
- **Impact:** For extreme lines, `prob_over` or `prob_under` returns exactly 0.0 even when the true probability is nonzero

Uniform samples are clipped to [0.01, 0.99], meaning no sample can exceed the extrapolated 99th percentile. This creates a hard floor/ceiling on probabilities, which could cause missed edge detection on extreme lines.

**Fix:** Extend the quantile function to p=0.001 and p=0.999, or use an analytic tail distribution beyond the extrapolated range.

---

### ISS-015: `_filter_best_bets` can discard valid bets on the opposite side from a different bookmaker

- **File:** `src/backtesting/backtest_harness.py:340-356`
- **Status:** Fixed
- **Impact:** Lost betting opportunities when best over and best under come from different bookmakers

Line shopping picks one row per player/stat based on `max(over_edge, under_edge)`. If Bookmaker A has the best over line and Bookmaker B has the best under line, only the one with the higher single-side edge is kept. The opposite-side bet from the other bookmaker is permanently lost.

**Fix:** Split line shopping into two stages — select best over line and best under line independently, then combine.

---

### ISS-016: Calibration evaluation swallows failures at DEBUG level

- **File:** `src/models/train_pipeline.py:435-437`
- **Status:** Fixed
- **Impact:** A subtle feature mismatch could cause widespread prediction failures, and the calibration report would silently report on the surviving subset

Prediction failures during combined calibration are logged at `DEBUG`, which is invisible at the configured `INFO` level. If many predictions fail, the calibration report is based on a biased survivor sample with no indication to the user.

**Fix:** Track failure count and log a `WARNING` at the end: "N of M predictions failed during calibration."

---

## LOW

### ISS-017: Ratio column names say "l15" but compute L3/L5

- **File:** `src/models/feature_store.py:325-333, 627-635, 1026-1034, 1342-1352`
- **Status:** Open
- **Impact:** Naming inconsistency; could mislead future developers (consistent across all paths so no functional bug)

Columns like `player_reb_l3_l15_ratio` actually compute `avg_reb_l3 / avg_reb_l5`, not L3/L15. Only `player_pts_l3_l15_ratio` actually uses L15 in the denominator. The code comment acknowledges this intentional design but the column names remain misleading.

---

### ISS-018: Pre-game inference requires game to already exist in `player_game_stats`

- **File:** `src/models/feature_store.py:1247-1260`
- **Status:** Open
- **Impact:** Live inference before a game is played silently returns None unless game rows are pre-populated

`_get_context_snapshots` queries `player_game_stats` for the target game. For true pre-game inference, the game hasn't been played yet, so there is no row. The function returns `None`, and `get_player_game_features` returns `None`.

---

### ISS-019: `team_ids` parameter in `_load_injury_features_bulk` is dead code

- **File:** `src/models/feature_store.py:1166`
- **Status:** Open
- **Impact:** Dead code; minor unnecessary data loading

The method signature accepts `team_ids` but the SQL query never uses it, and the caller always passes `[]`.

---

### ISS-020: `validate_features=False` disables XGBoost feature-order safety check

- **File:** `src/models/quantile_trainer.py:256`
- **Status:** Open
- **Impact:** If a feature ordering mismatch occurs between training and inference, there is no safety net

This works around a pandas 3.0 compatibility issue. The comment states "Feature order is guaranteed correct by the caller," but this is an implicit contract with no enforcement.

---

### ISS-021: Monotonicity enforcement is a slow row-by-row Python loop

- **File:** `src/models/quantile_trainer.py:265-287`
- **Status:** Open
- **Impact:** Performance degradation for batch predictions (~300+ players × 5 models)

Each non-monotonic row instantiates a new `IsotonicRegression` object and fits/transforms 5 data points. With hundreds of players, this is extremely slow due to the classic "slow pandas iterrows" anti-pattern.

**Fix:** Vectorize using numpy operations.

---

### ISS-022: `prob_over + prob_under != 1.0` (strict inequality)

- **File:** `src/models/monte_carlo.py:33-37`
- **Status:** Open
- **Impact:** Negligible in practice (continuous samples + half-integer lines), but API contract is inconsistent

`prob_over` uses strict `>` and `prob_under` uses strict `<`. Samples exactly equal to the line are counted in neither. The backtest harness uses `1 - over_prob` instead, which is inconsistent with calling `prob_under()` directly.

---

### ISS-023: Stage 2 dedup keeps only one stat per player per game

- **File:** `src/backtesting/backtest_harness.py:352-355`
- **Status:** Open
- **Impact:** Discards potentially valid uncorrelated stat bets (e.g., both pts-over and ast-under)

The filter drops duplicates on `["player_id", "game_id"]`, keeping only the highest-edge stat. Pts and ast may not be highly correlated, and the code doesn't account for side direction.

---

### ISS-024: `reset()` clears bets but doesn't reset `current_bankroll`

- **File:** `src/backtesting/bet_simulator.py:420-423`
- **Status:** Open (dormant — `reset()` is not called anywhere in production code)
- **Impact:** If `reset()` is ever used, the bankroll carries over from the previous run

```python
def reset(self) -> None:
    """Clear all bets."""
    self.bets = []  # does not reset self.current_bankroll
```

---

### ISS-025: `side` parameter in `should_bet()` is accepted but never used

- **File:** `src/backtesting/bet_simulator.py:148`
- **Status:** Open
- **Impact:** Dead parameter; no functional bug

---

### ISS-026: `--workers` CLI arg is accepted but parallelism is never used

- **File:** `src/backtesting/run_backtest.py:76`
- **Status:** Open
- **Impact:** Misleading to users; the harness always runs sequentially

The `args.workers` value is passed to `harness.run(max_workers=args.workers)`, but the harness `run()` method never uses the `max_workers` parameter.

---

### ISS-027: No date format validation at CLI level

- **File:** `src/backtesting/run_backtest.py:51-52`
- **Status:** Fixed
- **Impact:** Invalid date strings fail deep in the harness with confusing tracebacks

The `--start` and `--end` arguments are parsed as raw strings. Invalid formats like `01-2024-15` are not caught until much later.

**Fix:** Use `type=lambda s: datetime.strptime(s, "%Y-%m-%d").date()` in argparse.

**Resolution:** Added `parse_date()` helper function and used `type=parse_date` for `--start` and `--end` arguments. Invalid dates now produce a clear error message at parse time.

---

### ISS-028: No validation on `--bl-tau` range; negative values silently invert blending

- **File:** `src/backtesting/run_backtest.py:84-88`
- **Status:** Fixed
- **Impact:** Negative tau produces mathematically inverted blending (posterior moves away from the model)

The `BLConfig` dataclass has no validation. A negative tau produces a negative weight in `w = min(self.config.tau * confidence, self.config.max_weight)`, which inverts blending direction.

**Fix:** Add `if tau < 0: raise ValueError` in `BLConfig.__post_init__` or at argparse level.

**Resolution:** Added validation after `parser.parse_args()` to check that `--bl-tau` and `--bl-sizing-tau` are non-negative, using `parser.error()` for clear CLI feedback.

---

---

## DATA VAULT — CRITICAL

### ISS-029: Incremental script does NOT update player advanced stats

- **File:** `src/processing/populate_average_stats_incremental.py`
- **Status:** Fixed
- **Impact:** The entire Advanced tab in the Data Vault (ORtg, DRtg, USG%, TS%, eFG%, AST%, AST/TO, TOV%, REB%, OREB%, DREB%, Pace, PIE) shows stale data

The daily cron job (Step 6 in `daily_stats_job.py`) only runs `populate_average_stats_incremental.py`, which upserts to `player_average_game_stats` only. The `player_average_advanced_stats` table is never updated incrementally — it was last populated whenever the full `populate_average_stats.py --table player_advanced` was manually run. Since `player_stats_latest` view JOINs both tables, advanced columns are stuck at their last full-backfill values.

**Fix:** Either:
- (A) Add incremental advanced stats processing to `populate_average_stats_incremental.py` (fetch from `player_game_advanced_stats`, compute rolling averages, upsert to `player_average_advanced_stats`), or
- (B) Add a separate `populate_average_advanced_stats_incremental.py` script and wire it into `daily_stats_job.py` as a new step after Step 6.

---

### ISS-030: Incremental script does NOT update team stats

- **File:** `src/processing/populate_average_stats_incremental.py`, `src/orchestration/daily_stats_job.py`
- **Status:** Fixed
- **Impact:** The entire Teams tab in the Data Vault (all 3 sub-tabs: Offense, Defense, Overall) shows stale data

Same issue as ISS-029 but for team stats. `team_average_game_stats` is only populated by the full `populate_average_stats.py --table team`. There is no incremental team stats step in the daily cron pipeline. The `team_stats_latest` view shows values frozen at the last full backfill.

**Fix:** Create a `populate_team_stats_incremental.py` (or extend the existing incremental script) and add it as a new step in `daily_stats_job.py`.

---

### ISS-031: SZN expanding averages are wrong in incremental script

- **File:** `src/processing/populate_average_stats_incremental.py:172-175`
- **Status:** Fixed
- **Impact:** `avg_*_szn` values are inaccurate for any player with >20 games this season — shows ~20-game average instead of true season average

The incremental script fetches only the last `LOOKBACK_GAMES=20` games per player. The SZN columns use `shifted.expanding(min_periods=1).mean()` over those 20 games. For a player 50+ games into the season, this computes the expanding average over the last ~19 prior games, NOT the true full-season average. The L5 and L15 windows are correct (they only need 5/15 games of lookback), but the SZN window is fundamentally broken.

Example: LeBron with 36 season games. The script fetches games 17-36. `avg_pts_szn` = expanding mean of games 17-35 (shifted) = ~20-game average, NOT the 35-game season average.

**Fix:** Either:
- (A) Increase `LOOKBACK_GAMES` to cover the full season (~82, but defeats the "lightweight" purpose), or
- (B) Fetch the true season-to-date average from the full backfill table and only compute L5/L15 incrementally, or
- (C) Compute SZN from two components: the existing full-backfill SZN average (weighted by prior game count) and the new games (weighted by new game count) — a running average update formula: `new_szn_avg = (old_szn_avg * old_count + sum_new_games) / new_count`

---

## DATA VAULT — HIGH

### ISS-032: `games_szn` count query uses wrong season_id for cross-season lookback

- **File:** `src/processing/populate_average_stats_incremental.py:120-132`
- **Status:** Fixed
- **Impact:** Early in the season (first ~20 games, Oct-Nov), `games_szn` and `game_number` could go negative

The count query added in the games_szn fix uses a single subquery to determine the current season:
```sql
AND season_id = (
    SELECT season_id FROM player_game_stats
    WHERE player_id IN {player_tuple}
    ORDER BY game_date DESC LIMIT 1
)
```

Two problems:
1. **Cross-season lookback:** The main ranked_games CTE does not filter by season_id — it takes the last 20 games regardless of season. Early in the season (e.g., 5 games played), the lookback includes 15 games from last season. But `total_season_games` only counts current-season games (5). Offset = 5 - 20 = -15, making `game_number` start at -14 and `games_szn` negative.
2. **Single season for all players:** The subquery picks one season from any player in the tuple. While all players played on the same date and should share the same season, this is fragile.

**Fix:** Either filter the main CTE by season_id too, or handle the case where offset < 0 by clamping to 0. Also change the subquery to be per-player rather than global.

---

### ISS-033: Defense "Totals" tab values are cumulative sums, not per-game averages

- **Files:** `src/processing/backfill_opponent_allowed.py:100`, `dashboard/src/lib/stats/columns.ts:107-119`
- **Status:** Fixed
- **Impact:** Defense Totals tab shows raw cumulative sums (e.g., ~200 total guard PTS over L5) — harder to interpret and misleading for heatmap comparison when games_count varies

Both the full and incremental opponent-allowed scripts use `.rolling(...).sum()` (not `.mean()`) for all stat columns. This means `pts_allowed_l5` = total points allowed to that position group over the last 5 games, not a per-game average.

Issues:
- The heatmap percentile comparison is biased when teams have different `games_{window}` counts (e.g., 3 vs 5 games early season)
- Raw sums (100-200 range) are harder for users to interpret than per-game averages (20-40 range)
- The tooltips added in the recent UI update say "Per Game" which is factually wrong

**Fix options:**
- (A) Change rolling from `.sum()` to `.mean()` in both backfill scripts (breaking change — requires full re-backfill and may affect per-100 rate calculations)
- (B) Divide by games count in the frontend: `value / games_{window}` for display
- (C) Keep sums but fix tooltips to say "Total" and add a note to the UI

---

### ISS-034: Defense tooltips say "Per Game" but data is cumulative sums

- **File:** `dashboard/src/lib/stats/columns.ts:107-119`
- **Status:** Fixed
- **Impact:** Tooltips mislead users about what the numbers represent

All defense totals column tooltips added in the recent UI update use "Per Game" language (e.g., `tooltip: 'Points Allowed Per Game'`) but the underlying data from `team_allowed_by_position` uses rolling sums, not rolling means. The values represent totals over the window, not per-game averages.

**Fix:** Update tooltips to say "Total ... Allowed (L5/L15/SZN)" or divide by games count. Dependent on ISS-033 resolution.

---

## DATA VAULT — MEDIUM

### ISS-035: Team TOV Ratio displayed without % sign, inconsistent with Player TOV%

- **File:** `dashboard/src/lib/stats/columns.ts:102`
- **Status:** Fixed
- **Impact:** User confusion — same underlying metric displayed differently across tabs

Player Advanced tab: `TOV%` uses `rawPct1` format → displays "10.3%"
Team Overall tab: `TOV Ratio` uses `dec1` format → displays "10.3"

Both reference `avg_tov_ratio_{window}` which stores the NBA API's `turnover_ratio` as a raw percentage (10.29 = 10.29%). While team tab labels it "TOV Ratio" (technically a ratio), the value IS a percentage and should display with a % sign for clarity.

**Fix:** Change team `tov_ratio` format from `'dec1'` to `'rawPct1'`. Consider also renaming label to "TOV%" for consistency.

---

### ISS-036: Supabase view definitions not in version control

- **Files:** `sql/views/player_stats_latest.sql`, `sql/views/team_stats_latest.sql`, `sql/views/defense_by_position_latest.sql`
- **Status:** Fixed
- **Impact:** No source of truth for view logic; hard to audit, debug, or reproduce

The three Data Vault views (`player_stats_latest`, `team_stats_latest`, `defense_by_position_latest`) are documented in ARCHITECTURE.md but their actual SQL definitions are not tracked in the repository. If a view has incorrect JOIN logic, wrong DISTINCT ON ordering, or missing columns, there's no way to verify without logging into Supabase dashboard.

**Fix:** Export view definitions to a `sql/views/` directory and track them in git. Add a migration or setup script that can recreate them.

---

### ISS-037: Incremental SZN rolling window doesn't match full backfill behavior

- **File:** `src/processing/populate_average_stats_incremental.py:172-175` vs `src/processing/populate_average_stats.py:78-85`
- **Status:** Fixed (related to ISS-031, different angle)
- **Impact:** After incremental update, a player's SZN averages differ from what a full backfill would produce

Beyond the accuracy issue (ISS-031), the incremental script uses a different code path for expanding windows. The full script uses `rolling_with_groupby()` with proper group boundaries via `groupby(group_cols)`. The incremental script processes one player at a time with bare `shifted.expanding().mean()` — no season boundary enforcement. If the 20-game lookback spans two seasons, the expanding average bleeds across season boundaries.

The full script groups by `["player_id", "season_id"]`, ensuring season-to-date is truly within-season. The incremental script has no such guard.

**Fix:** Add season_id filtering to the incremental script's main query, or detect season boundaries in the fetched data and reset the expanding window at season transitions.

---

---

## BACKTESTING / INFERENCE — CRITICAL

### ISS-038: Backtesting uses game-day odds (lookahead bias)

- **File:** `src/backtesting/backtest_harness.py:499, 591`
- **Status:** Fixed
- **Impact:** Backtesting edges are systematically inflated because the backtest uses odds that were NOT available at prediction time

Both lines queries in the backtest harness use:
```sql
AND snapshot_time::date <= :game_date
```

This includes odds snapshots from **game day itself** (e.g., 10 AM, 2 PM, or even 6 PM ET on game day). But in production, `inference_job.py` runs at **6:30 PM ET** using only odds scraped by the 6:00 PM ET `lines_job`. The backtest selects the **latest** snapshot (`ORDER BY snapshot_time DESC`, `rn = 1`), so it preferentially picks the most recent game-day snapshot — exactly the one most likely to reflect late-breaking info (injury announcements, sharp money, etc.) that the live system never sees.

**Why this matters:**
- Late-breaking injury news at 6:45 PM ET causes significant line movement
- Sharp bettors moving lines at 7 PM ET adjust odds the model never saw
- The backtest captures these post-decision lines, making edges appear larger than reality

**Fix:** Use a timestamp-level cutoff matching production:
```sql
AND rp.snapshot_time < (:game_date::timestamp + interval '18 hours 30 minutes')
```
Or conservatively use `< :game_date` (only pre-game-day snapshots). The right choice depends on whether sufficient line data exists from the day before.

---

## BACKTESTING / INFERENCE — HIGH

### ISS-039: Scheduler has no job dependency enforcement

- **File:** `src/orchestration/scheduler.py`
- **Status:** Fixed
- **Impact:** If `daily_stats_job` fails at 9 AM, `inference_job` still runs at 6:30 PM with stale rolling averages

Each cron job runs independently with no awareness of whether upstream jobs succeeded:
- 9:00 AM: `daily_stats_job` — scrape results, update rolling averages
- 6:30 PM: `inference_job` — generate predictions using rolling averages

If the 9 AM job fails (DB timeout, API outage, Railway restart), the 6:30 PM job runs anyway using **yesterday's** rolling averages. Predictions use stale features with no warning. Similarly, if `lines_job` fails at all 3 scheduled times (12 PM, 4 PM, 6 PM), `inference_job` uses stale odds.

**Fix:** Add dependency checking — either:
- (A) Have `inference_job` check for a "daily_stats completed" marker before proceeding
- (B) Have the scheduler skip downstream jobs if upstream failed
- (C) At minimum, log a WARNING in `inference_job` if rolling averages haven't been updated today

---

### ISS-040: `run_daily.py` and `run_sweep.py` missing combined calibration offsets wiring

- **Files:** `src/orchestration/run_daily.py:151`, `src/backtesting/run_sweep.py:742`
- **Status:** Open (dormant — offsets file not deployed to production)
- **Impact:** If combined calibration offsets are ever re-enabled, these two paths would silently skip recalibration

The primary production path (`inference_job.py:155-161`) and the main backtest path (`run_backtest.py:183-193`) correctly load and pass `combined_calibration_offsets` to `MonteCarloPredictor`. However, two secondary paths do not:

1. `run_daily.py:151` — legacy orchestrator, missing import and parameter
2. `run_sweep.py:742` — sweep runner, missing import and parameter

Currently a no-op because `combined_calibration_offsets.json` was intentionally removed from production (hurt betting ROI). But if offsets are re-enabled, these paths would produce different predictions.

**Fix:** Add `load_combined_calibration_offsets` import and pass to both `MonteCarloPredictor()` calls.

---

### ISS-041: Incremental player stats upsert uses row-by-row loop instead of batch

- **File:** `src/processing/populate_average_stats_incremental.py:450-452`
- **Status:** Fixed
- **Impact:** Performance — incremental job takes 10-50x longer than necessary

```python
with engine.begin() as conn:
    for _, row in insert_df.iterrows():
        params = {c: (None if pd.isna(row[c]) else row[c]) for c in cols}
        conn.execute(text(upsert_sql), params)
```

This loops row-by-row using `iterrows()` (notoriously slow in pandas). The full backfill version uses batch operations. For a typical day with ~150 players × 3 tables = ~450 upserts, this adds unnecessary latency to the daily cron.

**Fix:** Use `executemany()` or convert to a batch VALUES clause.

---

## BACKTESTING / INFERENCE — MEDIUM

### ISS-042: View `DISTINCT ON` missing deterministic tiebreaker

- **Files:** `sql/views/player_stats_latest.sql`, `sql/views/team_stats_latest.sql`, `sql/views/defense_by_position_latest.sql`
- **Status:** Fixed
- **Impact:** If duplicate rows exist for the same (player, date), view returns a non-deterministic result

All three views use:
```sql
SELECT DISTINCT ON (player_id) *
FROM player_average_game_stats
WHERE season_id = '22025'
ORDER BY player_id, game_date DESC
```

If two rows have the same `game_date` for the same `player_id` (e.g., from a re-backfill), PostgreSQL picks one arbitrarily. Adding `game_id DESC` as a tiebreaker ensures deterministic results:
```sql
ORDER BY player_id, game_date DESC, game_id DESC
```

**Impact is low** since duplicate same-date rows shouldn't exist, but it's a correctness guard.

---

### ISS-043: `created_at` column overwritten on every UPSERT

- **Files:** `src/processing/backfill_opponent_allowed.py:234`, `src/processing/backfill_opponent_allowed_incremental.py:286`
- **Status:** Fixed
- **Impact:** Audit trail lost — `created_at` reflects last upsert, not original insertion

```sql
ON CONFLICT ... DO UPDATE SET ..., created_at = NOW();
```

Every re-upsert resets `created_at` to the current timestamp. If you need to track when a row was **first** inserted vs. last updated, this destroys that information.

**Fix:** Either remove `created_at = NOW()` from the UPDATE clause (keep original), or add a separate `updated_at` column.

---

## Remaining Open Issues

30 of 43 total issues have been fixed. 13 remain open:

### High — Dormant Code

1. **ISS-040** — `run_daily.py` and `run_sweep.py` missing combined offsets wiring (dormant — not used in production)

### Medium-term (require more design)

2. **ISS-012** — Move blowout factor outside per-stat loop (dormant — `enabled=False`)
3. **ISS-014** — Extend MC quantile function beyond [0.01, 0.99] for extreme lines
4. **ISS-023** — Split Stage 2 dedup to allow uncorrelated multi-stat bets per player

### Low priority / cosmetic

5. **ISS-017** — Fix misleading ratio column names (`l3_l15_ratio` computes L3/L5)
6. **ISS-018** — Pre-game inference requires game row to exist in `player_game_stats`
7. **ISS-019** — Dead `team_ids` parameter in `_load_injury_features_bulk`
8. **ISS-020** — `validate_features=False` disables XGBoost feature-order safety
9. **ISS-021** — Vectorize slow row-by-row monotonicity enforcement loop
10. **ISS-022** — `prob_over + prob_under != 1.0` strict inequality
11. **ISS-024** — `reset()` doesn't reset `current_bankroll` (dormant)
12. **ISS-025** — Dead `side` parameter in `should_bet()`
13. **ISS-026** — `--workers` CLI arg accepted but parallelism never used
