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

## Remaining Open Issues

16 of 28 issues have been fixed. The following 12 remain open:

### Medium-term (require more design)

1. **ISS-012** — Move blowout factor outside per-stat loop (dormant — `enabled=False`)
2. **ISS-014** — Extend MC quantile function beyond [0.01, 0.99] for extreme lines
3. **ISS-023** — Split Stage 2 dedup to allow uncorrelated multi-stat bets per player

### Low priority / cosmetic

4. **ISS-017** — Fix misleading ratio column names (`l3_l15_ratio` computes L3/L5)
5. **ISS-018** — Pre-game inference requires game row to exist in `player_game_stats`
6. **ISS-019** — Dead `team_ids` parameter in `_load_injury_features_bulk`
7. **ISS-020** — `validate_features=False` disables XGBoost feature-order safety
8. **ISS-021** — Vectorize slow row-by-row monotonicity enforcement loop
9. **ISS-022** — `prob_over + prob_under != 1.0` strict inequality
10. **ISS-024** — `reset()` doesn't reset `current_bankroll` (dormant)
11. **ISS-025** — Dead `side` parameter in `should_bet()`
12. **ISS-026** — `--workers` CLI arg accepted but parallelism never used
