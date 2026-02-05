# GameFlowData — Roadmap

## Session Summary (2026-02-05 — Session 12)

### What We Did

**Implemented lightweight incremental linker.** Added `incremental` command to `nba_linker_local.py` for daily automated linking without downloading the full 25M+ row `raw_player_props_combined` table.

**Changes to `src/processing/nba_linker_local.py`:**
- Added `link_incremental()` function (~240 lines)
- Added `normalize_player()` at module level (was previously a local function)
- Expanded `TEAM_NAME_ALIASES` to map all team names to 3-letter abbreviations (e.g., "Atlanta Hawks" → "ATL")
- Added `--batch-size` and `--limit` CLI arguments
- Incremental mode: queries only unlinked records (`WHERE player_id IS NULL`), matches against reference tables, updates directly via batched SQL

**Changes to `src/orchestration/run_daily.py`:**
- Fixed broken linker call on line 114 (was missing command argument)
- Changed to use `incremental` command for automated daily pipeline

**Test updates:**
- Updated `test_normalize_team_aliases` to expect 3-letter abbreviations
- All 518 tests pass (5 pre-existing failures unrelated to this work)

**Test results:**
- Player match rate: 99.3% (4,963/5,000)
- Game match rate: 40.7% (2,037/5,000) — lower because many props are for future games not yet in DB
- Total unlinked: ~2.8M records

### Next Step

1. **Retrain models** — Run training pipeline to generate hurdle model for THREES
2. **Validate C3** — Check THREES calibration gaps < 5% on holdout
3. **Run full incremental linker** — Link all ~2.8M unlinked combo market records
4. **E6** — Automate daily scheduling

---

## Session Summary (2026-02-05 — Session 11)

### What We Did

**Fixed BL confidence function (A3b).** Replaced the exponential confidence formula with linear ramp to enable meaningful BL blending weights for realistic betting edges.

**Implemented THREES hurdle model (C3).** Two-stage architecture for zero-inflated THREES distribution that can correctly predict Q0.10 = 0 when ~35% of samples are exactly zero.

**Changes to `src/models/black_litterman.py`:**
- Added `z_max` parameter to `BLConfig` (default 1.0)
- Changed `compute_confidence()` from `1 - exp(-0.5 * z²)` to `min(z / z_max, 1.0)`
- Added `--z-max` to sweep parameter grid

**Changes to `src/models/quantile_trainer.py`:**
- Added `HurdleQuantileModel` dataclass with two-stage architecture
- Added `train_hurdle_model()` function for training hurdle models
- Modified `PlayerPropsModelPipeline.train_rate_models()` to use hurdle for THREES
- Updated `save_all()` and `load_all()` to handle hurdle model artifacts
- Hurdle model includes isotonic regression calibration for P(zero) classifier

**Changes to `src/models/monte_carlo.py`:**
- Added `_sample_hurdle()` and `_sample_hurdle_from_quantiles()` methods
- Modified `_predict_copula()` to detect and use hurdle models
- Modified `predict_batch_for_date()` to handle hurdle models
- Bernoulli draw (zero vs positive) is independent of copula; copula affects positive rate magnitude only

**Changes to `src/models/train_pipeline.py`:**
- Added `_calibrate_hurdle_model()` method with zero-accuracy diagnostics
- Updated `_evaluate_calibration()` to evaluate hurdle models
- Updated `_evaluate_combined_calibration()` to include hurdle stats

**Test updates:**
- All 42 BL tests pass
- 518 of 523 tests pass (5 pre-existing failures unrelated to hurdle model)

**Impact:** THREES Q0.10 should now correctly return 0 when P(zero) > 0.10, fixing the +20.4% calibration gap. Enables betting on 4th stat.

### Next Step

1. **Retrain models** — Run training pipeline to generate hurdle model for THREES
2. **Validate C3** — Check THREES calibration gaps < 5% on holdout
3. **E6** — Automate daily scheduling

---

## Session Summary (2026-02-05 — Session 10)

### What We Did

**Reviewed project status and added `--bl-sizing-tau` parameter.** Implemented BL-blended probabilities for Kelly position sizing (separate from edge detection):

- Added `--bl-sizing-tau` CLI parameter to `run_backtest.py`
- Added `bl_sizing_blender` field to `BacktestHarness`
- Modified `_calculate_edges()` to compute `sizing_prob_over`/`sizing_prob_under` columns
- Modified `BetSimulator.place_bet()` to accept optional `sizing_prob` for Kelly calculation
- Verified implementation with one-day backtest (same bet count, different stakes)

**Key finding:** Model is no longer catastrophically overconfident (Brier improved from 0.2705 to 0.2506). The no-BL approach shows +3.5% ROI. However, the BL confidence function issue persists — it crushes sizing probs toward market, resulting in near-zero Kelly stakes even with BL sizing enabled.

**Verified testing scripts are still valid:**
- `src/models/analyze_calibration_drift.py` — minutes-rate correlation, combined calibration
- `src/models/analyze_minutes_bimodality.py` — spread/blowout handling
- `src/backtesting/visualize_results.py` — HTML dashboard generation

**Created specs for next items:**
- `.session/specs/A3b_BL_confidence_fix.md` — Linear ramp confidence function
- `.session/specs/C3_THREES_hurdle_model.md` — Zero-inflated model for THREES

**Verified combo markets scraping job (2026-01-31):** Confirmed ~35K new prop lines successfully added to `raw_player_props_combined`:
- `player_points_rebounds_assists` (12K rows)
- `player_points_rebounds` (8K rows)
- `player_points_assists` (6K rows)
- `player_rebounds_assists` (5K rows)
- `player_blocks_steals` (3K rows)
- `player_field_goals` (2K rows)

### Next Step

1. **A3b** — Fix BL confidence function (linear ramp) — Quick win, enables BL for sizing/filtering
2. **C3** — Implement THREES hurdle model — Enables betting on 4th stat
3. **E6** — Automate daily scheduling

---

## Session Summary (2026-02-04 — Session 9 continued)

### What We Did

**Built paper trading infrastructure (E5).** Created standalone CLI scripts to convert stored `daily_predictions` into paper bets with bet selection, outcome resolution, and P&L tracking.

**Implementation:**
- **Database tables:** `paper_bets` (individual bets) and `paper_trading_daily_log` (daily aggregates)
- **`src/paper_trading/paper_trader.py`** — Core `PaperTrader` class with `select_bets()`, `place_bets()`, `resolve_bets()` methods
- **`src/paper_trading/place_bets.py`** — CLI to place paper bets (supports `--dry-run`, `--edge-threshold`, `--kelly-fraction`)
- **`src/paper_trading/resolve_bets.py`** — CLI to resolve bets using actual game results
- **Unit tests:** 20 tests covering Kelly calculation, bet selection logic, resolution, and defaults

**Design decisions:**
- Standalone scripts (not integrated into `run_daily.py`) for future lightweight dashboard
- Supports pts, reb, ast stats only
- SQL tables for dashboard display

**Earlier in session:** Fixed daily injury pipeline (E4) — `--scrape-injuries` now uses RapidAPI + linker.

### Next Step

With E4 and E5 complete:
1. Retrain models with calibration fixes (E1b)
2. Automate daily scheduling (E6)
3. Paper trade for 2-4 weeks (E7)

---

## Session Summary (2026-01-31 — Session 8)

### What We Did

**Calibration fixes for zero-inflated distributions.** Training showed THREES rate model Q0.10 with +20.4% calibration gap (coverage 0.352 vs target 0.10). Root cause: zero-inflated distribution where 35%+ of `threes_per_min` is exactly 0. Three fixes applied:
1. **Conformal recalibration** (`quantile_trainer.py`) — computes validation residual offset when coverage gap > 3%, applied at prediction time. Standard technique in probabilistic forecasting.
2. **Zero-snap handling** (`monte_carlo.py`) — snaps quantile values below 1e-3 to exactly 0 in `_build_extended_quantile_fn()`. Ensures MC samples in zero-mass region map to 0 instead of tiny positive interpolated values.
3. **Threes in combined calibration** (`train_pipeline.py`) — `_evaluate_combined_calibration()` now dynamically evaluates all trained rate models instead of hardcoded `["pts", "reb", "ast"]`.

**BL parameter sweep analysis.** Ran full BL parameter sweep (40 configs across tau × edge × kelly):
- **No-BL configs profitable:** +3% ROI, 600-873 bets. REB is strongest at +7.9% ROI.
- **All BL configs eliminated:** 0-12 bets across all tau values (0.01-0.30).
- **Root cause identified:** BL confidence formula `1 - exp(-0.5 * z²)` produces near-zero confidence for realistic edges. For P(over)=0.55, z~0.13, confidence~0.008, w=tau*0.008 → edge crushed from 3% to 0.006%.
- **Conclusion:** Model DOES find edges (visible in no-BL results). BL confidence function is structurally broken for this use case — it demands z > 1.0 for meaningful weight, but profitable edges have z < 0.5.

### Next Step

Retrain models with calibration fixes (conformal recalibration + zero-snap). Then either:
1. Proceed to paper trading with no-BL strategy (model shows +3% ROI, REB +7.9%)
2. Redesign BL confidence function (fixed-weight tau, or linear confidence ramp) and re-sweep

---

## Session Summary (2026-01-31 — Session 7)

### What We Did

**Prediction storage + query tool.** Built full prediction persistence pipeline:
- `src/models/prediction_store.py` — stores daily predictions and gzip-compressed MC samples to PostgreSQL
- DB migration: `daily_predictions` (quantiles + edges) and `daily_prediction_samples` (compressed bytea) tables
- `src/tools/query_player.py` — CLI tool for querying stored predictions (line probability, player overview, top edges)
- `src/orchestration/run_daily.py` — wired `PredictionStore` into daily pipeline with `--skip-storage` flag

**Daily runner audit + refactor.** Comprehensive audit found 4 issues; all fixed:
- **Game discovery:** Replaced `team_game_stats` query (only has post-game data) with NBA API ScoreboardV2 as primary source. DB query retained as fallback for past dates.
- **Injury filtering:** Switched from `espn_injuries` (string name matching) to `rapidapi_injuries` (integer `player_id` matching), consistent with feature store and backtest harness.
- **Edge calculation:** Switched from 5-point quantile interpolation to MC samples empirical CDF (`(samples > line).mean()`) with quantile fallback. Consistent with backtest harness.
- **Line freshness:** Added `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY snapshot_time DESC)` to `_get_current_lines()` to use only the latest snapshot per line.

**Scraper resume capability.** Completed resume capability in `player_prop_scraper.py`:
- Market-aware progress file format (`{"markets": "...", "processed": [...]}`)
- Skip logic in main loop for already-processed events
- Progress saving after each snapshot and on interrupt/error
- `--no-resume` flag to start fresh

### Next Step

Run comprehensive BL parameter sweep backtest via `run_sweep.py` on the out-of-sample period
(2025-10-22 to 2026-01-29) to find optimal `(tau, edge_threshold, kelly_fraction)` configuration
and evaluate whether the model + BL blending produces positive edge.

---

## Session Summary (2026-01-30)

### What We Did

**Comprehensive bug fix sweep.** Created `ISSUES.md` with 28-issue pipeline audit. Fixed 12 issues
in a single commit — 1 critical, 5 high, 6 medium:

| Fixed | Severity | Summary |
|-------|----------|---------|
| ISS-001 | CRITICAL | Minutes model now uses tuned hyperparams (`self.config` → `config`) |
| ISS-002 | HIGH | `_run_date()` returns proper tuple instead of `None` |
| ISS-003 | HIGH | Non-BL edge path now devigs implied probabilities (was understating edges ~2-3%) |
| ISS-004 | HIGH | Injury LATERAL JOIN split into 2 subqueries (eliminates cross-product) |
| ISS-005 | HIGH | Training `min > 0` → `min >= 5` (matches inference threshold) |
| ISS-006 | HIGH | `early_stopping_rounds` now actually passed to `model.fit()` |
| ISS-007 | MEDIUM | Copula params computed before combined calibration; passed to MC predictor |
| ISS-008 | MEDIUM | Spread now team-directional (negative = player's team favored) |
| ISS-009 | MEDIUM | COALESCE defaults changed from 0 to league averages across all paths |
| ISS-011 | MEDIUM | Inference advanced stats JOIN matches bulk LATERAL pattern |
| ISS-015 | MEDIUM | Line shopping selects best over and best under independently |
| ISS-016 | MEDIUM | Calibration prediction failures logged as WARNING with count |

16 issues remain open (mostly low-priority). See `ISSUES.md` for details.

**Built parameter sweep tool.** `src/backtesting/run_sweep.py` (778 lines) — runs Phase 0-1
(DB fetch + MC predictions) once, then sweeps the full cartesian grid of `(tau, edge_threshold,
kelly_fraction)` configurations. Per-config output directories compatible with `visualize_results.py`.
651 lines of tests in `tests/test_run_sweep.py`.

**Expanded bookmaker coverage.** Added 11 US2/us_ex bookmakers to default lists: ballybet,
betopenly, betparx, espnbet, fliff, hardrockbet, novig, polymarket, prophetx, rebet, windcreek.

**Improved scraper CLI.** Both `daily_player_props_scraper.py` and `player_prop_scraper.py`
now support `--combos`, `--combos-only`, `--markets` flags. Historical scraper adds `--start-date`,
`--end-date`, and `--dry-run` (credit estimation without scraping).

**Daily runner sharpest-book selection.** `daily_runner.py` now fetches all bookmakers and selects
the lowest-vig (smallest booksum) line per player/game/market. Implied probabilities devigged.

**Models retrained.** Latest complete artifact: `run_20260129_205540` (trained on 22023+22024,
calibrated on 22025 through 2026-01-01). Includes all bug fixes, new features, copula params,
and feature selection. Models have early stopping active, use `min >= 5` threshold, and include
all B1-B4 + A4 features.

### Next Step

Run comprehensive BL parameter sweep backtest via `run_sweep.py` on the out-of-sample period
(2025-10-22 to 2026-01-29) to find optimal `(tau, edge_threshold, kelly_fraction)` configuration
and evaluate whether the model + BL blending produces positive edge.

---

## Session Summary (2026-01-28)

### What We Learned

**Minutes bimodality is not real.** Ran `analyze_minutes_bimodality.py` across all spread
segments. Bimodality coefficient *decreases* from 0.419 (close games) to 0.354 (extreme
blowouts) — the opposite of the hypothesis. The model's minutes predictions are well-calibrated
across all segments via the `line_spread` feature. No mixture model needed. This item is closed.

**The "winning strategy" was a Gaussian artifact.** The previous +27.86% ROI on PTS under +
REB over was computed using a Gaussian CDF edge calculation (`stats.norm.sf(z_score)`) applied
to non-Gaussian Monte Carlo distributions. Session 1 (Jan 27) replaced this with the correct
empirical CDF (`(samples > line).mean()`). Re-running the filtered backtest with the fix:

| Segment | Old (Gaussian CDF) | New (Empirical CDF) |
|---------|--------------------|--------------------|
| PTS under | +12.86% ROI | -8.84% ROI |
| REB over | +17.99% ROI | -5.18% ROI |
| Combined | +27.86% ROI @ 0.15 | -8.15% ROI |

The empirical CDF implementation was verified line-by-line — it is correct. The model is
well-calibrated (all quantiles within OK band) but does not beat the market. The 49.2% hit rate
is below the ~52.4% breakeven at -110 vig.

**Root cause: the model is catastrophically overconfident, not market-correlated.**
The market neutralization diagnostic (A1) revealed a surprise — the model is NOT a market
clone. R² of `model_prob` regressed on `implied_prob` is only **0.104** (10% explained).
The real problem is probability miscalibration against prop lines:

| Metric | Market | Model | Naive (50%) |
|--------|--------|-------|-------------|
| Brier score | 0.2495 | **0.2705** | 0.2500 |
| Correlation w/ outcome | 0.079 | 0.046 | — |
| Residual signal | — | **0.022** | — |

The model is **worse than a coin flip** on Brier score. When it predicts 84% over → actual
is 49.1%. Its quantile calibration is good (Q10–Q90 all within OK bands) but its *probability
calibration against prop lines* is catastrophically wrong. The model's MC distributions are
reasonable in shape, but translating `(samples > line).mean()` into a betting probability
produces extreme overconfidence because the distribution centers near the line (as it should
for a well-calibrated model) and small shifts in mean create large swings in P(over).

The model's residuals vs the market have essentially zero predictive signal (r = 0.022).
This means the model currently adds no independent information beyond what the market already
prices. Per Hubacek et al., profitability requires independent signal the market hasn't priced.

### New Strategic Direction

The path forward has three pillars:

1. **Probability recalibration** — the model's raw P(over) is useless. Black-Litterman
   blending anchors to the market's well-calibrated prior and extracts whatever small
   independent signal the model has. This is the critical first step.
2. **Market decorrelation** — restructure how the model relates to the market (residual
   modeling, remove market leakage features) to increase independent signal.
3. **New signal sources** — add features the market prices imperfectly (injury context,
   rest/fatigue, short-window trends, lineup effects)

---

## Track A: Probability Recalibration & Market Decorrelation (Critical Path)

These items address the fundamental problem: the model's raw probabilities are catastrophically
overconfident and contain no independent signal beyond the market. Ordered by effort (easiest first).

- [x] **A1. Run post-hoc market neutralization diagnostic** *(DONE — 2026-01-28)*
  Regressed `model_prob` on `implied_prob`. R² = 0.104 — model is NOT a market clone.
  However, the model is catastrophically overconfident (Brier 0.2705 vs naive 0.2500).
  Model residuals have zero predictive signal (r = 0.022 with outcomes).
  **Conclusion: the problem is probability miscalibration, not market correlation.**
  Black-Litterman blending is the correct first fix — anchor to market prior.

- [x] **A2. Remove `line_total` from rate features** *(DONE — 2026-01-28)*
  `line_total` (Vegas game total) was in `RATE_FEATURES_PTS`. Removed it to eliminate market
  leakage. `line_total` remains in `MINUTES_FEATURES` (genuinely predicts playing time).
  `line_spread` remains in `MINUTES_FEATURES` only.
  **Retrained** in `run_20260129_205540` — `line_total` removed from PTS rate features (though feature selection may still select it for other stats/quantiles where it provides signal).

- [x] **A3. Implement Black-Litterman blending layer** *(IMPLEMENTED — 2026-01-28, STRUCTURAL ISSUE FOUND — 2026-01-31)*
  New module `src/models/black_litterman.py` between `MonteCarloPredictor` and `BetSimulator`.
  The A1 diagnostic proved this is the correct fix: the model's raw P(over) is useless
  (Brier 0.2705), but the market is well-calibrated (Brier 0.2495). BL anchors to the market
  prior and only deviates when the model shows high-confidence disagreement.

  **Implementation (completed):**
  - `BlackLittermanBlender` class with `BLConfig` dataclass in `src/models/black_litterman.py`
  - **Prior**: Devigged market probability via multiplicative normalization (equivalent to Shin's method for 2-outcome markets)
  - **View**: Model's empirical P(over) from MC samples
  - **Confidence**: Z-score based: `z = |mean - line| / std`, `confidence = 1 - exp(-0.5z²)`
  - **Blending**: Log-odds space (not linear probability) to handle boundary effects:
    `posterior_logit = market_logit + w × (model_logit - market_logit)` where `w = min(tau × confidence, max_weight)`
  - **Integration**: Wired into `_calculate_edges()` in `backtest_harness.py`. Enabled via `--bl-tau` CLI flag on `run_backtest.py`. Disabled by default (backward compatible).
  - **Diagnostics**: Extra columns in predictions CSV: `model_over/under`, `market_over/under`, `confidence`, `posterior_over/under`
  - **Tests**: 39 unit tests in `tests/test_black_litterman.py` (all passing)

  **Structural issue found (2026-01-31):** BL parameter sweep (40 configs) showed ALL BL configs produce 0-12 bets while no-BL shows 600-873 bets at +3% ROI. The confidence formula `1 - exp(-0.5 * z²)` is near-zero for realistic edges (z < 0.5). For a 3% edge: z~0.13, confidence~0.008, w~0.0008. The BL layer demands z > 1.0 for meaningful weight, but profitable edges exist in the z < 0.5 range. This is a design flaw, not a model quality issue.

  **Fix applied (2026-02-05):** Replaced exponential confidence with linear ramp.

- [x] **A3b. Fix BL confidence function** *(IMPLEMENTED — 2026-02-05)*
  Replaced exponential confidence formula with linear ramp in `black_litterman.py`.

  **Changes:**
  - Added `z_max` parameter to `BLConfig` (default 1.0)
  - Changed `compute_confidence()`: `confidence = min(z / z_max, 1.0)`
  - Updated 3 existing tests with new expected values
  - Added 4 new tests for linear ramp behavior

  **Result:** At z=0.13 (3% edge), confidence now equals 0.13 instead of 0.008.
  This is a 16x improvement in effective weight for realistic betting edges.

  **Next step:** Run backtest with `--bl-tau 0.10` to verify meaningful bet counts

- [x] **A4. Residual modeling (Option A — feature-based)** *(IMPLEMENTED — 2026-01-28)*
  Added per-stat prop lines (`prop_line_pts`, `prop_line_reb`, `prop_line_ast`, `prop_line_threes`)
  as centering features to all rate models. The model now sees market expectation and learns
  deviations rather than absolute values.

  **Implementation:**
  - LATERAL JOIN to `raw_player_props_combined` in all 4 feature store query paths
    (`get_training_dataset`, `get_features_for_date`, `get_features_for_date_range`, `get_player_game_features`)
  - `DISTINCT ON (market_key)` deduplication: picks most recent snapshot per stat from pinnacle/draftkings
  - New `_get_player_prop_lines()` helper for single-player inference path
  - Each `RATE_FEATURES_*` list now includes its corresponding `prop_line_*`
  - COALESCE to 0 for missing lines (consistent with `line_spread`/`line_total` pattern)
  - Database index `idx_props_player_game` created on `(player_id, game_id)` for query performance
  - **Retrained** in `run_20260129_205540` — prop line features active and selected across all models.

- [ ] **A5. Residual modeling (Option B — binary classifier)**
  Build a separate model that directly predicts P(over | features, line) trained on historical
  over/under outcomes. Architecturally cleaner for decorrelation but a bigger lift than Option A.
  Evaluate after A4 results are in.

- [ ] **A6. Conditional rate modeling (minutes as rate feature)**
  Instead of modeling minutes and rates independently and combining via copula, pass the
  sampled minutes value as a feature into the rate model at inference time. The MC loop would
  sample minutes first, then condition rate predictions on sampled minutes. This directly models
  the dependency rather than approximating it via copula. Consider if copula-based combined
  calibration still shows drift after retraining.

---

## Track B: New Signal Sources (Parallel — High Impact)

These add information the market may price imperfectly, especially for non-star players
where bookmaker attention is lower.

- [x] **B1. Injury/lineup context features** *(IMPLEMENTED — 2026-01-29)*
  Historical injury data acquired via RapidAPI (2021-present, 88K+ rows). Player name-to-ID
  linking via 3-tier cascade (manual CSV → exact normalized → SequenceMatcher fuzzy, threshold 0.80).
  99.3% of injury records fully linked. Garbage API entries cleaned (142 rows deleted).

  **Features added to all rate models and minutes model (10 total):**
  - `team_out_count` — players listed Out on player's team
  - `team_out_min_sum` — total recent minutes of Out teammates
  - `team_out_pts_sum`, `team_out_reb_sum`, `team_out_ast_sum`, `team_out_usg_sum` — production of Out teammates
  - `opp_out_count`, `opp_out_min_sum` — opponent injury context
  - `player_is_questionable`, `player_is_probable` — player's own injury status (binary)

  Computed via SQL LATERAL JOINs in `feature_store.py`. Pre-game temporal integrity enforced
  (uses report_date <= game_date). Manual mappings for truncated API names (suffixes like "III", "Jr.").
  **Retrained** in `run_20260129_205540` — injury features active and selected by feature selection.

- [x] **B2. Rest days / back-to-back features** *(IMPLEMENTED — 2026-01-29)*
  Schedule density features pre-computed in `player_average_game_stats` via `calculate_b2_b3_b4_features()`.
  Added to `MINUTES_FEATURES` and all 4 `RATE_FEATURES_*` lists: `rest_days`, `is_back_to_back`, `games_in_last_7_days`.
  DB columns: `rest_days`, `games_last_7d`. `is_back_to_back` derived in SQL (`CASE WHEN rest_days = 1`).
  All 4 feature store query paths updated. Defaults: rest=3, b2b=0, games_7d=2.

- [x] **B3. Short-window + trend features** *(IMPLEMENTED — 2026-01-29)*
  L3 rolling averages and L5 std deviations pre-computed in `player_average_game_stats`.

  Features added (13 total across all feature lists):
  - `player_avg_{stat}_l3` (5 stats) — last 3 games rolling average
  - `player_{stat}_l3_l15_ratio` (4 stats: pts/reb/ast/fg3m) — momentum ratio (>1.0 = trending up)
  - `player_std_{stat}_l5` / `player_min_std_l5` (5 stats) — L5 standard deviation (consistency signal)

  DB columns: `avg_{stat}_l3` (5), `std_{stat}_l5` (5). Momentum ratios computed in SQL from L3/L15 averages.
  Shift(1) no-leakage pattern ensures features only use prior games.

- [x] **B4. Minutes stability features** *(IMPLEMENTED — 2026-01-29)*
  Minutes stability features pre-computed in `player_average_game_stats`.

  Features added to `MINUTES_FEATURES`:
  - `player_min_std_l5` — minutes variance (shared with B3 std computation)
  - `player_min_floor_l5` — minimum minutes in last 5 games
  - `player_games_started_l5` — games with 20+ minutes in last 5 (starter proxy)

  DB columns: `min_floor_l5`, `games_started_l5`. Starter threshold = 20 minutes.

---

## Track C: Calibration Refinement (Parallel — Lower Priority)

- [x] **C0. Gaussian copula for minutes-rate correlation** *(IMPLEMENTED — 2026-01-29)*
  Replaced the legacy post-hoc correlation adjustment (hardcoded bucket-based rate factors) with
  proper Gaussian copula sampling. This preserves both marginal distributions exactly while
  capturing the empirical rank dependency between minutes and per-minute rates.

  **Problem:** PTS (ρ=0.314) and AST (ρ=0.176) show significant minutes-rate correlation.
  Independent sampling + post-hoc multiplicative adjustment distorted the rate distribution
  and was the likely root cause of the AST Q10 combined calibration gap (+9.7%).

  **Implementation:**
  - `MonteCarloPredictor` accepts `copula_params: dict[stat → Spearman ρ]`
  - Training pipeline computes Spearman rank correlations and saves `copula_params.json` as artifact
  - `_predict_copula()`: shared z_minutes ~ N(0,1), per-stat z_rate = ρ·z_min + √(1-ρ²)·z_indep
  - Uniform transform via Φ(z), then inverse CDF mapping through each marginal
  - Both `predict()` and `predict_batch_for_date()` support copula path
  - `run_backtest.py` and `run_daily.py` auto-load copula params from model artifacts
  - Falls back to legacy adjustment when `copula_params.json` not present (backward compat)
  - Helper: `compute_copula_params_from_data()`, `load_copula_params()`
  - If copula still shows combined calibration drift, see A6 (conditional rate modeling)

- [x] **C1. Investigate Q10 over-coverage** *(PARTIALLY ADDRESSED — 2026-01-31)*
  Training showed THREES rate model Q0.10 at 35.2% coverage (worst case). Root cause: zero-inflated
  distribution — 35%+ of `threes_per_min` samples are exactly 0, which XGBoost's `quantileerror`
  cannot learn. Combined THREES Q0.10 showed +20.4% gap on holdout data. Combined AST Q0.10 showed
  +10.4% gap from discrete spike at 0 assists.

  **Fixes applied:**
  - Conformal recalibration in `quantile_trainer.py` — closes gaps > 3% via validation residual offset
  - Zero-snap in `monte_carlo.py` — values < 1e-3 snapped to 0 in inverse CDF
  - Dynamic stat inclusion in `train_pipeline.py` — combined calibration now evaluates all trained rate models

  **Status:** Code changes applied. Models need retraining to incorporate conformal offsets. Zero-snap
  and combined eval fixes will take effect on next retrain.

- [ ] **C2. Per-stat calibration breakdown**
  Run `analyze_calibration_drift.py` with the current model to get per-stat quantile coverage.
  This informs whether rate_factors or tail adjustments need stat-specific tuning.

- [x] **C3. Zero-inflated hurdle model for THREES** *(IMPLEMENTED — 2026-02-05)*
  Two-stage hurdle architecture to handle 35%+ zero mass in THREES distribution.
  Spec: `.session/specs/C3_THREES_hurdle_model.md`

  **Problem:** THREES Q0.10 had +20.4% calibration gap. XGBoost quantile regression cannot
  learn Q0.10 = 0 when it always predicts positive values. Conformal recalibration (offsets)
  cannot fix this — you can't offset a positive prediction to exactly 0.

  **Solution implemented:**
  - **Stage 1:** Binary classifier predicting P(threes = 0 | features) with isotonic calibration
  - **Stage 2:** Quantile regression on positive samples only (threes | threes > 0)
  - **Inference:** If q ≤ p_zero → quantile = 0, else interpolate positive distribution
  - **MC sampling:** Bernoulli draw independent of copula; copula affects positive rate magnitude

  **Files modified:**
  - `src/models/quantile_trainer.py` — Added `HurdleQuantileModel` class, `train_hurdle_model()`, pipeline integration
  - `src/models/monte_carlo.py` — Added `_sample_hurdle()`, `_sample_hurdle_from_quantiles()`, copula integration
  - `src/models/train_pipeline.py` — Added `_calibrate_hurdle_model()`, hurdle evaluation

  **Next:** Retrain to validate THREES Q0.10 gap < 5%

---

## Track D: Previous Model Improvement Items (Deprioritized)

These were the original Track B items. Most are superseded by the probability recalibration
findings — fixing per-stat biases won't help if the fundamental problem is overconfident
probabilities and zero independent signal. Revisit after Tracks A and B are complete.

- [ ] **D1. Investigate PTS upward bias** *(Superseded by A2)*
  PTS rate_factors going up to 1.30 may inflate upper tail. However, the market decorrelation
  work (Track A) should be done first. If removing `line_total` and adding residual modeling
  changes the prediction landscape, the rate_factors may need re-derivation anyway.
  - Compare predicted PTS distributions vs actuals (mean, median, skew)
  - Test dampening PTS rate_factors (e.g., cap at 1.15 instead of 1.30)

- [ ] **D2. Diagnose AST** *(Low Priority)*
  Lost money in both directions. Either poorly calibrated, weak features, or market is too
  efficient on assists. Revisit after decorrelation work.

- [ ] **D3. Investigate REB under** *(Low Effort)*
  REB under was -1.46% under old Gaussian CDF. May not be meaningful under empirical CDF.
  Re-evaluate after decorrelation work.

- [ ] **D4. Explore adding THREES** *(Unknown Value)*
  Model supports `threes` but wasn't in recent backtests. Revisit after core model improvements.

---

## Track E: Go-Live Pipeline (Blocked — Needs Edge First)

These items are blocked until the BL parameter sweep demonstrates positive ROI on the
out-of-sample period. Do not pursue E4+ until sweep results are in.

- [x] **E1. Retrain models** — *(DONE — 2026-01-30)* Retrained with all bug fixes and new features. Artifact: `run_20260129_205540`. Needs another retrain to incorporate calibration fixes from session 8.
- [x] **E2. Run BL parameter sweep** — *(DONE — 2026-01-31)* Ran `run_sweep.py` on OOS period (2025-10-22 to 2026-01-29). 40 configs: tau × edge × kelly.
- [x] **E3. Analyze sweep results** — *(DONE — 2026-01-31)* **Key finding:** No-BL is profitable (+3% ROI, REB +7.9%). BL confidence function is structurally broken — kills all edges. See A3 for details and fix options.
- [x] **E4. Fix daily injury pipeline** *(DONE — 2026-02-04)* — `run_daily.py --scrape-injuries` now calls `rapidapi_injury_backfill.py` (for target date) followed by `link_injury_data.py`. Both feature store and daily runner use `rapidapi_injuries` with `player_id` linking.
- [x] **E5. Paper trade infrastructure** *(DONE — 2026-02-04)* — Convert stored `daily_predictions` into paper bets:
  - `paper_bets` and `paper_trading_daily_log` tables for storage
  - `PaperTrader` class with bet selection (edge threshold + Kelly sizing), placement (UPSERT), and resolution
  - CLI scripts: `place_bets.py` (with `--dry-run`) and `resolve_bets.py`
  - 20 unit tests in `tests/test_paper_trader.py`
- [ ] **E6. Scheduling** — Automate daily pipeline (cron/Task Scheduler):
  - ~11am: Scrape game results, props, injuries
  - ~12pm: Run processing + feature store + predictions
  - ~6pm: Re-scrape props for line movement, re-run edge calc
- [ ] **E7. Paper trade** — Run live for 2-4 weeks, validate predictions vs outcomes
- [ ] **E8. Go live — minimum flat stakes**
- [ ] **E9. Scale to Kelly sizing**

---

## Track F: Market Expansion (Future — After Demonstrated Edge)

New market data scraped (2.6M rows, 2026-01-31) but not yet modeled. Expand when core
pts/reb/ast shows profitability.

- [ ] **F1. Backfill new markets further** — Currently scraped for recent window only. Extend `player_prop_scraper.py` run with `--markets player_field_goals player_frees_made player_frees_attempts player_blocks_steals` + combos back to 2024-10-22.
- [ ] **F2. Add FG/FT/BLK+STL rate features** — New `RATE_FEATURES_FG`, `RATE_FEATURES_FT`, `RATE_FEATURES_BLK_STL` in feature store. Need new `actual_*` target columns in training data.
- [ ] **F3. Train expanded models** — Add `fg`, `ft_made`, `ft_attempts`, `blk_stl` as stat targets. Train + calibrate.
- [ ] **F4. Add combo market edges** — PRA, P+R, P+A, R+A are sums of individual predictions. Compute from existing MC samples without additional models.
- [ ] **F5. DD/TD markets** — Binary outcomes, need separate classifier (not quantile regression). `player_double_double`, `player_triple_double`.

---

## Priority Matrix

| Item | Effort | Expected Value | Notes |
|------|--------|----------------|-------|
| ~~A1 (Market neutralization diagnostic)~~ | ~~Trivial~~ | ~~Critical~~ | **DONE** — R²=0.10, Brier 0.2705, overconfidence not correlation |
| ~~A3 (Black-Litterman blending)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — Implemented in `black_litterman.py`, 39 tests passing. Needs validation backtest. |
| ~~A2 (Remove line_total)~~ | ~~Low~~ | ~~High~~ | **DONE** — Removed from `RATE_FEATURES_PTS`. Retrained. |
| ~~B2 (Rest/B2B features)~~ | ~~Low~~ | ~~Medium-High~~ | **DONE** — `rest_days`, `is_back_to_back`, `games_in_last_7_days`. Retrained. |
| ~~B3 (L3 + trend features)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — 13 features (L3 avg, momentum ratios, L5 std). Retrained. |
| ~~B1 (Injury features)~~ | ~~Medium-High~~ | ~~High~~ | **DONE** — 10 injury features via LATERAL JOIN. 99.3% linked. Retrained. |
| ~~A4 (Residual modeling — features)~~ | ~~Medium~~ | ~~High~~ | **DONE** — Prop line centering in all 4 query paths. Retrained. |
| ~~B4 (Minutes stability)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — `min_std_l5`, `min_floor_l5`, `games_started_l5`. Retrained. |
| ~~C0 (Gaussian copula)~~ | ~~Medium~~ | ~~Medium-High~~ | **DONE** — Replaces hardcoded rate factors with proper copula sampling. Retrained. |
| ~~E1 (Retrain)~~ | ~~Low~~ | ~~Critical~~ | **DONE** — `run_20260129_205540`. Needs re-retrain with calibration fixes. |
| ~~E2 (BL Sweep)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — No-BL profitable (+3% ROI). BL kills all edges (confidence function flaw). |
| ~~E3 (Analyze sweep)~~ | ~~Low~~ | ~~Critical~~ | **DONE** — REB +7.9%, model finds genuine edges without BL. |
| ~~A3b (Fix BL confidence)~~ | ~~Low~~ | ~~High~~ | **DONE** — Linear ramp confidence. 42 tests passing. |
| ~~C3 (THREES hurdle model)~~ | ~~Medium-High~~ | ~~High~~ | **DONE** — Two-stage hurdle model implemented. Needs retraining to validate. |
| E1b (Retrain with calibration fixes) | Low | Medium | Conformal recalibration + zero-snap need retraining to take effect |
| ~~E4 (Daily injury pipeline)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — `--scrape-injuries` now uses RapidAPI + linker |
| ~~E5 (Paper trade infra)~~ | ~~Medium~~ | ~~High~~ | **DONE** — `PaperTrader` class, CLI scripts, 20 tests |
| E6 (Scheduling) | Low | High | cron/Task Scheduler automation |
| ~~C1 (Q10 investigation)~~ | ~~Low~~ | ~~Low-Medium~~ | **PARTIALLY DONE** — Root cause identified (zero-inflation), conformal recalibration applied |
| A5 (Residual modeling — classifier) | High | High | Only if A4 isn't sufficient |
| A6 (Conditional rate modeling) | Medium-High | Medium-High | Only if copula combined calibration still drifts |
| D1-D4 (Old model items) | Various | Low until recalibrated | Revisit after Track A |
| F1-F5 (Market expansion) | Various | Medium | After demonstrated edge on core markets |

---

## Key Findings Archive

### Market Neutralization Diagnostic (Closed — 2026-01-28)

Ran full diagnostic on `backtest_results/bt_20260128_145106/predictions.csv` (15,090 rows).

**Key results:**
- R² (model_prob ~ implied_prob) = 0.104 → model is NOT a market clone
- Per-stat: PTS R²=0.041, REB R²=0.187
- Model Brier score: 0.2705 (worse than naive 0.2500)
- Market Brier score: 0.2495 (well-calibrated)
- Model correlation with outcome: 0.046
- Market correlation with outcome: 0.079 (market is better predictor)
- Model residual correlation with outcome: 0.022 (essentially zero independent signal)
- Probability calibration: model says 84% over → actual 49.1% (catastrophic overconfidence)
- Model avg P(over): 0.604 (should be ~0.50 for balanced lines)

**Conclusion:** The original hypothesis ("model is correlated with the market") was wrong.
The model is overconfident, not correlated. Good quantile calibration does NOT imply good
probability calibration against prop lines. The MC distribution centers near the line (correct
behavior), but `(samples > line).mean()` amplifies small mean shifts into extreme probabilities.
Black-Litterman blending is the correct fix — anchor to the market's well-calibrated prior.

### Minutes Bimodality (Closed — 2026-01-28)

No bimodality detected. BC decreases with spread (0.419 close → 0.354 extreme blowout).
Model handles blowouts via `line_spread` feature. No mixture model needed.

### Empirical CDF Verification (Closed — 2026-01-28)

Implementation verified correct. `(samples > line).mean()` is textbook empirical P(over).
Sample routing via `(player_id, game_id, stat)` tuple keys confirmed correct. Edge
calculation, odds conversion, and bet resolution all verified. The Gaussian CDF was the
source of phantom edges, not a bug in the empirical replacement.
