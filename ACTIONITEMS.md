# GameFlowData — Roadmap

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
  **Note:** Existing model artifacts were trained WITH `line_total` in PTS features. Models
  must be retrained for this change to take effect. Re-backtest after retraining.

- [x] **A3. Implement Black-Litterman blending layer** *(IMPLEMENTED — 2026-01-28)*
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

  **Next step**: Run validation backtest with `--bl-tau 0.05` to confirm Brier score improvement and characterize edge distribution. Tau sweep `[0.01, 0.02, 0.05, 0.10, 0.15, 0.20]` on held-out period.

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
  - **Note:** Models must be retrained for this change to take effect. Re-backtest after retraining.

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
  **Note:** Models must be retrained for these features to take effect.

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

- [ ] **C1. Investigate Q10 over-coverage**
  Latest backtest shows Q10 at 13.2% vs 10% target (32% over-coverage). The lower tail is
  thinner than reality. Check whether this is concentrated in one stat (PTS vs REB vs AST).
  If PTS-specific, may need per-stat tail adjustment rather than global `lower_tail_multiplier`.

- [ ] **C2. Per-stat calibration breakdown**
  Run `analyze_calibration_drift.py` with the current model to get per-stat quantile coverage.
  This informs whether rate_factors or tail adjustments need stat-specific tuning.

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

These items from the original Track A are blocked until the model demonstrates real edge
under the correct empirical CDF calculation. Do not pursue until Tracks A+B produce a
backtest with positive ROI.

- [ ] **E1. Run filtered in-sample backtest** (after model improvements)
- [ ] **E2. Run out-of-sample backtest** (Oct 2025 – Jan 2026)
- [ ] **E3. Analyze OOS results**
- [ ] **E4. Build/verify live pipeline**
- [ ] **E5. Paper trade**
- [ ] **E6. Go live — minimum flat stakes**
- [ ] **E7. Scale to Kelly sizing**

---

## Priority Matrix

| Item | Effort | Expected Value | Notes |
|------|--------|----------------|-------|
| ~~A1 (Market neutralization diagnostic)~~ | ~~Trivial~~ | ~~Critical~~ | **DONE** — R²=0.10, Brier 0.2705, overconfidence not correlation |
| ~~A3 (Black-Litterman blending)~~ | ~~Medium~~ | ~~Critical~~ | **DONE** — Implemented in `black_litterman.py`, 39 tests passing. Needs validation backtest. |
| ~~A2 (Remove line_total)~~ | ~~Low~~ | ~~High~~ | **DONE** — Removed from `RATE_FEATURES_PTS`. Needs retrain + re-backtest. |
| ~~B2 (Rest/B2B features)~~ | ~~Low~~ | ~~Medium-High~~ | **DONE** — `rest_days`, `is_back_to_back`, `games_in_last_7_days` in MINUTES_FEATURES. Needs retrain + re-backtest. |
| ~~B3 (L3 + trend features)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — 13 features (L3 avg, momentum ratios, L5 std). Needs retrain + re-backtest. |
| ~~B1 (Injury features)~~ | ~~Medium-High~~ | ~~High~~ | **DONE** — 10 injury features via LATERAL JOIN. 99.3% linked. Needs retrain + re-backtest. |
| ~~A4 (Residual modeling — features)~~ | ~~Medium~~ | ~~High~~ | **DONE** — Prop line centering in all 4 query paths. Needs retrain + re-backtest. |
| ~~B4 (Minutes stability)~~ | ~~Low~~ | ~~Medium~~ | **DONE** — `min_std_l5`, `min_floor_l5`, `games_started_l5` in MINUTES_FEATURES. Needs retrain + re-backtest. |
| ~~C0 (Gaussian copula)~~ | ~~Medium~~ | ~~Medium-High~~ | **DONE** — Replaces hardcoded rate factors with proper copula sampling. Needs retrain + re-backtest. |
| C1 (Q10 investigation) | Low | Low-Medium | Calibration refinement |
| A5 (Residual modeling — classifier) | High | High | Only if A4 isn't sufficient |
| A6 (Conditional rate modeling) | Medium-High | Medium-High | Only if copula combined calibration still drifts |
| D1-D4 (Old model items) | Various | Low until recalibrated | Revisit after Track A |
| E1-E7 (Go-live) | Various | Blocked | Needs demonstrated edge first |

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
