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

- [ ] **A4. Residual modeling (Option A — feature-based)**
  Add the prop line as a centering feature to rate models. The model learns deviations from
  market expectation rather than absolute values. Requires adding `prop_line` to feature store
  from `raw_player_props_combined`, retraining, and re-backtesting.

- [ ] **A5. Residual modeling (Option B — binary classifier)**
  Build a separate model that directly predicts P(over | features, line) trained on historical
  over/under outcomes. Architecturally cleaner for decorrelation but a bigger lift than Option A.
  Evaluate after A4 results are in.

---

## Track B: New Signal Sources (Parallel — High Impact)

These add information the market may price imperfectly, especially for non-star players
where bookmaker attention is lower.

- [ ] **B1. Injury/lineup context features** *(Highest Impact)*
  The model has zero injury awareness. The ESPN injury scraper exists but was never deployed
  (`espn_injuries` table doesn't exist). No historical injury data has been collected.

  **Sub-tasks:**
  - Acquire historical injury data (2021-present). Options:
    - RapidAPI NBA Injury Reports API (has historical backfill from 2021, 3x daily snapshots,
      ~$10-20/mo). Provides date-queryable historical data needed for training/backtesting.
    - ESPN scraper (free, already built) for ongoing daily collection going forward.
    - Recommendation: Use RapidAPI for historical backfill, ESPN scraper for live.
  - Build player name-to-ID mapping layer (API returns names, DB uses integer IDs)
  - Design injury features for `feature_store.py`:
    - `team_out_players_count` — players listed Out on game day
    - `team_out_minutes_share` — % of team's recent minutes missing
    - `team_out_pts_share` — % of team's recent scoring missing
    - `player_injury_status` — is this player Questionable/Probable (affects own minutes)
    - `teammate_out_usage_boost` — estimated usage increase when key teammates are out
  - Ensure temporal integrity (use pre-game report only, never post-game)
  - Retrain with injury features and backtest

- [ ] **B2. Rest days / back-to-back features** *(Easy Win)*
  No schedule density features exist. Back-to-backs are one of the most studied effects in NBA
  analytics. Trivial to compute from `game_date` in `player_game_stats`.

  Features to add:
  - `days_since_last_game` (0 = back-to-back)
  - `games_in_last_7_days`
  - `is_back_to_back` (binary flag)

- [ ] **B3. Short-window + trend features** *(Easy Win)*
  Model only has L5 and L15 averages. No way to capture recent momentum.

  Features to add:
  - `player_avg_{stat}_l3` (last 3 games — captures very recent form)
  - `player_l3_l15_ratio_{stat}` (momentum: >1.0 = trending up)
  - `player_std_{stat}_l5` (consistency/variance signal)

- [ ] **B4. Minutes stability features** *(Medium Effort)*
  Model doesn't distinguish locked-in starters from volatile rotation players.

  Features to add:
  - `player_min_std_l5` (minutes variance)
  - `player_min_floor_l5` (minimum minutes in last 5 — floor games)
  - `player_games_started_l5` (starter consistency)

---

## Track C: Calibration Refinement (Parallel — Lower Priority)

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
| B2 (Rest/B2B features) | Low | Medium-High | Known strong signal, easy to compute |
| B3 (L3 + trend features) | Low | Medium | More granular than L5/L15 |
| B1 (Injury features) | Medium-High | High | Biggest feature gap, needs data acquisition |
| A4 (Residual modeling — features) | Medium | High | Structural decorrelation |
| B4 (Minutes stability) | Low | Medium | Better bet filtering |
| C1 (Q10 investigation) | Low | Low-Medium | Calibration refinement |
| A5 (Residual modeling — classifier) | High | High | Only if A4 isn't sufficient |
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
