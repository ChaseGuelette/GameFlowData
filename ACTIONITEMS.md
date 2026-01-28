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

**Root cause: the model is correlated with the market.** Features like `line_spread` and
`line_total` are direct restatements of the betting market's view. The model partially learns
to replicate the market. Per Hubacek et al., even an accurate model is unprofitable if it's
correlated with the bookmaker's model. Profitability requires *independent* signal — information
the market hasn't already priced.

### New Strategic Direction

The path forward has two pillars:

1. **Market decorrelation** — restructure how the model relates to the market (residual
   modeling, remove market leakage features, Black-Litterman blending)
2. **New signal sources** — add features the market prices imperfectly (injury context,
   rest/fatigue, short-window trends, lineup effects)

---

## Track A: Market Decorrelation (Critical Path — Must Do First)

These items address the fundamental problem: the model agrees with the market too closely
to generate edge. Ordered by effort (easiest first).

- [ ] **A1. Run post-hoc market neutralization diagnostic**
  Using the latest backtest output (`predictions.csv`), regress `model_prob` on `implied_prob`.
  If R^2 is high, the model is essentially a noisy market replica. This takes minutes and
  requires no code changes — just load the CSV and run a regression.
  Determines how urgent decorrelation is.

- [ ] **A2. Remove `line_total` from rate features**
  `line_total` (Vegas game total) is in PTS, AST, and THREES rate features. It's nearly a
  direct restatement of the scoring market. Remove it from all `RATE_FEATURES_*` lists in
  `feature_store.py`. Keep `line_spread` in MINUTES_FEATURES only (it genuinely predicts
  playing time and isn't directly bet on). Retrain and re-backtest.

- [ ] **A3. Implement Black-Litterman blending layer**
  New module between `MonteCarloPredictor` and `BetSimulator` that blends:
  - **Prior**: Market-implied probability (from prop odds)
  - **View**: Model's MC distribution (your prediction)
  - **Confidence**: Width of MC distribution (narrow = confident, wide = uncertain)

  When uncertain, posterior stays near market (no bet). When confident and disagreeing, posterior
  deviates (potential edge). No retraining needed — this is a post-prediction adjustment.

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

These were the original Track B items. Most are superseded by the market decorrelation
findings — fixing per-stat biases won't help if the fundamental problem is market correlation.
Revisit after Tracks A and B are complete.

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
| A1 (Market correlation diagnostic) | Trivial | Critical | 10-minute diagnostic, do first |
| A2 (Remove line_total) | Low | High | Reduces market leakage |
| B2 (Rest/B2B features) | Low | Medium-High | Known strong signal, easy to compute |
| B3 (L3 + trend features) | Low | Medium | More granular than L5/L15 |
| A3 (Black-Litterman blending) | Medium | High | Principled decorrelation dial |
| B1 (Injury features) | Medium-High | High | Biggest feature gap, needs data acquisition |
| A4 (Residual modeling — features) | Medium | High | Structural decorrelation |
| B4 (Minutes stability) | Low | Medium | Better bet filtering |
| C1 (Q10 investigation) | Low | Low-Medium | Calibration refinement |
| A5 (Residual modeling — classifier) | High | High | Only if A4 isn't sufficient |
| D1-D4 (Old model items) | Various | Low until decorrelated | Revisit after Track A |
| E1-E7 (Go-live) | Various | Blocked | Needs demonstrated edge first |

---

## Key Findings Archive

### Minutes Bimodality (Closed — 2026-01-28)

No bimodality detected. BC decreases with spread (0.419 close → 0.354 extreme blowout).
Model handles blowouts via `line_spread` feature. No mixture model needed.

### Empirical CDF Verification (Closed — 2026-01-28)

Implementation verified correct. `(samples > line).mean()` is textbook empirical P(over).
Sample routing via `(player_id, game_id, stat)` tuple keys confirmed correct. Edge
calculation, odds conversion, and bet resolution all verified. The Gaussian CDF was the
source of phantom edges, not a bug in the empirical replacement.
