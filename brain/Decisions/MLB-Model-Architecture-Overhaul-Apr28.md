> Part of [[Decisions]]

# MLB Model Architecture Overhaul — April 28, 2026

**Date**: April 28, 2026
**Status**: Code complete, pending retraining
**Session**: 053

---

## Background & Motivation

Both `batter_hits` and `pitcher_strikeouts` models had been retrained on recent data and still underperformed. The conclusion was that the **architecture itself** needed to change, not just the data or hyperparameters. Three specific problems were identified:

1. **Pitcher K model treats strikeouts as a single monolithic prediction** — but strikeouts are fundamentally a product of *how long a pitcher stays in the game* (IP) and *how many batters they strike out per inning* (K-rate). These have different drivers: IP depends on pitch count efficiency, fatigue, and bullpen management, while K-rate depends on stuff quality, opposing lineup tendencies, and the times-through-order penalty (TTOP). Predicting K directly collapses these into one target and loses the structural relationship.

2. **Batter hits model uses a point estimate for at-bats** — the existing Binomial model samples `Hits ~ Binomial(n=projected_ab, p)` where `projected_ab` is a simple formula `0.5 * avg_ab_l5 + 0.5 * position_pa`. This ignores AB uncertainty entirely. A batter batting 2nd who gets pulled in the 5th inning has very different AB distribution than one who plays the full game. The point estimate especially hurts tail calibration.

3. **Unused data sitting in the database** — `mlb_pitcher_inning_stats` (inning-level pitch data with velocity, whiff rates, CSW by inning) and `mlb_bullpen_daily_status` (opposing bullpen workload) existed but were never wired into the feature stores.

---

## Decision 1: Pitcher K → IP Model + K-Rate Model + Gaussian Copula

### What
Decompose `pitcher_strikeouts` into two sub-models:
- **IP Model**: QuantileModelSuite (Q10/Q25/Q50/Q75/Q90) trained on `actual_ip`
- **K-Rate Model**: QuantileModelSuite trained on `actual_so / actual_ip`

Join them with a **Gaussian copula** that preserves the correlation between IP and K-rate. The final prediction is `K_samples = IP_samples * K_rate_samples` (element-wise, copula-correlated).

### Why This Pattern
This is the **exact same architecture as the NBA model** (minutes × rate with Gaussian copula), which has been profitable for months. The NBA model decomposes stats like points into `minutes * points_per_minute`, capturing the fact that a player who plays 38 minutes has a different scoring distribution than one who plays 22 minutes. The copula handles the correlation (players with more minutes often have different per-minute rates due to fatigue, garbage time, etc.).

For pitcher K, the decomposition is:
- **IP** captures: pitch count efficiency, days rest, bullpen management tendencies, game script (blowouts get pulled early), fatigue patterns
- **K-rate** captures: stuff quality (whiff%, CSW%), opposing lineup K tendencies, TTOP (K-rate drops as batters see a pitcher multiple times), park factors

The **copula correlation** (Spearman ρ) is expected to be positive (~0.2-0.4): pitchers who go deeper into games tend to have slightly different K-rates than short-outing pitchers, because deep outings imply dominance.

### Implementation Details
- **Copula math**: Spearman ρ from training data → convert to Pearson via `ρ_p = 2 * sin(π * ρ_s / 6)` → generate correlated standard normals `z_ip, z_krate` → map through Φ to uniforms → inverse CDF through each marginal → multiply
- **Training**: `--copula` flag on `mlb_train_pipeline.py`. Trains IP model, K-rate model, computes Spearman ρ, saves all artifacts. Also trains the old single model for A/B comparison.
- **Inference**: `MLBPitcherKCopulaPredictor` in `mlb_monte_carlo.py`. Loads both sub-models + copula params, generates 10,000 correlated samples.
- **Model suite**: Copula model preferred if artifacts exist; falls back to single quantile model if not.
- **K-rate filter**: `actual_ip > 0` for training (exclude DNPs); `actual_ip >= 3` for copula ρ computation (exclude short relief appearances that skew the rate)

### Trade-offs
- **Pro**: Captures structural relationships that a single model misses. Proven pattern from NBA.
- **Pro**: Quantile regression on both sub-models handles non-stationarity (TTOP makes K-rate non-linear across innings).
- **Con**: Two models to train and maintain instead of one. More artifacts.
- **Con**: Copula assumes bivariate relationship can be captured by a single correlation parameter. If the IP/K-rate relationship is highly non-linear, this may underfit. Acceptable for V1.
- **Mitigation**: Old single model is preserved as fallback and for A/B testing.

---

## Decision 2: Batter Hits → AB NegBin + Compound Binomial

### What
Add a separate **at-bats prediction model** (NegBinModel) that feeds into compound Binomial sampling:
1. Draw `ab_samples ~ NegBin(features)` — a distribution of plausible AB counts
2. Get `p_hit` from existing Binomial model
3. For each sample: `hits[i] ~ Binomial(n=ab_samples[i], p=p_hit)`

### Why
The existing Binomial model uses `projected_ab = 0.5 * avg_ab_l5 + 0.5 * position_pa` as a point estimate. This formula:
- Ignores game context (blowouts → pinch hitters, extra innings → more ABs)
- Ignores opposing pitcher tendencies (a pitcher who goes deep means fewer ABs from the bullpen)
- Collapses all AB uncertainty into a single number, which hurts tail calibration

By modeling AB as a distribution, we propagate that uncertainty into the hit distribution. A batter with a 70% chance of getting 4 AB and a 30% chance of getting 3 AB will have a very different hit distribution than one guaranteed 3.5 AB.

### Implementation Details
- **AB model**: NegBinModel with NO exposure parameter (AB is the count itself, not a rate). Feature selection via NLL-based selector or correlation fallback.
- **Training order**: AB model trains first, then Binomial model trains as before. Both saved to same run directory.
- **Inference**: `MLBCompoundBinomialPredictor` in `mlb_monte_carlo.py`. Loads both models, does compound sampling.
- **Model suite**: Compound model preferred if `batter_ab` artifacts exist alongside `batter_hits` binomial artifacts; falls back to standard `MLBBinomialPredictor` if not.
- **AB model artifact names**: `batter_ab_xgblss_booster.json` + `batter_ab_negbin_meta.json`

### Trade-offs
- **Pro**: Better tail calibration — the hit distribution now reflects AB uncertainty.
- **Pro**: AB model can learn game context features that the formula ignores.
- **Con**: Adds a NegBin model to the hit pipeline (more training time, more artifacts).
- **Con**: p_hit from the Binomial model was trained with ground-truth AB. If the AB model is miscalibrated, the compound distribution could drift.
- **Mitigation**: Standard Binomial path preserved as fallback.

---

## Decision 3: Wire 13 New Features from Existing Tables

### Pitcher Feature Store — 6 Inning-Level Fatigue Features

| Feature | Definition | Default | Rationale |
|---------|-----------|---------|-----------|
| `pitcher_velo_drop_late_l5` | Avg velo innings 1-3 minus innings 5+ over L5 starts | 0 | Measures fatigue-induced velocity loss. Larger drop → less effective late → fewer Ks in late innings |
| `pitcher_avg_whiff_rate_late_l5` | Pitch-weighted whiff rate in innings 5+ over L5 starts | 0 | Direct measure of stuff quality when fatigued |
| `pitcher_avg_k_rate_early_l5` | K/BF in innings 1-3 over L5 starts | 0 | Early-inning dominance predicts total K upside |
| `pitcher_avg_pitches_per_inning_l5` | Avg pitches per inning over L5 starts | 15 | Efficiency proxy — lower = more innings = more K opportunities |
| `pitcher_avg_csw_rate_l5_inning` | Pitch-weighted CSW rate over L5 starts | 0 | Called-strike + whiff rate, better than whiff alone for K prediction |
| `pitcher_deep_inning_pct_l5` | Fraction of L5 starts reaching inning 6+ | 0.5 | Workload tendency — directly predicts IP for the copula |

**Source**: `mlb_pitcher_inning_stats` table (pitch-level data aggregated by inning per game). Already scraped and synced locally. Used via LATERAL JOIN on L5 most recent starts.

### Batter Feature Store — 4 Bullpen + 3 Opposing Pitcher Inning Features

| Feature | Definition | Default | Rationale |
|---------|-----------|---------|-----------|
| `opp_bullpen_ip_last_3d` | Opposing team's bullpen IP last 3 days | 0 | Taxed bullpen → worse relievers → more hits late |
| `opp_bullpen_era_last_7d` | Opposing team's bullpen ERA last 7 days | 4.50 | Quality measure of the relievers a batter may face |
| `opp_relievers_available` | Opposing relievers with 2+ days rest | 5 | Fewer rested arms → manager forced to use tired relievers |
| `opp_bullpen_pitches_last_3d` | Opposing bullpen pitches last 3 days | 0 | Pitch count stress on bullpen arms |
| `opp_pitcher_velo_drop_late_l5` | Opposing starter's velocity decay | 0 | Fatigued starter → earlier bullpen → different hitting environment |
| `opp_pitcher_avg_pitches_per_inning_l5` | Opposing starter's pitch efficiency | 15 | Inefficient starter → earlier exit → more bullpen ABs |
| `opp_pitcher_deep_inning_pct_l5` | How often opposing starter goes 6+ | 0.5 | Deep-going starter means fewer bullpen ABs |

**Source**: `mlb_bullpen_daily_status` (precomputed daily) + `mlb_pitcher_inning_stats` (same as pitcher features, but applied to the opposing starter).

### Why These Specific Features
- The pitcher inning features capture **fatigue dynamics** that the existing rolling averages miss. `avg_so_l5` tells you how many Ks a pitcher averaged, but not *how their stuff degrades across innings*. The TTOP (times-through-order penalty) is one of the most well-documented effects in baseball analytics — K-rate drops ~15-20% by the 3rd time through the order. These features let the model learn that pattern.
- The bullpen features capture **game environment beyond the starting pitcher**. A batter facing a team whose bullpen threw 15 IP in the last 3 days has a very different late-game environment than one facing a fully rested bullpen. This is especially important for the AB model — bullpen quality affects whether a batter gets a 4th or 5th plate appearance.

---

## Files Modified

| File | Lines Added | Changes |
|------|-------------|---------|
| `src/models/mlb/mlb_feature_store.py` | +163 | 6 pitcher features, LATERAL joins, `actual_ip` target, inference helper |
| `src/models/mlb/mlb_batter_feature_store.py` | +111 | 7 batter features, bullpen JOIN, inference helpers |
| `src/processing/mlb/mlb_batter_matchup_features.py` | +111 | 2 new functions (single-game + bulk) for opposing pitcher inning stats |
| `src/models/mlb/mlb_monte_carlo.py` | +255 | `MLBPitcherKCopulaPredictor` + `MLBCompoundBinomialPredictor` |
| `src/models/mlb/mlb_train_pipeline.py` | +144 | Copula training methods, `--copula` flag |
| `src/models/mlb/mlb_batter_train_pipeline.py` | +63 | AB NegBin training step in binomial pipeline |
| `src/models/mlb/mlb_model_suite.py` | +64 | Copula + compound model loading with fallbacks |
| **Total** | **+1011** | |

---

## What's Next

1. **Retrain pitcher K** with `--copula --tune --tuning-trials 100` on 2024-2025 data
2. **Retrain batter hits** (AB model will train automatically alongside Binomial)
3. **Validate**: Check IP Q50 predictions (~5-6 IP for good starters), K-rate Q50 (~1.0-1.5 K/IP for strikeout pitchers), AB predictions in 2-5 range
4. **Backtest**: Run sweep on 2025 holdout to compare copula vs single model, compound vs standard binomial
5. **Deploy**: Copy production artifacts, update Railway

---

## Tags
#mlb #model-architecture #pitcher-k #batter-hits #copula #compound-binomial #feature-engineering
