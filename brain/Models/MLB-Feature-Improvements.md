# MLB Model Feature Improvements Plan

> Part of [[Models]]

## Overview

Comprehensive list of feature improvements for the MLB pitcher strikeouts and batter hits models, organized into prioritized batches. Derived from diagnostic analysis of 2026 OOS performance degradation and academic literature review.

**Feature config locations:**
- Pitcher K features: `src/models/mlb/mlb_feature_store.py` lines 29-80 (`PITCHER_K_FEATURES`)
- Batter hits features: `src/models/mlb/mlb_batter_feature_store.py` lines 52-111 (`BATTER_BASE_FEATURES` + `BATTER_HITS_FEATURES`)

---

## Batch 0: Ablation Test (Zero Cost)

Run BEFORE adding any new features. Retrain batter hits model with these 4 features removed, then backtest on the same 2026 OOS window (Apr 13-28). If hit rate improves, the noise was hurting.

| Feature to Remove | Model | Reason |
|---|---|---|
| `batter_avg_hr_szn` | Batter Hits | HR is tiny fraction of hits; `batter_iso_szn` captures power better |
| `batter_avg_hr_vs_hand_l20` | Batter Hits | Redundant with ISO for power signal |
| `batter_avg_r_l5` | Batter Hits | Runs are context-dependent (baserunners, lineup), not batter skill |
| `batter_avg_rbi_szn` | Batter Hits | RBIs depend on who's on base, not hit probability |

**How to run:** Remove from `BATTER_BASE_FEATURES` in `mlb_batter_feature_store.py`, retrain with identical settings, sweep on same OOS window, compare hit rate and ROI.

---

## Batch 1: Ship and Retrain (~16-18 hours total)

### Trivial Wins (~2-3 hours)

#### 1. Wire GB/FB tendency into batter hits model
- **Model:** Batter Hits
- **Impact:** HIGH
- **Effort:** 0 (1-line change)
- **Status:** Data already computed in `mlb_player_average_statcast_batting` as `avg_gb_pct_l10`, `avg_fb_pct_l10`. Just not in the hits feature list.
- **What to do:** Add `batter_gb_pct_l10` and `batter_fb_pct_l10` to `BATTER_HITS_FEATURES`.
- **Why it matters:** Ground-ball hitters have higher BABIP variance. Trajectory profile adds information beyond `batter_hard_hit_pct_l5`.

#### 2. Pitch count efficiency
- **Model:** Pitcher K
- **Impact:** MEDIUM
- **Effort:** 0.25 hours
- **Status:** `avg_pitches_thrown_l3` and `avg_ip_l3` both exist.
- **What to do:** Add derived feature `pitcher_pitches_per_ip_l5 = avg_pitches_thrown_l5 / avg_ip_l5` in `mlb_feature_store.py`.
- **Why it matters:** Proxy for IP prediction without a separate model. High pitches/IP = early exits = fewer K opportunities.

#### 3. First-5-innings K rate
- **Model:** Pitcher K
- **Impact:** MEDIUM
- **Effort:** 0.75 hours
- **Status:** `mlb_pitcher_inning_stats` has per-inning strikeouts. `pitcher_avg_k_rate_early_l5` may already partially capture this.
- **What to do:** Compute `pitcher_avg_k_first_5ip_l5` from inning-level data.
- **Why it matters:** K-rate is highest in first time through the order (Brill et al. 2023 TTOP). Useful if pursuing F5 markets.

#### 4. Explicit interaction features (4 features)
- **Model:** Both
- **Impact:** HIGH (especially for tail calibration)
- **Effort:** 1-2 hours
- **Status:** All component features already exist.
- **What to do:** Add these derived features:

**Pitcher K interactions:**
- `pitcher_k_per_9_l5 * opp_team_k_pct_l10` — core K interaction
- `pitcher_whiff_pct_l5 * opp_team_whiff_pct_l10` — swing-and-miss interaction

**Batter Hits interactions:**
- `batter_babip_szn * opp_pitcher_babip_against_l5` — contact quality interaction (depends on BABIP-against feature being built first)
- `projected_ab * batter_avg_h_l10` — opportunity x recent form

- **Why it matters:** XGBoost learns interactions implicitly via tree splits, but with 33-35 features and moderate depth, it may not consistently find important two-way interactions. The 27pp overconfidence at high-confidence pitcher K predictions is exactly the regime where multiplicative interactions should compress predictions toward the mean.
- **Note:** Do NOT add `pitcher_avg_ip_l5 * pitcher_avg_so_l5` (reconstructs total Ks, redundant) or `batter_avg_h_vs_hand_l20 * is_same_hand` (zero-product problem when is_same_hand=0).

### Low-Effort, High-Impact Pitcher K Features (~7 hours)

#### 5. Pitch repertoire diversity
- **Model:** Pitcher K
- **Impact:** HIGH
- **Effort:** 2-3 hours
- **Status:** `mlb_pitcher_inning_stats` already stores `fastball_pct`, `breaking_pct`, `offspeed_pct` per game. Not aggregated into rolling averages.
- **What to do:**
  1. Add rolling aggregation of pitch type percentages in `mlb_populate_statcast_averages.py`
  2. Engineer 3 features: `pitcher_velo_range_l5` (max pitch type velocity - min), `pitcher_movement_diversity_l5` (std of vertical movement across types), `pitcher_num_pitch_types` (distinct types thrown >5% of time)
- **Why it matters:** Martin (2019) showed distance between pitch clusters is more predictive than any single pitch metric. Likely decorrelated from bookmaker lines.
- **Source:** Martin 2019

#### 6. Opposing lineup K-rate (player-level)
- **Model:** Pitcher K
- **Impact:** HIGH
- **Effort:** 2.5 hours
- **Status:** Lineup data in `mlb_game_lineups` + player K% in `mlb_player_season_stats`. Currently only team-level `opp_team_k_pct_l10` in model.
- **What to do:** At inference time, compute `projected_lineup_k_pct` as PA-weighted average K% of confirmed lineup batters. Fall back to team average if lineup not yet confirmed.
- **Why it matters:** A team's 25% K-rate overall vs tonight's lineup with 20% K-rate is a real difference the model currently averages over.

#### 7. Pitcher handedness x lineup composition
- **Model:** Pitcher K
- **Impact:** HIGH
- **Effort:** 2-2.5 hours
- **Status:** `mlb_players.throws` and `mlb_players.bats` exist. Lineup data exists. No platoon features in pitcher K model.
- **What to do:** Compute `pct_opp_lineup_same_hand` (% of opposing batters with same handedness as pitcher) from confirmed lineup + player handedness data.
- **Why it matters:** Batters strike out more against opposite-hand pitchers. A lefty facing 6 righties vs 6 lefties is materially different. This is the biggest blind spot in the current pitcher K feature set.

### Shared Infrastructure (~6-8 hours)

#### 8. Umpire identity / zone tendency
- **Model:** Both (Pitcher K + Batter Hits)
- **Impact:** HIGH
- **Effort:** 6-8 hours
- **Status:** No umpire data anywhere in the system. MLB Stats API exposes umpire assignments via `/game/{gameId}/boxscore`.
- **What to do:**
  1. New scraper: `mlb_umpire_scraper.py` fetching home plate ump from MLB Stats API
  2. New table: `mlb_game_umpires` (game_id, umpire_id, umpire_name, position)
  3. Rolling features: `umpire_called_strike_rate_above_expected_l20` (preferred — normalized by pitcher quality in those games) or simpler `umpire_avg_k_per_game_l20` (confounded by pitcher quality)
  4. Wire into both feature stores
- **Why it matters:** Different umps have materially different zone sizes. Wide-zone ump = more called strikes = more pitcher-friendly counts = more Ks. Backed by Yee & Deshpande 2024, Hsu 2024, Chen-Moskowitz-Shue gambler's fallacy paper. Probably worth 0.5-1% hit rate improvement because it's variance the model currently treats as noise.
- **Normalization note:** Simple `umpire_avg_k_per_game_l20` is confounded by pitcher quality (ump who works Dodgers games looks high-K because of the staff). At minimum, normalize by subtracting the average K/game of the pitchers who started in those games.
- **Sources:** Yee & Deshpande 2024, Hsu 2024, Chen-Moskowitz-Shue

### Medium-Effort Features (promoted to Batch 1)

#### 9. Opposing pitcher BABIP-against
- **Model:** Batter Hits
- **Impact:** HIGH
- **Effort:** 4-6 hours
- **Status:** No pitcher BABIP in any table. Statcast has ball-in-play outcome data but it's not aggregated into BABIP.
- **What to do:**
  1. Compute pitcher BABIP = hits_in_play / (hits_in_play + outs_in_play) from Statcast batted ball data
  2. Add to `mlb_player_average_statcast_pitching` as rolling `avg_babip_against_l5`
  3. Wire into batter feature store as `opp_pitcher_babip_against_l5`
- **Why it matters:** BABIP-against directly predicts hit conversion rate on balls in play — the mechanism through which batter hits happen. Some pitchers consistently allow higher BABIPs due to fly-ball/ground-ball tendencies and soft contact rates. More important than ERA for predicting hits.
- **Dependency:** The `batter_babip_szn * opp_pitcher_babip_against_l5` interaction feature (item 4) depends on this being built first.

---

## Batch 2: After Measuring Batch 1 Delta

Ship these after batch 1 is deployed and measured. Lower priority or dependent on batch 1 results.

#### 10. Sprint speed
- **Model:** Batter Hits
- **Impact:** MEDIUM-HIGH
- **Effort:** 3-4 hours
- **Status:** Statcast has sprint speed data but scraper doesn't extract it.
- **What to do:** Modify `mlb_statcast_scraper.py` to extract `sprint_speed` from Statcast responses. Add to schema. Season-level average is sufficient (stable metric).
- **Why it matters:** Fast batters turn infield grounders into hits more often. Directly affects BABIP and hit conversion rate.

#### 11. Day/night indicator
- **Model:** Batter Hits
- **Impact:** LOW-MEDIUM
- **Effort:** 1 hour
- **Status:** `game_time_utc` exists in `mlb_game_schedule`. Just needs boolean derivation.
- **What to do:** Derive `is_night = game_time_utc local hour >= 17`. Add to feature store.
- **Why it matters:** Some batters have meaningful day/night splits. Lighting affects batted-ball outcomes. Cheap to add.

#### 12. First-5-innings K rate (if pursuing F5 markets)
- **Model:** Pitcher K
- **Impact:** MEDIUM
- **Effort:** 0.75 hours
- **Status:** Already listed in batch 1 trivial wins. Move here if not pursuing F5 markets immediately.

---

## Batch 3: Only If Batches 1-2 Show Meaningful Improvement

High effort, uncertain ROI. Only pursue if the model is responding well to new features.

#### 13. Manager pull tendency / bullpen state
- **Model:** Pitcher K
- **Impact:** MEDIUM
- **Effort:** 7-11 hours
- **Status:** Bullpen workload scraper exists (`mlb_bullpen_workload_scraper.py` → `mlb_bullpen_daily_status`). Manager decision patterns need play-by-play scraping.
- **What to do:** Build `opp_manager_avg_starter_ip_l10` or `pitcher_avg_game_score_to_pull_l5`. For bullpen: `bullpen_workload_last_3d` (if pen is gassed, starter goes deeper = more K opportunity).
- **Why it matters:** IP variance is a real driver of pitcher K losses (54-62% of high-confidence losses at ≤5 IP per diagnostic). But uncertain whether this is predictable vs random.

#### 14. Opposing team defensive quality
- **Model:** Batter Hits
- **Impact:** LOW-MEDIUM
- **Effort:** 4-6 hours
- **Status:** No OAA/DRS data in schema. Would need FanGraphs or Baseball Savant scraping.
- **What to do:** Scrape team-level OAA from Baseball Savant or compute simple team fielding percentage.
- **Why it matters:** Same quality contact yields more hits against worse defenses. Effect is real but small and consistent.

---

## Features Already Present (No Work Needed)

These were suggested but the system already has them:

| Feature | Model | Current Implementation |
|---|---|---|
| Opp pitcher WHIP / hits-allowed rate | Batter Hits | `opp_pitcher_avg_whip_l5`, `opp_pitcher_avg_h_allowed_l5` in feature store |
| Lineup position | Batter Hits | `mlb_lineup_scraper.py` → `mlb_game_lineups` → feature store with `avg_batting_position_l20` fallback |
| GB/FB tendency (data) | Batter Hits | Computed in `mlb_player_average_statcast_batting` — just not wired into hits feature list (see Batch 1 item 1) |
| Inning-level K features | Pitcher K | `pitcher_avg_k_rate_early_l5`, `pitcher_velo_drop_late_l5`, etc. already in model |
| Park factors | Both | `park_so_factor` (pitcher K), `park_hits_factor` (batter hits) |
| Weather | Both | `air_density_idx` (both), `wind_out_mph` (pitcher K) |

---

## Execution Notes

- **Ablation first (Batch 0):** Zero cost. Retrain batter hits with 4 features removed, backtest, compare. Do this before any new feature engineering.
- **Umpire scraper is shared infrastructure:** One build serves both models. Schedule the scraper to run with enough lead time before inference (ump assignments available ~24h before game time).
- **BABIP-against is a batch 1 dependency:** The `batter_babip × opp_pitcher_babip_against` interaction feature can't ship until BABIP-against is built.
- **Retrain both models after batch 1:** Run full sweep on 2026 OOS window to measure aggregate improvement before proceeding to batch 2.
- **Feature importance extraction:** After batch 1 retrain, save XGBoost `feature_importances_` to artifact directory for ongoing monitoring.
