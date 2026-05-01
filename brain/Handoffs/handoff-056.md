# Handoff 056

> Part of [[Handoffs]]

**Date**: May 1, 2026

## Summary

Backtest comparison confirmed the copula pitcher K model is worse than the single model (no-BL baseline entirely negative). Diagnosed root cause as genuinely independent causal pathways for IP and K/IP (not ratio suppression). Decided on Option 4: improve IP sub-model with 2 missing features, then feed IP quantile predictions as volume-channel features into the direct K model. Full implementation plan written to brain/Decisions/.

## What Was Done

- Confirmed backtest sweep auto-detects copula vs single-model via MLBModelSuite.from_directory()
- Ran copula vs single-model backtest (2026-04-13 to 2026-04-26): copula no-BL all negative, single model +1.45–2.88% at moderate edges
- Extracted key number: ρ(IP, K/IP) = -0.0179 from pitcher_k_copula_params.json
- Analyzed 4 architectural options for fixing the IP model
- Resolved: causal pathway independence (not ratio suppression) explains ρ ≈ 0; ρ(IP, K_total) substantial via volume channel — this is why Option 4 works
- Identified 2 missing features: `pitcher_min_ip_l5` (not in mlb_player_average_pitching), `team_bullpen_ip_last_3d` (in bullpen table, wired for batters only)
- Created `brain/Decisions/Pitcher-K-IP-Feature-Source-May01.md` — full rationale + 5-phase implementation plan

## Decisions Made

1. **Copula NOT archived** — architecture stays, but not promoted to production. Single model remains in production.
2. **Option 4 chosen** — IP model as feature source. Not multiplicative decomposition (K = IP × rate), but IP quantile predictions as risk-adjustment features fed into direct K model.
3. **ρ ≈ 0 causal explanation** — IP and K/IP are genuinely independent after conditioning on features because their residual variance comes from different mechanisms. This supports Option 4: IP→K_total has substantial correlation via volume, not rate.
4. **Validation gate required** — before wiring predicted_ip_q25 into K model, check corr(predicted_ip_q25, pitcher_avg_ip_l5). >0.85 → use delta feature; 0.5–0.7 → use raw q25; <0.5 → investigate.
5. **opp_team_pitches_per_pa_l10 deferred** — data not in existing tables, too much work for unclear gain right now.

## Blockers and Open Questions

- No code changes made yet — all planning
- Need to find the populate averages script that computes mlb_player_average_pitching (likely src/processing/mlb/)
- `pitcher_min_ip_l5` requires DB migration on LOCAL postgres + populate script update + re-run
- If corr(predicted_ip_q25, pitcher_avg_ip_l5) > 0.85, the approach reduces to just "add pitcher_min_ip_l5 to K model" which is much simpler — worth checking before doing the full two-stage pipeline

## Recommended Next Steps

1. Find the populate averages script: `src/processing/mlb/` — look for script that writes to `mlb_player_average_pitching` table, uses `rolling_with_groupby`
2. Add `min_ip_l5` computation (MIN instead of AVG, same window=5) to that script
3. Run local DB migration: `ALTER TABLE mlb_player_average_pitching ADD COLUMN IF NOT EXISTS min_ip_l5 FLOAT`
4. Add `team_bullpen_ip_last_3d` LEFT JOIN to pitcher training query in `mlb_feature_store.py` (pattern from batter side but `bull.team_id = pgs.team_id`)
5. Update PITCHER_K_FEATURES and get_player_game_features() in mlb_feature_store.py
6. Retrain copula (--copula flag) — IP sub-model will pick up new features via dynamic selection
7. Run validation: compute corr(predicted_ip_q25, pitcher_avg_ip_l5) on holdout
8. Based on correlation: implement two-stage K model training OR just rely on static min feature

## Files to Read on Resume

- [[Pitcher-K-IP-Feature-Source-May01]] — Full implementation plan with code snippets
- [[MLB-Model-Architecture-Overhaul-Apr28]] — Background on copula architecture
- `src/models/mlb/mlb_feature_store.py` — Add new features here (PITCHER_K_FEATURES, SQL, inference)
- Populate averages script (find in src/processing/mlb/) — Add min_ip_l5 computation
