> Part of [[Handoffs]]

**Date**: April 20, 2026 at 12:54 PM

## Summary

Completed the full MLB Feature Gap Sprint: added handedness-stratified (L/R) park factors, weather features (air density, wind, precipitation via Open-Meteo), and park_hr_factor to HRR. Retrained all 4 MLB models, ran early season backtest sweeps, and promoted new configs with direction restrictions removed — model now finds profitable edges in both directions for all stats.

## What Was Done

### Phase 1 — HRR park_hr_factor
- Added `park_hr_factor` to `BATTER_HRR_FEATURES` in `mlb_batter_feature_store.py`

### Phase 2 — Handedness-Stratified Park Factors
- DB migration: added 6 L/R columns to `mlb_park_factors` (hits/hr/runs factor for L and R)
- Extended `PARK_FACTORS` dict in `mlb_reference.py` with FanGraphs L/R values for all 30 parks
- Refactored training SQL in both `mlb_batter_feature_store.py` and `mlb_batter_matchup_features.py` with CASE expressions (switch hitters: vs RHP→L factor, vs LHP→R factor)
- Refactored `_get_park_factors()` for inference (new `bats`/`opp_throws` params)
- Added `backfill_handedness()` to `mlb_reference.py` using MLB Stats API `/people` endpoint
- Ran park factor reseed (116 records) and handedness backfill

### Phase 3 — Weather Integration
- Created `mlb_game_weather` table (DB migration)
- Built `src/scrapers/mlb/mlb_weather_scraper.py` — originally OWM One Call 3.0, switched to Open-Meteo (free, no API key)
- Batched backfill: 1 API call per venue-month (~720 calls vs ~9,700 per-game), 8,216 records stored
- Physics: `air_density_idx` (temp+humidity+pressure normalized), `wind_out_mph` (tailwind toward outfield), `has_precip`
- Added weather features to batter base features and pitcher K features
- Added weather LEFT JOINs to all training + inference SQL
- Created scheduler job at 10:40 AM ET, registered in `sync_local_db.py`

### Model Training
- Retrained all 4 models locally (`--local` flag) with `--tune --tuning-trials 100`
- All models calibrated well (HRR: NLL 1.73, hits: NLL 1.10, RBIs: NLL 0.84, pitcher K: gap 0.009)
- Fixed `_save_training_metadata` JSON serialization bug (date object) in both pipelines

### Backtest Sweeps + Config Promotion
- Ran 9 early season sweeps (Apr-Jun 2025) across all stats, both directions
- **batter_hits** #475: `tau=0.9, z_max=0.25, edge=0.10` — 361 bets, 63.2% win, +28.1% ROI, Sharpe 2.20
- **pitcher_strikeouts** #417: `tau=0.75, z_max=0.25, mw=0.8, edge=0.12` — 174 bets, 63.2% win, +24.0% ROI, Sharpe 2.78
- **batter_rbis** #5: no BL, `edge=0.12` — 241 bets, 64.7% win, +9.5% ROI, Sharpe 1.36
- **ALL direction restrictions removed** — both directions for all stats
- Added `None` BL support to `mlb_daily_runner.py` (raw model probability path)
- batter_hrr: no sportsbook HRR prop lines in DB (Kalshi KXMLBHRR only)

## Decisions Made

1. **Open-Meteo over OpenWeatherMap**: OWM One Call 3.0 requires paid subscription (401 error). Open-Meteo is free, no key, full historical archive back to 1940. Same data quality (ERA5 reanalysis).
2. **Batched weather backfill**: 1 call per venue-month (~720 calls) vs 1 per game (~9,700). Stays well within Open-Meteo's 10k/day free tier.
3. **Direction restrictions removed**: Weather + L/R park factors give the model enough signal to find edges in both directions. Pitcher K both-directions: +24% ROI vs +15.7% under-only. RBIs both-directions: +9.5% vs +44% over-only (but 2x volume and more stable).
4. **No BL for RBIs**: Raw model outperforms BL-blended for early season RBIs. `STAT_BL_CONFIGS` now supports `None` values.
5. **Training always uses --local**: Never hit Supabase directly for training. Sync tables first with `sync_local_db.py`.

## Blockers and Open Questions

- **Late season configs**: Only early season (Apr-Jun) sweeps were run. Need Jul-Sep sweeps before mid-season to set late season BL configs.
- **RBI early season edge is thin**: +9.5% ROI, Sharpe 1.36 — weakest of the 3 stats. Consider disabling early season RBIs if live performance disappoints.
- **batter_hrr untradeable on sportsbooks**: No `batter_hits_runs_rbis` market key in props data. Only Kalshi KXMLBHRR. Need separate Kalshi-specific config if pursuing.

## Recommended Next Steps

1. **Run late season sweeps** (Jul-Sep 2025) for all 3 stats to set late season configs — commands in `.thoughts.md`
2. **Monitor early season live performance** — first clean production run with new features. Watch for weather data quality issues.
3. **Kalshi live trading bot** — still pending account funding and 2-3 day paper validation
4. **Consider Kalshi-specific edge thresholds** — lower edge for volume (0.08 vs 0.10 for sportsbooks)

## Files to Read on Resume

- [[mlb_stat_config.py]] — `src/models/mlb/mlb_stat_config.py` — current BL configs and edge thresholds
- [[Handoffs]] — this handoff for full context
- `.thoughts.md` — backtest commands and results for reference
- `src/scrapers/mlb/mlb_weather_scraper.py` — weather scraper (Open-Meteo, scheduler integration)
- `src/models/mlb/mlb_batter_feature_store.py` — L/R park factor CASE expressions and weather JOINs
