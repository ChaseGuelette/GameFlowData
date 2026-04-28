> Part of [[Handoffs]]

**Date**: April 27, 2026 (session recovered from bugged terminal d1125247)

## Summary

Deep investigation into MLB paper trader underperformance post-RBI removal. 2026 backtests confirmed **model drift** (not a production bug) — both batter_hits and pitcher_strikeouts models trained on 2023-2024 data are ~11pp weaker on 2026 data. Decision: retrain both models on 2024-2025, calibrate on early 2026. Also added `--local` flag to pitcher pipeline and identified an excluded bookmaker leak inflating production bet volume.

## What Was Done

- **Diagnosed MLB paper trader performance gap** via SQL analysis + 2026 backtests
  - batter_hits: 63.2% backtest (2025) → 51.6% backtest (2026) → 53.7% production (14-day)
  - pitcher_K: 63.2% backtest (2025) → 53.6% backtest (2026) → ~50% production
  - 2026 backtest matches production almost exactly → pipeline is correct, model is stale
  - Both models still profitable on 2026 data (+12-13% ROI) but much weaker than 2025

- **Added `--local` flag to pitcher pipeline** (`src/models/mlb/mlb_train_pipeline.py`)
  - 4-line change: `local=False` in `__init__`, `get_engine(local=local)`, argparse flag, constructor pass-through
  - Pitcher pipeline previously hit Supabase directly; now can use local Postgres like the batter pipeline

- **Established full retraining + backtest plan** (commands ready to run):

```bash
# Step 1 — Sync all training tables
python scripts/sync_local_db.py --tables mlb_game_schedule mlb_park_factors mlb_players mlb_player_season_advanced mlb_game_weather mlb_player_game_stats_batting mlb_player_game_stats_pitching mlb_player_average_batting mlb_player_average_pitching mlb_player_average_statcast_batting mlb_player_average_statcast_pitching mlb_raw_game_lines mlb_raw_player_props

# Step 2 — Retrain (train on 2024-2025, calibrate on 2026 Mar 25 - Apr 12)
python src/models/mlb/mlb_batter_train_pipeline.py --local --stat batter_hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100
python src/models/mlb/mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100

# Step 3 — OOS backtest on holdout window (Apr 13-26, never seen during calibration)
python src/backtesting/mlb/run_mlb_sweep.py --local --start 2026-04-13 --end 2026-04-26 --stats batter_hits --tau 0.9 --edge 0.10 --kelly 0.125 --z-max 0.25 --max-weight 0.50
python src/backtesting/mlb/run_mlb_sweep.py --local --start 2026-04-13 --end 2026-04-26 --stats pitcher_strikeouts --tau 0.75 --edge 0.12 --kelly 0.125 --z-max 0.25 --max-weight 0.80

# Step 4 — After retrain, do full BL sweep on holdout window to find new optimal configs
```

## Decisions Made

- **Model drift confirmed, not a pipeline bug**: The 2026 backtest reproducing production numbers exactly rules out any inference or bookmaker-selection bug as the primary cause.
- **Retrain on 2024-2025, calibrate on 2026**: Eliminates the 2-year stale data gap. 33 days of 2026 is enough for calibration; split Mar 25-Apr 12 for cal, Apr 13-26 as OOS holdout.
- **No probability floor bandaid**: Adding a `model_prob >= 0.65` floor was discussed but rejected — it's a bandaid to a model staleness problem. Fix the root cause with retraining instead.
- **BL blending is NOT the problem**: BL is correctly pulling probabilities down (5-9pp); raw model is the overconfident one.
- **Backtest is NOT DK-only**: Both backtest and production use sharpest line from all non-excluded books. Previous assumption was wrong.

## Findings (Key Data)

**Calibration bucket analysis (14-day production):**
- batter_hits: only profitable when model_prob >= 0.65 (+$5,789). Below 0.65: -$2,507 (model overshoots by 18-19pp)
- pitcher_K: well calibrated at 0.55-0.65 (+$364), catastrophically overconfident at 0.65+ (model 73% → actual 46%, -$732)

**batter_hits was actually profitable overall (+$3,227 over 14 days).** The bankroll crater was almost entirely batter_rbis (-$11,620 over its run). The win rate didn't spike after RBI removal because 52-54% IS the current model's true performance on 2026 data.

## Blockers and Open Questions

- **Excluded bookmaker leak**: `dabble_us_dfs`, `novig`, `betonlineag` appear as sources in `mlb_daily_predictions` despite being on the exclusion list. The backtest code excludes them correctly, but a different code path may be writing excluded-book rows to `mlb_daily_predictions.sportsbook`. This inflates production bet volume vs backtest (~12 hits bets/day production vs ~4.8/day backtest). **Fix still pending.**
- **No --stat flag on pitcher pipeline**: `mlb_train_pipeline.py` is hardcoded for pitcher_strikeouts. Not a problem now but worth adding if other pitcher stats are added later.
- **After retraining**: Need a full BL sweep (tau, z_max, edge, max_weight) on the holdout window to find optimal configs for the new models — old configs may not be optimal.

## Recommended Next Steps

1. **Run the retraining** (manual, user does this locally): Sync tables → train batter_hits → train pitcher_K → run OOS backtests on Apr 13-26 holdout
2. **Fix excluded bookmaker leak**: Trace why excluded books appear in `mlb_daily_predictions` — likely a different code path in the daily runner that doesn't apply the exclusion filter
3. **BL sweep after retraining**: Once new model artifacts are in place, run sweep on holdout window to get new optimal tau/edge/z_max/mw configs
4. **Deploy retrained models to production**: After confirming OOS backtest shows win rate improvement (target: 60%+)

## Files to Read on Resume

- [[handoff-050]] — previous session (Kalshi cap enforcement + early inference timing)
- [[MLB-Models]] — model architecture and training pipeline
- `src/models/mlb/mlb_train_pipeline.py` — pitcher pipeline (just added --local)
- `src/models/mlb/mlb_batter_train_pipeline.py` — batter pipeline
- `src/backtesting/mlb/run_mlb_sweep.py` — backtest sweep harness
