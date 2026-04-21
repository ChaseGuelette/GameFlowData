> Part of [[Handoffs]]

**Date**: April 21, 2026 at 12:30 PM

## Summary

Short, focused session: implemented the MLB feature pipeline fix so that `feat_*` columns in `mlb_daily_predictions` are now populated instead of NULL. The AnalysisModal will now show L5 avg, rest days, and park factor insight bullets for MLB picks automatically. Also fixed a volume anomaly risk in `batter_rbis` paper trading by adding per-stat daily bet caps to `MLB_STATS`.

---

## What Was Done

### MLB Feature Store → Predictions Pipeline (full implementation)

**`src/models/mlb/mlb_stat_config.py`**
- Added `max_daily_bets` caps to three stats:
  - `pitcher_strikeouts`: 10/day
  - `batter_hits`: 15/day
  - `batter_rbis`: 8/day (guards against volume anomaly — backtest avg 2.65/day, was seeing 32/day)

**`src/models/mlb/mlb_daily_runner.py`**
- Added module-level `_STAT_L5`, `_STAT_SZN`, `_STAT_PARK` dicts mapping each stat to its feature store key name
- `_run_pitcher_predictions` — now builds `features_lookup: dict[(player_id, game_id), dict]` and returns 3-tuple `(predictions, samples, features_lookup)`
- `_run_batter_predictions` — same treatment; features_lookup populated from the `batter_features` list; returns 3-tuple
- `run_for_date` — unpacks 3-tuples from both calls, merges into `all_features: dict[(player_id, game_id), dict]`, passes to `_map_features_to_predictions`
- `_map_features_to_predictions` — fully implemented (was a stub that only wrote `feat_opp_abbrev`):
  - Looks up features by `(player_id, game_id)` from the lookup
  - Writes `feat_player_avg_stat_l5`, `feat_player_avg_stat_szn`, `feat_park_factor`, `feat_days_rest`, `feat_lineup_position`
  - Uses `.get()` everywhere — missing features stay `None`, no crashes
- MLB_COLD_OVER filter strengthened: now catches `feat_l5 is None` AND `feat_l5 <= 0.1` (previously only caught `feat_l5 == 0`, which never fired when features were NULL)
- Per-stat daily bet caps: after main BL loop, sorts recommended picks by best edge descending and trims to `max_daily_bets` for any stat that specifies it

**`src/orchestration/mlb_edge_refresh_job.py`**
- Same MLB_COLD_OVER filter fix applied (mirror of daily runner change)

---

## Decisions Made

### Feature lookup keyed by (player_id, game_id), not (player_id, game_id, stat)
Each batter gets one feature dict covering all stats (base features computed once). The features dict contains `batter_avg_rbi_l5`, `batter_avg_h_l5`, etc. all at once. Keying by `(player_id, game_id)` is correct — the same features dict is used for all of a player's stat predictions in a game.

### MLB_COLD_OVER threshold: `<= 0.1` not `== 0`
A player with L5 avg of 0.05 RBIs should still be treated as "cold" for over purposes. Also, now that features are populated, `feat_l5 is None` should no longer be common — but catching it prevents edge cases during rollout.

### `max_daily_bets=8` for batter_rbis
The backtest (241 bets over Apr-Jun 2025) averaged 2.65/day. Seeing 32/day in production — 12x higher. Most likely cause: incomplete sportsbook line coverage in historical data. Cap at 3x backtest average (8) to throttle paper trading volume to a comparable range. Only takes highest-edge picks when capped.

### No frontend changes needed
The dashboard's `select('*')` already fetches all columns, and `insights.ts` already generates bullets for all the `feat_*` columns. Once the DB rows have real values, the insights appear automatically.

---

## Blockers and Open Questions

- **Verify in DB**: After next MLB inference run, query `SELECT feat_player_avg_stat_l5, feat_days_rest, feat_park_factor FROM mlb_daily_predictions WHERE game_date = CURRENT_DATE LIMIT 10` to confirm non-NULL values
- **batter_hrr L5 key**: `batter_avg_hrr_l5` and `batter_avg_hrr_szn` are mapped but need to confirm these keys exist in the HRR feature store (may not — HRR is a derived composite stat)
- **Apr 19 PnL fix**: `scripts/fix_apr19_pnl.py` still not run — 21 NBA bets have garbage PnL
- **NBA trading**: Still `NBA_TRADING_ENABLED=false` — paused post-Apr 19
- **MLB late-season sweep**: Jul-Sep BL configs not yet run

---

## Recommended Next Steps

1. **Monitor next MLB inference run** — check `mlb_daily_predictions` for non-NULL `feat_*` columns. Open AnalysisModal on an MLB pick — should show L5 avg, rest days, park factor bullets.
2. **Verify batter_hrr feature keys** — confirm `batter_avg_hrr_l5` exists in the HRR feature store output, or adjust `_STAT_L5` to use the correct key.
3. **Implement Manual Paper Trader (Phase 10)** — scope was written last session. DB migration (`is_paper_trade` column), AnalysisModal button, History tab toggle.
4. **Run fix_apr19_pnl.py** — fixes the 21 Apr 19 bets with null fill_price.
5. **Re-enable NBA trading** — once v2 playoff model paper results look clean for 2-3 days.

---

## Files to Read on Resume

- [[handoff-038]] — this session
- [[handoff-037]] — previous session (bankroll manager, nixpacks fix, manual paper trader scope)
- [[Execution-Plan]] — Phase 10 (Manual Paper Trader) scoped and ready
- [[Operations/Kalshi-Live-Trading-Startup]] — NBA re-enable checklist
