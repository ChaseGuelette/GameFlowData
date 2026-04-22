> Part of [[Handoffs]]

**Date**: April 21, 2026 at 09:34 PM

## Summary

Investigated and fixed two MLB paper trading data quality issues: the batter_rbis 12x volume anomaly (root cause: sportsbook market expansion, not a bug) and multi-run opposing-direction bets (real bug — now fixed). Also added a bookmaker exclusion list to both the live inference pipeline and backtest sweep to strip out books the user can't bet at (novig, betonlineag) and DFS platforms with invalid pricing.

## What Was Done

### Volume Anomaly Investigation
- **Root cause confirmed**: DraftKings, ESPNBet, BetMGM expanded player prop market coverage 4x between 2025 and 2026 (3.7 → 15.3 distinct players/day for batter stats). Not a bug — the market grew.
- **"Both sides bet" concern debunked**: Proved mathematically impossible (`bl_under_edge = -bl_over_edge` always; they're exact negatives). The 2.06 bets/player figure used the wrong denominator.
- **Backtest vs production gap explained**: Backtest used stale bookmaker whitelist (effectively DK-only); production shops all books for edge calculation. Produces incomparable volumes.

### Bug Fixes
- **`src/paper_trading/mlb_paper_trader.py`** — `select_bets()`: added dedup by `(player_id, stat_type)` keeping highest-edge bet. Prevents multiple inference runs from pushing opposing-direction bets that cancel out.
- **`src/paper_trading/mlb_paper_trader.py`** — `place_bets()`: changed `ON CONFLICT` key from 4-col `(game_date, player_id, stat_type, bet_direction)` to 3-col `(game_date, player_id, stat_type)`. Added `bet_direction` to `DO UPDATE SET` so direction refreshes on subsequent runs.
- **`database/migrations/028_mlb_paper_bets_dedup_constraint.sql`** (new): Drops old 4-col constraint, adds new 3-col constraint `mlb_paper_bets_unique_player_stat_day`. **Migration already applied to Supabase.** 0 existing duplicate rows were deleted (table was already clean).

### Caps Removed
- **`src/models/mlb/mlb_stat_config.py`**: Removed `max_daily_bets` from all stats (`pitcher_strikeouts`, `batter_hits`, `batter_rbis`). These were wrong guardrails masking the volume anomaly.

### Bookmaker Exclusion List
- **`src/models/mlb/mlb_daily_runner.py`**: Added `_EXCLUDED_BOOKMAKERS` constant. `_get_current_lines()` query now has `AND bookmaker NOT IN :excluded_bookmakers`. Excluded: `novig`, `betonlineag`, `dabble_us_dfs`, `betr_us_dfs`, `pick6`, `prizepicks`, `underdog`.
- **`src/backtesting/mlb/run_mlb_sweep.py`**: Added `EXCLUDED_BOOKMAKERS` constant. `_fetch_lines_for_date()` switched from stale whitelist (`--bookmakers` arg) to exclusion approach. `--bookmakers` CLI arg removed. `bookmakers` param removed from `run_shared_phases()` and `_process_date_shared()`.

## Decisions Made

- **Exclusion list over whitelist for bookmakers**: The 2026 sportsbook landscape changes too fast (FanDuel pullback, new state-specific books like HardRockBet_OH appearing). A blacklist is more robust — takes everything TheOddsAPI returns except explicitly invalid books, rather than requiring the whitelist to be maintained.
- **FanDuel absence is not fixable**: FanDuel reduced their TheOddsAPI data sharing agreement. ESPNBet (5,811 rows/day) and BetMGM (3,249) are now the dominant books. The lowest-vig selector handles this automatically.
- **No API switch needed for now**: TheOddsAPI already uses `regions="us,us2,us_ex,us_dfs"` — every available region is already being pulled. FanDuel's absence is FanDuel's decision, not a config issue.

## Blockers and Open Questions

- **2026-era backtest not yet run**: The sweep's bookmaker exclusion fix is deployed, but no sweep has been run against 2026 data yet to validate how the model performs with the new book mix. Run this before adjusting BL configs for late season.
- **Late-season BL config sweep still pending**: MEMORY.md notes this as TODO. Current configs validated on Apr-Jun 2025 only.
- **FanDuel display in dashboard**: User noted they'd prefer to see FanDuel lines in the UI. Only option is switching to OddsJam or Unabated (both reportedly still have FanDuel). No immediate action planned.

## Recommended Next Steps

1. **Run 2026 backtest** to validate current BL configs against the new sportsbook mix — `python src/backtesting/mlb/run_mlb_sweep.py --start 2026-04-01 --end 2026-04-20 --combined --local`
2. **Run late-season sweep** (Jul-Sep 2025) to get late-season BL configs before the mid-season switch — `python src/backtesting/mlb/run_mlb_sweep.py --start 2025-07-01 --end 2025-09-28 --combined --local`
3. **Monitor MLB paper trader volume** over next 1-2 days to verify the dedup fix is working — expect ~10-20 bets/day total, one bet per player per stat per day

## Files to Read on Resume

- `src/paper_trading/mlb_paper_trader.py` — dedup fix in `select_bets()`, updated `ON CONFLICT` in `place_bets()`
- `src/models/mlb/mlb_daily_runner.py` — `_EXCLUDED_BOOKMAKERS` constant, updated `_get_current_lines()` query
- `src/backtesting/mlb/run_mlb_sweep.py` — `EXCLUDED_BOOKMAKERS` constant, reworked `_fetch_lines_for_date()`
- `src/models/mlb/mlb_stat_config.py` — caps removed, current edge thresholds
