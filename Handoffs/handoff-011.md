> Part of [[Handoffs]]

**Date**: April 16, 2026 at 11:27 PM

## Summary

Full MLB paper trader audit revealing a $13,633 drag from direction-enforcement bugs. Three code fixes deployed across `mlb_daily_runner.py`, `mlb_paper_trader.py`, and `sport-config.ts`. All 25 tests pass. Tomorrow's run will be the first clean production day.

## What Was Done

- **Audit**: Ran `scripts/audit_kalshi_bets.py`-style analysis on `mlb_paper_bets` DB table; confirmed paper ROI (+0.8%) was misleading — inflated by `batter_total_bases` and `batter_runs_scored` bets (not shown on frontend)
- **Bug 1 fixed** — `src/models/mlb/mlb_daily_runner.py` `_compute_bl_recommendations()`: was marking `is_recommended=True` based on `max(bl_over_edge, bl_under_edge)` ignoring `allowed_directions` entirely. Now enforces config: picks best allowed direction, falls back to other direction, sets `best_edge=-1` if no allowed direction has edge
- **Bug 2 fixed (discovered mid-session)** — `src/paper_trading/mlb_paper_trader.py` `select_bets()`: was re-deriving direction from raw edge comparison (`bl_over_edge >= bl_under_edge`) without any config awareness. Now imports `MLB_STATS` and checks `allowed_directions` before assigning direction — two-layer enforcement
- **Bug 3 fixed** — `src/paper_trading/mlb_paper_trader.py`: added `AND stat IN ('pitcher_strikeouts', 'batter_hits', 'batter_rbis')` stat filter to SQL — paper trader now only bets stats visible on frontend Model Picks page
- **Bug 4 fixed** — `dashboard/src/lib/sport-config.ts` `perStatConfig`: corrected stale display values (`pitcher_strikeouts` edge 0.05→0.08; `batter_rbis` edge 0.12→0.08, z_max 0.25→0.50, max_weight 0.80→0.65)
- **MEMORY.md updated** with audit findings, two-era distinction, and expected post-fix volume
- **DB investigation**: confirmed all 232 bad-direction bets are from Apr 4–16 production era (NOT sweep data). Sweep era (Mar 25–Apr 1) had correct behavior but 138–287 bets/day (too high for production)

## Decisions Made

- **Two-layer enforcement**: both `_compute_bl_recommendations()` AND `select_bets()` now independently enforce `allowed_directions`. Belt-and-suspenders — if the DB flag is ever wrong, the paper trader won't bet the wrong direction.
- **Stat filter is permanent**: paper trader should always mirror exactly what's on the frontend. Adding new stats to the frontend should be the trigger to update the filter too.
- **Historical paper bets are tainted**: the 1,608 rows in `mlb_paper_bets` span two incomparable eras. Don't use aggregate stats for performance evaluation — always filter to `game_date >= 2026-04-17` (post-fix) for clean signal.

## Blockers and Open Questions

- **Post-fix volume is only ~10–20 good-direction bets/day** (well below the ~50/day expectation). Could be edge threshold too aggressive, or model not generating enough qualified predictions for `batter_hits`. Worth monitoring after a week of clean data.
- **232 bad-direction bets (53 pending)** sitting in `mlb_paper_bets` from Apr 4–16 will resolve with wrong context (they were bet in the wrong direction). PnL history through Apr 16 is unreliable.
- **`batter_hrr` still 0 recommendations** — 341 predictions, 0 recommended. 15% edge threshold may be too high for current model. Monitor after BL sweep completes.

## Recommended Next Steps

1. **Monitor Apr 17 paper bets** — run `SELECT stat_type, bet_direction, COUNT(*) FROM mlb_paper_bets WHERE game_date = '2026-04-17' GROUP BY 1,2` to confirm: no `pitcher_strikeouts OVER`, no `batter_rbis UNDER`, no `batter_total_bases` / `batter_runs_scored` / `batter_home_runs`
2. **Complete batter_hrr sweep (Step 1.9)**: backfill `batter_hits_runs_rbis` odds 2023–2025 → run linker backfill → run BL sweep → promote if ROI>0% Z>1.5
3. **Update `allowed_directions` note in sport-config.ts comment** — add a comment that the stat filter in paper trader SQL must stay in sync with frontend `statTypes` array
4. **Consider marking mlb_paper_bets pre-Apr-17 as "legacy"** — either a `era` column or just a convention in analysis scripts to filter by date

## Files to Read on Resume

- [[Execution-Plan]] — Step 1.9 (batter_hrr sweep) is the active MLB task
- [[handoff-011]] — this file
- `src/paper_trading/mlb_paper_trader.py` — direction enforcement is now in `select_bets()` lines 184–195
- `src/models/mlb/mlb_daily_runner.py` — direction enforcement is now in `_compute_bl_recommendations()` lines 873–894
- `src/models/mlb/mlb_stat_config.py` — source of truth for `allowed_directions` per stat
