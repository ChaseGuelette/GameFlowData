> Part of [[Handoffs]]

**Date**: April 24, 2026 at ~7:00 PM

## Summary
Infrastructure bug-fix session. Fixed three production issues: MLB edge refresh CTE query failure, Kalshi orderbook refresh overrun, and a systemic UTC/ET timezone boundary bug affecting 11 SQL callsites across 6 files. The timezone bug was the root cause of 19 failed live trades on April 22.

## What Was Done

### Fix 1: MLB Edge Refresh CTE Failure
- **File**: `src/orchestration/mlb_edge_refresh_job.py`
- Replaced `pd.read_sql(fresh_query, conn, params={"target_date": ...})` with `conn.execute(text(fresh_query).bindparams(target_date=...))` — SQLAlchemy 2.0 does not reliably bind named params through `pd.read_sql`
- Symptom was `psycopg2.ProgrammingError (sqlalche.me f405)` at 2:30 PM ET daily

### Fix 2: Kalshi Orderbook Refresh Overrun
- **File**: `src/orchestration/kalshi_refresh_job.py`
- Parallelized serial orderbook fetch with `ThreadPoolExecutor(max_workers=10)`
- Was: ~1,055 tickers × ~0.5–1s = 16–17 min (every other run skipped on 10-min schedule)
- Now: ~1 min

### Fix 3: Global numpy.int64 psycopg2 Adapter
- **File**: `src/db/client.py`
- Added `psycopg2.extensions.register_adapter(np.int64, ...)` at module level
- Fixes NBA EdgeRefreshJob silent per-player `can't adapt type 'numpy.int64'` errors

### Fix 4: UTC/ET Timezone Bug — 11 Callsites (Systemic)
- **Root cause of Apr 22 failed trades**: `snapshot_time::date = :target_date` compares UTC-stored timestamp against ET date. After 8 PM ET (midnight UTC), evening game snapshots roll into the next UTC day, causing them to appear as "tomorrow's markets" or as "today's markets" on the next ET day.
- **Apr 22 incident**: 19 failed queue entries (5 unique players × ~4 re-queue cycles). April 21 ET game snapshots had UTC date April 22 → appeared in April 22 trade selection → Kalshi rejected orders on settled markets.
- **Fix**: All 11 occurrences of `snapshot_time::date` and `placed_at::date` changed to `(snapshot_time AT TIME ZONE 'America/New_York')::date`
- Files fixed: `market_matcher.py` (4), `kalshi_live_trader.py` (2), `kalshi_paper_trader.py` (1), `arb_paper_trader.py` (1), `kalshi_edge.py` (1), `kalshi_refresh_job.py` (2)
- Zero bare `::date` casts remain (verified)

## Decisions Made
- **UTC fix uses `America/New_York`** (not `US/Eastern`) — consistent with the existing safe pattern already used in `daily_runner.py` and `edge_refresh_job.py`
- **`select_trades()` confirmed**: Does NOT use `mlb_daily_predictions.is_recommended` — queries `kalshi_markets` directly with live model_prob. Stale predictions were NOT the cause of Apr 22 failures.
- **GLM unplanned addition**: Discord alert block added to `mlb_edge_refresh_job.py` post-upsert. Non-breaking (try/except), left in place.

## Blockers and Open Questions
- **batter_rbis still disabled**: `max_daily_bets=8` cap still in place pending 2026-era backtest on expanded player pool
- **Late season MLB configs**: TODO — Jul–Sep backtests needed before mid-season switch
- **NBA trading still paused**: `NBA_TRADING_ENABLED=false` since Apr 19 incident. Playoff v2 model deployed and ready.
- **Star-hitter filter**: Temporary fix for batter_hits NO bets on elite contact hitters. Will be obsoleted when lineup_position pipeline is complete (~90% done).

## Recommended Next Steps
1. **Deploy to Railway** — push current branch to trigger redeploy. All 3 production fixes (MLB edge refresh, Kalshi overrun, numpy adapter) will take effect immediately.
2. **Verify Kalshi refresh timing** — after deploy, check Railway logs to confirm orderbook refresh completes in < 5 min (was 16–17 min).
3. **Re-enable NBA trading** — `NBA_TRADING_ENABLED=true` when ready. Playoff v2 model (`nba_run_20260419_153328`) is deployed and tested.
4. **Lineup position pipeline** — finish the remaining ~10% to obsolete the star-hitter filter.
5. **batter_rbis 2026 backtest** — run on expanded DK player pool to determine if `max_daily_bets=8` cap can be lifted.

## Files to Read on Resume
- [[Execution-Plan]] — current build order and status
- [[handoff-043]] — this file
- `src/paper_trading/kalshi_live_trader.py` — if investigating any live trading issues
- `src/orchestration/kalshi_refresh_job.py` — if checking refresh performance
