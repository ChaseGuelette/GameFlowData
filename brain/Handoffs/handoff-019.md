> Part of [[Handoffs]]

**Date**: April 01, 2026 at 4:48 PM

## Summary
Aligned the Kalshi paper trader with the live trader 1:1 (taker fees, 15% edge threshold, $80 daily exposure cap, Discord alerts, position accumulation). Added overflow bet tracking so bets skipped due to the exposure cap are stored and resolved separately for hypothetical performance analysis.

## What Was Done

### Paper Trader Alignment (`src/paper_trading/kalshi_paper_trader.py`)
- Switched from maker to **taker fees** throughout (imports, Kelly sizing, edge calculations)
- Added missing NBA stats: `stl`, `blk`, `3pm` (mapped to `fg3m`), bringing total to 10
- Added combined stat resolution: `batter_hits_runs_rbis` for MLB
- Changed defaults to match live trader: bankroll $100, min edge 15%, max contracts 50, daily exposure $80, min volume 20, max spread 15
- Added **position accumulation awareness** — dedup same-day tickers via DB query
- Added **Discord notifications** (blue embeds) on every bet placed and resolved
- Added `_send_trade_alert()` helper method

### Overflow Bet Tracking (NEW)
- When $80 daily exposure cap is hit, bets that would have been taken are logged with `status='overflow'`
- `_store_overflow_bets()` method — inserts with `ON CONFLICT DO NOTHING`
- `resolve_bets()` now queries both `pending` and `overflow` bets
- Overflow bets resolve to `overflow_won`/`overflow_lost`/`overflow_cancelled`
- Overflow P&L is **excluded** from daily log metrics (real paper P&L is clean)
- Log messages include hypothetical overflow P&L for comparison
- Fixed dict mutation bug in partial fill logic (used `dict()` copy instead of mutating shared reference)

### Database Changes
- Updated `kalshi_paper_bets_status_check` constraint to allow: `overflow`, `overflow_won`, `overflow_lost`, `overflow_cancelled`
- Exposure/dedup queries now explicitly exclude overflow statuses

### Live Trader Updates (`src/paper_trading/kalshi_live_trader.py`)
- `max_daily_exposure` updated to $80 (from $30)
- `_fetch_actuals` now handles `COMBINED_STAT_RESOLUTION` for MLB

### Discord Alerts (`src/discord_bot/alerts.py`)
- Added `mode` parameter to embed builders — "paper" (blue) vs "live" (green)
- Title/footer distinguish mode: "KALSHI PAPER TRADE PLACED" vs "KALSHI LIVE TRADE PLACED"
- Channel routing: `DISCORD_CHANNEL_KALSHI` → `DISCORD_CHANNEL_PREDICTIONS` fallback

### Environment
- Added `DISCORD_CHANNEL_KALSHI=""` placeholder to `.env`

## Decisions Made
- **Overflow = entirely skipped bets only** — partial fills are already tracked as real bets; overflow only includes bets that came after the cap was fully exhausted. This avoids double-counting.
- **Separate overflow statuses** (`overflow_won` etc.) rather than a boolean flag — simpler SQL filtering, daily log naturally excludes them via `status NOT LIKE 'overflow%'`.
- **$80 daily exposure** (up from initial $30) — gives more signal while still capping risk at 80% of $100 bankroll.
- **No Discord alerts for overflow resolutions** — they're hypothetical, alerting on them would be noisy and confusing.

## Blockers and Open Questions
- **20 stale bets from old parameters** exist for April 1 with $999.99 exposure (from the $5000/old config). These will resolve normally but represent the old sizing. New parameters will take effect starting April 2.
- **Discord channel not yet created** — `DISCORD_CHANNEL_KALSHI` is empty in `.env`. User needs to create `#kalshi-bot` in Discord and paste the channel ID.
- **Changes not deployed to Railway** — all modifications are local only.

## Recommended Next Steps
1. **Create Discord `#kalshi-bot` channel** and set `DISCORD_CHANNEL_KALSHI` in `.env` + Railway env vars
2. **Deploy to Railway** — commit changes and push to get the aligned paper/live trader running in production
3. **Monitor overflow data** — after a few days, query `SELECT status, COUNT(*), SUM(pnl) FROM kalshi_paper_bets WHERE status LIKE 'overflow%' GROUP BY status` to see if the exposure cap is leaving money on the table
4. **Continue with Execution Plan** — Phase 3 (Stripe monetization) or Phase 1.3/1.6 (MLB batter retrain/backtests) are next priorities
5. **Enable live trading** — when paper trading shows consistent profitability, set `KALSHI_LIVE_TRADING_ENABLED=true` with a funded $100 account

## Files to Read on Resume
- [[Kalshi-Integration-Design]] — Updated design doc with Phase 4+5 alignment details
- [[Execution-Plan]] — Step 7.10 added for paper/live alignment
- `src/paper_trading/kalshi_paper_trader.py` — Core paper trader with overflow logic
- `src/paper_trading/kalshi_live_trader.py` — Live trader (mirrors paper 1:1)
- `src/discord_bot/alerts.py` — Discord integration with paper/live mode
