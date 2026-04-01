# Handoff 012 — Kalshi Live Trading Bot

> Part of [[Handoffs]]

**Date**: April 01, 2026 at 9:55 AM

## Summary

Built the complete Kalshi auto-trading bot: real order placement via API, portfolio management, 3-layer circuit breaker system, and Discord alerts per trade. This extends the paper trading system (Session 11) into a production live trading system gated by `KALSHI_LIVE_TRADING_ENABLED=true`. Migration applied — all 3 DB tables created.

## What Was Done

- **`src/scrapers/kalshi/kalshi_client.py`** — Extended with `_request_with_body()` for POST/DELETE, plus 5 trading endpoints: `get_balance()`, `get_positions()`, `create_order()`, `cancel_order()`, `get_fills()`
- **`src/paper_trading/kalshi_live_trader.py`** — New `KalshiLiveTrader` class: circuit breakers (30% drawdown halt, $15 daily loss pause, 5-streak pause), Kelly sizing with taker fees, 15% min edge, position accumulation awareness via API, fill reconciliation, resolution with same stat-lookup logic as paper trader, daily log updates
- **`src/discord_bot/alerts.py`** — Added `send_kalshi_trade_alert_sync()` supporting 3 alert types: placed (green), resolved (green/red), circuit_breaker (red). Routes to `DISCORD_CHANNEL_KALSHI_LIVE` with fallback chain
- **`src/orchestration/kalshi_refresh_job.py`** — Added Step 4.5 (between paper trading and Discord alerts): resolve settled positions, reconcile fills, select + execute new trades. Double-gated by `skip_live` flag + env var. Added `--skip-live` CLI flag
- **`migrations/kalshi_live_trading.sql`** — 3 tables: `kalshi_live_orders`, `kalshi_live_trading_daily_log`, `kalshi_live_trading_config` (singleton circuit breaker state). Migration applied directly via Python DB client

## Decisions Made

1. **Taker orders (not limit)** — Instant fills, simpler logic, guaranteed execution. The Kalshi market is thin enough that taker is the right strategy for a $100 account
2. **15% min edge (vs 5% paper)** — Sniper mode to compensate for higher taker fees (7% vs 1.75% maker) and maximize EV per trade
3. **Drawdown halt requires manual reset** — Most protective breaker. Other two (daily loss, streak) auto-reset. This prevents a cascading loss scenario
4. **Position accumulation check via API** — Prevents doubling into same market. Checks existing open positions before each trade
5. **Balance checked from API (not DB)** — Always use real Kalshi balance, never trust cached/calculated values
6. **Ran migration via Python client** — Small CREATE TABLE statements, safe to execute directly rather than Supabase SQL Editor

## Blockers and Open Questions

- **Kalshi account needs funding** — $100 initial deposit required before live trading
- **Dedicated Discord channel** — Optional but recommended: set `DISCORD_CHANNEL_KALSHI_LIVE` to separate live trade alerts from paper/edge alerts
- **Taker fee impact unknown** — 7% taker fee is 4x maker fee. Paper trading uses maker fee. Real edge erosion unknown until live data accumulates

## Recommended Next Steps

1. **Fund Kalshi account** — Deposit $100 (or less for initial testing)
2. **Set `KALSHI_LIVE_TRADING_ENABLED=true`** — In `.env` or Railway vars to go live
3. **Monitor first few trades** — Watch Discord alerts, check `kalshi_live_orders` table, verify fills match expectations
4. **Consider Stripe integration** — Phase 3 is the next major product milestone
5. **MLB model retraining** — `batter_total_bases` and `batter_runs_scored` still need at_bats leakage fix (Step 1.3)

## Files to Read on Resume

- [[Execution-Plan]] — Phase 7 Kalshi steps all completed, see what's next
- `src/paper_trading/kalshi_live_trader.py` — Core live trading logic
- `src/scrapers/kalshi/kalshi_client.py` — Trading API methods
- `src/orchestration/kalshi_refresh_job.py` — Pipeline integration (Step 4.5)

#kalshi #live-trading #circuit-breakers #session-12
