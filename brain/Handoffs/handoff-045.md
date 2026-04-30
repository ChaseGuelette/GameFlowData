> Part of [[Handoffs]]

**Date**: April 24, 2026 at 03:41 PM

## Summary

Three Kalshi live trading improvements shipped: orderbook price sweep with edge failsafe (real-time price validation before order placement), NBA trading re-enabled (approval queue is now the safety guard), and a complete Discord notification overhaul for the trade approval queue (every-10-min pings for both new and pending trades).

## What Was Done

- **Orderbook sweep system** added to `src/paper_trading/kalshi_live_trader.py`:
  - `_get_best_available_price(ticker, side, target_cents)` — queries live Kalshi orderbook before placing any order
  - Key finding: Kalshi `yes`/`no` arrays contain **bids** (not asks), sorted highest-first. YES ask = 100 - highest NO bid.
  - Two guards: price delta > `KALSHI_SWEEP_MAX_CENTS` (10c default) → skip; recalc edge < original × `KALSHI_SWEEP_EDGE_RETENTION` (0.50) → skip
  - Falls through gracefully on orderbook failure (snapshot+buffer fallback)
  - Discord "placed" alert now includes sweep info (`swept_from → swept_to`) when price moved
  - New env vars: `KALSHI_SWEEP_MAX_CENTS=10`, `KALSHI_SWEEP_EDGE_RETENTION=0.50`

- **NBA trading re-enabled** via Railway env var: `NBA_TRADING_ENABLED=true`
  - NBA uses identical approval queue flow as MLB (propose → queue → dashboard → execute)
  - Approval queue is the safety guard that was missing during Apr 19 incident
  - NBA v2 playoff model (`nba_run_20260419_153328`) already deployed and ready

- **Discord notification system overhauled** in `src/orchestration/kalshi_refresh_job.py` + `src/discord_bot/alerts.py`:
  - `_get_pending_queue_trades()` — queries DB for currently pending trades before calling `select_trades()`
  - `_send_reminder_alert()` — fires every 10 min when no new trades but pending ones exist (gold embed `0xF1C40F`)
  - `_send_trade_approval_alert()` updated with `already_pending` count (shown in embed)
  - `_build_kalshi_reminder_embed()` added to alerts.py; `"approval_reminder"` wired into `send_kalshi_trade_alert_sync()`
  - Result: every 10-min Kalshi refresh now pings Discord whether proposing new trades or reminding about existing ones

## Decisions Made

- **Reminder frequency = 10 min (no throttle)**: User explicitly wanted every-10-min reminders matching the refresh cadence. The 30-min throttle idea was rejected.
- **`already_pending` captured BEFORE `propose_trades()`**: So the count in the new-trade embed reflects trades that were pending from prior runs, not including the batch just added.
- **NBA priority preserved by scheduling**: MLB fires at `:00`, NBA at `:02`. Cross-sport `prior_exposure` param exists in `select_trades()` but requires same-process execution — not applicable with separate Railway jobs. Human can enforce priority via dashboard (approve MLB first).
- **MLB edge refresh job remains separate from live trading**: `mlb_edge_refresh_job.py` has no `KalshiLiveTrader` integration — only `kalshi_refresh_job.py` handles trade proposal for all sports. Correct by design (Kalshi lines ≠ sportsbook lines).

## Blockers and Open Questions

- **MLB priority at execution time**: When both MLB and NBA trades are approved simultaneously, `execute_approved_trades()` processes in DB order (not sport-priority order). Could add an ORDER BY sport to prefer MLB if this becomes an issue.
- **`prior_exposure` never used**: The `select_trades(prior_exposure=)` parameter for cross-sport cap sharing is dead code in the current separate-job architecture. Fine for now, but if both sports ever run in the same process, this should be wired up.
- **Star-hitter filter is temporary**: `KALSHI_STAR_HITS_YES_PRICE=72` blocks NO bets on line=1 batter_hits when yes_price ≥ 72. Root cause is `lineup_position=0` at inference (OOD). Fix: lineup_position pipeline (90% done per MEMORY.md).

## Recommended Next Steps

1. **Monitor NBA trades** — first NBA bets should appear on dashboard after next Railway deploy. Verify they show up in approval panel and the per-sport Discord channel.
2. **Verify sweep logs in Railway** — look for "SWEEP ACCEPTED/REJECTED" log lines after next MLB refresh. Confirm orderbook API response format matches implementation (YES ask = 100 - NO bid).
3. **Verify reminder pings** — after 10 min with pending trades in queue, confirm gold-colored reminder embed fires in Discord.
4. **Lineup position pipeline** — complete the remaining 10% to fix star-hitter model OOD issue and retire the `KALSHI_STAR_HITS_YES_PRICE` filter.
5. **MLB execution priority** — consider adding `ORDER BY sport ASC` to `execute_approved_trades()` query so MLB always executes before NBA when both are approved together.

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — full playbook with env vars, circuit breakers, scaling plan (already updated)
- [[Kalshi-Integration-Design]] — architectural decisions for the approval queue flow
- [[Bot-Tracker]] — dashboard approval panel implementation details
- [[Operations]] — daily runbook and invariants
