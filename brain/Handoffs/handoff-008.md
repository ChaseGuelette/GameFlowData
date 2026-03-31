# Handoff 008

> Part of [[Handoffs]]

**Date**: March 31, 2026 at 4:57 PM

## Summary

Full Kalshi Prediction Markets integration built end-to-end across 7 phases: API client with RSA-PSS auth, market scraper with player fuzzy matching, database schema (2 tables + RPC), edge calculator using empirical CDF, scheduler job (every 10 min), dedicated dashboard page with sortable/filterable table and detail modal, and Discord alerts with violet embeds. Also resolved merge conflicts with remote repo (MLB lines jobs integrated alongside Kalshi additions). All code committed and pushed.

## What Was Done

### New Files Created (12)
- `src/scrapers/kalshi/__init__.py` — Package init
- `src/scrapers/kalshi/kalshi_client.py` — API client, RSA-PSS SHA256 auth, rate limiting, retries
- `src/scrapers/kalshi/kalshi_utils.py` — Fee calc, probability conversion, stat maps
- `src/scrapers/kalshi/kalshi_market_scraper.py` — Ticker parsing, player linking, mock/dry-run
- `src/models/kalshi_edge.py` — Edge calculator, empirical CDF, sportsbook comparison
- `src/orchestration/kalshi_refresh_job.py` — 4-step pipeline: scrape, orderbooks, edges, alerts
- `dashboard/src/types/kalshi.ts` — TypeScript interfaces
- `dashboard/src/components/prediction-markets/KalshiCountdown.tsx` — Market closure countdown
- `dashboard/src/components/prediction-markets/KalshiMarketDetail.tsx` — Detail modal
- `dashboard/src/components/prediction-markets/KalshiMarketsTable.tsx` — Sortable/filterable table
- `dashboard/src/app/(protected)/prediction-markets/page.tsx` — Prediction Markets page

### Modified Files (5)
- `requirements.txt` — Added `cryptography>=42.0.0`
- `src/orchestration/scheduler.py` — Added Kalshi refresh job (*/10 min, 11AM-11PM ET) + MLB lines jobs from remote
- `dashboard/src/components/layout/Navbar.tsx` — Added "Markets" link gated by `predictionMarkets` feature flag
- `dashboard/src/lib/sport-config.ts` — Added `predictionMarkets` feature flag (NBA: true, MLB: false)
- `src/discord_bot/alerts.py` — Added Kalshi alert embeds (violet, top 5 by edge)

### Database Migration Applied
- `add_kalshi_tables`: `kalshi_markets` + `kalshi_orderbook_snapshots` tables, indexes, RLS, `get_kalshi_edges` RPC

### Merge Resolution
- Resolved `.thoughts.md` conflict markers (MLB sweep commands + model analysis from remote)
- Integrated MLB lines job additions from remote into scheduler.py alongside Kalshi additions

## Decisions Made

1. **Dedicated Prediction Markets page** (not embedded in AnalysisModal) — Kalshi's data model (YES/NO contracts, order books, bid/ask) is fundamentally different from sportsbook odds. Deserves its own UI.
2. **Empirical CDF always** for model probabilities — `(samples > line).mean()`, never Gaussian CDF. Critical invariant.
3. **Feature flags control nav visibility** — `predictionMarkets: true` for NBA, `false` for MLB. Flip when MLB Kalshi data available.
4. **Graceful no-op when no API credentials** — All Kalshi code exits cleanly when `KALSHI_API_KEY` not set, enabling dry-run/mock development.
5. **Maker vs taker fee display** — Both shown in detail modal so users can see the advantage of limit orders.

## Blockers and Open Questions

- **No Kalshi API credentials yet** — Need to create account and generate RSA key pair before live data flows
- **Paper trading not implemented** — Phase 7.8 in execution plan. Design exists in [[Kalshi-Integration-Design]]
- **Phase 1 Step 1.3 still in_progress** — MLB batter model training commands ready but not yet executed

## Recommended Next Steps

1. **Create Kalshi account and generate API keys** — Required to get live market data flowing. Set `KALSHI_API_KEY` and `KALSHI_PRIVATE_KEY_B64` env vars.
2. **Train MLB batter models** (Step 1.3) — Training commands ready, execute and validate with backtests.
3. **Build Kalshi paper trading** (Step 7.8) — `kalshi_paper_bets` table + `KalshiPaperTrader` class. Design complete in brain.
4. **Stripe monetization** (Phase 3) — Subscribe page, webhook handler, customer portal. Unblocked and high priority.

## Files to Read on Resume

- [[Kalshi-Integration-Design]] — Full design doc with all implementation details
- [[Execution-Plan]] — Updated with Phase 7 (Kalshi) steps and status
- [[Dashboard-Pages]] — Updated with `/prediction-markets` route
- [[Scheduling]] — Updated with Kalshi refresh job schedule

#handoff #kalshi #prediction-markets
