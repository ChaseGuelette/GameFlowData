# Handoff 011

> Part of [[Handoffs]]

**Date**: March 31, 2026 at 5:27 PM
**Session**: 11

## Summary

Implemented Kalshi paper trading (Step 7.8) — the final piece before live trading can be justified. Created the database tables, built the `KalshiPaperTrader` class with cents-based Kelly sizing and binary contract P&L, and wired it into the Kalshi refresh job as a non-fatal pipeline step.

## What Was Done

- **Database migration applied** — Created `kalshi_paper_bets` (individual trade tracking with YES/NO sides, cents pricing, contract sizing, maker fees) and `kalshi_paper_trading_daily_log` (daily P&L aggregation, cumulative bankroll). Both tables have RLS with public read policies. Indexes on `(game_date, status)` and `(sport, game_date)`.
- **Created `src/paper_trading/kalshi_paper_trader.py`** — `KalshiPaperTrader` dataclass with:
  - `select_bets()` — queries `kalshi_markets` for fee-adjusted edges, filters by volume/spread, determines YES/NO side, Kelly-sizes in contracts, enforces daily exposure cap
  - `place_bets()` — idempotent UPSERT into `kalshi_paper_bets` (conflict on `game_date, ticker, side`)
  - `resolve_bets()` — resolves against actual stats from `player_game_stats` (NBA) and `mlb_player_game_stats_*` (MLB), binary resolution (no push), cents-based P&L with fee deductions
  - `resolve_all_pending()` — multi-day catchup resolution
  - `_kelly_contracts()` — fee-aware fractional Kelly for binary contracts
  - `_update_daily_log()` — cumulative P&L and bankroll tracking
- **Modified `src/orchestration/kalshi_refresh_job.py`** — Added Step 4 (paper trading) between edge computation and Discord alerts. Resolves previous pending bets, then selects/places new ones. Non-fatal (failures logged, don't block pipeline). Skipped in `--dry-run` and `--mock` modes. Added `--skip-paper` CLI flag.
- **Verified** — Import + init pass, Kelly sizing returns correct contract counts, pipeline runs cleanly with `--mock --skip-discord`, dashboard build succeeds.

## Decisions Made

- **Paper trading skipped in mock mode**: Consistent with the pattern that mock data doesn't have real edges, so paper trading would produce meaningless results. Only runs against real market data.
- **Maker fee assumption for paper trading**: All paper trades assume maker fills (75% cheaper fees). This is realistic for limit orders but optimistic for market orders. Matches the plan to use limit orders only in live trading (Step 7.9).
- **Binary resolution (no push)**: Kalshi contracts are strictly binary — `actual >= line` means YES wins, `actual < line` means NO wins. No push/tie outcome unlike sportsbook props.
- **Reuse of MLB resolution mappings**: Imported `MLB_STAT_RESOLUTION` from `mlb_paper_trader.py` rather than duplicating. NBA resolution uses a different format (`tuple[str, list[str]]` for combo stats like PRA) defined inline.

## Blockers and Open Questions

- **No Kalshi data with edges yet in production** — Paper trading won't generate bets until `kalshi_markets` has rows with non-null `model_prob` and `maker_fee_adjusted_edge`. The edge calculator runs but depends on MC samples matching Kalshi tickers. Need to verify end-to-end with real API data.
- **Step 7.9 (live trading) gated on paper trading profitability** — Need several weeks of paper trading data before justifying live capital.
- **MLB models still need retraining** (from Session 10) — `batter_total_bases` and `batter_runs_scored` have `at_bats` feature leakage.

## Recommended Next Steps

1. **Run the Kalshi pipeline against real API data** — Execute `python src/orchestration/kalshi_refresh_job.py` without `--mock` when markets are open to verify end-to-end: scrape → edges → paper bets → alerts.

2. **Retrain `batter_total_bases` and `batter_runs_scored`** (from Session 10) — Replace `at_bats` with `batter_avg_ab_l5` in training. Highest-priority MLB fix.

3. **Monitor Kalshi paper trading P&L** — After a week of data, check `kalshi_paper_trading_daily_log` for ROI trends. This determines whether Step 7.9 (live trading) is viable.

4. **Phase 3 (Stripe monetization)** — The product is feature-rich enough to monetize. Next big unlock for the business.

## Files to Read on Resume

- [[Execution-Plan]] — Phase 7 status (7.8 now completed)
- `src/paper_trading/kalshi_paper_trader.py` — New Kalshi paper trader
- `src/orchestration/kalshi_refresh_job.py` — Updated pipeline with paper trading step
- [[MLB-Model]] — Outstanding retraining needs from Session 10

#handoff #kalshi #paper-trading
