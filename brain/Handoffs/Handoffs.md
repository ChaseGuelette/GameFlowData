# Handoffs

> Part of [[BRAIN-INDEX]]

Session continuity notes. Each handoff captures what was done, decisions made, and recommended next steps.

## Session History
- [[handoff-000]] - Brain initialization
- [[handoff-001]] - Multi-sport dashboard (NBA + MLB) implementation
- [[handoff-002]] - MLB batter pipeline aligned with distributional model
- [[handoff-003]] - Mobile UI responsiveness overhaul — eliminated horizontal scroll
- [[handoff-004]] - Binomial model for MLB batter hits + all 5 batter trainers ready
- [[handoff-005]] - MLB Launch Prep: scheduler ungated, RLS policies, analysis modal, scoreboard API
- [[handoff-006]] - Code health: ESLint errors fixed, ruff clean, all 721 tests passing
- [[handoff-007]] - MLB Discord alerts, dashboard sport guards, pipeline audit
- [[handoff-008]] - Kalshi Prediction Markets full integration (7 phases), merge resolution
- [[handoff-009]] - DFS Slip Builder & Entry Tracking (parlay Kelly sizing, history tab, backend resolver)
- [[handoff-010]] - MLB inference bugfixes: naming, prop lines, at_bats leakage, RLS policies
- [[handoff-011]] - Kalshi paper trading: DB tables, KalshiPaperTrader, pipeline integration
- [[handoff-012]] - Kalshi live trading bot: real orders, circuit breakers, Discord alerts, migration applied
- [[handoff-013]] - MLB backtest timeout fix + at_bats data leakage code fix
- [[handoff-014]] - Fixed failing Python test: game ID zero-padding assertion
- [[handoff-015]] - Database optimization (47 GB freed), RLS fixes, local Postgres sync system
- [[handoff-016]] - MLB pipeline debugging: Supavisor timeout, bet resolution, Discord alerts
- [[handoff-017]] - Local Postgres setup, gameflow_local created, MLB sweep commands for hits/home_runs
- [[handoff-018]] - AI Chat revamp: persistence, enriched data (depth chart, advanced stats, injury timeline), markdown rendering
- [[handoff-019]] - Kalshi paper/live trader alignment, overflow bet tracking, Discord alerts with mode distinction
- [[handoff-020]] - MLB model evaluation: HR dropped, hits backtested (+36.3% ROI), TB/runs retrained
- [[handoff-021]] - Bot Tracker page + admin access control (admin_users, is_admin, RLS, middleware, /bot-tracker)
- [[handoff-022]] - NBA calibration health check (HOLD, +9.8% ROI) + DFS bookmaker dashboard bug fix
- [[handoff-023]] - Black-Litterman Kalshi blending + Railway pipeline debugging (env var fix, 607 markets matched)
- [[handoff-024]] - MLB model promotion: 3 stats live (pitcher K, hits, RBIs), 3 dropped (TB, runs, HR), per-stat BL configs, dashboard updated
- [[handoff-025]] - MLB + NBA paper trader fixes: per-stat BL configs (MLB), stored BL values + removed sanity checks (NBA)
- [[handoff-026]] - Kalshi NO-only overhaul: cut YES bets, stat whitelist, bankroll-proportional exposure, BL fix in live trader, analysis script, live trading startup playbook
- [[handoff-027]] - Bot Tracker enhancements (Value column + Kalshi link), ruff fixes, MLB Model Picks per-stat params, BL tau restored for MLB, MLB Stats Vault (migration 023 pending apply)
- [[handoff-028]] - Polymarket-Kalshi Arbitrage Scanner: full pipeline (scraper, matcher, scanner, job), 2 DB tables, Discord alerts, Railway env var
- [[handoff-029]] - Track Record page (migration 026, CSV import, 5 components, hook, page, navbar link) + History page edit/delete for all bet statuses
- [[handoff-030]] - Backtesting 55x speedup docs, MLB sweep fast path, playoff model deployed (tau=0.9/z_max=0.25/mw=0.8/edge=0.12, +19.3% ROI OOS), NBA_PLAYOFF_MODE=true on Railway
- [[handoff-031]] - Kalshi game-level arb pipeline activated: KALSHI_GAME_SERIES populated (27 series via /events API), 4 bugs fixed, MIN_KALSHI_BID=3c filter, 29 clean pure arbs from 203 matched pairs
- [[handoff-032]] - Stripe subscription integration complete: DB migration, 5 new files (webhook/checkout/portal routes, stripe lib, admin client), 6 modified files (subscribe/account/pricing pages, types, subscription lib, middleware), paywall toggle (`SUBSCRIPTION_REQUIRED`), Stripe v22 dahlia API breaking changes documented
- [[handoff-033]] - Arb Scanner dashboard page (Step 9.6), Phase 9 complete, batter_hrr + NBA checks marked done
- [[handoff-034]] - Kalshi in-play contamination guards: close_time column, price filter, near-close detection, Bot Tracker Placed/Game Start columns
- [[handoff-035]] - Non-sports arb: deterministic structured field extractor replacing SequenceMatcher 0.55, "eth"→"ether" keyword fix
- [[handoff-036]] - Elections + Politics non-sports arb expansion: 200 → 6,439 markets, candidate disambiguation, 144 matched pairs
- [[handoff-037]] - Bankroll manager overhaul (per-sportsbook balances + override), bot tracker fill-price fix, Railway libz.so.1 fix, Manual Paper Trader scoped (Phase 10)
- [[handoff-038]] - MLB feature pipeline fix: feat_* columns now populated (L5 avg, rest days, park factor), MLB_COLD_OVER filter strengthened, per-stat daily bet caps added
- [[handoff-039]] - Phase 10 Manual Paper Trader shipped: is_paper_trade column, Paper Trade button, Real/Paper toggle, resolver + scheduler wiring, onConflict + isPaperTrade forwarding bug fixes
- [[handoff-040]] - Calibration Discord alerts: sample-size awareness (LOW_CONFIDENCE_THRESHOLD=75, relaxed thresholds, severity cap, "Early Signal" title)
- [[handoff-041]] - Kalshi sportsbook line alignment, live trader SQL bug fix, star-hitter filter (yes_price >= 72), approval panel SB line display
- [[handoff-042]] - Fixed broken Kalshi contract links (series from ticker prefix, all lowercase); added BetAnalysisModal to bot tracker (L5 history + bet metadata for historical bets)
- [[handoff-043]] - Infrastructure bug-fix: MLB edge refresh CTE fix, Kalshi orderbook parallelization (16 min → 1 min), numpy.int64 psycopg2 adapter, systemic UTC/ET timezone fix (11 callsites across 6 files — root cause of Apr 22 failed trades)
- [[handoff-044]] - Kalshi fill polling (5-min job), reconcile_fills() date bug fix, Discord alerts for placed/filled/resolved, live trader daily performance summary
- [[handoff-045]] - Orderbook price sweep, NBA trading re-enabled, Discord queue notifications every 10 min
- [[handoff-046]] - Kalshi failed trade visibility + retry: Discord failure alert, failed orders section in bot-tracker, one-click Retry button
- [[handoff-047]] - Kalshi live trader: sweep resize, fill_price/game_start_time fixes, F821 lint fix, cap-aware resizing identified as bug
- [[handoff-048]] - Stale fill cancellation queue: detection job, human-approval dashboard panel, execution job, cancel API routes; Kalshi Kelly bet sizing analysis
- [[handoff-049]] - NBA Analysis Modal fixes (headshots, team display, combo stats), TradeApprovalPanel NBA team bug, sport gate safe default + renewal bypass fix
- [[handoff-050]] - Cap-aware exposure clamp in execute_trades; MLB/NBA early window shifted ~1hr earlier; reprice_stale_orders SQL bug fixed
- [[handoff-051]] - MLB model drift confirmed (2026 backtest: hits 51.6%/+12% ROI, K 53.6%/+13%); retrain plan ready (2024-2025 train, 2026 cal); --local flag added to pitcher pipeline; excluded bookmaker leak identified
- [[handoff-052]] - Kalshi resolution pipeline 4-bug fix (reconcile_fills): fill data preserved from cancellation, fill_price derivation fallback, pending-to-filled promotion, WHERE clause expanded. 32 incorrectly cancelled orders restored, 21 resolved. batter_hits kill zone identified (yes_price 65-71), DB P&L approximate (expected vs actual fill prices).
- [[Session-Archive]] - Full archive of Sessions 1-87 (Jan 27 - Mar 24, 2026)
