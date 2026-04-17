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
- [[Session-Archive]] - Full archive of Sessions 1-87 (Jan 27 - Mar 24, 2026)
