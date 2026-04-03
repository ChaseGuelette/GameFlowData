# GameFlowDataBrain

> Sports betting prediction platform — NBA live & profitable, MLB/NCAAB expanding, business growth ahead.

**Created**: March 24, 2026
**Owner**: Chase — Founder & Solo Developer

## Folders
- [[Models]] - NBA/MLB/NCAAB model development, calibration, backtesting
- [[Pipeline]] - Scrapers, linkers, processing, orchestration, scheduling
- [[Product]] - Dashboard features, UX decisions, frontend architecture
- [[Infrastructure]] - Railway, Vercel, Supabase, Discord, monitoring
- [[Business]] - Monetization, Stripe, pricing, growth strategy
- [[Operations]] - Daily runbooks, invariants, incident response, maintenance
- [[Decisions]] - Key technical and business decisions with rationale
- [[Handoffs]] - Session continuity notes
- [[Templates]] - Reusable note structures

## Root Files
- [[CLAUDE.md]] - Brain DNA and agent instructions
- [[Execution-Plan]] - Phased build roadmap with task tracking
- [[Assets]] - Images, videos, PDFs, and other media

## Agents
- [[builder]] - Implements features, ships code, makes technical decisions
- [[strategist]] - Product thinking, monetization, growth, go-to-market
- [[analyst]] - Model performance, calibration, backtesting, data analysis
- [[ops]] - Infrastructure, monitoring, pipeline health, incident response

## Templates
- [[Templates]] - Reusable note structures

## Session Log
- **Session 0**: Brain initialized. March 24, 2026
- **Session 1**: March 24, 2026 — Multi-sport dashboard (NBA + MLB) implemented. 2 files created, 12 modified. Build clean. Phase 2 (MLB Dashboard Features) added to Execution Plan.
- **Session 2**: March 24, 2026 at 2:05 PM — MLB batter pipeline aligned with distributional model. NLL-based feature selection, PMF calibration, Optuna NegBin tuner built. 1 file created, 2 modified. Step 1.1 completed, Step 1.3 in progress.
- **Session 3**: March 24, 2026 at 2:09 PM — Mobile UI responsiveness overhaul. Eliminated horizontal scroll on all protected pages. Scrollable tab bars, collapsible DFS filters, mobile card layout for DFS table, responsive HeatmapTable. 9 files modified. Build clean.
- **Session 4**: March 24, 2026 at 10:20 PM — Built Binomial model for MLB batter hits (custom XGBoost objective, logit link, at-bats via weights). Created BinomialModel + BinomialHyperparameterTuner. Updated training pipeline routing, feature selection, feature store, model suite. All 5 batter stat trainers ready. 2 files created, 6 modified.
- **Session 5**: March 24, 2026 at 10:22 PM — MLB Launch Prep. Ungated scheduler (removed April-October gate), applied RLS policies for all MLB tables, added MLB game history + bookmaker lines to Analysis Modal, built MLB scoreboard API, flipped scoreboard feature flag. 6 files modified, 1 migration applied. Build clean. Phase 1 steps 1.2/1.5/1.6 completed, Phase 2 steps 2.1-2.3 completed.
- **Session 6**: March 24, 2026 at 10:45 PM — Code health sweep. Fixed all ESLint errors (DfsTable refactor, performance page immutability, unused vars), all ruff errors (TYPE_CHECKING imports, unused var), confirmed 721 Python tests pass. 10 files modified.
- **Session 7**: March 24, 2026 at 10:48 PM — MLB Discord alerts wired up with sport-specific channel routing (falls back to shared). DFS + Stats Vault pages get sport guards for MLB. Full pipeline audit confirms MLB ready for first production run. 5 files modified. Build clean.
- **Session 8**: March 31, 2026 at 4:57 PM — Kalshi Prediction Markets full integration: API client (RSA-PSS auth), market scraper (ticker parsing, player fuzzy matching), DB schema (2 tables + RPC), edge calculator (empirical CDF), scheduler job (*/10 min), dashboard page (sortable table, detail modal, countdown), Discord alerts (violet embeds). Resolved merge conflicts with remote MLB lines jobs. 12 files created, 5 modified, 1 migration applied. Build clean.
- **Session 9**: March 31, 2026 at 5:12 PM — DFS Slip Builder & Entry Tracking: user-facing leg selection on DFS page, parlay Kelly sizing, Supabase entry placement, history tab with DFS entries/summary/P&L, backend Python resolver integrated into daily job. 10 files created, 5 modified. Build clean. Phase 2.5 added to Execution Plan.
- **Session 10**: March 31, 2026 at 5:17 PM — MLB inference bugfixes: fixed model naming mismatch (batter_runs_scored), added per-stat prop line bulk fetch, mapped at_bats→projected_ab proxy, fixed RLS policies blocking dashboard. Identified 2 models needing retrain (total_bases, runs_scored — at_bats leakage). 2 files modified, 1 migration applied. 688 tests pass, linter clean.
- **Session 11**: March 31, 2026 at 5:27 PM — Kalshi paper trading (Step 7.8): DB tables (`kalshi_paper_bets`, `kalshi_paper_trading_daily_log`), `KalshiPaperTrader` class (Kelly sizing, cents-based P&L, liquidity filters), pipeline integration with `--skip-paper` flag. 1 file created, 1 modified, 1 migration applied. Build clean.
- **Session 12**: April 01, 2026 at 9:55 AM — Kalshi live trading bot (Step 7.9): `KalshiLiveTrader` with 3 circuit breakers (drawdown/daily loss/streak), taker-fee Kelly sizing, 15% edge threshold, position accumulation awareness, Discord alerts per trade. Extended `KalshiClient` with 5 trading endpoints. 2 files created, 3 modified, 1 migration applied. Phase 7 complete.
- **Session 13**: April 01, 2026 at 2:48 PM — Test maintenance: fixed 1 failing test (`test_player_prop_scraper.py`) caused by game ID zero-padding mismatch. Verified `zfill(10)` is essential across 14+ pipeline locations. 1 file modified. 688 tests pass.
- **Session 14**: April 01, 2026 at 3:08 PM — Major DB optimization: dropped 47 GB of unused indexes (116 GB → 69 GB), fixed 9 RLS policies (`auth.uid()` → `(select auth.uid())`), built local Postgres sync system (`scripts/sync_local_db.py`) with incremental support, added `--local` flag to 5 training/backtest scripts. MLB backtest matchup cache optimization. Phase 5 steps 5.2, 5.6, 5.7 completed.
- **Session 15**: April 01, 2026 at 3:24 PM — MLB pipeline debugging: fixed Supavisor timeout killing Railway daily stats (SET statement_timeout in averages script), fixed pitcher_outs column mismatch, added MLB Discord P&L notifications, disabled paper bets until leaky models retrained, backfilled 2026 game stats locally (946 bets resolved). 4 files modified, deployed to Railway.
- **Session 16**: April 01, 2026 at 3:57 PM — Local Postgres setup: confirmed PostgreSQL 18.1 installed, created `gameflow_local` database, configured credentials in `.env`, MLB data synced. Provided parallel sweep commands for `batter_hits` and `batter_home_runs`.
- **Session 17**: April 01, 2026 at 4:04 PM — AI Chat revamp: conversation persistence (2 new tables + RLS), expanded enrichment (25-game log with advanced stats, positional depth chart, 45-day injury timeline), markdown rendering, clear-chat button, anti-hallucination rules. 3 files modified, 2 created, 1 migration applied. Build clean.
- **Session 18**: April 01, 2026 at 4:48 PM — Kalshi paper/live trader alignment: taker fees, 15% edge threshold, $80 daily exposure cap, Discord alerts (blue=paper, green=live), position accumulation dedup. Added overflow bet tracking (bets skipped by cap stored as `overflow`, resolved separately, excluded from daily log). Fixed dict mutation bug. DB CHECK constraint updated. 3 files modified, 1 DB constraint altered. Step 7.10 completed.
- **Session 19**: April 01, 2026 at 5:31 PM — MLB model evaluation: `batter_home_runs` DROPPED (no edge, max 8 bets/month, -12.3% ROI). `batter_hits` backtested best config: tau=0.75 z_max=1.0 mw=0.65 edge=0.08 (+36.3% ROI, 3.52 Sharpe). `batter_runs_scored` and `batter_total_bases` retrained clean, ready for sweeps. Brain updated.
- **Session 20**: April 03, 2026 at 11:48 AM — Bot Tracker page + admin access control. Built `/bot-tracker` admin-only dashboard (circuit breaker, KPIs, live/paper tabs, orders table, daily P&L). Created `admin_users` table, `is_admin()` function, RLS on 5 Kalshi trading tables, middleware admin gating, navbar conditional link. 8 files created, 2 modified, 2 migrations applied. Build clean. Step 7.11 completed.
