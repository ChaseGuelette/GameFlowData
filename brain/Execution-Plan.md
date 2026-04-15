# Execution Plan

> Part of [[BRAIN-INDEX]]

Phased roadmap for GameFlowData. The NBA system is live and profitable. Focus now shifts to MLB expansion, monetization, and growth.

---

## Phase 1: MLB Pipeline (Current Priority)

**Goal**: Get MLB models trained and ready for the 2026 season.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 1.1 | Finish MLB batter pipeline | completed | None | NLL feature selection, PMF calibration, Optuna tuner built. Pipeline ready for training. |
| 1.2 | Train pitcher K model | completed | Data backfills | Artifact exists (`run_20260313_195757`), backtested with good results. |
| 1.3 | Train batter hits/total_bases models | completed | 1.1 | `batter_hits` promoted (tau=0.75, z_max=1.0, mw=0.8, edge=8%, +33.2% ROI). `batter_rbis` promoted (tau=0.9, z_max=0.25, mw=0.8, edge=12%, +44.2% ROI). DROPPED: `batter_total_bases` (0/540 profitable), `batter_runs_scored` (trivial edge), `batter_home_runs` (no edge). |
| 1.4 | Build MLB daily runner | completed | 1.2, 1.3 | `src/models/mlb/mlb_daily_runner.py` is production-ready — game discovery, pitcher K predictions, batter predictions scaffolded, prop lines, edge calc, paper bets. Mirrors NBA architecture. |
| 1.5 | Build MLB paper trading | completed | 1.4 | `src/paper_trading/mlb_paper_trader.py` — full bet selection, placement, and resolution. Session 31: `select_bets()` redesigned to mirror Model Picks exactly (queries `is_recommended=true`, uses stored BL probs). No more independent BL re-blending. |
| 1.6 | Run MLB backtests | completed | 1.2, 1.3 | All 3 stats backtested individually + combined (Jul 1-Sep 28, 1,064 bets, +21.25% ROI, 1.19 Sharpe). Per-stat optimal BL configs promoted. TB/runs/HR confirmed non-viable and dropped. |
| 1.7 | Add MLB to Railway scheduler | completed | 1.4, 1.6 | MLB jobs in `scheduler.py`: stats at 10/10:30 AM ET, inference at 1:30/6:30 PM ET. Month gate removed. Kalshi split into NBA/MLB separate refresh jobs. Supavisor timeout fix deployed (Session 15). |

**Done when**: MLB pitcher K and batter models are backtested with >5% ROI and running daily on Railway. ✅ **COMPLETE** — 3 models promoted, combined +21.25% ROI, dashboard updated, frontend deployed.

---

## Phase 2: MLB Dashboard Features

**Goal**: Enable the remaining dashboard features for MLB (currently disabled via feature flags in `sport-config.ts`).

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 2.1 | MLB Analysis Modal — game history | completed | MLB `player_game_stats` equivalent | Queries `mlb_player_game_stats_batting`/`pitching` in AnalysisModal, L5 chart + table. |
| 2.2 | MLB Analysis Modal — bookmaker lines | completed | MLB lines in `mlb_raw_player_props` | Line comparison panel queries `mlb_raw_player_props` with same dedup/staleness logic. |
| 2.3 | MLB live scoreboards | completed | MLB scoreboard API endpoint | `/api/scoreboard?sport=mlb` via MLB Stats API, `scoreboard: true` in MLB config. |
| 2.4 | MLB injury reports | not_started | MLB injury data source + scraper | Injury badges on prop cards, flip `injuries: true` |
| 2.5 | MLB DFS | not_started | MLB DFS salary data source | Salary scraper + DFS optimizer page for MLB |
| 2.6 | MLB Stats Vault | completed | MLB historical stats tables | Batters + Pitchers tabs with Box/Rates/Consistency categories. DB views `mlb_batters_latest` (1,242 rows) + `mlb_pitchers_latest` (1,512 rows) applied via migration 023. RLS policies on 4 MLB tables. WindowToggle supports L3/L5/L10/L20/SZN. `dec3` format for AVG/OBP/SLG/OPS. `statsVault: true` in MLB config. ✅ LIVE. |

**Done when**: All feature flags in MLB config are `true` and backed by real data.

---

## Phase 3: Monetization (Stripe)

**Goal**: Enable paid subscriptions and transition from free beta.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 3.1 | Add Stripe columns to `user_subscriptions` | not_started | None | `stripe_customer_id`, `stripe_subscription_id` |
| 3.2 | Build `/subscribe` page with Checkout | not_started | 3.1 | Stripe Checkout session creation |
| 3.3 | Build Stripe webhook handler | not_started | 3.1 | `api/stripe/webhook/route.ts` |
| 3.4 | Add Customer Portal to `/account` | not_started | 3.1 | Self-service billing management |
| 3.5 | Test full subscription flow | not_started | 3.2, 3.3, 3.4 | Sign up → pay → access → cancel |
| 3.6 | Set pricing tiers | not_started | Market research | Competitive analysis needed |

**Done when**: Users can subscribe, access predictions, manage billing, and cancel — all self-service.

---

## Phase 2.5: NBA Product Features

**Goal**: Add user-facing features to the NBA dashboard that increase engagement and stickiness.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 2.5.1 | DFS Slip Builder & Tracking | completed | DFS page live | User-facing slip builder with parlay Kelly sizing, entry tracking, history tab, backend resolution. See [[DFS-Slip-Builder]]. DB migration pending (run in Supabase SQL Editor). |

---

## Phase 4: NBA Model Maintenance

**Goal**: Keep the NBA model profitable and catch degradation early.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 4.1 | Monitor ROI weekly | in_progress | None | 14-day rolling ROI > 8% threshold. Apr 3 check: +9.8% ROI (65 bets), HOLD. Next check Apr 13. |
| 4.2 | Run calibration checks | in_progress | None | ECE < 0.06, quantile gaps < 3%. Apr 3: bias improved all stats, PTS UNDER flagged (36.4% win), 15%+ edge bucket underperforming. |
| 4.3 | Clean old model backups | not_started | Model validated in live trading | Remove `production_old_20260210/`, `production_old_20260323/` |
| 4.4 | Backtest combo validation | not_started | None | Short backtest with PRA/PR/PA/RA |
| 4.5 | Tune drift threshold | not_started | Production monitoring | Increase from 1.0 if >30 players/cycle |

**Done when**: NBA model age < 3 weeks, ROI > 8%, all calibration metrics within bounds.

---

## Phase 5: Database & Performance

**Goal**: Address the 67M+ row elephant in the room and improve query performance.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 5.1 | Archive old `raw_player_props_combined` rows | not_started | None | Biggest performance win |
| 5.2 | Drop unused indexes on large tables | completed | None | Session 15: Dropped 47 GB of unused indexes (116 GB → 69 GB). 7 indexes on `raw_player_props_combined` (45 GB), 25+ across other tables, duplicates cleaned. |
| 5.3 | Optimize `get_dfs_lines` query | not_started | 5.1 | Currently 9-14s |
| 5.4 | Add pagination to history/performance | not_started | None | SCALING.md Tier 1 |
| 5.5 | Add React Query caching | not_started | None | SCALING.md Tier 2 |
| 5.6 | Fix RLS `auth.uid()` → `(select auth.uid())` | completed | None | Session 15: Fixed 9 policies on `user_subscriptions`, `user_profiles`, `user_bets` to avoid per-row re-evaluation. |
| 5.7 | Local Postgres for training/backtesting | completed | None | Session 15: `scripts/sync_local_db.py` + `--local` flag on all training/backtest scripts. No more statement timeouts or resource exhaustion. |

**Done when**: DFS queries < 3s, history/performance pages paginated, prop table under control.

---

## Phase 6: NCAAB Activation

**Goal**: Bring the code-complete NCAAB pipeline to production.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 6.1 | Apply migrations 009-011 | not_started | None | Supabase schema changes |
| 6.2 | Add `cbbpy` to requirements.txt | not_started | None | Missing dependency |
| 6.3 | Backfill historical data | not_started | 6.1 | CBBpy + Barttorvik + Odds API |
| 6.4 | Train spread + total models | not_started | 6.3 | Validate with backtester |
| 6.5 | Re-add NCAAB cron jobs | not_started | 6.4 | Removed in Session 65 |

**Done when**: NCAAB models backtested, daily pipeline running on Railway.

---

## Phase 7: Kalshi Prediction Markets

**Goal**: Integrate Kalshi exchange data as a parallel edge source with dedicated scraper, edge calculator, dashboard page, and Discord alerts.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 7.1 | Kalshi API client + utils | completed | None | RSA-PSS SHA256 auth, rate limiting, fee calculators, stat mapping. `src/scrapers/kalshi/` |
| 7.2 | Market scraper + player linking | completed | 7.1 | Ticker parsing, title fallback regex, SequenceMatcher fuzzy match (0.85), mock/dry-run modes |
| 7.3 | Database schema | completed | None | `kalshi_markets`, `kalshi_orderbook_snapshots` tables, RLS, `get_kalshi_edges` RPC |
| 7.4 | Edge calculator | completed | 7.1, 7.3 | Empirical CDF, fee-adjusted edges (maker/taker), sportsbook comparison, **Black-Litterman blending** (tau=0.5, z_max=1.0, sportsbook devigged prior). `src/models/kalshi_edge.py` |
| 7.5 | Scheduler integration | completed | 7.2, 7.4 | `kalshi_refresh_job.py`, every 10 min 11AM-11PM ET, silent on success |
| 7.6 | Dashboard prediction markets page | completed | 7.3, 7.4 | `/prediction-markets` route, sortable/filterable table, detail modal, countdown, fee breakdown |
| 7.7 | Discord alerts | completed | 7.4 | Violet embed, top 5 edges, `send_kalshi_alert_sync()`, `DISCORD_CHANNEL_KALSHI` fallback |
| 7.8 | Kalshi paper trading | completed | 7.4 | `kalshi_paper_bets` + `kalshi_paper_trading_daily_log` tables, `KalshiPaperTrader` class (Kelly sizing, cents-based P&L, liquidity filters), integrated into `kalshi_refresh_job.py` as Step 4, `--skip-paper` CLI flag |
| 7.9 | Kalshi live trading | completed | 7.8 | Taker market orders, `KalshiLiveTrader` class, 3 circuit breakers (drawdown/daily loss/streak), Kelly sizing with taker fees, 15% edge threshold, position accumulation awareness, Discord alerts per trade, DB tables migrated, integrated into `kalshi_refresh_job.py` Step 4.5 with `--skip-live` flag. Gated by `KALSHI_LIVE_TRADING_ENABLED=true`. |
| 7.10 | Paper/live trader alignment | completed | 7.8, 7.9 | Paper trader mirrors live 1:1: taker fees, 15% edge, $80 daily cap, Kelly sizing, Discord alerts (blue=paper, green=live), position accumulation dedup, overflow bet tracking (overflow_won/lost/cancelled statuses, excluded from daily log). DB CHECK constraint updated. |
| 7.11 | Bot Tracker dashboard page | completed | 7.9, 7.10 | Admin-only `/bot-tracker` page: circuit breaker card, summary KPIs, live/paper tab toggle, date range filter, sortable orders table, daily P&L log. `admin_users` table + `is_admin()` function + RLS on all 5 Kalshi trading tables. Middleware admin gating + navbar conditional link. |

**Done when**: Kalshi markets scraped, edges computed, displayed on dashboard, paper trading profitable.

---

## Phase 8: Growth & Community

**Goal**: Build the user base and establish GameFlowData as a trusted tool.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 8.1 | Deploy Discord bot as Railway service | not_started | None | Persistent slash commands |
| 8.2 | Content marketing (methodology posts) | not_started | None | ROI proof is the #1 acquisition tool |
| 8.3 | Social media pick cards | not_started | None | Pillow image gen in `src/social/` |
| 8.4 | SEO optimization for landing page | not_started | None | Target "sports betting model" keywords |
| 8.5 | Referral program | not_started | 3.5 | Requires Stripe integration first |

**Done when**: Growing Discord community, organic traffic to dashboard, positive user feedback loop.

---

## Phase 9: Polymarket-Kalshi Arbitrage Scanner

**Goal**: Monitor Polymarket prediction markets for cross-platform arbs against Kalshi and mispricings vs. sportsbook consensus.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 9.1 | Core arb scanner pipeline | completed | Kalshi integration | `polymarket_utils`, `polymarket_client`, `polymarket_market_scraper`, `market_matcher`, `arb_scanner`, `arb_scan_job`. 2 DB tables (`polymarket_markets`, `arb_opportunities`). Runs every 10 min on Railway. DISCORD_CHANNEL_ARB=1492934576467611700. |
| 9.2 | Dry-run validation | not_started | 9.1 deployed | Run `--dry-run` on Railway, verify Polymarket API connectivity, check 20-30 matched markets for false positives, confirm Discord alerts fire |
| 9.3 | Arb paper trader | not_started | 9.2 | `arb_paper_bets` table (2-leg structure), `ArbPaperTrader` class (P&L for pure/soft arbs, sportsbook directional), resolution via existing stat results |
| 9.4 | Arb dashboard page | not_started | 9.1, 9.2 | `/arbitrage` page showing live opportunities from `arb_opportunities` table, sortable by margin/discrepancy |

**Done when**: Arb scanner running stably with accurate matches, paper trader tracking simulated P&L, results visible on dashboard.
