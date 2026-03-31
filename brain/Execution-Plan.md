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
| 1.3 | Train batter hits/total_bases models | in_progress | 1.1 | Pipeline code complete for all 5 stats (hits/TB/RBI/runs/HR). Binomial model built for hits (Session 4). Training commands ready — not yet executed. |
| 1.4 | Build MLB daily runner | completed | 1.2, 1.3 | `src/models/mlb/mlb_daily_runner.py` is production-ready — game discovery, pitcher K predictions, batter predictions scaffolded, prop lines, edge calc, paper bets. Mirrors NBA architecture. |
| 1.5 | Build MLB paper trading | completed | 1.4 | `src/paper_trading/mlb_paper_trader.py` — full bet selection, placement, and resolution. |
| 1.6 | Run MLB backtests | completed | 1.2, 1.3 | Backtests completed and validated. |
| 1.7 | Add MLB to Railway scheduler | completed | 1.4, 1.6 | MLB jobs in `scheduler.py`: stats at 10/10:30 AM ET, inference at 1:30/6:30 PM ET. Month gate removed — jobs run year-round (handle off-season gracefully). |

**Done when**: MLB pitcher K and batter models are backtested with >5% ROI and running daily on Railway.

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
| 2.6 | MLB Stats Vault | not_started | MLB historical stats tables | Player lookup with historical stats for MLB |

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
| 4.1 | Monitor ROI weekly | in_progress | None | 14-day rolling ROI > 8% threshold |
| 4.2 | Run calibration checks | in_progress | None | ECE < 0.06, quantile gaps < 3% |
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
| 5.2 | Drop unused `idx_props_dfs_latest` index | not_started | None | Quick cleanup |
| 5.3 | Optimize `get_dfs_lines` query | not_started | 5.1 | Currently 9-14s |
| 5.4 | Add pagination to history/performance | not_started | None | SCALING.md Tier 1 |
| 5.5 | Add React Query caching | not_started | None | SCALING.md Tier 2 |

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
| 7.4 | Edge calculator | completed | 7.1, 7.3 | Empirical CDF, fee-adjusted edges (maker/taker), sportsbook comparison. `src/models/kalshi_edge.py` |
| 7.5 | Scheduler integration | completed | 7.2, 7.4 | `kalshi_refresh_job.py`, every 10 min 11AM-11PM ET, silent on success |
| 7.6 | Dashboard prediction markets page | completed | 7.3, 7.4 | `/prediction-markets` route, sortable/filterable table, detail modal, countdown, fee breakdown |
| 7.7 | Discord alerts | completed | 7.4 | Violet embed, top 5 edges, `send_kalshi_alert_sync()`, `DISCORD_CHANNEL_KALSHI` fallback |
| 7.8 | Kalshi paper trading | not_started | 7.4 | `kalshi_paper_bets` table, Kelly sizing, fill simulation. Design in [[Kalshi-Integration-Design]] |
| 7.9 | Kalshi live trading | not_started | 7.8 proven | Limit orders only, gated by `KALSHI_LIVE_TRADING_ENABLED`. Future phase. |

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
