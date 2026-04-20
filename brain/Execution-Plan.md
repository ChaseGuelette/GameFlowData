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
| 1.5 | Build MLB paper trading | completed | 1.4 | `src/paper_trading/mlb_paper_trader.py` — full bet selection, placement, and resolution. Session 31: `select_bets()` redesigned to mirror Model Picks exactly (queries `is_recommended=true`, uses stored BL probs). Session 41: 3 additional fixes — (1) `allowed_directions` enforced in direction pick (imports `MLB_STATS`), (2) stat filter `AND stat IN ('pitcher_strikeouts','batter_hits','batter_rbis')` added, (3) `mlb_daily_runner._compute_bl_recommendations()` now enforces `allowed_directions` before setting `is_recommended`. First clean run: Apr 17. |
| 1.6 | Run MLB backtests | completed | 1.2, 1.3 | All 3 stats backtested individually + combined (Jul 1-Sep 28, 1,064 bets, +21.25% ROI, 1.19 Sharpe). Per-stat optimal BL configs promoted. TB/runs/HR confirmed non-viable and dropped. |
| 1.7 | Add MLB to Railway scheduler | completed | 1.4, 1.6 | MLB jobs in `scheduler.py`: stats at 10/10:30 AM ET, inference at 1:30/6:30 PM ET. Month gate removed. Kalshi split into NBA/MLB separate refresh jobs. Supavisor timeout fix deployed (Session 15). |
| 1.8 | Build MLB lineup + roster scrapers | completed | None | `mlb_lineup_scraper.py` + `mlb_roster_scraper.py` built. Jobs wired into scheduler (9:30 AM roster, 12:45 PM + 6:10 PM lineup). `_filter_batters_by_lineup()` added to daily runner — filters to confirmed batters per game, falls back gracefully if lineup not yet posted. Migration 025 applied (`mlb_game_lineups` + `mlb_active_roster` tables). |
| 1.9 | Train + promote batter_hrr model | completed | 1.1 | Model trained ✅ (87,336 rows, 28 features, bias ratio=0.9950, val NLL=1.7210). Artifacts at `mlb_run_batter_hrr_20260415_122937/`. Model suite fix applied (`batter_hrr` added to `STAT_TO_NEGBIN_MODEL_NAME` + `STAT_TO_NEGBIN_SHORT`). Sweep fix applied (`BATTER_STAT_FS_MAP` + `STAT_TO_MARKET_KEY` for `batter_hits_runs_rbis` proxy). Promoted to production. KXMLBHRR ticker verified and in SUPPORTED_STATS whitelist. |

**Done when**: MLB pitcher K and batter models are backtested with >5% ROI and running daily on Railway. ✅ **COMPLETE** — 3 models promoted, combined +21.25% ROI, dashboard updated, frontend deployed. batter_hrr in progress (step 1.9).

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
| 2.7 | MLB Ask AI | completed | MLB tables populated | `/api/ask` MLB branch (Session 35): 2-round parallel data fetching from `mlb_player_game_stats`, `mlb_player_average_batting/pitching`, `mlb_game_schedule`, `mlb_park_factors`, `mlb_players`. Opposing pitcher enrichment for batters (ERA, K/9, WHIP, last 5 starts, handedness). Binary model framing for Bernoulli stats. `askChat: true` flipped in MLB sport config. ✅ LIVE. |

**Done when**: All feature flags in MLB config are `true` and backed by real data.

---

## Phase 2.6: Track Record & Credibility

**Goal**: Give users and prospects a verifiable public-facing track record that doubles as a marketing asset.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 2.6.1 | DB migration 026 + RPCs | completed | None | `user_bets.prediction_id/player_id` nullable, `source` column added (`prop_card`/`manual`/`csv_import`), new unique constraint `(user_id, game_date, player_name, stat_type, bet_direction)`. New table `user_bets_daily_log` (RLS). RPCs `rebuild_user_daily_log` + `get_track_record_summary`. All applied to Supabase. |
| 2.6.2 | CSV import pipeline | completed | 2.6.1 | `dashboard/src/lib/csv/parseBets.ts` — parses Excel export format, stat normalization, date handling, PnL calculation, error/warning reporting. `CsvUpload.tsx` — drag-and-drop, preview table, batched upsert (100/batch), calls rebuild RPC. |
| 2.6.3 | Track Record page | completed | 2.6.1, 2.6.2 | `/track-record` route. Source toggle (My Bets / Paper / Combined). KPI banner (4 cards). `BankrollChart`. `MonthlyGrid` — expandable monthly cards with daily drilldown + per-bet list. `StatBreakdown`. `ModelMetrics` — edge accuracy buckets, streaks, profitable days. Manual bet entry form (`ManualBetForm`). Navbar link added. Build clean. |
| 2.6.4 | History page edit/delete | completed | None | `EditBetModal.tsx` — pre-filled form, UPDATE by id, auto-calculates PnL, optional override. Delete: two-step confirm on ALL bet statuses (not just pending). Both operations call `rebuild_user_daily_log` to keep track record in sync. |
| 2.6.5 | Make track record shareable | not_started | 2.6.3 | Public URL for admin's track record. `get_track_record_summary(uuid)` RPC is already built with auth.uid() check. Needs public route + toggle for Chase's user_id. |

**Done when**: Chase can import his Excel history, track record page renders accurate monthly P&L, page is shareable with prospects.

---

## Phase 3: Monetization (Stripe)

**Goal**: Enable paid subscriptions and transition from free beta.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 3.1 | Add Stripe columns to `user_subscriptions` | completed | None | Migration `add_stripe_columns` applied. `stripe_customer_id`, `stripe_subscription_id` + 4 indexes. |
| 3.2 | Build `/subscribe` page with Checkout | completed | 3.1 | Monthly $19.99 + Annual $199 cards. 7-day trial. Calls `POST /api/stripe/checkout`. Trial abuse prevention. |
| 3.3 | Build Stripe webhook handler | completed | 3.1 | `api/stripe/webhook/route.ts`. Handles 4 events. Service-role admin client. Stripe v22 dahlia API compatible. |
| 3.4 | Add Customer Portal to `/account` | completed | 3.1 | Dynamic status badge, Manage Billing button, Subscribe Now CTA. Portal via `POST /api/stripe/portal`. |
| 3.5 | Test full subscription flow | not_started | 3.2, 3.3, 3.4 | **Blocked**: need Stripe account + env vars filled in. Test with `stripe listen` + test card 4242... |
| 3.6 | Set pricing tiers | completed | Market research | $19.99/mo + $199/yr (saves 17%). 7-day trial. Middleware gate: `SUBSCRIPTION_REQUIRED=false` (flip when ready). |

**Done when**: Users can subscribe, access predictions, manage billing, and cancel — all self-service. ✅ Code complete — pending Stripe Dashboard setup + env vars to activate.

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
| 4.1 | Monitor ROI weekly | completed | None | 14-day rolling ROI > 8% threshold. Apr 10 check: +10.9% ROI (87 bets), HOLD. REB UNDER dragging (-15.1% ROI) — monitoring. |
| 4.2 | Run calibration checks | completed | None | ECE < 0.06, quantile gaps < 3%. Apr 10: run_20260323_212931, all stats within bounds. AST Q10 gap structural (zero-assist games). Model's Q10 "miscalibration" confirmed as the edge — no correction deployed. |
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
| 7.12 | Post-incident safety overhaul | completed | 7.9 | Apr 19 incident: 21 NBA bets/$233 in 16s. Fixes: (1) resolution decoupled from trading gate, (2) 9:15 AM morning resolution job, (3) per-sport trading gate (`NBA_TRADING_ENABLED`), (4) edge sanity cap (40%), (5) trade approval flow (queue table + dashboard UI + API), (6) shared $200 exposure cap, (7) game start time in bot tracker. See [[handoff-016]]. |

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

**Goal**: Monitor ALL Polymarket markets for pure and soft cross-platform arbs against Kalshi — not limited to sports. Purely price-based: no sportsbook data, no internal model. Primary targets by expected opportunity density:
- **Season-long sports futures** (World Series, division winners, award markets) — illiquid enough that pricing gaps persist for minutes/hours
- **NRFI/YRFI** (No/Yes Run First Inning) — niche MLB market present on both platforms, slower price discovery
- **Game-level moneylines and totals** — high volume but faster arbitrage bots; gaps close in 2-7 seconds
- **Non-sports categories** (politics, economics, crypto, weather, culture) — thousands of markets, far fewer arb bots watching them, persistent gaps likely
- **Player props: dropped** — Polymarket confirmed to have thin player prop coverage; not worth pursuing

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 9.1 | Core arb scanner pipeline | completed | Kalshi integration | `polymarket_utils`, `polymarket_client`, `polymarket_market_scraper`, `market_matcher`, `arb_scanner`, `arb_scan_job`. 2 DB tables (`polymarket_markets`, `arb_opportunities`). Runs every 10 min on Railway. DISCORD_CHANNEL_ARB=1492934576467611700. Sportsbook comparison, model-based mispricing, and player props removed (Apr 15). |
| 9.2 | Rebuild scraper for game-level + non-sports | completed | 9.1 | Full rewrite of `polymarket_market_scraper` to ingest ALL Polymarket categories (sport tag filter was broken — returned 0 events). Fixes: (1) prices from Gamma API `outcomePrices` field (CLOB /midpoints returned 400), (2) `clobTokenIds` is a JSON string — required `json.loads()`, (3) upsert on `condition_id` only — one row per market updated in place (was creating 69K new rows per hourly run), (4) batch executemany 500/chunk (was row-by-row, ~30 min). Added `category` column (DB migration), made `sport` nullable. Category detection via event tag IDs + slug/title keywords. Market types: `nrfi`, `season_future`, `binary`, `player_prop`, `moneyline`, `total`, `spread`. Result: **70,651 markets across 7 categories** stored in ~2 min. Scheduler: hourly all-categories job at :30 past each hour (9:30AM–11:30PM ET). Deployed to Railway. |
| 9.3 | Cross-platform market matcher: sports futures + NRFI | completed | 9.2 | Full infrastructure built: `team_normalizer.py` (30 NBA + 30 MLB teams, slug/question parsing), `kalshi_discovery.py` (one-time series enumeration via /events API — NOT /markets, which lacks series_ticker), `KALSHI_GAME_SERIES` populated with 27 confirmed series in `kalshi_utils.py` (MLB game/futures ×9, NBA game/playoffs/futures ×13, NHL ×5). `market_matcher.py` extended with `match_game_markets()` (frozenset team-key matching). DB migration applied: `kalshi_markets` + `arb_opportunities` extended with `market_type`/`team1`/`team2` columns. DB migration: `kalshi_markets.line` made nullable (game markets have no line). UTC boundary fix in `_load_kalshi_game_markets` (late-night scrapes land on +1 UTC day). Dry-run formatter crash fixed for game markets (player_name=None). 1,442 MLB markets in DB. 29 pure arbs detected after `MIN_KALSHI_BID=3c` filter (removes stale 1c season future placeholders). |
| 9.4 | Cross-platform market matcher: non-sports | completed | 9.2 | Session 36: Elections + Politics expansion complete. Dynamic category-scrape mode: `_CAT_ELECTIONS` (649 series, ~4,769 markets) + `_CAT_POLITICS` (332 series, ~1,470 markets) discovered at runtime via `list_all_events`. Per-config `fallback_threshold=0.65`, `min_kalshi_volume` (5000 elections / 500 politics), `min_poly_liquidity` (50000 / 5000). Candidate name disambiguation in fuzzy fallback (name_sim ≥ 0.65, structural mismatch check). 144 matched pairs, scan in ~2 min. Diagnostic: `scripts/inspect_nonsports_matches.py`. **False positive fixes applied**: (1) GDP country mismatch — `_parse_gdp_country()` + `country` field in `MarketFields` + check in `match_score()`, (2) same-race placement — `_extract_placement()` + rank check in fuzzy fallback. Running on Railway. Next: add `_CAT_FINANCE` / `_CAT_ENTERTAINMENT` / `_CAT_SCOTUS`. |
| 9.5 | Arb paper trader | completed | 9.3 | `arb_paper_bets` + `arb_paper_trading_daily_log` tables, `ArbPaperTrader` class, resolution via outcome. 70 bets placed, 8 resolved with profit. |
| 9.6 | Arb dashboard page | completed | 9.3, 9.4 | `/arb-scanner` admin-only page: 4 summary cards (P&L, win rate, active bets, 24h detected), sortable paper bets table, daily P&L log tab, date range filter. Navbar "Arb" link added. RLS authenticated_read policies applied to both arb tables. |

**Done when**: Scanner ingesting all Polymarket categories, matching on sports futures + NRFI + non-sports, paper trader tracking simulated P&L, results visible on dashboard.
