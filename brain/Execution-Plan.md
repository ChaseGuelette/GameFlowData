# Execution Plan

> Part of [[BRAIN-INDEX]]

Phased roadmap for GameFlowData. The NBA system is live and profitable. Focus now shifts to MLB expansion, monetization, and growth.

---

## Phase 1: MLB Pipeline (Current Priority)

**Goal**: Get MLB models trained and ready for the 2026 season.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 1.1 | Finish MLB batter pipeline | completed | None | NLL feature selection, PMF calibration, Optuna tuner built. Pipeline ready for training. |
| 1.2 | Train pitcher K model | not_started | Data backfills | Use `mlb_train_pipeline.py` with pitcher SO data |
| 1.3 | Train batter hits/total_bases models | in_progress | 1.1 | Pipeline ready. Run with `--tune --tuning-trials 100` for initial training. |
| 1.4 | Build MLB daily runner | not_started | 1.2, 1.3 | Inference pipeline mirroring NBA `daily_runner.py` |
| 1.5 | Build MLB paper trading | not_started | 1.4 | Automated bet placement and resolution |
| 1.6 | Run MLB backtests | not_started | 1.2, 1.3 | Validate with `run_mlb_sweep.py` |
| 1.7 | Add MLB to Railway scheduler | not_started | 1.4, 1.6 | Cron jobs for MLB scraping + inference |

**Done when**: MLB pitcher K and batter models are backtested with >5% ROI and running daily on Railway.

---

## Phase 2: MLB Dashboard Features

**Goal**: Enable the remaining dashboard features for MLB (currently disabled via feature flags in `sport-config.ts`).

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 2.1 | MLB Analysis Modal — game history | not_started | MLB `player_game_stats` equivalent | Need historical box score table so the history chart works for MLB players |
| 2.2 | MLB Analysis Modal — bookmaker lines | not_started | MLB lines in `raw_player_props_combined` or equivalent | Line comparison panel in the modal |
| 2.3 | MLB live scoreboards | not_started | MLB scoreboard API endpoint | Build `/api/scoreboard` MLB support, flip `scoreboard: true` |
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

## Phase 7: Growth & Community

**Goal**: Build the user base and establish GameFlowData as a trusted tool.

| Step | Task | Status | Dependencies | Details |
|------|------|--------|--------------|---------|
| 7.1 | Deploy Discord bot as Railway service | not_started | None | Persistent slash commands |
| 7.2 | Content marketing (methodology posts) | not_started | None | ROI proof is the #1 acquisition tool |
| 7.3 | Social media pick cards | not_started | None | Pillow image gen in `src/social/` |
| 7.4 | SEO optimization for landing page | not_started | None | Target "sports betting model" keywords |
| 7.5 | Referral program | not_started | 3.5 | Requires Stripe integration first |

**Done when**: Growing Discord community, organic traffic to dashboard, positive user feedback loop.
