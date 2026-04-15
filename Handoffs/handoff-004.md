> Part of [[Handoffs]]

**Date**: April 15, 2026 at 1:36 PM

## Summary

Continued from a previous session that ran out of context. Completed the `bet_reasoning` JSONB feature for Kalshi paper bets, then investigated and fixed two Discord alert issues: the daily summary showing stale/wrong records (timezone + data pipeline bugs), and the high-edge market alert showing zero-volume untradeable markets.

---

## What Was Done

### bet_reasoning Feature — Complete

Added model context storage to every Kalshi paper bet (both real and overflow). This mirrors the `bet_context` field already on manual sportsbook bets.

**DB migration already applied (previous session):**
- `ALTER TABLE kalshi_paper_bets ADD COLUMN IF NOT EXISTS bet_reasoning JSONB`

**`src/paper_trading/kalshi_paper_trader.py`:**
- Added `import json` at top
- Added `_fetch_prediction_context(target_date, pos_keys)` method — queries `daily_predictions` with `DISTINCT ON (player_id, stat)` ordered by `is_recommended DESC, created_at DESC`. Returns dict mapping `(player_id, stat_type)` → context with Q10–Q90 quantiles, L5/L3 averages, opp, rest_days, is_back_to_back, team_out_count, is_home, usg_pct_l5, min_floor_l5, bl_confidence
- `select_bets()`: already had `_pred_ctx` enrichment; `bet_reasoning` dict already built in greedy allocation loop with full model probability chain + market context
- `place_bets()` INSERT: added `bet_reasoning` column with `CAST(:bet_reasoning AS jsonb)`, serializes dict to JSON string before inserting; also added to `DO UPDATE SET` clause; passes `bet_reasoning` to Discord alert data
- `_store_overflow_bets()` INSERT: same `bet_reasoning` addition so overflow bets retain context

**`src/discord_bot/alerts.py`:**
- `_build_kalshi_trade_placed_embed()`: added "Model Context" field (appears when `bet_reasoning` is present) showing Q10/50/90 distribution, L5/L3 averages, opponent/rest/B2B/team-outs, BL probability chain, sportsbook line comparison

### Kalshi Daily Summary — Bug Fixes (carried from earlier in session)

Three bugs fixed:
1. **Scheduler timezone bug**: `BlockingScheduler(timezone=...)` doesn't propagate to individual `CronTrigger` calls on Railway. Fixed by adding `timezone="America/New_York"` to all 22 CronTrigger calls in `src/orchestration/scheduler.py`
2. **Wrong data source for bet record**: Summary was using `resolve_all_pending()` return value (0 if refresh job already resolved them). Fixed by reading `bets_won/bets_lost/total_bets` from the daily log table in `_get_yesterday_daily_log()`
3. **4 AM fire time**: Summary was firing at 8 AM UTC = 4 AM ET before the daily log existed. Moved to 12:30 PM ET

Also added one-shot test job (`DateTrigger` for noon ET Apr 15) to verify the new embed format in Discord.

### Overflow Tracking in Daily Summary

- Added `_get_overflow_stats(engine, game_date)` to `src/orchestration/kalshi_daily_summary_job.py` — queries `kalshi_paper_bets` for overflow_won/overflow_lost counts and P&L
- Daily summary Discord embed now shows an "Overflow (Cap-Limited)" field with count, percentage of eligible, and hypothetical P&L
- `src/discord_bot/alerts.py` `_build_kalshi_pnl_summary_embed()` updated to accept and show overflow data

### Kalshi Analysis Embed — Clarity Overhaul

- Removed confusing "95% CI" field entirely
- Overall field now reads: `"41.1% actual win | 30.2% needed to break even | +10.9% edge"`
- Z-Score field renamed "Statistical Confidence (Z-Score)" with `*(>2σ = likely real edge, >3σ = strong edge)*` legend
- By Stat: shows `win% / BE% = alpha edge` format with ⚠️ flags for negative alpha
- By Edge Bucket: same format with ✅/⚠️ flags
- `src/paper_trading/kalshi_analysis.py`: added `break_even` and `alpha` fields to by_stat and by_edge_bucket dicts

### Greedy Allocation for Bet Sizing

Replaced proportional scaling with greedy allocation in `select_bets()`:
- Root cause of ~97% overflow on early dates: proportional scale + integer floor meant `floor(kelly × 0.006) = 0` for most bets
- Greedy: sort by edge desc, give each bet full Kelly if cap allows, otherwise partial (min 1 contract), otherwise overflow
- This guarantees ~15–50 real bets/day vs ~15 with the old approach

### High-Edge Discord Alert — Liquidity Filter

- Root cause of "Vol: 0" on all markets: alert query had no volume/spread filter, showing untradeable markets (extreme lines, 99c spreads, zero trades)
- Zero volume IS accurate — Draymond Green 2+ rebounds at YES 91c has had 0 trades all day; rational
- Fix in `src/orchestration/kalshi_refresh_job.py`: added `AND volume >= 20 AND bid_ask_spread <= 15` to `_send_high_edge_alerts()` query — matches paper trader filters exactly

---

## Decisions Made

- **`bet_reasoning` uses `daily_predictions` for NBA only**: MLB stats like `pitcher_strikeouts` won't have entries in `daily_predictions` (NBA-only table). The context will just be empty `{}` for MLB, which is handled gracefully.
- **greedy allocation over proportional scaling**: Proportional scaling causes floor-to-zero problem at any reasonable bankroll size when many candidates compete for the cap. Greedy is simpler, fair, and eliminates the problem.
- **Volume=0 is accurate, not a bug**: Markets with 0 volume genuinely have no trading activity (extreme lines, market maker quotes but no takers). We just shouldn't show them in alerts.

---

## Blockers and Open Questions

- **One-shot test job still in scheduler**: `kalshi_daily_summary_test_apr15` (DateTrigger noon ET Apr 15) should be removed on the next deploy after confirming it fired correctly.
- **`batter_hrr` model still untrained**: Plumbing exists across 6 files (Session 30) but the model hasn't been trained. `plans/batter_hrr_model.md` has the plan.
- **NBA calibration check overdue**: Due Apr 13 per MEMORY.md (3-week model age trigger). ROI was +10.9% Apr 10, decelerating. Should run soon.
- **Stripe monetization (Phase 3)**: Still not started. Next big priority after Kalshi ops are stable.

---

## Recommended Next Steps

1. **Remove the one-shot test job** from `src/orchestration/scheduler.py` (the `kalshi_daily_summary_test_apr15` DateTrigger) — it already fired at noon ET Apr 15.
2. **NBA calibration check**: Model is 23+ days old, above the 3-week trigger. Run `/check-calibration` to check ROI + ECE + bias.
3. **Verify daily summary Discord message** at 12:30 PM ET today now shows correct record (should show yesterday's actual wins/losses instead of "0 bets resolved").
4. **Train `batter_hrr` model**: Follow `plans/batter_hrr_model.md`.
5. **Stripe monetization (Phase 3)**: First subscription-gated revenue.

---

## Files to Read on Resume

- `src/paper_trading/kalshi_paper_trader.py` — `_fetch_prediction_context()` method, updated `place_bets()` INSERT, greedy allocation in `select_bets()`
- `src/discord_bot/alerts.py` — `_build_kalshi_trade_placed_embed()` Model Context field, updated P&L summary embed
- `src/orchestration/scheduler.py` — Remove one-shot test job, verify all CronTriggers have timezone=ET
- [[Execution-Plan]] — Phase 1 (MLB complete), Phase 3 (Stripe not started)
- `MEMORY.md` — NBA calibration status, MLB pitcher K seasonal config
