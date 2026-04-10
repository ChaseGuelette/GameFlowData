> Part of [[Handoffs]]

**Date**: April 10, 2026 at 4:33 PM

## Summary

Overhauled the Kalshi paper and live traders based on analysis of 1,871 resolved paper bets: cut all YES bets (confirmed structurally negative at -$499), added a stat-type whitelist, switched to bankroll-proportional exposure, added bet pool logging, and fixed the live trader's BL probability gap. Also determined the system is **ready to launch live trading** pending 2-3 days of NO-only paper validation, wrote the startup playbook, and built a full analysis script. All changes committed and deployed to Railway at 3:47 PM EDT.

---

## What Was Done

### Code changes (all committed, pushed, live on Railway)

- **`src/paper_trading/kalshi_paper_trader.py`**
  - NO-only mode by default (`KALSHI_ALLOW_YES_BETS` env var toggle, default false)
  - `SUPPORTED_STATS` whitelist — filters out `batter_hits_runs_rbis`, `batter_total_bases`, `batter_home_runs` (no trained models for these)
  - Bankroll-proportional exposure: `effective_cap = clamp(bankroll × 60%, $80, $500)`
  - Bet pool logging at >0%, >3%, >5%, >10%, >15% edge tiers before each selection pass
  - Final log shows `[NO-only]` badge and selected bets vs cap

- **`src/paper_trading/kalshi_live_trader.py`**
  - Same NO-only, whitelist, bankroll cap, bet pool changes as paper trader
  - **BL probability fix**: SQL query now fetches `bl_model_prob, bl_edge`; uses BL-blended value when available (was using raw `model_prob` — bug now fixed)

- **`src/orchestration/kalshi_refresh_job.py`**
  - Added `--yes-bets` CLI flag (sets `KALSHI_ALLOW_YES_BETS=true` for local testing)
  - Startup log now shows current mode (NO-only vs YES+NO)

- **`src/discord_bot/alerts.py`**
  - `[NO-ONLY]` badge on trade placed and trade resolved embeds

### New files

- **`brain/Operations/Kalshi-Live-Trading-Startup.md`** — Complete live trading launch playbook:
  - Statistical evidence (999 NO bets, +5.8σ, STRONG EDGE)
  - Pre-flight checklist (5 items before flipping `KALSHI_LIVE_TRADING_ENABLED=true`)
  - $300 recommended starting bankroll with reasoning
  - Full Railway env var block
  - Circuit breaker reference table
  - Scaling plan (2-week/4-week milestones)
  - Daily/weekly monitoring cadence
  - Failure mode guide
  - 5 key invariants

- **`scripts/analyze_kalshi_paper_bets.py`** — Runnable analysis script:
  - Loads all resolved bets (real + overflow) from DB
  - Correct `price` handling: uses `actual_price = 100 - YES_price` for NO bets
  - Tables: side comparison (real only / combined), by stat type, by cost bucket, by edge bucket (combined + real only), by sport, overflow impact, daily trend
  - Z-scores and 95% CI for statistical significance
  - CLI flags: `--sport`, `--days`, `--date-start`, `--date-end`

### Brain files updated

- **`brain/Decisions/Kalshi-Integration-Design.md`** — Added NO-Only Overhaul section with full detail
- **`brain/Operations/Critical-Invariants.md`** — Added invariants 13, 14, 15 (Kalshi: NO-only, Q10 edge, stat whitelist)
- **`brain/Operations/Operations.md`** — Added link to `Kalshi-Live-Trading-Startup`

---

## Decisions Made

**Cut YES bets permanently (default)**: 872 YES bets resolved at -$499, -8.7% ROI, -1.2σ — no edge. 999 NO bets at +$3,101, +38.8% ROI, +5.8σ — STRONG EDGE. The asymmetry is structural: model Q10 undershoots → UNDER wins more than market implies → NO side wins.

**Keep `batter_hits_runs_rbis` filtered out**: User challenged this. Investigation confirmed zero model coverage — no MC predictor, no feature engineering, no trainer. The bets use wrong inputs from single-stat models → structurally noisy. Result: 48 bets, 0.0σ, essentially random at -$23. Filtering is correct.

**$300 starting bankroll**: Below $133, the $80 floor dominates and proportional sizing adds no value. $300 gives $180/day cap — comfortable headroom, meaningful volume, manageable risk ($30/day loss limit, halt at $210).

**Wait 2-3 more days before flipping live**: Even with 5.8σ evidence, validate that NO-only mode generates clean Discord alerts and correct bet counts for 2-3 days before committing real money.

**`price` column = YES price**: Key insight discovered during script development. `price` in `kalshi_paper_bets` stores the YES market price, not what the NO bettor pays. For NO bets: actual cost = `100 - price`. Break-even formula must use actual cost. Fixed in analysis script.

---

## Blockers and Open Questions

- **2-3 day paper validation still needed**: NO-only mode went live ~4:00 PM EDT today. First fully clean day is tomorrow (Apr 11). Need to observe bet counts and Discord alerts Apr 11-12 before enabling live.
- **YES bets in today's DB**: The 3 NBA YES pending + 1 MLB YES pending bets from today were placed before the 4:00 PM deployment — from old code. These are expected, not a bug.
- **NBA calibration check due Apr 13**: Model is 18 days old, next check in 3 days. Run `check-calibration` before then if performance looks odd.
- **Stripe integration**: Still TODO — subscribe page, Customer Portal, webhook route, DB columns.

---

## Recommended Next Steps

1. **Tomorrow morning (Apr 11)**: Run `python scripts/analyze_kalshi_paper_bets.py` to see today's bets resolved. Verify no YES bets placed after 4:00 PM EDT today in the DB output.

2. **Apr 11-12 monitoring**: Each day, check Discord `#kalshi` for `[NO-ONLY]` badge. Confirm 5-20 NBA + 20-50 MLB real bets/day. After 2 days clean, proceed to live launch.

3. **Live trading launch (Apr 12-13)**: Set Railway env vars per `brain/Operations/Kalshi-Live-Trading-Startup.md`. Fund Kalshi with $300. Flip `KALSHI_LIVE_TRADING_ENABLED=true`.

4. **NBA calibration check (Apr 13)**: Model hits 3-week age trigger. Run `check-calibration`. If ROI still above 8% and ECE below 0.06, hold. Do NOT retrain preemptively.

5. **Stripe integration**: Next major product milestone. See `brain/Business/Stripe-Plan.md`.

---

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — Pre-flight checklist and exact env vars for live launch
- [[Kalshi-Integration-Design]] — Full overhaul details from this session (NO-Only section)
- [[NBA-Model]] — Model status, next calibration check due Apr 13
- `scripts/analyze_kalshi_paper_bets.py` — Run this first thing tomorrow to verify clean NO-only data
- [[Critical-Invariants]] — Updated with 3 new Kalshi invariants (13, 14, 15)
