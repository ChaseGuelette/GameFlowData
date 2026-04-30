# Handoff 052 — Kalshi Resolution Pipeline Bug Fix + Live Trader Analysis

> Part of [[Handoffs]]

**Date**: April 27, 2026 at 7:26 PM

## What Was Done

### New File Created
- `scripts/kalshi_bet_category_analysis.py` — New diagnostic script with 5 sections: per-category breakdown (stat_type x line x side), model calibration per category, yes_price bucket analysis (batter_hits focus), weekly trend per category, and player-level losers. CLI args: `--sport`, `--days`, `--table` (paper/live/both), `--csv-out`, `--stat-type`.

### Code Fixes in `src/paper_trading/kalshi_live_trader.py` — 4 Compounding Bugs Fixed in `reconcile_fills()`

1. **WHERE clause expanded** — Now picks up `status='filled' AND fill_price IS NULL` orders in addition to `status='pending'`
2. **fill_price derivation fallback** — When Kalshi API returns no fill history for old/settled markets, derives fill_price from `total_cost / fill_count` instead of skipping
3. **Pending-to-filled promotion** — Pending orders that already have fill data in DB and are no longer resting on Kalshi get promoted to 'filled' directly, bypassing the stale API
4. **CRITICAL: Never cancel orders with fill data** — The cancellation logic was destroying real filled bets (32 orders, 299 contracts, $109.82 in cost). Now checks for existing fill data before cancelling.

### DB Fixes Applied
- Backfilled fill_price for 21 orders stuck with `status='filled', fill_price=NULL` using `100 - ROUND(total_cost / fill_count * 100)`
- Promoted 22 pending orders with fill data to 'filled' status
- Un-cancelled 32 incorrectly cancelled orders that had valid fill data, restored to 'filled'
- Successfully resolved 21 orders (9W/12L) via `--resolve-only`

## Key Findings from Analysis

**Live trader analysis (95 bets, -$12 P&L, -4.3% ROI per DB — but DB data is unreliable):**
- `batter_hits 1.0 NO` is the main loser: 32 bets, 31.2% win rate, -$16
- Model calibration is badly off: model predicts 57-64% win rate, actual is 20-37% (20-44pp gap)
- Yes_price 65-71 is the "kill zone": 14 bets, 14.3% win rate, -$32
- `pitcher_strikeouts 5.0 NO` also bleeding: 6 bets, 16.7% win, -$17
- `batter_hrr 3.0+ NO` is strong: 13 bets, 77% win, +$23

**DB reliability issue discovered:** fill_prices stored in DB are "expected values" from placement time (snapshot of the orderbook), not actual execution prices. The Kalshi CSV export shows actual prices differ by 1-10 cents. This makes all DB P&L calculations approximate, not exact.

## Decisions Made
- Resolution pipeline chain: `pending` → (reconcile_fills) → `filled` → (resolve_settled) → `won`/`lost`
- Never cancel an order that has fill_price and fill_count > 0 — it is a real bet even if the API returns no data for it
- For orders stuck without fill_price, derive it mathematically from `total_cost / fill_count`
- The star-hitter filter (yes_price >= 72) is cutting the wrong zone — the actual bleed zone is 65-71

## Blockers / Open Questions
1. **DB P&L is still approximate** — fill_prices are expected values, not actuals. The Kalshi CSV (`~/Downloads/Kalshi-Transactions-2026.csv`) is the only ground-truth source. Need to add `--csv` flag to the analysis script to read directly from Kalshi CSV, bypassing DB entirely.
2. **Apr 26 bets just un-cancelled** — need to run `--resolve-only` again to get their P&L calculated.
3. **Code changes not deployed to Railway** — all fixes are local only. Need to push to Railway so the scheduled morning resolution job uses the fixed `reconcile_fills`.
4. **batter_hits model needs recalibration** — 20-44pp miscalibration on the core betting category.

## Recommended Next Steps (Priority Order)
1. Add `--csv` flag to `scripts/kalshi_bet_category_analysis.py` to read Kalshi transaction CSV directly for ground-truth analysis
2. Run `--resolve-only` for both MLB and NBA to resolve the 32 un-cancelled orders
3. Deploy code fixes to Railway so the scheduled morning resolution job uses fixed `reconcile_fills`
4. Implement yes_price 65-71 filter for batter_hits NO bets in the live trader (replace or supplement the star-hitter filter at 72)
5. Investigate batter_hits model recalibration — P(0 hits) tail is too high for mid-tier hitters

## Files to Read on Resume
- `scripts/kalshi_bet_category_analysis.py` (new diagnostic script)
- `src/paper_trading/kalshi_live_trader.py` (reconcile_fills fixes around line 1542-1680)
- `brain/Operations/Operations.md` (for incident context)
- This handoff file
