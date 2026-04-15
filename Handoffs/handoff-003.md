> Part of [[Handoffs]]

**Date**: April 15, 2026 at 11:53 AM

## Summary

Debugging and fixing session focused on the MLB paper trader. Diagnosed why only 9 bets were taken on Apr 13 (should be ~40+) and fixed all root causes. Also fixed the MLB date dropdown on the dashboard which only ever showed today, and applied the pending migration 023 to bring the MLB Stats Vault live.

---

## What Was Done

### Migration 023 Applied — MLB Stats Vault Now Live
- Applied `023_mlb_stats_vault_views.sql` via Supabase MCP
- Created `mlb_batters_latest` (1,242 rows) and `mlb_pitchers_latest` (1,512 rows) views
- Added RLS SELECT policies on `mlb_player_average_batting`, `mlb_player_average_pitching`, `mlb_players`, `mlb_teams`
- Step 2.6 is now fully complete — Stats Vault is live in production

### MLB Paper Trader — Root Cause Diagnosis (9 bets on Apr 13)
Three independent causes identified:
1. **batter_rbis odds filter**: Under odds on Apr 13 ranged -140 to -4000. The hard `min_odds=-200` blocked 107 of 782 predictions. Only 16 could pass, giving 7 bets.
2. **batter_hits BL shrinkage**: With `tau=0.75`, BL blending reduced most raw 9.3% edges to ~7% — below the 8% threshold. Only 2 of 64 raw-eligible bets survived.
3. **pitcher_strikeouts UNDER-only**: 4 pitcher predictions had OVER edges ≥8% but were blocked by `allowed_directions: ["under"]`. No UNDER edges qualified that day.

### min_odds Fix: -200 → -500
- Changed `min_odds` from -200 to -500 in `mlb_paper_trader.py`
- Also bumped `max_odds` to +500 symmetrically
- The -200 to -500 bucket had 68 batter_rbis bets on Apr 13; those now pass through

### MLB Date Dropdown Fixed
- Root cause: direct query used `.limit(30)` which limited *rows* not distinct dates. With 782+ predictions per date, this always returned today only.
- Fix: created `get_mlb_prediction_dates` RPC (Migration 024) with `DISTINCT prediction_date` for last 60 days
- Updated `dashboard/page.tsx` to call the RPC for MLB (mirrors how NBA uses `get_prediction_dates`)
- Granted EXECUTE to `authenticated` and `anon` roles

### Paper Trader Redesigned to Mirror Model Picks Exactly
- **Problem**: Paper trader ran independent BL blending on raw MC samples, applied `allowed_directions` filter, applied odds range filter — all causing divergence from Model Picks page. Result: 33 bets taken vs 49 shown on Model Picks.
- **Fix**: Completely rewrote `select_bets()` to query `WHERE is_recommended = true` and use the stored `bl_over_edge`/`bl_under_edge` and `bl_over_prob`/`bl_under_prob` from the DB.
- Direction is whichever BL edge is higher — mirrors inference job logic exactly.
- Removed: `gzip`, `numpy`, `BlackLittermanBlender` imports, `_load_samples_for_date()`, `_get_edge_threshold()`, `_bl_blenders`, `_default_bl_blender`, `SUPPORTED_STATS`, odds range filter, `allowed_directions` filter.
- Paper trader now takes exactly the bets the Model Picks page shows, guaranteed.
- Files modified: `src/paper_trading/mlb_paper_trader.py`, `dashboard/src/app/(protected)/dashboard/page.tsx`

---

## Decisions Made

- **Paper trader source of truth = `is_recommended` flag**: Rather than having two independent systems computing bets, the inference job is the single source of truth. Paper trader just reads what inference decided. This eliminates drift permanently.
- **No odds filter in paper trader**: Model Picks page doesn't filter on odds, so paper trader shouldn't either. Kelly formula handles extreme odds naturally.
- **No `allowed_directions` in paper trader**: The inference job's `is_recommended` logic already incorporates stat-level edge thresholds. Direction restrictions should be enforced at the inference layer, not paper trader layer, so the systems stay in sync.
- **min_odds -500 but not -1000**: The -200 to -500 bucket (68 bets on Apr 13) is meaningful. The -1000+ bucket (45 bets, usually 0.5-RBI lines with -4000 odds) is near-certain territory where Kelly stakes would be tiny and the practical value is low.

---

## Blockers and Open Questions

- **Today's bets (Apr 14) already placed with old settings**: 33 bets vs 49 Model Picks. The new paper trader will take effect on the next inference run (Apr 15 at 1:30 PM ET). Historical discrepancy stands.
- **`batter_hrr` model still untrained**: Plumbing exists across 6 files (Session 30) but the model hasn't been trained. `plans/batter_hrr_model.md` has the plan. Requires backtest gate before deploying.
- **NBA calibration check due**: Next check was due Apr 13 per MEMORY.md. Should run a calibration health check given the 3-week model age trigger.

---

## Recommended Next Steps

1. **Verify paper trader parity today**: After the 1:30 PM ET inference run, confirm `mlb_paper_bets` count matches the Model Picks page count for Apr 15.
2. **NBA calibration check**: Model is 23 days old (above 3-week trigger). Run `/check-calibration` or equivalent. ROI was +10.9% on Apr 10, decelerating.
3. **Train `batter_hrr` model**: The H+R+RBI combined model has all its plumbing but needs training. Follow `plans/batter_hrr_model.md`.
4. **Stripe monetization (Phase 3)**: No steps started. Next priority after MLB paper trading is solid.

---

## Files to Read on Resume

- [[Execution-Plan]] — Check Phase 1 (MLB complete), Phase 2.6 (Stats Vault live), Phase 3 (Stripe not started)
- `src/paper_trading/mlb_paper_trader.py` — Redesigned `select_bets()` to understand the new architecture
- `MEMORY.md` — NBA calibration status, MLB pitcher K seasonal config reminder
- [[handoff-002]] — batter_hrr groundwork context still relevant
