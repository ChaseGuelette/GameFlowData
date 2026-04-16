> Part of [[Handoffs]]

**Date**: April 16, 2026 at 12:00 PM

## Summary
Session focused on dashboard UX polish and MLB feature completeness. Collapsed the cluttered 10-control filter header into a single clean row, fixed a critical bug where sportsbook lines at alternative line values (e.g. 0.5 hits) were disappearing, improved the MLB analysis modal with full batting context, and fully implemented Ask AI for MLB players.

## What Was Done

### Dashboard Filter Redesign
- Created `dashboard/src/components/predictions/FilterPopover.tsx` — gear icon trigger with active-count badge, click-outside + Escape pattern, contains State/Books/Game Status/Direction filters + Reset button
- Modified `dashboard/src/app/(protected)/dashboard/page.tsx` — header now has exactly 4 visible controls: FilterTabs → All Bets/Model Picks toggle → Date → ⚙ Filters → Build Slate
- Removed `edgeThreshold`, `blTau`, `filtersOpen` states entirely; hardcoded 0.03 edge threshold
- Removed BL client-side blending block (Model Picks uses pre-computed DB values; All Bets hardcodes 0.03)

### Scrollbar Removal
- Modified `dashboard/src/app/globals.css` — added `scrollbar-width: none` and `::-webkit-scrollbar { display: none }` to body

### Analysis Modal: MLB Batting Context
- Modified `dashboard/src/components/analysis/AnalysisModal.tsx`
- Batting history table now shows all 6 stats (AB, H, TB, HR, RBI, R), with target stat column highlighted
- Binary model detection: when `q10=q25=q50=0 && q90≤1`, shows P(stat ≥ 1) / P(No stat) cards instead of misleading quantile bars

### Analysis Modal: Sportsbook Lines Bug Fix
- Deduplication key changed from `bookmaker` → `` `${bookmaker}:${line}` ``
- Bug: DraftKings with both a 0.5 line and 2.5 line would collapse to one entry, mismatching odds from two different lines → failing the completeness check → 0.5 line never shown
- Fix ensures each bookmaker+line pair is independently deduplicated and displayed
- Updated staleness `snapshotMap` key to match

### MLB Ask AI — Full Implementation
- Modified `dashboard/src/app/api/ask/route.ts` — added `buildMlbSystemPrompt()` function and MLB branch in POST handler
- Detection: `prediction.stat.startsWith('batter_') || prediction.stat.startsWith('pitcher_')` routes to MLB before NBA
- Round 1 fetches (parallel): game log (15/10 games), rolling averages, player info, game schedule
- Round 2 fetches (parallel, after team_id resolved): park factors, opposing pitcher avgs, opposing pitcher last 5 starts, pitcher name/handedness
- System prompt sections: game log, rolling averages, venue/game context, park factors, opposing pitcher (batters only), quantile/binary prediction, sportsbook lines
- Handles both batters and pitchers with sport-appropriate context
- Flipped `askChat: true` in `dashboard/src/lib/sport-config.ts` for MLB

## Decisions Made

- **Removed Edge/BL tau selectors**: These were power-user controls nobody used. Model Picks already uses optimal per-stat DB values; All Bets can safely hardcode 0.03. Simpler is better.
- **Binary model display**: Batter hits/HR models are Bernoulli — showing 0/0/0/1/1 quantiles to users was confusing. Showing P(stat ≥ 1) is more honest and actionable.
- **Bookmaker+line dedup key**: A single bookmaker can offer multiple line values for the same stat (e.g. DK offers 0.5 and 2.5 for batter hits). Keying on just `bookmaker` caused the lower line to get dropped.
- **MLB Ask AI architecture**: 2-round parallel fetch pattern (same as NBA) keeps latency low. Opposing pitcher context is the most important MLB-specific enrichment for batter props.

## Blockers and Open Questions

- **Model accuracy**: Lawrence Butler batter_hits model predicts 29% P(hit) vs ~70% market implied. Likely early-season feature staleness (rolling averages distorted by small sample). Not a code bug — track over coming weeks.
- **MLB `mlb_game_lineups` table is empty**: Lineup-based batter filtering in the daily runner falls back gracefully, but the Ask AI can't reference batting order position from lineups (only from `lineup_position` in game_stats).

## Recommended Next Steps

1. **Monitor MLB Ask AI in production** — verify it's pulling opponent pitcher data correctly, check that park factors populate (need valid venue_id → mlb_park_factors join). Watch for any 500 errors in Vercel logs.
2. **batter_hrr backfill + sweep** (Step 1.9) — this is the main unfinished pipeline task. Commands were laid out in handoff-005: backfill `batter_hits_runs_rbis` odds 2023-2025, run linker backfill, run BL sweep, promote if ROI > 0% with Z > 1.5.
3. **NBA calibration check** (due Apr 13, now overdue) — run the check-calibration skill; model is 24 days old (past the 21-day threshold). Apr 10 check showed +10.9% ROI but REB UNDER concerning (-15.1%).
4. **Phase 3 (Stripe)** — monetization has been deferred but is the next business priority after MLB pipeline stabilizes.
5. **Make track record shareable** (Step 2.6.5) — public URL for Chase's track record is the main remaining credibility tool.

## Files to Read on Resume

- [[handoff-006]] (this file — start here)
- [[Execution-Plan]] — Step 1.9 (batter_hrr) is the main in-progress pipeline task
- [[AI-QA-Chat]] — updated with full MLB enrichment architecture
- [[Dashboard-Pages]] — updated with filter redesign + analysis modal changes
- `dashboard/src/app/api/ask/route.ts` — MLB branch starts after line ~593
