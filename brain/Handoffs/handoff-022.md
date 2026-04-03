# Handoff 022

> Part of [[Handoffs]]

**Date**: April 03, 2026 at 4:49 PM

## Summary

Ran a mid-cycle NBA calibration health check (model is 11 days old, next scheduled check was Apr 13). Model is healthy at +9.8% ROI. Also diagnosed and fixed a dashboard bug where DFS-only predictions (e.g., Quenton Jackson from PrizePicks) appeared on the dashboard with "no sportsbooks offering this line."

## What Was Done

### NBA Calibration Health Check
- Queried `paper_bets` for Mar 20 - Apr 2: **65 bets, 64.6% win rate, +$23,050 PnL, +9.8% ROI**
- Computed per-stat/direction breakdown: AST UNDER dominant (81% win, +31.9% ROI), PTS UNDER flagged (36.4% win, -47.9% ROI)
- Computed global quantile coverage (n=2,297 per stat): same structural Q10 over-coverage pattern
- Computed bias: PTS +0.5% (improved from -3.6%), REB +1.4% (from -3.3%), AST -6.7% (from -10.8%)
- Edge accuracy: 5-10% bucket outperforming, 15%+ bucket underperforming (55.6% vs ~70%, n=18)
- Weekly trend: accelerating (+1.7% ROI week 1 → +16.3% week 2)
- **Decision: HOLD** — all thresholds pass, strong momentum
- Updated `memory/calibration_log.md` with full Apr 3 entry
- Updated `memory/MEMORY.md` "Latest Check" section

### Dashboard DFS Bookmaker Bug Fix
- **Root cause**: `daily_predictions` stores `bookmaker` column indicating line source. Some predictions had `bookmaker = 'prizepicks'` (DFS) or `bookmaker = 'fliff'` (sweepstakes). Dashboard query only filtered `.not('line', 'is', null)`, so these slipped through. But `AnalysisModal` excludes DFS platforms from sportsbook lines display → "no sportsbooks" message.
- **Fix (3 files)**:
  - `dashboard/src/lib/sportsbook-availability.ts` — Added shared `DFS_BOOKMAKERS` constant (prizepicks, underdog, pick6, betr_us_dfs, fliff)
  - `dashboard/src/app/(protected)/dashboard/page.tsx` — Added `.not('bookmaker', 'in', ...)` filter to predictions query
  - `dashboard/src/components/analysis/AnalysisModal.tsx` — Removed local `DFS_BOOKMAKERS`, now imports from shared constant
- **Impact**: Filters out ~19 predictions/day (14 prizepicks + 5 fliff), including false Model Picks
- Build passes clean

## Decisions Made

1. **Calibration: HOLD** — ROI 9.8% exceeds 8% threshold, model only 11 days old, strong weekly momentum. No action needed until Apr 13 age-based check.
2. **PTS UNDER: Monitor only** — 36.4% win rate on 11 bets is concerning but sample is too small for action. Watch over next week.
3. **DFS filter scope** — Added `fliff` to DFS_BOOKMAKERS since it's a sweepstakes platform (not a real sportsbook). Did NOT filter offshore books (novig, prophetx, bovada) because they ARE real sportsbooks — the existing state filter handles them when users select their state.

## Blockers and Open Questions

- **PTS UNDER weakness**: If win rate stays <40% by Apr 13 check, may need to investigate model confidence on PTS under bets specifically.
- **15%+ edge bucket**: Underperforming at 55.6% actual vs ~70% expected. Could indicate model overconfidence on high-edge plays. Only 18 bets — need more data.
- **Offshore book predictions**: 115 novig + 75 prophetx predictions today have no lines from state-licensed books unless user selects "All States." Not a bug per se, but worth considering if this confuses users without a state selected.

## Recommended Next Steps

1. **Deploy dashboard** — The DFS filter fix needs to be deployed to Vercel for users to see the improvement
2. **Apr 13 calibration check** — Model will be 21 days old (3-week limit). Full calibration review needed, especially PTS UNDER and edge accuracy
3. **MLB backtest sweeps** — `batter_total_bases` and `batter_runs_scored` models are retrained and waiting for backtest sweeps (Phase 1, Step 1.3/1.6)
4. **Stripe integration** — Phase 3 is untouched, next major business milestone

## Files to Read on Resume

- [[Execution-Plan]] — Current phase priorities and step statuses
- [[NBA-Model]] — Production model details and recent fix history
- [[Dashboard-Pages]] — Frontend architecture and route map
- `memory/calibration_log.md` — Full calibration history including today's check
- `memory/MEMORY.md` — Project-wide memory with calibration triggers and thresholds
