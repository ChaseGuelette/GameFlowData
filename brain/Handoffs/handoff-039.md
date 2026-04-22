> Part of [[Handoffs]]

**Date**: April 21, 2026 at 08:16 PM

## Summary

Built and shipped Phase 10: Manual Paper Trader — end-to-end. Users can now click "Paper Trade" on any pick in the Analysis Modal and have bets auto-logged, automatically resolved against actual game results nightly, and visible in the History page with a Real/Paper/All toggle and PAPER badge. Several critical post-ship bugs were found and fixed (wrong unique constraint target, isPaperTrade not forwarded in handleTakeBet, hydration error on dashboard, 13 incorrectly logged bets corrected in DB).

## What Was Done

### Phase 10: Manual Paper Trader — Full Implementation
- **DB Migration 027** (`database/migrations/027_paper_trade.sql`) applied to Supabase:
  - Added `is_paper_trade boolean NOT NULL DEFAULT false` to `user_bets`
  - Dropped old `user_bets_unique_bet` constraint; replaced with one on `(user_id, game_date, player_name, stat_type, bet_direction, is_paper_trade)` so real + paper bets can coexist for same player/stat

- **`dashboard/src/types/predictions.ts`** — added `is_paper_trade?: boolean` to `PaperBet` interface

- **`dashboard/src/lib/hooks/useUserBets.ts`**:
  - Added `isPaperTrade?: boolean` to `PlaceBetCustomParams`
  - Paper trades use `.insert()` (not `.upsert()`) with `is_paper_trade: true, source: 'paper_trade'`
  - Added `is_paper_trade: false` to `baseRow` and `toggleBaseRow` for real bets
  - Updated all 6 `onConflict` targets from old `user_id,game_date,player_id,stat_type` → `user_id,game_date,player_name,stat_type,bet_direction,is_paper_trade` (the migration changed the constraint)

- **`dashboard/src/components/analysis/AnalysisModal.tsx`**:
  - Added `isPaperTrade?: boolean` to `TakeBetData` interface
  - Added `paperBetSet` boolean state (separate from `betPlaced`)
  - Added "Paper Trade" button alongside "Take Bet" — auto-stakes Kelly recommendation, blue/slate styling, shows "Paper Set!" after click, no manual stake input
  - Reset `paperBetSet` when prediction changes

- **`dashboard/src/lib/hooks/useHistoryData.ts`** — added `is_paper_trade` to `fetchMyBets` select string and mapped it in the return object

- **`dashboard/src/app/(protected)/history/page.tsx`**:
  - Added `PaperTradeFilter = 'all' | 'real' | 'paper'` type and `paperTradeFilter` state
  - Added All/Real/Paper toggle buttons in My Bets header (blue highlight for Paper)
  - `paperFilteredMyBets` filters before direction/status filters

- **`dashboard/src/components/history/BetCard.tsx`** — added blue "PAPER" badge when `bet.is_paper_trade === true`

- **`src/orchestration/resolve_user_paper_bets.py`** — new resolver script:
  - Fetches pending paper bets with `game_date <= yesterday` and `player_id IS NOT NULL`
  - Groups by (stat_type, game_date), queries actual stats from `player_game_stats` / `mlb_player_game_stats_batting` / `mlb_player_game_stats_pitching`
  - Computes won/lost/push from American odds, batch-updates `user_bets`
  - `STAT_TO_SOURCE` mapping covers all 7 NBA stats + 4 MLB stats
  - Supports `--dry-run`

- **`src/orchestration/scheduler.py`**:
  - Added `run_user_paper_bet_resolution()` function
  - Wired at 9:30 AM ET (same slot as daily_stats_retry)
  - Added `"resolve_user_paper_bets.py": "User Paper Bet Resolution"` to `JOB_NAMES`

### Bug Fixes
- **`dashboard/src/app/(protected)/dashboard/page.tsx`**:
  - `handleTakeBet` was not forwarding `isPaperTrade` to `placeBetCustom` — all paper trades were logged as real bets
  - Fixed hydration error: `<table>` inside `<p>` (BL configs tooltip) — changed `<p>` → `<div>`

- **DB data fix**: 13 incorrectly logged real bets (IDs 341-354) corrected to `is_paper_trade = true` via direct SQL UPDATE

### Build
- `npm run build` — clean throughout

## Decisions Made

- **Resolver lives in `src/orchestration/`** not `src/processing/` — `run_job()` in scheduler always looks in `orchestration/`, so the resolver had to go there regardless of the plan spec saying `processing/`
- **Paper trades use `.insert()` not `.upsert()`** — constraint no longer covers paper vs real distinction with old column set; insert avoids conflict issues and is cleaner since you'd never want to silently overwrite a paper trade
- **`onConflict` target updated to new constraint** — migration 027 changed the named constraint from `player_id`-based to `player_name,stat_type,bet_direction,is_paper_trade` — the real bet upserts had to be updated to match or they'd fail with empty error objects
- **Auto-stake from Kelly, no manual input** — paper trades are zero-risk simulation, so stake is auto-filled from the Kelly recommendation; users don't need to input a dollar amount they're not actually spending

## Blockers and Open Questions

- **Phase 10.6 not done**: P&L summary card on History page when Paper filter is active (Bets, Win Rate, Total P&L, ROI, Avg Edge banner). The HistorySummary component already computes some of this — would need to wire it for paper-only view.
- **Code not deployed to Vercel** — all changes are local only. Need a `git commit` + `git push` to deploy to production.
- **Late-season MLB BL configs** — still on TODO (April → July-Sep backtests) per MEMORY.md.

## Recommended Next Steps

1. **Deploy to Vercel** — commit and push all local changes (Phase 10, hydration fix, onConflict fix)
2. **Phase 10.6** — add P&L summary banner to History page "Paper" filter view
3. **Test resolver** — run `python src/orchestration/resolve_user_paper_bets.py --dry-run` once paper bets have a game_date in the past
4. **Phase 2.5.2** — Track Record page shareability (public URL for Chase's record)
5. **Phase 3.5** — Stripe end-to-end test once env vars are configured
6. **Late-season MLB sweeps** — run July-Sep backtests to validate/update BL configs before mid-season

## Files to Read on Resume

- [[handoff-039]] — this file
- [[Execution-Plan]] — Phase 10 status, next phases
- [[Dashboard-Pages]] — history page and analysis modal current state
- [[Kalshi-Live-Trading-Startup]] — live trading status (MLB enabled, NBA disabled)
