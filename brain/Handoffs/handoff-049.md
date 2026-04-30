> Part of [[Handoffs]]

**Date**: April 25, 2026

## Summary

Fixed the NBA Analysis Modal on the Bot Tracker page (BetAnalysisModal) — three root causes resolved: wrong sport context for player headshots, missing team info, and incomplete NBA stat column mapping. Also patched two critical bugs where NBA trades kept generating despite the sport gate being set to disabled.

## What Was Done

### NBA Analysis Modal Fixes (`BetAnalysisModal.tsx`)
- Added `sportOverride` prop to `PlayerAvatar` component — bypasses global `useSport()` context that was serving MLB headshot URLs for NBA players when the user had MLB selected
- Extended `PlayerGameStats` type with optional fields: `stl`, `blk`, `tov`, `team_id`, `matchup`
- Expanded NBA Supabase query from `'game_date, pts, reb, ast, fg3m, min'` to include `stl, blk, tov, min, team_id, matchup`
- Added full `NBA_STAT_COLUMN` mapping (handles `pts`, `reb`, `ast`, `stl`, `blk`, `fg3m`, `tov`)
- Added `NBA_COMBO_COMPONENTS` for combo stats (`pra`→pts+reb+ast, `pr`→pts+reb, `pa`→pts+ast, `ra`→reb+ast, `sb`→stl+blk)
- L5 average calculation now handles combo stats via component reduce
- `Last5Chart` now receives precomputed `values` array for combo stats
- Table dynamically shows extra columns (STL/BLK/3PM/TOV) when the bet stat is relevant
- Team abbreviation and matchup now display in modal header using `NBA_CONFIG.teams[team_id]`

### TradeApprovalPanel Team Display Fix (`TradeApprovalPanel.tsx`)
- Bug: `teamAbbrev` was only resolved for `sport === 'mlb'`, leaving NBA trades with `undefined` → "???" in the Analysis Modal
- Fix: Added NBA branch using `NBA_CONFIG.teams[data.team_id as number]`

### Sport Gate Default Fix (`kalshi_live_trader.py`)
- Bug: `select_trades()` defaulted sport gate to `"true"` if env var not set — NBA would fire if `NBA_TRADING_ENABLED` was missing from Railway
- Fix: Changed default from `"true"` → `"false"` (safe default: trading disabled unless explicitly enabled)

### Sport Gate Bypass Fix (`kalshi_refresh_job.py`)
- Bug: `renew_expired_queue_trades()` ran before the sport gate check — disabled-sport trades had their 30-min timer reset every 10 minutes, keeping them alive indefinitely
- Fix: Wrapped the entire renew + select + propose block in a sport gate check; if sport disabled, logs a skip message and sets `"live_trading": {"selected": 0, "proposed": 0, "renewed": 0}`

## Decisions Made

- **Safe defaults**: Sport gates now default to `"false"` (disabled) rather than `"true"`. Principle: any system that can place real money trades must fail safe.
- **Renewal must respect gate**: Renewing queue expiry is functionally equivalent to placing a new trade — it should not happen if the sport is disabled. Renewal is now inside the gate check, not outside.
- **`sportOverride` prop pattern**: Rather than plumbing sport through a prop chain or adding NBA-specific logic to `useSport()`, a simple override prop on `PlayerAvatar` is cleaner and doesn't change existing callers.

## Blockers and Open Questions

- **Changes uncommitted**: All 6 modified files are staged locally but not committed or pushed. Vercel/Railway won't pick up the sport gate fixes until deployed.
- **Verify `NBA_TRADING_ENABLED=false` on Railway**: Even with the safe default fix, the env var should be explicitly set to avoid ambiguity.
- **GLM/OpenCode handoff rule violated**: The BetAnalysisModal changes (~80+ lines across 3 files) should have been handed off to GLM per the CLAUDE.md threshold (20+ lines OR 2+ files). Was implemented directly instead. No functional issue, but worth noting for future sessions.

## Recommended Next Steps

1. **Commit and push** — stage all 6 modified files and push to deploy fixes to Vercel + Railway:
   - `dashboard/src/components/shared/PlayerAvatar.tsx`
   - `dashboard/src/types/predictions.ts`
   - `dashboard/src/components/bot-tracker/BetAnalysisModal.tsx`
   - `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`
   - `src/paper_trading/kalshi_live_trader.py`
   - `src/orchestration/kalshi_refresh_job.py`
2. **Verify Railway env vars**: Confirm `NBA_TRADING_ENABLED=false` is explicitly set (not just relying on new default).
3. **Test NBA modal on deployed frontend**: After deploy, open Bot Tracker → click analysis icon on an NBA trade → verify headshot loads, team shows, Last 5 chart has data.

## Files to Read on Resume

- [[Bot-Tracker]] — current state of bot tracker features and known issues
- [[Kalshi-Live-Trading-Startup]] — sport gate invariants and go-live checklist
- [[handoff-048]] — previous session (stale fill cancellation queue)
