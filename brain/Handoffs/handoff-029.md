# Handoff 029

> Part of [[Handoffs]]

**Date**: April 15, 2026 at 03:55 PM

---

## Summary

Built the full Track Record & Diagnostics page (`/track-record`) from scratch — migration, CSV import pipeline, 5 new components, data hook, and page — plus added edit/delete support to every bet in the History page's My Bets tab. Both features call `rebuild_user_daily_log` after mutations so the track record always reflects accurate cumulative P&L.

---

## What Was Done

### Track Record Page (Phase 2.6)

**Database (Migration 026 — applied to Supabase)**
- `user_bets.prediction_id` and `player_id` made nullable (allows CSV/manual bets without a linked prediction)
- `user_bets.source` column added (`'prop_card'` | `'manual'` | `'csv_import'`)
- Old unique constraint `(user_id, game_date, player_id, stat_type)` replaced with `user_bets_unique_bet (user_id, game_date, player_name, stat_type, bet_direction)` — enables CSV deduplication by name, and allows OVER + UNDER on same player/stat
- New table `user_bets_daily_log` with full RLS
- `rebuild_user_daily_log(uuid)` RPC — deletes and re-chains daily log from `user_bets`, uses `initial_bankroll` from `user_profiles` as starting point
- `get_track_record_summary(uuid)` RPC — monthly aggregation, requires `auth.uid() = target_user_id`

**New files created**
- `database/migrations/026_track_record.sql`
- `dashboard/src/lib/csv/parseBets.ts` — full CSV/TSV parser. Handles M/D/YYYY dates, 30+ column name aliases, stat normalization (NBA + MLB), PnL auto-calculation, unsigned-odds toggle, summary-row skipping, detailed error/warning reporting
- `dashboard/src/lib/hooks/useTrackRecordData.ts` — React Query hook supporting `'my_bets'` / `'paper'` / `'combined'` sources, monthly aggregate builder, KPI builder, stat breakdown builder, daily log merger for combined mode
- `dashboard/src/components/track-record/MonthlyGrid.tsx` — expandable monthly cards (W-L-P, P&L, ROI, win rate, bet count), collapses to show DailyBreakdown
- `dashboard/src/components/track-record/DailyBreakdown.tsx` — daily rows with click-to-expand per-bet list (player, stat, line, direction, odds, result, P&L)
- `dashboard/src/components/track-record/CsvUpload.tsx` — drag-and-drop CSV importer: upload → preview (10 rows) → batched upsert (100/batch) → rebuild daily log → done
- `dashboard/src/components/track-record/ManualBetForm.tsx` — manual bet entry, auto-calculates PnL, upsert with `source='manual'`, calls rebuild RPC
- `dashboard/src/components/track-record/ModelMetrics.tsx` — edge accuracy bars (10%+, 8-10%, 5-8%), streak tracker (current/longest win/lose), profitable days count
- `dashboard/src/app/(protected)/track-record/page.tsx` — full page with source toggle, KPI banner, BankrollChart, MonthlyGrid, StatBreakdown, ModelMetrics, CSV modal, manual bet modal

**Modified files**
- `dashboard/src/components/layout/Navbar.tsx` — "Track Record" link added to desktop nav and mobile menu

### History Page Edit/Delete (Phase 2.6.4)

**New file**
- `dashboard/src/components/history/EditBetModal.tsx` — modal with pre-filled form for all editable fields (date, player, stat, line, direction, odds, stake, result, book). PnL auto-calculated from odds+stake+status with optional manual override checkbox. On save: `UPDATE user_bets WHERE id = ?` then calls `rebuild_user_daily_log`.

**Modified files**
- `dashboard/src/components/history/BetCard.tsx` — edit button (pencil) + delete button on ALL bet statuses (not just pending). Delete shows inline "Confirm / ✕" prompt on first click to prevent accidents.
- `dashboard/src/components/history/BetList.tsx` — added `onEdit` prop, passes to `BetCard`
- `dashboard/src/app/(protected)/history/page.tsx` — imports `EditBetModal`, added `editingBet` state, `handleEditSaved`, `onEdit={setEditingBet}` on My Bets `BetList`. `handleRemoveBet` now also calls `rebuild_user_daily_log` after deletion.

**Build**: Clean — 26 routes generated, no TypeScript errors.

---

## Decisions Made

- **New unique constraint uses `player_name` + `bet_direction`** instead of `player_id` + no direction. CSV imports don't have player IDs, and users can legitimately take both OVER and UNDER on the same prop from different books.
- **PnL is stored, not calculated on read** — keeps track record immutable for historical accuracy. `rebuild_user_daily_log` is the single source of truth recomputation.
- **`rebuild_user_daily_log` is SECURITY DEFINER** — runs as postgres, so it can write to the table even though the user's auth token has 8s timeout. 30s statement timeout set explicitly.
- **Confirm-before-delete on BetCard** — resolved bets (won/lost) affect cumulative P&L. One mis-click shouldn't destroy history. Two-step inline confirmation (no modal) keeps friction low without being annoying.
- **`get_track_record_summary` RPC gated by `auth.uid() = target_user_id`** — designed for future sharing (Chase can share his track record with prospects) but safe by default since it only works if the caller IS the target user. Step 2.6.5 in the plan will add a public route for Chase's specific user_id.

---

## Blockers and Open Questions

- **CSV import needs real data test** — migration 026 is applied, components are built, but Chase hasn't imported his Excel file yet. The column aliases should cover it (Date, Sports book, Player, Prop Type, Line, Over/Under, Odds, Stake, Win/Loss, Profit), but real data may surface edge cases.
- **Track record page currently shows empty state** — `user_bets_daily_log` is empty until Chase imports his Excel history OR takes bets normally (which rebuild via the "Add Bet" form or CSV). The prop-card bets already in `user_bets` need `rebuild_user_daily_log` called once to populate the log.
- **First-run log population** — existing `user_bets` rows (placed via prop cards before this session) won't appear in `user_bets_daily_log` until `rebuild_user_daily_log` is called once. Chase should do this manually or trigger it via the "Add Bet" form after loading the page.
- **Step 2.6.5 (shareable track record)** — `get_track_record_summary` is built but the public route is not. Low priority until Chase wants to share with prospects.

---

## Recommended Next Steps

1. **Import Excel history** — go to `/track-record`, click "Import CSV", drag the Excel file (export as CSV). Verify rows appear and monthly cards render.
2. **Trigger first-run log rebuild** — if the monthly view is empty but bets exist, call `rebuild_user_daily_log` via Supabase SQL editor: `SELECT rebuild_user_daily_log('<your-user-id-uuid>');`
3. **Continue batter_hrr sweep** (Step 1.9) — backfill `batter_hits_runs_rbis` odds 2023-2025, run linker backfill, then run BL parameter sweep.
4. **Polymarket scraper rebuild for all categories** (Step 9.2) — rewrite `polymarket_market_scraper.py` to ingest all market categories, not just sports tag filtered.
5. **NBA calibration check due Apr 13** (overdue) — run the model diagnostics per `memory/calibration_log.md` process.

---

## Files to Read on Resume

- [[handoff-029]] — this file (latest context)
- [[Dashboard-Pages]] — updated with track record + history edit routes
- [[Execution-Plan]] — Phase 2.6 now documented with step-by-step status
- [[MLB-Model]] — Step 1.9 (batter_hrr) still pending sweep + promotion
- `memory/calibration_log.md` — NBA model check overdue
