> Part of [[Handoffs]]

**Date**: April 12, 2026 at 12:00 PM

## Summary

Three product improvements shipped: BotOrdersTable got a "Value" column and Kalshi market link icon, ruff linting was fixed in two Python files, and the MLB Stats Vault was fully implemented (Batters + Pitchers tabs with Box/Rates/Consistency categories, heatmap table, L3–L20 windows). Also fixed MLB Model Picks params display (full per-stat BL config: edge, τ, z_max, max_weight) and restored the BL tau dropdown for MLB after it was incorrectly hidden. Build is clean.

---

## What Was Done

### Bot Tracker — Orders Table enhancements
- **`dashboard/src/components/bot-tracker/BotOrdersTable.tsx`**
  - Added `getKalshiUrl(ticker, sport)` helper — extracts matchup from ticker index 1, builds sport-specific URL (NBA: `kxnbagame`, MLB: `kxmlbgame`)
  - Added "Value" column: live orders use `total_cost` directly, paper bets calculate `contracts × entryPrice / 100`
  - Added external-link icon column — opens Kalshi game market page in new tab
  - Updated colSpan 11 → 13

### Ruff linting fixes (zero errors, behavior preserved)
- **`scripts/analyze_kalshi_paper_bets.py`**: Restructured imports (sys.path before third-party, load_dotenv() after all imports) to fix E402, expanded one-liner if/returns to fix E701, removed unused `overflow_analysis` variable (F841)
- **`src/paper_trading/kalshi_analysis.py`**: Expanded `_edge_bucket()` one-liner if/returns to fix E701

### MLB Model Picks params display
- **`dashboard/src/lib/sport-config.ts`**
  - Added `modelPicksEdge: number`, `modelPicksTau: number | null` to `SportConfig` interface
  - Added full `perStatConfig` (4 params: edge, tau, z_max, max_weight per stat) — replaces `perStatEdge`
  - MLB values mirror `src/models/mlb/mlb_stat_config.py` exactly (K: 5%/0.90/0.25/0.80, Hits: 8%/0.75/1.0/0.80, RBIs: 12%/0.90/0.25/0.80)
  - NBA: `modelPicksEdge: 0.09, modelPicksTau: 0.50, perStatConfig: {}`
- **`dashboard/src/app/(protected)/dashboard/page.tsx`**
  - Per-stat chips (K ≥5%, Hits ≥8%, RBIs ≥12%) shown in Model Picks header for MLB
  - Tooltip upgraded to full param table showing all 4 params per stat
  - Dropdowns styled with `appearance-none` + custom chevron overlay
  - MLB-specific edge options: K model (≥5%), Hits model (≥8%), RBIs model (≥12%)

### BL tau restored for MLB
- **`dashboard/src/app/(protected)/dashboard/page.tsx`**
  - Removed `{sport !== 'mlb' && (...)}` wrapper — tau dropdown shows for all sports
  - Added MLB-specific options: τ=0.75 (Hits model), τ=0.90 (K/RBI model)
  - Fixed mobile badge count (was excluding BL tau from count for MLB)

### MLB Stats Vault (Step 2.6 — COMPLETED)
- **`database/migrations/023_mlb_stats_vault_views.sql`** — NEW
  - `mlb_batters_latest` view: DISTINCT ON latest per player, joins mlb_players + mlb_teams, all batting columns
  - `mlb_pitchers_latest` view: DISTINCT ON latest per pitcher, joins mlb_players + mlb_teams, all pitching columns
  - RLS SELECT policies for `authenticated` on 4 underlying tables (mlb_player_average_batting, mlb_player_average_pitching, mlb_players, mlb_teams)
- **`dashboard/src/types/stats.ts`** — Added `'l3' | 'l10' | 'l20'` to `WindowSuffix`, added `'dec3'` format type
- **`dashboard/src/components/stats/HeatmapTable.tsx`** — Added `dec3` case (`.toFixed(3)` for AVG/OBP/SLG/OPS)
- **`dashboard/src/components/stats/WindowToggle.tsx`** — Added optional `options` prop (defaults to NBA L5/L15/SZN)
- **`dashboard/src/lib/stats/columns.ts`** — Added 6 MLB column sets: `mlbBatterBoxColumns`, `mlbBatterRatesColumns`, `mlbBatterConsistencyColumns`, `mlbPitcherBoxColumns`, `mlbPitcherRatesColumns`, `mlbPitcherConsistencyColumns`
- **`dashboard/src/components/stats/MLBStatsPage.tsx`** — NEW component
  - Batters/Pitchers main tabs
  - Category tabs: Box Score / Rates / Consistency per tab
  - Batter windows: L5/L10/L20/SZN; Pitcher windows: L3/L5/SZN
  - Filters: search, team dropdown, min GP/GS
  - Reuses existing `HeatmapTable` and `CategoryTabs` components
- **`dashboard/src/app/(protected)/stats/page.tsx`** — Replaced "NBA only" stub with `<MLBStatsPage />` for MLB
- **`dashboard/src/lib/sport-config.ts`** — `statsVault: true` for MLB

---

## Decisions Made

**MLB BL tau should NOT be hidden**: MLB has per-stat tau values (0.75 for Hits, 0.90 for K/RBIs) and the tau dropdown is useful for manual exploration even though BL is applied server-side in "Model Picks" mode. Hiding it was wrong.

**MLB Stats Vault uses existing HeatmapTable infrastructure**: Rather than building a new table component, `MLBStatsPage` is a new component that reuses `HeatmapTable`, `CategoryTabs`, `WindowToggle`, and `HeatmapLegend`. Clean composition.

**Separate component pattern for MLB stats page**: Instead of shoehorning MLB into the existing NBA stats page hooks, `MLBStatsPage` is a standalone component to avoid React hooks violations from conditional hook calls.

**Batting rate stats are windowless (L10 only)**: AVG, OBP, SLG, OPS are computed from rolling 10-game sums rather than per-game averages. They exist only at L10. Marked `windowless: true` with `dec3` format.

**Pitcher rate stats are windowless (L5 only)**: ERA, WHIP, K/9, BB/9 derived from L5 rolling sums. Windowless.

---

## Blockers and Open Questions

- **Migration 023 not yet applied**: `database/migrations/023_mlb_stats_vault_views.sql` needs to be run in Supabase SQL Editor before the MLB Stats Vault will work. The views and RLS policies don't exist yet.
- **Kalshi live trading**: Apr 11-12 paper validation window — ready to flip `KALSHI_LIVE_TRADING_ENABLED=true` after confirming clean NO-only data today/yesterday. See `brain/Operations/Kalshi-Live-Trading-Startup.md`.
- **NBA calibration check due Apr 13**: Model is now 20 days old, approaching the 3-week trigger. Run `check-calibration`.

---

## Recommended Next Steps

1. **Apply migration 023**: Open Supabase SQL Editor, paste `database/migrations/023_mlb_stats_vault_views.sql`, run. Then verify by visiting the Stats Vault page in MLB mode.

2. **Kalshi live trading launch**: If Apr 11-12 Discord alerts show clean [NO-ONLY] badge with 5-20 NBA + 20-50 MLB bets/day, follow `brain/Operations/Kalshi-Live-Trading-Startup.md` to flip the live trading switch. Fund Kalshi with $300.

3. **NBA calibration check (Apr 13)**: Model hits 3-week age trigger. Run `check-calibration`. If ROI still above 8% and ECE below 0.06, hold.

4. **Stripe integration (Phase 3)**: Next major product milestone. See `brain/Business/Stripe-Plan.md`.

5. **Phase 2 remaining**: Step 2.4 (MLB injury reports) and 2.5 (MLB DFS) are the only unfinished Phase 2 items.

---

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — Pre-flight checklist for live trading launch
- [[NBA-Model]] — Model status, calibration check due Apr 13
- [[Dashboard-Pages]] — Updated with MLB Stats Vault details
- `database/migrations/023_mlb_stats_vault_views.sql` — Run this in Supabase to enable MLB Stats Vault
- [[Stripe-Plan]] — Next major product milestone after live trading launch
