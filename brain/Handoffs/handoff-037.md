> Part of [[Handoffs]]

**Date**: April 21, 2026 at 10:41 AM

## Summary

Mixed session covering three areas: frontend UX (bankroll manager overhaul on account page, bot tracker fill-price display fix), infrastructure debugging (Railway libz.so.1 crash killing both Lines Scraper and Edge Refresh), and a full feature scope for a manual paper trader on the frontend. The nixpacks fix was deployed to Railway; the bankroll manager is live; the paper trader scope is ready to implement.

---

## What Was Done

### Bankroll Manager (Account Page)
- **DB migration applied**: `add_user_sportsbooks` — added `bankroll_override numeric(12,2)` to `user_profiles`, created `user_sportsbooks` table (per-book balances, notes, sort_order) with RLS policy
- **New file**: `dashboard/src/lib/hooks/useSportsbookManager.ts` — `PRESET_BOOKS` (15 books), `syncEffectiveBankroll()`, `useSportsbooks/useAddSportsbook/useUpdateSportsbook/useRemoveSportsbook` React Query hooks
- **Modified**: `dashboard/src/lib/hooks/useUserPreferences.ts` — added `bankrollOverride: number | null` field, syncs from/to DB, window event listener for `bankrollUpdate` dispatched by `syncEffectiveBankroll`
- **Modified**: `dashboard/src/app/(protected)/account/page.tsx` — removed simple "Current Bankroll" number input, added `SportsbookSection` inline component (per-book rows with balance/notes inputs, auto-total, Auto-total / Manual Override toggle)

### Bot Tracker P&L Fix
- **Modified**: `dashboard/src/components/bot-tracker/BotOrdersTable.tsx` — fixed Fill column: live orders were showing YES market price (e.g., 82¢) for NO bets instead of actual NO price paid (18¢). Same side-adjustment as paper bets now applied to live orders. Root cause: `fill_price` in DB is stored as YES market price; NO entryPrice = `100 - fill_price`.

### ruff Fix
- **Modified**: `scripts/fix_apr19_pnl.py` — added `# noqa: E402` to 4 local imports that must come after `sys.path.insert(0, ".")` and `load_dotenv()`

### Railway libz.so.1 Fix
- **Modified**: `nixpacks.toml` — added `cmds` to setup phase that explicitly copies `libz.so.1`, `libstdc++.so.6`, `libgcc_s.so.1` from nix store to `/app/lib/` during the build. Updated `LD_LIBRARY_PATH = "/app/lib:/root/.nix-profile/lib"`. Deployed to Railway (commit `c1656da`). Both Lines Scraper and Edge Refresh were failing for the same reason — nba_linker_local.py is a subprocess that also imports numpy/pandas.

### Manual Paper Trader — Feature Scope Written
- Full scope documented (see Decisions below). Added to Execution Plan as Phase 10.

### API Fix
- **Modified**: `dashboard/src/app/api/arb/verify/route.ts` — moved Supabase client creation from module level into the POST handler (was causing `supabaseKey is required` crash during Next.js static build on Railway CI)

---

## Decisions Made

### Bot Tracker Fill Display
`fill_price` in `kalshi_live_orders` is stored as the YES market price, not the side-adjusted price. So for a NO bet at 82¢ YES market, actual cost per contract = 18¢. The table now shows 18¢ (side-adjusted), consistent with paper bets. The Value ($0.18) and P&L ($0.18 loss) columns from the DB are correct — they reflect actual Kalshi money (1 real contract at 18¢ = $0.18 cost).

### nixpacks libz Fix Strategy
The nix store path includes a content hash that changes on every rebuild, making `/root/.nix-profile/lib` a broken symlink after redeploys. Fix: copy the actual `.so` files to `/app/lib/` during the setup phase — this is a real filesystem path baked into the image that survives restarts and is stable across future rebuilds.

### Manual Paper Trader Architecture
Extend `user_bets` with `is_paper_trade boolean DEFAULT false` rather than creating a new table. Rationale: same schema, same history tab, same edit/delete flow — just a flag and a filter toggle. Auto-resolution via Python resolver checking `player_game_stats`. Sportsbook odds captured at time of bet (same as real bets) so P&L is meaningful.

---

## Blockers and Open Questions

- **Apr 19 PnL fix**: `scripts/fix_apr19_pnl.py` is written and ruff-clean but not yet run. 21 NBA bets still have garbage PnL from null fill_price. Run locally when Kalshi account has real fills to verify against.
- **Railway rebuild**: nixpacks fix was deployed — monitor next Lines Scraper + Edge Refresh runs to confirm the fix holds.
- **NBA trading**: `NBA_TRADING_ENABLED=false` — still paused post-Apr 19 incident. Will re-enable after confirming v2 playoff model is producing clean paper results for 2-3 days.
- **MLB late-season BL configs**: `batter_hits`, `pitcher_strikeouts`, `batter_rbis` sweeps were run for Apr-Jun. Jul-Sep configs not yet run — flagged as TODO before mid-season.
- **Stripe**: End-to-end test still pending (needs Stripe Dashboard + env vars filled in → then flip `SUBSCRIPTION_REQUIRED=true`).

---

## Recommended Next Steps

1. **Monitor Railway** — check next Lines Scraper + Edge Refresh runs post-nixpacks deploy. If still failing, check `/app/lib/` actually has the .so files (add a debug echo in install cmds).
2. **Implement Manual Paper Trader (Phase 10)** — scope is ready. Start with DB migration (`is_paper_trade` column), then AnalysisModal button, then History tab toggle + P&L column, then Python resolver.
3. **Run fix_apr19_pnl.py** — fixes the 21 Apr 19 bets with garbage PnL. Verify results before running.
4. **Re-enable NBA trading** — once v2 playoff model paper results look clean for 2-3 days with correct fill prices showing in bot tracker.
5. **MLB late-season sweep** — run Jul-Sep backtest for all 3 MLB stats and update BL configs before ~May 15.

---

## Files to Read on Resume

- [[handoff-037]] — this session
- [[Execution-Plan]] — Phase 10 (Manual Paper Trader) now scoped and ready
- [[Operations/Kalshi-Live-Trading-Startup]] — re-enable NBA trading checklist
- [[Models/NBA-Model]] — v2 playoff model details and current BL config
