# Handoff 017 — Re-Enable Kalshi Live Trading

> Part of [[Handoffs]]

**Date**: April 21, 2026 at 11:16 AM

## Summary

Re-enabled Kalshi live trading for MLB (NBA stays off). Deployed HWM-based drawdown, Discord approval embed, dynamic exposure cap, and fixed the TradeApprovalPanel crash. Hit a Railway `libz.so.1` container build issue that blocked numpy imports — root-caused to `COPY . /app` overwriting `/app/lib/`, fixed by moving libs to `/opt/lib/`.

## What Was Done

### DB Changes
- Added `hwm_dollars DECIMAL(10,2)` column to `kalshi_live_trading_config` (migration applied)
- Cleared `is_halted=false`, set `starting_bankroll=150`, `hwm_dollars=150` (then auto-updated to $167.68 by bot)
- Fixed 13 null `fill_price` records on Apr 19 NBA bets in `kalshi_live_orders` (set to 0)
- Inserted test trade (ID 187) into `kalshi_trade_queue` for frontend verification

### Code Changes (all on `main`, pushed)
- `src/paper_trading/kalshi_live_trader.py`: HWM-based drawdown in `check_circuit_breakers()` — ratchets up on new portfolio highs, drawdown measured from peak. Updated `_ensure_config()` to initialize `hwm_dollars`.
- `src/discord_bot/alerts.py`: Added `_build_kalshi_approval_embed()` (orange embed with trade list, exposure, edge range, 30-min expiry). Added `"approval_needed"` case to `send_kalshi_trade_alert_sync()`.
- `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`: Fixed crash — replaced `useEffect(() => setSelected(new Set()), [trades])` with `useMemo`-based `validSelected` pruning (no re-render loop).
- `nixpacks.toml`: Changed lib copy target from `/app/lib` to `/opt/lib` (fixes `libz.so.1` missing at runtime — `COPY . /app` was overwriting `/app/lib/`).

### Railway Env Vars Set
- `KALSHI_LIVE_TRADING_ENABLED=true`
- `NBA_TRADING_ENABLED=false` (already was)
- `MLB_TRADING_ENABLED=true` (already was)
- `KALSHI_DAILY_EXPOSURE_PCT=0.70` (was 0.90)
- `KALSHI_MIN_DAILY_EXPOSURE=20` (was 200)
- `KALSHI_MAX_DAILY_EXPOSURE=1000` (was 200)
- `LD_LIBRARY_PATH=/opt/lib:/root/.nix-profile/lib`

### Brain/Ops Updated
- `brain/Operations/Kalshi-Live-Trading-Startup.md`: Updated exposure config section + drawdown reference to HWM

## Decisions Made

1. **Dynamic exposure cap (70% of live API balance)** instead of hardcoded $200. The bot queries Kalshi balance every run, computes cap dynamically. Min/max are safety rails, not drivers.
2. **HWM-based drawdown** instead of static `starting_bankroll` anchor. The old approach broke after the Apr 19 losses — balance dropped below the static floor permanently. HWM only goes up, so recovery is always possible.
3. **Simultaneous Kelly not yet implemented** — user identified that Kelly assumes sequential betting but bot places 5-15 bets at once. Discussed 3 options (reduce fraction, sqrt(N) scaling, portfolio Kelly). User reduced Kelly fraction to 0.05 as Option A. sqrt(N) scaling (Option B) is the correct theoretical fix but deferred.

## Blockers and Open Questions

1. **Railway `libz.so.1` — fix pushed but NOT YET VERIFIED**. The `nixpacks.toml` change (libs to `/opt/lib`) was pushed to `main`. Railway needs to rebuild and deploy. Check that the next deployment has no `ImportError: libz.so.1` in logs.
2. **Vercel `SUPABASE_SERVICE_ROLE_KEY`** — must be set in Vercel env vars for the approval panel to work in production (the `/api/kalshi/queue` endpoint needs it). Currently only set locally.
3. **Test trade (ID 187) in queue** — delete it or let it expire after verifying the UI.
4. **Simultaneous Kelly (Option B)** — sqrt(N) scaling deferred. Current 0.05 Kelly fraction + 70% cap is conservative enough for now.

## Recommended Next Steps

1. **Verify Railway deploy is clean** — check logs for no `libz.so.1` errors after latest build completes
2. **Set `SUPABASE_SERVICE_ROLE_KEY` on Vercel** — otherwise prod approval panel is broken
3. **Watch first live trading cycle** — confirm trades appear in queue, approve one, verify it executes and shows in `kalshi_live_orders`
4. **Clean up test trade** — delete queue entry ID 187
5. **Consider implementing sqrt(N) Kelly scaling** — better than just reducing the fraction

## Files to Read on Resume

- [[Kalshi-Live-Trading-Startup]] — updated ops playbook with HWM + dynamic cap
- [[handoff-016]] — previous session's post-mortem context
- `src/paper_trading/kalshi_live_trader.py` — all guardrails live here
- `nixpacks.toml` — Railway build config (verify `/opt/lib` fix worked)
- `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx` — approval UI
