# Handoff 007 — MLB Discord Alerts, Dashboard Sport Guards, Pipeline Audit

> Part of [[Handoffs]]

**Date**: March 24, 2026 at 10:48 PM

## Summary

Wired up MLB Discord alerts with sport-specific channel routing, added sport guards to DFS and Stats Vault pages so MLB users see clean messages instead of broken NBA content, and ran a comprehensive pipeline audit confirming MLB is production-ready for tomorrow's first run.

## What Was Done

- **MLB Discord alerts activated**: Updated `src/discord_bot/alerts.py` with `sport` parameter on `send_predictions_alert_sync()` and `send_pnl_summary_sync()`. MLB alerts route to `DISCORD_MLB_CHANNEL_PREDICTIONS` / `DISCORD_MLB_CHANNEL_PERFORMANCE` env vars, falling back to shared NBA channels if not set. Embeds show sport-specific titles ("MLB Predictions Ready!" in blue) and footers.
- **MLB inference job wired up**: Replaced Discord stub in `src/orchestration/mlb_inference_job.py` with actual `send_predictions_alert_sync(preds, target_date, sport="mlb")` call.
- **DFS page sport guard**: Added `useSport()` check in `dashboard/src/app/(protected)/dfs/page.tsx` — shows "DFS analysis is currently available for NBA only" for non-NBA sports.
- **Stats Vault sport guard**: Added `useSport()` check in `dashboard/src/app/(protected)/stats/page.tsx` — shows "Data Vault is currently available for NBA only" for non-NBA sports.
- **Environment vars**: Added `DISCORD_MLB_CHANNEL_PREDICTIONS` and `DISCORD_MLB_CHANNEL_PERFORMANCE` placeholders to `.env`.
- **Full pipeline audit**: Confirmed all 5 MLB pipeline files (scheduler, daily stats job, inference job, daily runner, paper trader) are production-ready with no blockers. All edge cases handled, all imports resolve, no hardcoded date gates.
- **Dashboard audit**: Confirmed History and Performance pages are already fully sport-aware. DFS and Stats are correctly NBA-only with guards now in place.

## Decisions Made

- **Fallback channel strategy**: MLB alerts fall back to shared NBA channels when sport-specific env vars are empty. This means alerts work immediately — separate channels can be created in Discord at any time without code changes.
- **Sport guards over feature flags**: DFS and Stats pages now check `config.sport` directly rather than relying on feature flags. This is cleaner because the pages are fundamentally NBA-only (hardcoded NBA tables/RPCs), not just missing data.

## Blockers and Open Questions

- **Discord MLB channels**: Need to create "mlb-predictions" and "mlb-performance" channels in the Discord server and paste their IDs into the env vars (locally + Railway). Until then, MLB alerts go to shared channels.
- **Batter models (Step 1.3)**: Still `in_progress` — training commands ready but not yet executed. Tomorrow's run will only have pitcher K predictions.
- **MLB stat labels**: The Discord alert uses `stat.upper()` for display (e.g., "PITCHER_STRIKEOUTS"). May want to add a display-name mapping for cleaner formatting.

## Recommended Next Steps

1. **Create Discord MLB channels** and add channel IDs to `.env` and Railway env vars
2. **Commit and push** — triggers auto-deploy for scheduler (Railway) and dashboard (Vercel)
3. **Train batter models** (Step 1.3) — last gate before full MLB stat coverage
4. **Monitor first MLB run** tomorrow at 10 AM ET (stats) / 1:30 PM ET (inference)
5. **Phase 3: Stripe integration** — next major workstream

## Files to Read on Resume

- [[Execution-Plan]] — Current status of all phases
- `src/discord_bot/alerts.py` — Sport-aware alert routing
- `src/orchestration/mlb_inference_job.py` — MLB inference with Discord integration
- `dashboard/src/lib/sport-config.ts` — MLB feature flags and config
