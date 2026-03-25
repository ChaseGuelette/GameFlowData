# Handoff 005 — MLB Launch Prep: Scheduler, Dashboard, Paper Trading

> Part of [[Handoffs]]

**Date**: March 24, 2026 at 10:22 PM

## Summary

Full MLB launch prep session. Ungated the Railway scheduler so MLB jobs run year-round, added RLS policies for all MLB tables, wired up the Analysis Modal with MLB game history and bookmaker lines, built an MLB scoreboard API via MLB Stats API, and flipped the scoreboard feature flag. Dashboard builds clean. MLB is ready to go live tomorrow.

## What Was Done

- **Scheduler ungated**: Removed `month='4-10'` from all 4 MLB cron triggers in `src/orchestration/scheduler.py`. Jobs now fire daily year-round (off-season handled gracefully by returning empty results).
- **RLS policies applied**: Supabase migration added SELECT policies for all MLB tables — subscription-gated for predictions/paper-bets/logs, public read for stats/props/schedule/players.
- **Analysis Modal — MLB game history**: `AnalysisModal.tsx` now queries `mlb_player_game_stats_pitching` (for K's) or `mlb_player_game_stats_batting` (for hits/TB/HR/RBI/R). L5 chart, stat table, and L5 average all work for MLB.
- **Analysis Modal — MLB bookmaker lines**: Queries `mlb_raw_player_props` with same dedup/staleness logic as NBA. Added MLB stat-to-market identity mapping.
- **Scoreboard API**: `/api/scoreboard` now accepts `?sport=nba|mlb`. MLB handler uses `statsapi.mlb.com/api/v1/schedule` with linescore hydration. Maps game states to Pre/Live/Final with inning info.
- **useGameStatus hook**: Now passes `config.sport` to the scoreboard API endpoint.
- **Feature flag flipped**: MLB `scoreboard: true` in `sport-config.ts`.
- **Execution Plan updated**: Steps 1.2, 1.5, 1.6 marked completed. Phase 2 items 2.1-2.3 marked completed. Step 1.7 updated to note month gate removal.

## Decisions Made

- **Year-round scheduler**: Rather than shifting the month gate to March, we removed it entirely. The MLB jobs already handle off-season gracefully (no games found → empty results → no errors). This avoids having to update the gate for spring training, postseason, etc.
- **Supabase migration (not local SQL)**: Applied RLS policies via Supabase MCP `apply_migration` to ensure they're tracked and repeatable.
- **Concrete select() for Supabase**: Dynamic template strings in `.select()` break Supabase's TypeScript parser. Used concrete column lists for pitching/batting tables to keep the build clean.

## Blockers and Open Questions

- **No MLB predictions data yet**: Tables are empty. First predictions will populate once games start and the inference jobs fire (1:30 PM / 6:30 PM ET).
- **Batter models (Step 1.3)**: Still `in_progress` — training commands ready but not yet executed. Pitcher K model is live, but batter hits/TB/HR/RBI/runs models need training runs.
- **MLB paper trading resolution**: `mlb_paper_trader.py` exists but hasn't been run in production yet. First real test will be once predictions + games exist.

## Recommended Next Steps

1. **Train batter models** (Step 1.3) — Run `--tune --tuning-trials 100` for all 5 batter stats. This is the last gate before full MLB coverage.
2. **Deploy scheduler to Railway** — Commit and push triggers auto-deploy. Verify MLB jobs fire tomorrow at 10 AM ET.
3. **Verify end-to-end MLB flow** — Once inference runs, check: predictions page shows MLB data, analysis modal opens with chart, scoreboard shows game status.
4. **Stripe integration** (Phase 3) — Next major workstream after MLB is stable.

## Files to Read on Resume

- [[Execution-Plan]] — Current status of all phases
- `src/orchestration/scheduler.py` — MLB job definitions
- `dashboard/src/components/analysis/AnalysisModal.tsx` — MLB history + lines logic
- `dashboard/src/app/api/scoreboard/route.ts` — MLB scoreboard API
- `dashboard/src/lib/sport-config.ts` — MLB feature flags
