---
title: Handoff 089 — MLB/Kalshi trading-readiness blocker fix docs
type: handoff
domain: handoffs
status: draft
owner: Chase
effective_date: 2026-05-26
tags: [handoff, mlb, kalshi, trading-readiness, production-readiness]
---

# Handoff 089 — MLB/Kalshi trading-readiness blocker fix docs

> Part of [[Handoffs]]

**Date**: 2026-05-26

## Summary

Investigated the production trading-readiness blockers from the prior audit and wrote focused fix documents under GameFlowData `.hermes/plans/trading-readiness-fixes-2026-05-26/`. Chase explicitly said NBA can remain broken, so NBA lines-linker work was documented as deferred and not promoted as a blocker for MLB/Kalshi readiness.

Remote DB evidence shows MLB source stats/lineups are fresh through `2026-05-25`, but MLB derived model inputs are stale, recent MLB prediction/sample rows are absent, and recent Kalshi model/edge linkage is not producing current paper/live/queue rows. Kalshi query-timeout investigation found non-sargable ET-date casts against large `kalshi_markets` query paths.

## What Was Done

- Created trading-readiness fix docs:
  - `README.md`
  - `01-mlb-derived-feature-freshness.md`
  - `02-prediction-and-kalshi-linkage-verification.md`
  - `03-kalshi-query-timeouts.md`
  - `04-nba-lines-linker-deferred.md`
  - `05-mlb-feature-model-validation-gates.md`
- Ran a SELECT-only remote DB audit through the delegated SQL-runner pattern.
- Verified current remote DB findings:
  - `mlb_player_game_stats_batting` max `2026-05-25`
  - `mlb_player_game_stats_pitching` max `2026-05-25`
  - `mlb_game_lineups` max `2026-05-25`
  - `mlb_player_average_batting` max `2026-05-12`
  - `mlb_player_average_pitching` max `2026-05-11`
  - `mlb_bullpen_daily_status` max `2026-05-11`
  - `mlb_active_roster` max scraped_at `2026-04-25 13:00:56+00`
  - `mlb_daily_predictions` and `mlb_daily_prediction_samples`: 0 rows in the last 7 days
- Confirmed relevant code paths:
  - `mlb_daily_stats_job.py` calls rolling averages but not bullpen workload derivation.
  - `mlb_populate_averages_incremental.py` defaults to `date.today()` and can no-op if same-day source rows do not exist yet.
  - `mlb_roster_scraper_job.py` exists/scheduled but remote roster data is stale.
  - `mlb_inference_job.py` stores predictions/samples but current remote output rows are absent.
  - `selection_loader.py`, `market_matcher.py`, and related Kalshi paths use `(snapshot_time AT TIME ZONE 'America/New_York')::date = :target_date` query shapes.
- Attempted fresh Railway log check, but Railway MCP auth is expired: `Unauthorized. Please run railway login again.`

## Decisions Made

- Defer NBA `lines_job.py` / stats.nba.com / NBA inference issues unless they block shared MLB/Kalshi infrastructure.
- Treat MLB derived feature freshness as the first production-readiness fix.
- Treat prediction-output verification as a required gate before trusting any inference job success markers.
- Fix Kalshi timeout-prone queries by rewriting ET-date casts to sargable UTC timestamp ranges before considering DDL/indexes.
- Keep MLB feature/model promotion blocked until quote-clean CLV, edge-ranking, book concentration, and intraday stability gates pass.

## Blockers and Open Questions

- The new `.hermes/plans/trading-readiness-fixes-2026-05-26/` docs are untracked in GameFlowData and need review/commit decision.
- Railway auth is expired, so fresh production log/readiness certification is pending.
- Canonical GameFlowBrain handoff sync could not be completed from this session because Tailscale SSH to `gameflow-agent` requested interactive auth and local WSL did not contain `/home/chase/GameFlowBrain`.
- Need decide whether to implement the fix docs directly, hand them to an implementation worker, or commit docs first.

## Recommended Next Steps

1. Review/commit the new fix docs in GameFlowData.
2. Restore Railway auth (`railway login`) before production-log certification.
3. Implement/fix MLB derived freshness first:
   - make `mlb_daily_stats_job.py` target latest completed source date;
   - add bullpen workload derivation;
   - harden roster scraper success/failure criteria;
   - verify table max dates after run.
4. Add a read-only prediction/Kalshi output verifier and make MLB inference fail non-zero when games exist but predictions/samples are absent.
5. Rewrite Kalshi `kalshi_markets` loaders to use UTC timestamp ranges instead of ET-date casts.
6. Only after current predictions/edges exist, return to the MLB batter_hits model promotion gates from `05-mlb-feature-model-validation-gates.md`.

## Files to Read on Resume

- `.hermes/plans/trading-readiness-fixes-2026-05-26/README.md`
- `.hermes/plans/trading-readiness-fixes-2026-05-26/01-mlb-derived-feature-freshness.md`
- `.hermes/plans/trading-readiness-fixes-2026-05-26/02-prediction-and-kalshi-linkage-verification.md`
- `.hermes/plans/trading-readiness-fixes-2026-05-26/03-kalshi-query-timeouts.md`
- `.hermes/plans/trading-readiness-fixes-2026-05-26/04-nba-lines-linker-deferred.md`
- `.hermes/plans/trading-readiness-fixes-2026-05-26/05-mlb-feature-model-validation-gates.md`
- `src/orchestration/mlb_daily_stats_job.py`
- `src/processing/mlb/mlb_populate_averages_incremental.py`
- `src/scrapers/mlb/mlb_bullpen_workload_scraper.py`
- `src/orchestration/mlb_roster_scraper_job.py`
- `src/orchestration/mlb_inference_job.py`
- `src/models/mlb/mlb_prediction_store.py`
- `src/models/kalshi_edge.py`
- `src/trading/kalshi/selection_loader.py`
- `src/arbitrage/market_matcher.py`
