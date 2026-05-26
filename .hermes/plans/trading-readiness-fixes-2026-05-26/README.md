# Trading readiness blocker fix docs — 2026-05-26

Scope: investigation and fix plans for the five blockers from the production-readiness audit. NBA runtime breakage is intentionally not a priority unless it blocks shared infrastructure.

## Documents

1. `01-mlb-derived-feature-freshness.md`
2. `02-prediction-and-kalshi-linkage-verification.md`
3. `03-kalshi-query-timeouts.md`
4. `04-nba-lines-linker-deferred.md`
5. `05-mlb-feature-model-validation-gates.md`

## Current overall posture

- MLB source stats and lineups are fresh through `2026-05-25` on remote production DB.
- MLB derived feature inputs are stale (`mlb_player_average_*`, `mlb_bullpen_daily_status`, `mlb_active_roster`).
- Remote `mlb_daily_predictions` and `mlb_daily_prediction_samples` have zero rows in the last 7 days, despite job-success signals observed earlier.
- Kalshi trading/linkage tables have historical model/edge rows, but no recent production candidates/queue rows; `kalshi_markets` is large and the live loader uses non-sargable ET-date casts.
- Railway auth was unavailable during this doc-writing pass (`Unauthorized; run railway login again`), so Railway log evidence is from the preceding bounded audit plus DB state here.

## Safety notes

- Do not enable live money from these docs alone.
- Do not run destructive DB work or broad full-table backfills without explicit approval.
- Keep SQL reads isolated through the GameFlow SQL-runner pattern.
- Do not add indexes/keys on giant tables without approval; if needed, specify DDL/risk first.
- Probabilities must remain empirical from samples, e.g. `(samples > line).mean()` / Kalshi integer-line `>=` semantics where intentionally implemented.
