# Fix plan 04 — NBA lines linker is intermittently failing, but deferred

## Verdict

Chase said we can leave NBA broken. Do not spend implementation time on NBA stats.nba.com, NBA inference, or NBA lines linker failures unless they block shared MLB/Kalshi infrastructure.

## Evidence from pasted context / previous audit

- Logs showed at least one `stats.nba.com` timeout path from NBA inference/daily runner area.
- CDN fallback also hit one `403`.
- `lines_job.py` was intermittent, not dead:
  - 116 successes in 24h
  - 17 failures in 24h
  - latest detected failure: `2026-05-26 16:05 UTC`
- The failing area was described as NBA `Linking Props (Incremental)`.

## Current decision

- Defer NBA runtime/linker fixes.
- Do not let NBA failures block MLB production-readiness work.
- Continue to respect invariant: no advanced `stats.nba.com` scraping from Railway; `daily_stats_job` should remain CDN-only.

## Implementation status

Implemented 2026-05-26 as a guardrail-only hotfix:

- Added scheduler-level deferred-failure tagging for `lines_job.py`.
- Failed NBA `lines_job.py` alerts now display as `Lines Scraper (NBA deferred)`.
- Failed NBA `lines_job.py` alert/error text and `job_executions.error_message` are prefixed with `NBA deferred — intermittent NBA lines linker failure is parked`.
- MLB jobs, Kalshi MLB refresh, source scraping, and linker algorithms were not changed.

Validation run:

- `./venv/Scripts/python.exe -m ruff check src/orchestration/scheduler.py tests/test_pipeline_resilience.py` — passed.
- `./venv/Scripts/python.exe -m py_compile src/orchestration/scheduler.py tests/test_pipeline_resilience.py` — passed.
- `./venv/Scripts/python.exe -m pytest tests/test_pipeline_resilience.py::TestJobStatusTracking -q` — 5 passed, 1 warning.
- Full `tests/test_pipeline_resilience.py` still has unrelated live-DB fallback failures in `TestCheckDependency` because current `job_executions` rows can satisfy dependency checks.

## Guardrail fix only, if needed

If NBA failures make shared scheduler state noisy or create false red status for MLB/Kalshi readiness, implement a minimal isolation/observability fix, not a full NBA repair:

1. Ensure MLB jobs and Kalshi MLB refresh do not depend on `lines_job.py` success.
2. Ensure scheduler/job dashboard distinguishes NBA `lines_job.py` from `mlb_lines_job.py`.
3. If alert noise is high, downgrade NBA linker failure notifications or tag them `NBA deferred`.
4. Do not change source scraping behavior or linker algorithms in this lane.

## Future repair sketch, intentionally parked

When NBA becomes a priority again:

1. Inspect `src/orchestration/lines_job.py` and the NBA incremental linker path.
2. Confirm whether failures are raw prop table query timeouts, CDN/API failures, or player/game linker issues.
3. Rewrite any broad latest-props scans to use short windows, latest scrape batches, and existing indexes.
4. Preserve Railway CDN-only invariant; do not reintroduce `stats.nba.com` calls from Railway.
5. Add output verification similar to MLB: rows scraped, linked rows, max snapshot, and game/date coverage.

## Non-goals for this workstream

- No NBA code changes.
- No stats.nba.com fixes.
- No NBA model/inference validation.
- No broad `raw_player_props_combined` DDL/index work.
