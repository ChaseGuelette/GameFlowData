# Daily NBA advanced producer on Railway

Owner authorization: Chase approved recurring Railway advanced ingestion, scoped commit/push/deployment and bounded native verification on 2026-11-08. The earlier three-game trial was not recurring authorization.

## Runtime

- The existing 09:00 America/New_York basic job stays CDN-only.
- With `NBA_ADVANCED_STATS_ENABLED=true`, successful basic ingestion is followed by `nba_advanced_stats_job.py --railway --skip-basic`.
- Advanced source caps: 30 missing current-season games, 60 HTTP GET attempts including retries, at most 2 attempts/game, 30 seconds/request, 900 seconds for source ingestion. No proxy or redirect-following outside the request budget.
- Then run the existing strict derived chain with `--skip-resolution --skip-linker` (1800-second child-tree budget), including its CDN recheck and coverage gates. This does not place bets or promote models.
- Readiness is recorded only after the full chain exits successfully and the current raw advanced completeness check passes.
- 09:30 recovery reuses successful basic work from today, never yesterday's in-memory status. A single scheduler-chain lock prevents overlap between the primary/retry triggers.
- Local/cloud advanced wrappers share a canonical PostgreSQL advisory lock. This protects updated wrapper invocations, not old copies or direct low-level scraper commands.
- `NBA_ADVANCED_STATS_ENABLED=false` disables the follow-up; basic ingestion remains enabled. No separate 12:15 timer exists.

## Owner catch-up

From the GameFlowData root in PowerShell:

    .\venv\Scripts\python.exe -X utf8 src\orchestration\nba_advanced_stats_job.py --advanced-limit 120 --request-timeout 30 --max-retries 2 --http-client curl --no-proxy

Current-season missing-game catch-up writes canonical DATABASE_URL, then verifies the derived chain. It is not a local-mirror sync or an all-season repair. Keep the local checkout updated so it shares the cloud lock. Do not launch direct low-level scraping concurrently.

## Validation / deployment gate

Implemented and verified offline: actual CLI child commands; cloud caps; basic-success ordering; failed-basic blocking; today/yesterday retry behavior; duplicate-chain suppression; flag disabled/enabled behavior; real scheduler return values; producer lock refusal/source failure/incomplete coverage/success; actual curl/requests GET-budget boundaries; native child success/nonzero exit. All three changed Python files passed Ruff and `git diff --check`. No new unit-test files were added.

Deployment and native acceptance are tracked in the active session handoff; offline checks alone do not prove them. Preserve source success, raw/derived completeness, native wrapper completion and future scheduled reliability as separate claims.

The protected AGENTS.md update was not applied because its tool approval prompt timed out. The existing CDN-only Daily Stats invariant remains preserved. No unrelated scheduler-timeout or model/training edits are included in this lane.
