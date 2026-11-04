# NBA Injury API Pilot Delivery — November 4, 2026

## Verdict

The file-only pilot is implemented and exercised against real API responses. Capture/replay mechanics pass. The actual game-level audit returns **SOURCE_BLOCKED**, not training readiness. The full scrape is scoped, not launched.

- Code: `src/scrapers/nba_injury_pilot.py`, `scripts/run_nba_injury_pilot.py`, and one additive raw-response method in `src/scrapers/rapidapi_injury_backfill.py`.
- Durable results: `C:/Users/Chase/GameFlowArtifacts/nba-injury-pilot-20261104/`.
- Next-job scope: [full scrape](../../.hermes/plans/2026-11-04-nba-injury-full-scrape.md).
- Approved baseline design: [pilot plan](../../.hermes/plans/2026-11-03-nba-injury-api-recovery-pilot.md).

## Delivered behavior

- `plan` and `capture` without `--execute` validate exact requests without network/DB access.
- `capture --execute` reuses the existing RapidAPI client; single-attempt requests, no redirects/retries, explicit six-attempt ceiling, bounded time/bytes. Raw bytes and safe HTTP receipts are saved before parsing.
- `replay` verifies receipt hashes/lengths, preserves request date and payload game date separately, retains spillover, quarantines malformed records, and writes auditable counts.
- `audit` verifies/replays captures and reports the actual source/target gaps. It handles an explicitly blocked zero-game target-selection receipt as exit 2 with zero accepted games, not an exception or a fabricated cohort.
- Existing production fetch/store/CLI, daily jobs, linkers, schema and historical feature consumers are unchanged. Task D publication/edition integration is still conditional, not represented by caller-supplied PASS flags.
- The first MVP intentionally does not implement the design's provisional `--source-evidence` interface. There is no verified source contract to interpret yet; accepting arbitrary JSON as authority would conceal the blocker. The implemented audit is a diagnostic, not a publication adapter.

## Real execution

| Request date | Rows | HTTP |
|---|---:|---:|
| 2023-10-31 | 85 | 200 |
| 2024-10-31 | 124 | 200 |
| 2025-10-31 | 92 | 200 |

Exactly three new authenticated GET attempts, no retries. New capture: 301 rows, four invalid-status records quarantined, approximately 2.45 seconds elapsed.

Original saved November 1 responses: 324 rows; replay reproduced 248 valid target candidates, 73 retained spillover and three quarantined records.

Combined six-response replay: **625 = 418 target candidates + 200 spillover + 7 quarantined**. All six raw hashes, byte lengths and HTTP outcomes were checked independently. There are 37 raw game-date/team/player keys with differing status/reason across captures, including 20 differing statuses. These comparisons retain raw names and do not claim canonical identity resolution or full edition history.

Example: Precious Achiuwa's November 1, 2023 record is Doubtful from the October 31 request and Out from the November 1 request, despite identical `07PM` labels. Full collection must retain both request dates and cannot treat cycle labels alone as edition IDs.

## Actual target audit

The isolated canonical SQL runner used four SELECT attempts against exact dates/game IDs. Candidate counts: six on November 1, 2023; six on November 1, 2024; five on November 1, 2025. None of these 17 candidates had a qualifying nonnull-player prop row in `raw_player_props_combined`; none had a matching `nba_game_schedule` row. No commence time or roster was invented.

The resulting target manifest contains zero games with explicit blocked reasons. Running the real audit CLI against it produced `SOURCE_BLOCKED`, exit 2, with zero accepted games. This sampled current-table result is not proof that all historical props are absent or that a linker is broken. No archive lookup, broader linker repair or replacement-date crawl was launched.

The SQL supplied a transaction-local timeout setting inside a SELECT CTE; this receipt records the requested five-second setting, not an independent certification that PostgreSQL enforced a timer on the same statement. Queries completed without reported timeouts. Future SQL preflight should set timeout before the data statement.

Outstanding source facts remain publication clock, edition selection and complete submitted team semantics. The API pilot therefore does **not** clear first training launch.

## Verification

- Existing `test_rapidapi_injury_backfill`, `test_nba_source_boundaries`, `test_nba_mode_loaders`: 12 passed before/after implementation under the offline guard; no live DB/network/model-fit attempts in those checks.
- Scoped Ruff, compile and tracked-file whitespace checks passed.
- Real replay/plan/combined-replay and blocked-target audit were exercised through the CLI under the offline guard.
- Five short controlled probes passed: request-budget rejection, tampered-digest rejection, non-JSON rejection, API-error-object rejection and empty-list-not-source-acceptance.
- One bounded read-only code review returned PASS: no material capture/replay, accounting, quota, secret-handling or output-truthfulness defect. The later small empty-target-report branch was coordinator-reviewed and exercised with the actual SQL manifest (exit 2); it is not represented as independently reviewed bytes. Final scoped Ruff and the same 12 guarded existing tests passed. No broad hardening/audit loop was added.

## Reproduce without API calls

From `C:\Users\Chase\Projects\GameFlowData`, use a **new output directory** for each run:

` .\venv\Scripts\python.exe scripts/run_nba_injury_pilot.py plan --request-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\requests.json --max-requests 3 `

` .\venv\Scripts\python.exe scripts/run_nba_injury_pilot.py replay --input-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\combined\manifest.json --target-dates 2023-11-01 2024-11-01 2025-11-01 --output-dir C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\replay-again `

` .\venv\Scripts\python.exe scripts/run_nba_injury_pilot.py audit --input-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\combined\manifest.json --target-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\sql\target-manifest.json --output-dir C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\audit-again `

The last command is expected to exit 2 with the saved real source/target blockers. Do not repeat the authenticated capture merely to reproduce these reports.

## Scope boundary and next step

The full-scrape plan selects actual fitting/calibration dates, requests D and D−1, reuses verified captures, and adds one thin resumable batch wrapper. Its conservative calendar ceiling is 1,097 request dates, not an observed required count. Given the zero-target sample, a nonempty retained fitting cohort must be verified before authorizing bulk collection.

No DB writes, schema change, full scrape, training, commits, pushes or deployments occurred. Existing unrelated NCAAB/linkage/training-exclusion changes remain untouched. Source changes are local and uncommitted on main.

Suggested commit: `feat(nba): add bounded injury API capture and replay pilot`.
