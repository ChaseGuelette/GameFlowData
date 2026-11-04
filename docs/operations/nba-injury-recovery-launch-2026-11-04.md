# Full NBA Injury Recovery — Ready for Owner Launch

> **Superseded by the DB-first correction.** The file download below completed and its captures are preserved, but these file-only launch flags are retired. Use [DB ingestion, import and Railway instructions](nba-injury-db-first-2026-11-04.md). The new normal recovery command writes the existing injury table; applying the two-column migration and performing the import/deploy remain owner-run steps.

## Status

**Raw injury collection is ready. The corrected manifest passed the actual dry-run. No full scrape has been launched by the agent.**

Owner-facing entrypoint: `scripts/run_nba_injury_recovery.py`.

Chase corrected the acquisition design: injury scraping uses historical NBA game dates/IDs, not prop-linked training eligibility. Policy is now `historical_game_dates_v1`, with no props join or player-minute filter. A single canonical read-only streaming SELECT exported 90,856 stats rows and these distinct games:

| Season | Historical games |
|---|---:|
| 22023 | 1,230 |
| 22024 | 1,230 |
| 22025 | 1,321 |

Verified plan: 3,781 games, 534 game dates, 567 request dates after D/D−1 union, six reusable captures, **561 new requests in 94 batches**. Launch ceilings are 600 request slots / 100 batches, allowing modest explicit-resume headroom. Those are safety limits, not a request to make 600 calls.

The actual dry-run exited 0 under the offline guard, making no API/DB/model calls. Disk check: 3,634,364,416 estimated bytes required, 907,109,294,080 free. The pilot's last observed provider quota was 499,988 remaining; this is dated response evidence, not a fresh billing guarantee. `run.json` did not exist at handoff. Earlier zero-props findings remain separate training/source-availability issues and do not block raw injury collection.

## Implemented

- Validates canonical historical games, literal identities/dates, season labels/counts and nonempty requested seasons; no training/props eligibility filter.
- Selects unique D and D−1 request dates, excluding current-season evaluation from this recovery scope.
- Reuses digest-verified responses; completed empty lists remain separate unverified-coverage evidence.
- Reserves global request/batch slots before capture and journals every received response. An interrupted batch conservatively keeps its reserved slots; an explicit resume may need a higher approved ceiling.
- Captures sequentially in batches of at most six using the existing API client, with no automatic retries and no DB writes.
- Reuses successful responses from partially failed batches. Normalizes in bounded partitions and resumes completed/partial normalization without overwriting raw evidence.
- Produces `plan.json`, `coverage.csv`, `run.json`, `execution.journal.jsonl`, per-batch captures, `partitions.json`, normalized/quarantine files and `summary.json`.
- Source acceptance remains BLOCKED and training readiness false; raw collection is not a historical publication/edition adapter.

Pilot helper changes were limited to a reusable one-receipt verifier, explicit body-completion markers and an optional per-response progress callback. Production scraper/writer behavior is unchanged.

## Verification

- Existing three suites: 12 passed with the offline guard.
- Scoped Ruff and compilation passed.
- Genuine six-capture replay reproduced 625 records / seven quarantined.
- Controlled structural-cohort exercise, using those genuine saved bodies: cached-only execution and completed resume both used zero HTTP calls.
- Controlled interruption after two saved-response replays: resume reused those two and replayed only the remaining four; a second completed resume issued no calls. Reservation accounting was ten conservative slots for six simulated attempts. These are explicitly mocked capture tests, not new API evidence or an approved training cohort.
- Insufficient global budget was rejected before capture.
- Corrected canonical game-history manifest: dry-run PASS, zero API/DB/model access under the offline guard. Controlled resume probes were also rerun after the policy correction.

No full scrape, database mutation, new permanent unit tests, model training, commits or pushes were performed.

## Real input receipts

Directory: `C:/Users/Chase/GameFlowArtifacts/nba-injury-recovery-20261104-inputs/`

- `cohort-game-history.json`: complete canonical historical games, independent of props.
- `game-history-summary.json`: verified game/date counts.
- `game-history-exact.sql`: read-only export receipt.
- Do not use the older props-filtered `cohort-streamed.json` or incomplete `cohort.json` for this scrape.

Successful dry-run receipt: `C:/Users/Chase/GameFlowArtifacts/nba-injury-recovery-20261104/preflight.json` (DRY_RUN_PASSED). The same directory holds `plan.json` and `coverage.csv`.

## Commands

Run from `C:\Users\Chase\Projects\GameFlowData` in PowerShell.

### Repeat the verified dry-run

` .\venv\Scripts\python.exe scripts/run_nba_injury_recovery.py --cohort-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-recovery-20261104-inputs\cohort-game-history.json --reuse-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\combined\manifest.json --output-dir C:\Users\Chase\GameFlowArtifacts\nba-injury-recovery-20261104 --dry-run `

### Owner launch command

The raw collection job is ready to launch; this does not start training or write the legacy injury table:

` .\venv\Scripts\python.exe scripts/run_nba_injury_recovery.py --cohort-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-recovery-20261104-inputs\cohort-game-history.json --reuse-manifest C:\Users\Chase\GameFlowArtifacts\nba-injury-pilot-20261104\combined\manifest.json --output-dir C:\Users\Chase\GameFlowArtifacts\nba-injury-recovery-20261104 --execute --max-total-requests 600 --max-batches 100 `

The required new count is 561. The ceilings bound operator retries/resume; the job does not make extra requests just to fill its budget. Raw responses, normalized partitions and quarantined records are retained under the output directory, with `summary.json` and `coverage.csv` for review.

To continue an interrupted launched run, repeat its exact command with `--resume`. Cohort hash and request dates must match. The command does not silently reset the request budget; raise a ceiling explicitly only after reviewing the saved reservation/attempt counts.

## After collection

Match injuries to game/team/player identities and establish the publication/edition/completeness evidence for T-60 features. Those are downstream acceptance steps, not prerequisites for collecting raw history. Training eligibility and quote availability remain separate. The earlier props-based acquisition gate was a design error, not an API requirement.

Related: [full-scrape scope](../../.hermes/plans/2026-11-04-nba-injury-full-scrape.md), [pilot delivery](nba-injury-pilot-delivery-2026-11-04.md).

Suggested commit: `feat(nba): add resumable injury history recovery runner`.
