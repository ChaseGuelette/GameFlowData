# NBA daily / advanced ownership

## Ownership

- Railway runs `daily_stats_job.py --basic-only`: CDN basics, bounded basic derived repair, and props linking. A green run does **not** establish advanced readiness.
- The earlier local DailyStats BAT also uses basic-only mode and skips the cloud-owned props linker, so missed Windows tasks starting together cannot make it implicitly depend on advanced data.
- The enabled local BAT owns the ordered advanced chain: CDN basics first, bounded `stats.nba.com` advanced ingestion, then strict full Daily Stats with `--skip-resolution --skip-linker`. Cloud Daily Stats owns props linking, avoiding a recurring cross-process linker-lock collision.
- NBA inference is fail-closed until `nba_advanced_stats_job.py` has a successful `job_executions` record through the latest completed source date and raw advanced coverage is complete.
- Railway never runs the local advanced producer. Mirror sync remains a separate owner action.

## Owner commands

Immediate 14-game repair:

```powershell
& .\venv\Scripts\python.exe -X utf8 .\src\orchestration\nba_advanced_stats_job.py --advanced-limit 14 --request-timeout 30 --max-retries 2 --http-client curl --no-proxy
```

Normal automatic-chain equivalent (BAT default is 120):

```powershell
& .\scripts\run_advanced_scraper.bat
```

DB/network-free order preview:

```powershell
& .\venv\Scripts\python.exe -X utf8 .\src\orchestration\nba_advanced_stats_job.py --dry-run
```

## Current status

The approved 14-game advanced catch-up completed locally at 09:26:02 with all 14 games saved and exit code 0. Independent canonical readback confirmed314/314 played-player advanced rows and28/28 complete team rows; the exact production missing-advanced predicate fell from342 to0 through November4. Receipt: Hermes scratch `gameflow_advanced_repair_readback_2026-11-05.result.json`.

Parent verification:39 existing tests passed; scoped Ruff, dry-run order and diff checks passed. Controlled worker checks covered ordered execution, stop-on-failure, basic-only separation, strict mode, linker omission and inference readiness. Child Python processes explicitly inherit UTF-8 for redirected Windows task logs.

The new complete local advanced producer completed with exit0 on November5 at09:36:07ET. The strict derived stage populated November4 advanced rolling averages, position history, league-position averages and opponent-allowed outputs, then verified raw advanced coverage through November4. Props linking and resolution were explicitly skipped. Persisted readiness-record readback and deployment verification are the remaining acceptance checks.

No training, mirror sync, Task Scheduler changes or bet placement was authorized. Existing scheduled BAT actions pick up the code changes automatically, but the workstation still must be available; missing local output must block inference rather than masquerade as complete advanced readiness.