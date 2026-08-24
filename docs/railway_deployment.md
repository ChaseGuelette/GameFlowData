# Railway deployment

GameFlowData deploys one always-on Railway worker. Railway starts `/app/venv/bin/python src/orchestration/scheduler.py`; APScheduler owns all NBA/MLB timing in `America/New_York`.

`railway.toml` does not define separate Railway cron services. Do not maintain a second schedule table in Railway or convert the worker into per-job crons without an explicit architecture change.

## Files

- `railway.toml` — Nixpacks builder, worker start command, restart policy.
- `nixpacks.toml` — Python 3.11 virtual environment and native library setup.
- `requirements.txt` — production dependencies.
- `src/orchestration/scheduler.py` — job registrations and ET schedule authority.
- `docs/daily_pipeline_automation.md` — operator-facing schedule summary.

## Required configuration

Set secrets through Railway variables; never commit or print their values. The retained worker needs the database connection and only the provider/Discord variables used by enabled NBA/MLB jobs. Use the Supabase session-pooler connection on port 5432 for backend writes; port 6543 can behave read-only in this environment.

Common variable names include:

- `DATABASE_URL`
- `ODDS_API_KEY`
- `RAPIDAPI_KEY`
- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ALERTS`
- `DISCORD_CHANNEL_PREDICTIONS`
- `DISCORD_CHANNEL_PERFORMANCE`
- MLB-specific Discord channel overrides when desired
- explicit feature gates such as `NBA_FULL_LINES_ENABLED` and the dense-CLV job gate

Inspect source before adding variables; retired Kalshi, Polymarket, arbitrage, and NCAAB variables are not active deployment requirements.

## Deployment

Deployment is a separately authorized action. After local tests and a clean-clone build pass:

```powershell
railway login
railway link
railway up
```

A successful upload is not verification. Read back the deployment state and inspect startup logs to confirm:

1. the worker starts with the virtual-environment Python;
2. scheduler startup enumerates only retained NBA/MLB and maintenance jobs;
3. timezone is `America/New_York`;
4. no retired job is registered;
5. no immediate import, database, or provider failure occurs.

## Model artifacts

Only deployable suites are tracked:

- `src/models/artifacts/production/`
- `src/models/artifacts/production_playoffs/`
- `src/models/mlb/artifacts/production/`

Local `run_*`, sweep, ablation, backup, and rejected artifact directories are ignored and archived outside Git. Never promote by copying a directory based only on its name. Promotion requires artifact verification, backtest/evaluation evidence, and separate human approval.

MLB artifact preflight:

```powershell
.\venv\Scripts\python.exe scripts\audit_mlb_model_artifacts.py --model-dir src\models\mlb\artifacts\production --json
```

The audit is an artifact/functionality gate only. Quote-clean CLV, ranking, timing stability, and paper evidence remain separate gates.

## Railway-specific invariants

- `daily_stats_job.py` uses `nba_unified_scraper.py --cdn-only`.
- Never call `stats.nba.com` from Railway and never move advanced-stat scraping there.
- Use `sys.executable` for subprocesses so jobs remain inside `/app/venv`.
- Do not run training, sweeps, or broad backfills on this worker.
- Do not introduce blocking index creation on `raw_player_props_combined`.

## Monitoring and rollback

Use the linked project/service in the Railway dashboard or CLI to inspect deployment and runtime logs. A missing job execution is not proven healthy because the worker process exists; verify scheduler registration and the job's latest completion evidence.

If a deployment regresses:

1. stop or roll back the affected deployment through Railway;
2. preserve logs and the failed deployment identity;
3. verify the prior worker is running and schedules are restored;
4. retest the original failure path before closing the incident.

Do not combine a code deployment with a DB migration, model promotion, or broad backfill unless each action was separately approved.
