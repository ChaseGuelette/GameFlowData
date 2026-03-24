# Railway Setup

> Part of [[Infrastructure]]

## Architecture
- Single always-on worker process running APScheduler
- Build: Nixpacks (`nixpacks.toml`) — Python 3.11 venv, zlib, stdenv.cc for numpy/scipy/xgboost C extensions
- Start command: `/app/venv/bin/python src/orchestration/scheduler.py`
- Explicit `LD_LIBRARY_PATH` for Nix-installed shared libraries

## Environment Variables
- `DATABASE_URL` — Supabase PostgreSQL connection string
- `ODDS_API_KEY` — The Odds API key
- `RAPIDAPI_KEY` — RapidAPI key (injuries)
- `DISCORD_CHANNEL_ALERTS` — Discord webhook URL

## Model Artifacts
- Production models committed to git: `src/models/artifacts/production/`
- `run_*/` directories are gitignored
- Promote models via `scripts/promote_model.py`

## Build Files
- `nixpacks.toml` — Nixpacks build config
- `railway.toml` — Railway-specific settings
- `requirements.txt` — Production dependencies

## Deployment
- Push to `main` branch triggers Railway deploy
- No CI/CD pipeline — deploys are manual git push

## Important Notes
- NEVER run stats.nba.com scraping from Railway (datacenter IP ban)
- daily_stats_job uses `--cdn-only` flag
- All subprocess calls use `sys.executable` for venv compatibility

#railway #infrastructure #deployment
