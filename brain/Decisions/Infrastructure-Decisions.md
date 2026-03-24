# Infrastructure Decisions

> Part of [[Decisions]]

## Railway for Python Backend
- APScheduler handles all job orchestration in a single process
- Nixpacks build for numpy/scipy/xgboost C extension compatibility
- Always-on worker is simpler than managing individual cron containers

## Vercel for Dashboard
- Next.js 16 native hosting with zero config
- Edge network for global performance
- Environment variable management built in

## Supabase for Database
- PostgreSQL with built-in auth (email/password)
- RLS for row-level access control
- `postgres` role for backend, `authenticated` for frontend
- RPCs for complex queries with timeout overrides

## Local-Only Advanced Stats
stats.nba.com blocks datacenter IPs (Railway, GitHub Actions, etc.). Advanced stats scraping runs only on local Windows machine via Task Scheduler. No proxy — proxies also get blocked.

## Model Artifacts in Git
Production model folder (`src/models/artifacts/production/`) is committed directly to git. This ensures Railway always has the latest model without needing external artifact storage (S3, etc.). `run_*/` training output directories are gitignored.

## 5-Minute Refresh Over Webhooks
Props scraping + edge refresh every 5 minutes is simpler and more reliable than webhook-based real-time updates. The fuzzy cache optimization makes each cycle fast enough (<1s linker + 2-3min edge refresh).

#infrastructure #decisions #architecture
