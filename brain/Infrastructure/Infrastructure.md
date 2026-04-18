# Infrastructure

> Part of [[BRAIN-INDEX]]

Deployment, hosting, database, monitoring, and third-party services. The system runs across Railway (Python backend), Vercel (Next.js dashboard), Supabase (PostgreSQL), Discord (alerts), and local Windows (advanced stats).

## Key Files
- [[Railway-Setup]] - Python backend deployment on Railway
- [[Vercel-Setup]] - Next.js dashboard deployment on Vercel
- [[Supabase-DB]] - Database architecture, tables, RPCs, and performance
- [[Discord-Bot]] - Automated alerts and slash commands
- [[Environment-Vars]] - All environment variables across services

## Recent Infrastructure Changes (Session 13)
- **Railway scheduler job added**: `kalshi_nonsports_refresh` — runs every 10 min, 11AM-11PM ET via `src/orchestration/kalshi_nonsports_refresh_job.py`. Scrapes 9 non-sports Kalshi series (economics + crypto). Exits gracefully when Kalshi credentials absent.
- **DB migration applied**: `make_kalshi_markets_sport_nullable` — dropped NOT NULL constraint on `kalshi_markets.sport`. Non-sports markets stored with `sport=NULL`. Existing sports markets unaffected.
