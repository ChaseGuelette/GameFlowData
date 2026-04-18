# Pipeline

> Part of [[BRAIN-INDEX]]

Data ingestion, linking, processing, and orchestration. The pipeline runs daily on Railway with a 5-minute refresh cadence for props and edges.

## Key Files
- [[Daily-Flow]] - The complete daily orchestration flow
- [[Scrapers]] - All data sources and scraper modules
- [[Linker-System]] - How NBA/MLB/NCAAB data gets linked
- [[Scheduling]] - Railway APScheduler configuration and job definitions
- [[Data-Sources]] - External APIs, CDNs, and data providers
- [[Component-Docs]] - Detailed module-level documentation from docs/ folder

## Kalshi Non-Sports Scraping (Session 13 — Complete)
- `src/scrapers/kalshi/kalshi_utils.py` — `KALSHI_NON_SPORTS_SERIES` defines 9 series: 3 economics (KXGDP, KXFED, KXCPI) + 6 crypto (KXBTC, KXBTCD, KXETH, KXETHD, KXDOGE, KXXRP)
- `src/scrapers/kalshi/kalshi_market_scraper.py` — `scrape_non_sports_and_store()` fetches all non-sports series and upserts to `kalshi_markets` with `sport=NULL`
- `src/orchestration/kalshi_nonsports_refresh_job.py` — standalone job, exits gracefully without Kalshi credentials, used by Railway scheduler
- Scheduler: `run_kalshi_nonsports_refresh()` runs every 10 min, 11AM-11PM ET (same cadence as NBA/MLB Kalshi refresh)
- 1,044 non-sports Kalshi markets in DB; arb scanner reads them via `_load_kalshi_non_sports()` (filters `sport IS NULL OR sport = ''`)
- Non-sports arb matching: 0 matches currently — question wording too different between Kalshi and Polymarket at SequenceMatcher threshold 0.80. Infrastructure correct; matching threshold is future work.
