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
