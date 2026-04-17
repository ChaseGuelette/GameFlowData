# Scrapers

> Part of [[Pipeline]]

## NBA Scrapers (`src/scrapers/`)

| Module | Source | Schedule |
|--------|--------|----------|
| `nba_unified_scraper.py` | NBA CDN (boxscores) | 11 AM daily |
| `daily_player_props_scraper.py` | Odds API (us/us2/us_ex/us_dfs) | 12 PM, 4 PM, every 5 min |
| `daily_game_lines_scraper.py` | Odds API | 12 PM, 4 PM |
| `rapidapi_injury_backfill.py` | RapidAPI (88K+ rows) | 12 PM, 4 PM |
| `play_type_scraper.py` | stats.nba.com (Synergy) | LOCAL ONLY — 9 AM via Task Scheduler |
| `nba_unified_scraper.py` | stats.nba.com (advanced) | LOCAL ONLY — 9 AM via Task Scheduler |

**CRITICAL**: Advanced stats scrapers NEVER run on Railway. stats.nba.com blocks datacenter IPs. Always local, never with a proxy.

## MLB Scrapers (`src/scrapers/mlb/`)

| Module | Source |
|--------|--------|
| `mlb_stats_scraper.py` | MLB Stats API (free, no auth) |
| `mlb_statcast_scraper.py` | Baseball Savant via pybaseball |
| `mlb_fangraphs_scraper.py` | FanGraphs via pybaseball |
| `mlb_player_props_scraper.py` | Odds API (historical backfill) |
| `mlb_daily_player_props_scraper.py` | Odds API (live daily) |
| `mlb_daily_game_lines_scraper.py` | Odds API (live daily) |

## Kalshi Scrapers (`src/scrapers/kalshi/`)

| Module | Source | Schedule |
|--------|--------|----------|
| `kalshi_client.py` | Kalshi Trade API v2 (RSA-PSS auth) | Via refresh job |
| `kalshi_market_scraper.py` | Kalshi API (market discovery, ticker parsing, player linking, game-level markets) | Every 10 min, 11AM-11PM |
| `kalshi_utils.py` | N/A (fee calc, probability conversion, stat mapping, KALSHI_GAME_SERIES) | Utility module |
| `kalshi_discovery.py` | Kalshi /events API (one-time series enumeration) | Run locally as needed |

**Note**: Kalshi API works from any IP (proper API with key auth). Can run on Railway. Private key base64-encoded as env var for Railway deployment.

**IMPORTANT**: Use the `/events` endpoint (not `/markets`) to enumerate series tickers. The `/markets` endpoint does NOT include `series_ticker` in its response. `kalshi_discovery.py` pages through events and classifies each series_ticker into game/prop/future/non-sports categories.

**KALSHI_GAME_SERIES**: Populated in `kalshi_utils.py` with 27 confirmed series (Apr 16, 2026):
- MLB game: KXMLBGAME (moneyline), KXMLBRFI (yrfi), KXMLBTOTAL, KXMLBSPREAD, KXMLBTEAMTOTAL, KXMLBF5, KXMLBF5SPREAD, KXMLBF5TOTAL
- MLB futures: KXMLB (WS), KXMLBAL, KXMLBNL, 6 division winners
- NBA game: KXNBAGAME, KXNBASPREAD, KXNBATOTAL, KXNBATEAMTOTAL, H1/H2 halftime ×6
- NBA playoffs: KXNBASERIES, KXNBASERIESGAMES, KXNBASERIESSCORE, KXNBASERIESSPREAD
- NHL: KXNHLSERIES, KXNHLSERIESGAMES, KXNHLSERIESSCORE, KXNHLSERIESSPREAD, KXNHL

## NCAAB Scrapers (`src/scrapers/ncaab/`)

| Module | Source |
|--------|--------|
| `ncaab_cbbpy_scraper.py` | ESPN via CBBpy |
| `ncaab_barttorvik_scraper.py` | Barttorvik bulk CSV |
| `ncaab_game_lines_scraper.py` | Odds API |

#scrapers #pipeline #data
