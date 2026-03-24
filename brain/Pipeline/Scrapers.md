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

## NCAAB Scrapers (`src/scrapers/ncaab/`)

| Module | Source |
|--------|--------|
| `ncaab_cbbpy_scraper.py` | ESPN via CBBpy |
| `ncaab_barttorvik_scraper.py` | Barttorvik bulk CSV |
| `ncaab_game_lines_scraper.py` | Odds API |

#scrapers #pipeline #data
