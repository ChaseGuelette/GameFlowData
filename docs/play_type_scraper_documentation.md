# Play Type Scraper Documentation

## Overview

`src/scrapers/play_type_scraper.py` fetches team-level offensive and defensive play type data from the NBA API's SynergyPlayTypes endpoint. This data powers the "Play Types" tab on the Data Vault page.

## Data Source

**Endpoint:** `nba_api.stats.endpoints.synergyplaytypes.SynergyPlayTypes`

The Synergy Sports API provides play-by-play classification of every NBA possession into 11 play types, tracking both frequency (what % of possessions) and efficiency (points per possession) for each team.

## Play Types

| API Key | Description |
|---------|-------------|
| Isolation | One-on-one play |
| Transition | Fast break / early offense |
| PRBallHandler | Pick & roll ball handler |
| PRRollman | Pick & roll roll man |
| Postup | Post-up play |
| Spotup | Spot-up / catch-and-shoot |
| Handoff | Dribble handoff |
| Cut | Basket cut |
| OffScreen | Coming off screens |
| OffRebound | Putback after offensive rebound |
| Misc | Miscellaneous / uncategorized |

## API Calls

Each scrape makes 22 API calls: 11 play types x 2 groupings (Offensive + Defensive).

- Rate limited: 0.6–1.2s random delay between calls
- Retry logic: up to 3 attempts per call with exponential backoff
- Ban cooldown: 5 minutes if rate-limited
- Total runtime: ~18–25 seconds

## Database Table

**Table:** `team_play_types`

| Column | Type | Description |
|--------|------|-------------|
| `season_id` | text | Season identifier (e.g., "2025-26") |
| `team_id` | bigint | NBA team ID |
| `team_abbreviation` | text | 3-letter team code |
| `team_name` | text | Full team name |
| `play_type` | text | Play type key (e.g., "Isolation") |
| `type_grouping` | text | "Offensive" or "Defensive" |
| `percentile` | numeric | Team's percentile rank for this play type |
| `gp` | integer | Games played |
| `poss_pct` | numeric | % of possessions using this play type (decimal, e.g., 0.059) |
| `ppp` | numeric | Points per possession |
| `fg_pct` | numeric | Field goal percentage |
| `ft_poss_pct` | numeric | Free throw possession percentage |
| `tov_poss_pct` | numeric | Turnover possession percentage |
| `sf_poss_pct` | numeric | Shooting foul possession percentage |
| `plusone_poss_pct` | numeric | And-one possession percentage |
| `score_poss_pct` | numeric | Scoring possession percentage |
| `efg_pct` | numeric | Effective field goal percentage |
| `poss` | numeric | Total possessions |
| `pts` | numeric | Total points |
| `fgm` | numeric | Field goals made |
| `fga` | numeric | Field goals attempted |
| `fgmx` | numeric | Field goals missed |
| `scraped_at` | timestamptz | Timestamp of scrape |

**Primary Key:** `(season_id, team_id, play_type, type_grouping)`
**RLS:** Public read access
**Index:** `idx_team_play_types_season` on `season_id`

Expected row count: 660 per season (30 teams x 11 play types x 2 groupings).

## Refresh Strategy

Full refresh per season: DELETE all rows for the season, then INSERT fresh data. This ensures data consistency since play type stats are cumulative season totals that change every game.

## Usage

```bash
# Scrape current season (default: 2025-26)
python src/scrapers/play_type_scraper.py

# Scrape specific season
python src/scrapers/play_type_scraper.py --season 2024-25
```

## Pipeline Integration

Runs as Step 8 in `daily_stats_job.py`, after opponent-allowed stats and before bet resolution. This ensures play type data is refreshed daily with the latest game results.

## Dependencies

- `nba_api` — NBA Stats API wrapper
- `pandas` — Data manipulation
- `sqlalchemy` — Database connection
- `python-dotenv` — Environment variable loading
