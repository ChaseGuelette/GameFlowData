# Data Sources

> Part of [[Pipeline]]

## NBA
| Source | Auth | Data | Notes |
|--------|------|------|-------|
| NBA CDN | None | Boxscores, game results | Primary daily source on Railway |
| nba_api (ScoreboardV2) | None | Game discovery, schedules | Used for game time lookup |
| stats.nba.com | None | Advanced stats, play types | LOCAL ONLY — blocks datacenter IPs |
| The Odds API | API key | Props (us/us2/us_ex/us_dfs regions), game lines | `ODDS_API_KEY` env var |
| RapidAPI | API key | Injuries (88K+ rows, 2021-present) | `RAPIDAPI_KEY` env var |

## MLB
| Source | Auth | Data |
|--------|------|------|
| MLB Stats API (statsapi.mlb.com) | None | Schedules, boxscores, player reference |
| pybaseball (Baseball Savant) | None | Statcast pitch-level data (exit velo, barrel%, xBA, xwOBA) |
| pybaseball (FanGraphs) | None | Season-level advanced stats (wRC+, FIP, WAR) |
| The Odds API | API key | Props + game lines (sport key: `baseball_mlb`) |

## NCAAB
| Source | Auth | Data |
|--------|------|------|
| CBBpy (ESPN wrapper) | None | D1 box scores, schedules |
| Barttorvik (bulk CSV) | None | Adjusted efficiency ratings (AdjOE, AdjDE, Barthag) |
| The Odds API | API key | Game lines only (sport key: `basketball_ncaab`) — no player props (regulatory) |

## API Keys Required
- `ODDS_API_KEY` — The Odds API
- `RAPIDAPI_KEY` — RapidAPI (NBA injuries)
- `ANTHROPIC_API_KEY` — Claude Haiku (AI Q&A on dashboard)
- `DISCORD_CHANNEL_ALERTS` — Discord webhook URL

#data-sources #pipeline #api
