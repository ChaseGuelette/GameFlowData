# GameFlowData

An NBA analytics and machine learning pipeline that scrapes real-time NBA game and player statistics, stores them in a PostgreSQL database, and prepares feature-engineered datasets for predictive modeling.

## Main Goals

1. **Data Collection** - Continuously scrape live NBA statistics from the official NBA API
2. **Data Aggregation** - Consolidate historical and current-season data into a normalized PostgreSQL database
3. **Feature Engineering** - Create advanced statistical features for machine learning models
4. **Predictive Analytics** - Build ML models to predict NBA game outcomes based on team and player statistics

## Project Structure

```
MachineAlgo/
├── PythonScrapers/              # Production scraping scripts
│   ├── dailyUpdateScript.py     # Master orchestrator for daily updates
│   ├── dailyPlayerStatScraper.py
│   └── dailyTeamStatScraper.py
│
├── JupyterScrapers/             # Exploratory & one-time scripts
│   ├── predictionScripts/       # ML feature engineering
│   ├── oneTimeScripts/          # Historical data collection
│   └── dailyUpdateScripts/      # Notebook versions of scrapers
│
├── SQLScripts/                  # Database utilities
├── WebScrapers/                 # Additional web scraping notebooks
└── Learning/                    # Educational notebooks
```

## Key Components

### Data Pipeline
- **Daily Player Stats Scraper** - Updates individual player game statistics for ~500 active NBA players
- **Daily Team Stats Scraper** - Updates team-level game statistics with configurable lookback periods
- **Rate Limiting** - Built-in delays to respect NBA API limits

### Database (Supabase/PostgreSQL)
- `team_game_stats` - Team box score data
- `player_game_stats` - Individual player statistics
- `team_average_game_stats` - Aggregated team averages
- `player_average_game_stats` - Aggregated player averages

### Feature Engineering

- Team vs. opponent stat differentials
- Top 10 players per team aggregation
- Historical rolling averages
- Multi-step feature selection pipeline (variance filtering, correlation pruning, ANOVA, mutual information)

## Tech Stack

- **Python** - Core language
- **nba_api** - Official NBA stats API wrapper
- **pandas/numpy** - Data manipulation
- **SQLAlchemy/psycopg2** - Database connectivity
- **scikit-learn** - Feature selection and ML
- **Supabase** - PostgreSQL database hosting
- **Jupyter Notebooks** - Exploratory analysis

## Data Coverage

- **Current Season**: 2024-25 (actively updated)
- **Historical Seasons**: 2022-23, 2023-24

## Setup

1. Clone the repository
2. Install dependencies: `pip install nba_api pandas sqlalchemy psycopg2-binary python-dotenv scikit-learn`
3. Create a `.env` file with your `DATABASE_URL`
4. Run `PythonScrapers/dailyUpdateScript.py` for daily updates

<!-- SOLOKIT_SESSION_MANAGEMENT -->

## Session-Driven Development

This project uses [Solokit](https://github.com/anthropics/solokit) for Session-Driven Development with AI assistants.

### Quick Start

```bash
sk start           # Begin a session with context briefing
sk end             # Complete session with quality gates
sk work-new        # Create a new work item
sk work-list       # View all work items
sk status          # Check current session status
```

### Session Commands

| Command | Description |
|---------|-------------|
| `sk start [id]` | Start session with comprehensive briefing |
| `sk end` | Complete session with quality gates |
| `sk status` | View current session status |
| `sk validate` | Run quality checks without ending session |

### Work Item Commands

| Command | Description |
|---------|-------------|
| `sk work-new` | Create new work item interactively |
| `sk work-list` | List all work items |
| `sk work-show <id>` | Show work item details |
| `sk work-update <id>` | Update work item fields |
| `sk work-next` | Get recommended next item |
| `sk work-delete <id>` | Delete a work item |
| `sk work-graph` | Visualize work item dependencies |

### Learning Commands

| Command | Description |
|---------|-------------|
| `sk learn` | Capture a learning |
| `sk learn-show` | Browse captured learnings |
| `sk learn-search <query>` | Search learnings by keyword |
| `sk learn-curate` | Deduplicate and organize learnings |

### Session Files

The `.session/` directory contains:

- **specs/** - Work item specifications
- **briefings/** - Session context briefings
- **history/** - Session summaries
- **tracking/** - Work items and learnings data

---

Adopted with Solokit v0.3.0
