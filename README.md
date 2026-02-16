# GameFlowData

An NBA player prop betting analytics platform that combines machine learning predictions with real-time sportsbook odds to identify profitable betting opportunities.

## Overview

GameFlowData is a comprehensive data pipeline and prediction system for NBA player props. It scrapes game statistics and betting lines, trains quantile regression models with Monte Carlo simulation, and surfaces high-edge betting opportunities through a web dashboard and Discord bot.

## Features

- **Data Pipeline**: Automated scraping of NBA stats, player props, and injury reports
- **ML Predictions**: Quantile regression + Monte Carlo simulation for probability distributions
- **Edge Detection**: Compares model probabilities against sportsbook odds to find value
- **Paper Trading**: Simulated betting with Kelly criterion sizing and P&L tracking
- **Web Dashboard**: Next.js app for viewing predictions, analyzing players, and tracking performance
- **Discord Bot**: Slash commands for daily picks and automated alerts
- **Cloud Deployment**: Railway (cron jobs) + Vercel (dashboard) + Supabase (database)

## Technology Stack

| Layer | Technologies |
|-------|-------------|
| **Backend** | Python 3.11+, SQLAlchemy, Pandas, NumPy |
| **ML** | XGBoost, Scikit-Learn, Optuna |
| **Database** | PostgreSQL (Supabase) |
| **Dashboard** | Next.js 16, TypeScript, Tailwind CSS, Recharts |
| **Bot** | Discord.py 2.6+ |
| **Deployment** | Railway (jobs), Vercel (dashboard) |
| **Testing** | Pytest (575 tests, 60% coverage target) |

## Project Structure

```
GameFlowData/
├── src/
│   ├── scrapers/           # Data collection (NBA API, Odds API, ESPN)
│   ├── processing/         # Data linking, rolling averages, feature engineering
│   ├── models/             # ML pipeline: training, inference, storage
│   ├── backtesting/        # Historical replay and bet simulation
│   ├── paper_trading/      # Paper bet placement and resolution
│   ├── orchestration/      # Daily job scripts and scheduler
│   ├── discord_bot/        # Discord bot with slash commands
│   └── db/                 # Database client
├── dashboard/              # Next.js web application
├── tests/                  # Unit and integration tests
├── docs/                   # Component documentation
├── notebooks/              # Research notebooks
└── database/               # SQL schema definitions
```

## Quick Start

### 1. Clone and Install

```bash
git clone <repository-url>
cd GameFlowData
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file:

```env
DATABASE_URL=postgresql://user:password@host:port/database
ODDS_API_KEY=your-odds-api-key
RAPIDAPI_KEY=your-rapidapi-key
DISCORD_BOT_TOKEN=your-discord-bot-token
```

### 3. Run Daily Pipeline

```bash
# Scrape stats (after games complete)
python src/orchestration/daily_stats_job.py

# Scrape props and injuries (multiple times daily)
python src/orchestration/lines_job.py

# Generate predictions (before games start)
python src/orchestration/inference_job.py
```

### 4. Start Dashboard

```bash
cd dashboard
npm install
npm run dev
# Open http://localhost:3000
```

## Deployment

### Railway (Cron Jobs)

The scheduler runs all jobs automatically:

| Job | Schedule (ET) | Purpose |
|-----|---------------|---------|
| daily_stats_job | 9:00 AM | Scrape NBA game results |
| lines_job | 12 PM, 4 PM, 6 PM | Scrape props and injuries |
| inference_job | 6:30 PM | Generate predictions |

See `docs/railway_deployment.md` for setup guide.

### Vercel (Dashboard)

Dashboard deploys automatically from `/dashboard` directory.

Live at: `game-flow-data.vercel.app`

## Usage

### Query Predictions

```bash
# Player probability at a line
python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5

# Top edges for today
python src/tools/query_player.py --top 20
```

### Paper Trading

```bash
# Place bets from predictions
python src/paper_trading/place_bets.py --date 2026-02-15

# Resolve bets after games complete
python src/paper_trading/resolve_bets.py --date 2026-02-15
```

### Backtesting

```bash
# Run historical backtest
python src/backtesting/run_backtest.py --start 2026-01-01 --end 2026-01-31

# Parameter sweep
python src/backtesting/run_sweep.py --start 2026-01-01 --end 2026-01-31 \
    --tau none 0.05 0.10 --edge 0.05 0.07 --kelly 0.10 0.125
```

## Testing

```bash
pytest                          # Run all tests
pytest tests/test_paper_trader.py -v  # Run specific test file
pytest --cov=src --cov-report=html    # With coverage report
```

## Documentation

- `ARCHITECTURE.md` — System design and data flows
- `ACTIONITEMS.md` — Roadmap and session summaries
- `CHANGELOG.md` — Version history
- `docs/` — Component-level documentation

## Key Files

| File | Purpose |
|------|---------|
| `src/models/daily_runner.py` | Daily prediction pipeline |
| `src/models/monte_carlo.py` | Monte Carlo simulation engine |
| `src/paper_trading/paper_trader.py` | Bet selection and Kelly sizing |
| `src/orchestration/scheduler.py` | APScheduler-based job runner |
| `dashboard/src/app/page.tsx` | Main predictions dashboard |

---

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
| `sk work-show <id>`| Show work item details |
| `sk work-update <id>`| Update work item fields |
| `sk work-next` | Get recommended next item |
| `sk work-delete <id>`| Delete a work item |
| `sk work-graph` | Visualize work item dependencies |

### Learning Commands

| Command | Description |
|---------|-------------|
| `sk learn` | Capture a learning |
| `sk learn-show` | Browse captured learnings |
| `sk learn-search <query>`| Search learnings by keyword |
| `sk learn-curate` | Deduplicate and organize learnings |

### Session Files

The `.session/` directory contains:

- **specs/** - Work item specifications
- **briefings/** - Session context briefings
- **history/** - Session summaries
- **tracking/** - Work items and learnings data

---

Adopted with Solokit v0.3.0
