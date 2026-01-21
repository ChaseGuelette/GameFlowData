# GameFlowData

An NBA analytics and machine learning pipeline that scrapes real-time NBA game and player statistics, stores them in a PostgreSQL database, and prepares feature-engineered datasets for predictive modeling.

## Overview

GameFlowData is a comprehensive Python-based data pipeline and analysis project focused on NBA (National Basketball Association) data. It scrapes, processes, stores, and analyzes a wide range of information, including game statistics, player data, betting lines, and injury reports. The project also includes components for developing predictive models and serves data via a FastAPI backend.

## Features

- **Data Scraping**: Collects data from various sources:
    - ESPN for injury reports.
    - NBA APIs for player positions, stats, and game details (`nba_unified_scraper`).
    - Game and betting lines.
- **Data Processing**: Cleans, links, and backfills data to create a robust and consistent dataset. This includes handling team IDs, player names, and opponent-adjusted stats.
- **Database Management**: Uses a SQL database with Alembic for schema migrations to store the collected data.
- **Predictive Modeling**: Includes notebooks and scripts for developing machine learning models (e.g., Monte Carlo simulations, quantile trainers).
- **API**: A FastAPI application to serve the collected and processed data.

## Technology Stack

- **Backend**: Python, FastAPI
- **Data Manipulation**: Pandas, NumPy
- **Database**: SQLAlchemy, SQLModel, Alembic, PostgreSQL
- **Testing**: Pytest, pytest-asyncio
- **Linting & Formatting**: Ruff
- **Development**: Jupyter Notebooks

## Project Structure

```
├───src/                # Core source code
│   ├───scrapers/       # Data collection scripts
│   ├───processing/     # Data cleaning and transformation scripts
│   ├───models/         # Predictive modeling components
│   └───db/             # Database client and interaction logic
├───tests/              # Unit and integration tests
├───notebooks/          # Jupyter notebooks for exploration and research
├───database/           # SQL schema definitions
├───alembic.ini         # Alembic migration configuration
├───pyproject.toml      # Project metadata and dependencies
└───requirements.txt    # Production dependencies
```

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd GameFlowData
    ```
2.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    pip install -r requirements-dev.txt # For development and running tests
    ```
4.  **Setup Environment Variables:**
    Create a `.env` file in the root directory and add your `DATABASE_URL`.
    ```
    DATABASE_URL="postgresql://user:password@host:port/database"
    ```
5.  **Setup the database:**
    - Ensure you have a running PostgreSQL instance.
    - Configure the database connection in your `.env` file.
    - Run migrations:
        ```bash
        alembic upgrade head
        ```

## Usage

### Running the API Server

To start the FastAPI application, use uvicorn:
```bash
uvicorn src.main:app --reload
```

### Running Tests

To run the test suite, use pytest:
```bash
pytest
```

### Running Scrapers

Scrapers can be run as individual scripts. For example:
```bash
python -m src.scrapers.espn_injury_scraper
```

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