# Database Health Check Documentation

## Overview

The `db_health_check.py` script provides comprehensive validation of data integrity, freshness, and linkage across all tables in the GameFlowData system. It helps identify issues before they impact the ML pipeline or predictions.

**Location:** `src/diagnostics/db_health_check.py`

## Usage

```bash
# Basic run (last 7 days)
python src/diagnostics/db_health_check.py

# Extended check period
python src/diagnostics/db_health_check.py --days 14

# Detailed output
python src/diagnostics/db_health_check.py --verbose

# JSON output for automation
python src/diagnostics/db_health_check.py --json
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | Warnings present |
| 2 | Critical errors found |

## Health Check Categories

### 1. Data Freshness

Checks latest dates for key tables:
- `player_game_stats` — Most recent game data
- `raw_player_props_combined` — Latest prop snapshots
- `rapidapi_injuries` — Most recent injury report
- `daily_predictions` — Latest prediction date

**Alert:** Warns if any table is >1 day stale on NBA game days.

### 2. Game Data Completeness

Validates game data integrity:
- Games per date in recent period
- Player count per game (expected ~26 per game)
- Matching team_game_stats entries

**Alert:** Warns if game has <20 players or missing team stats.

### 3. Prop Linking Health

Monitors the linker pipeline effectiveness:
- Props with NULL `game_id` (unlinked to games)
- Props with NULL `player_id` (unlinked to players)
- Props with NULL `team_id` but valid `game_id`
- Breakdown by date range (recent vs historical)

**Alert:** Warns if >10% of recent props are unlinked.

### 4. Aggregation Sync

Ensures rolling average tables are up to date:
- Games in `player_game_stats` missing from `player_average_game_stats`
- Identifies gaps in feature computation

**Alert:** Warns if any recent games (last 3 days) are missing aggregations.

### 5. Injury Linking

Validates injury data pipeline:
- Injuries with NULL `player_id`
- Recent injuries (last 7 days) unlinked percentage

**Alert:** Warns if >20% of recent injuries are unlinked.

### 6. Position History Coverage

Checks player position tracking:
- Players with games in last 7 days missing position snapshots

**Alert:** Warns if active players lack position data.

### 7. Prediction Coverage

Monitors prediction pipeline:
- Game dates with games but no predictions
- Predictions without corresponding game stats (orphaned)

**Alert:** Warns if recent games have no predictions.

### 8. Foreign Key Integrity (Soft)

Validates referential integrity:
- `player_game_stats.player_id` → `players.player_id`
- `player_game_stats.team_id` → `teams.team_id`
- Props with `player_id` not in players table

**Alert:** Warns if orphaned references found.

## Output Format

### Standard Output

```
============================================================
DATABASE HEALTH CHECK - 2026-02-13 18:30:00
============================================================

[1/8] DATA FRESHNESS
  ✓ player_game_stats: 2026-02-12 (1 day ago)
  ✓ raw_player_props_combined: 2026-02-13 (today)
  ✓ rapidapi_injuries: 2026-02-13 (today)
  ⚠️ daily_predictions: 2026-02-11 (2 days ago) — STALE

[2/8] GAME DATA COMPLETENESS
  Last 7 days:
    2026-02-12: 3 games, 79 players ✓
    2026-02-11: 14 games, 362 players ✓
    ...

... etc

============================================================
SUMMARY
============================================================
✓ Passed: 5
⚠️ Warnings: 3
✗ Failed: 0

Issues found:
  ⚠️ data_freshness: daily_predictions is 2 days stale
  ⚠️ aggregation_sync: 598 missing entries
  ⚠️ prediction_coverage: Feb 6-9 and Feb 12 have no predictions
```

### JSON Output

```json
{
  "timestamp": "2026-02-13T18:30:00",
  "days_checked": 7,
  "results": [
    {
      "check": "data_freshness",
      "status": "warning",
      "message": "daily_predictions is 2 days stale",
      "details": {
        "player_game_stats": "2026-02-12",
        "daily_predictions": "2026-02-11"
      }
    }
  ],
  "summary": {
    "passed": 5,
    "warnings": 3,
    "failed": 0
  }
}
```

## Integration

### Manual Runs

Run after daily stats job to verify data integrity:

```bash
python src/orchestration/daily_stats_job.py
python src/diagnostics/db_health_check.py
```

### Automation

Add to cron for monitoring:

```bash
# Daily health check at 10 AM
0 10 * * * cd /path/to/GameFlowData && python src/diagnostics/db_health_check.py --json > logs/health_check.json
```

### CI/CD Integration

Use exit codes for pipeline control:

```bash
python src/diagnostics/db_health_check.py
if [ $? -eq 2 ]; then
    echo "Critical errors found, stopping pipeline"
    exit 1
fi
```

## Troubleshooting

### Common Issues

#### Stale Predictions

**Cause:** Inference job didn't run or failed.

**Fix:** Run inference manually:
```bash
python src/orchestration/inference_job.py
```

#### Missing Aggregations

**Cause:** `populate_average_stats_incremental.py` didn't process all players.

**Fix:** Run full aggregation:
```bash
python src/processing/populate_average_stats.py --table player
```

#### High Unlinked Prop Rate

**Cause:** New props from future games not yet in database.

**Fix:** This is expected for props 1+ days in future. Check historical rate specifically.

#### Orphaned Predictions

**Cause:** Predictions generated for games that were cancelled or postponed.

**Fix:** Review and clean up orphaned records if needed.

## Architecture

```
src/diagnostics/
├── __init__.py           # Package init
└── db_health_check.py    # Main health check script
    ├── CheckResult       # Dataclass for results
    ├── DatabaseHealthChecker  # Main class
    │   ├── _check_data_freshness()
    │   ├── _check_game_data_completeness()
    │   ├── _check_prop_linking()
    │   ├── _check_aggregation_sync()
    │   ├── _check_injury_linking()
    │   ├── _check_position_history()
    │   ├── _check_prediction_coverage()
    │   └── _check_foreign_keys()
    └── main()            # CLI entry point
```

## Dependencies

- `sqlalchemy` — Database queries
- `argparse` — CLI argument parsing
- `json` — JSON output formatting
- `src.db.client` — Database connection

## Related Documentation

- [Feature Store Documentation](feature_store_documentation.md)
- [Daily Pipeline Automation](daily_pipeline_automation.md)
- [Model Pipeline Runbook](model_pipeline_runbook.md)
