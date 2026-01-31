# Query Player Documentation

## Overview
CLI tool for querying stored daily predictions from the `daily_predictions` and `daily_prediction_samples` tables. Supports three query modes: line probability, player overview, and top edges.

## Inputs and Dependencies
- Environment variables: `DATABASE_URL`
- Database tables: `daily_predictions`, `daily_prediction_samples`, `players`
- Module: `src.models.prediction_store.PredictionStore`

## Query Modes

### Mode 1: Line Probability
Given a player, stat, and line, computes the over/under probability from stored MC samples.

```bash
python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5
python src/tools/query_player.py --player "Cade Cunningham" --stat pts --line 25.5 --odds -110
```

Output includes:
- Prediction mean, median, and quantile distribution
- Model P(over) and P(under) from MC samples
- Market line and implied probabilities (if available)
- Over/under edge vs market
- Expected value at given odds (if `--odds` provided)

### Mode 2: Player Overview
Shows all predictions for a player on a date.

```bash
python src/tools/query_player.py --player "Cade Cunningham"
python src/tools/query_player.py --player "Cade Cunningham" --date 2026-01-29
```

### Mode 3: Top Edges
Shows top N predictions by absolute edge for a date.

```bash
python src/tools/query_player.py --top 20
python src/tools/query_player.py --date 2026-01-29 --top 10
```

Output table shows player, stat, mean, line, side, model prob, market prob, edge, and odds.

## Key Logic
- `PredictionStore.get_player_id_by_name()` — fuzzy name lookup via case-insensitive LIKE
- `PredictionStore.get_samples()` — decompresses gzip bytea into numpy array
- Probability: `(samples > line).mean()` for P(over), `(samples <= line).mean()` for P(under)
- EV calculation: `prob * profit - (1 - prob)` where profit = American odds to decimal multiplier

## CLI Arguments
| Argument | Type | Description |
|----------|------|-------------|
| `--player` | str | Player name (partial match supported) |
| `--stat` | str | One of: pts, reb, ast, threes |
| `--line` | float | Prop line to evaluate |
| `--odds` | float | American odds for EV calculation |
| `--date` | str | Prediction date, YYYY-MM-DD (default: today) |
| `--top` | int | Show top N edges for the date |

## Related Documentation
- [Documentation Index](index.md)
- [Feature Store](feature_store_documentation.md)
- [Black-Litterman Blending](black_litterman_documentation.md)
