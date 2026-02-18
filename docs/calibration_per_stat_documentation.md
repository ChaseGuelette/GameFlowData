# Per-Stat Calibration Diagnostic Documentation

## Overview

The `calibration_per_stat.py` script produces a per-stat (PTS, REB, AST) calibration report to diagnose where the model is miscalibrated. It computes quantile coverage, bias, interval sharpness, probability calibration (ECE/Brier), and auto-diagnoses issues exceeding configurable tolerances.

**Location:** `src/diagnostics/calibration_per_stat.py`
**Track:** C2 (Calibration Refinement)

## Motivation

Backtesting showed stats perform very differently (REB +7.9% ROI, AST marginal/negative), but the model treats PTS, REB, and AST identically during calibration — same conformal offsets, same variance inflation, same tolerances. This tool provides per-stat visibility to answer "where exactly is a stat miscalibrated?" before deciding on per-stat fixes.

## Usage

```bash
# From backtest CSV (primary use case)
python -m src.diagnostics.calibration_per_stat --csv backtest_results/predictions.csv

# From production DB
python -m src.diagnostics.calibration_per_stat --db --start 2025-01-01 --end 2025-02-15

# With JSON export
python -m src.diagnostics.calibration_per_stat --csv predictions.csv --output report.json

# Custom tolerance
python -m src.diagnostics.calibration_per_stat --csv predictions.csv --tolerance 0.05

# Filter to specific stats
python -m src.diagnostics.calibration_per_stat --csv predictions.csv --stats pts reb
```

## CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--csv PATH` | Path to backtest predictions CSV | (required if not `--db`) |
| `--db` | Load from production DB | (required if not `--csv`) |
| `--start DATE` | Start date for DB mode (YYYY-MM-DD) | (required with `--db`) |
| `--end DATE` | End date for DB mode (YYYY-MM-DD) | (required with `--db`) |
| `--output PATH` | Export structured JSON report | None |
| `--tolerance FLOAT` | Quantile coverage gap tolerance for diagnosis flags | 0.03 |
| `--stats STAT [STAT ...]` | Stats to analyze | pts reb ast |

## Data Sources

### CSV Path (Primary)

Reads a backtest `predictions.csv` file produced by `run_backtest.py`. Expected columns:

- `stat` — stat type (pts, reb, ast)
- `actual` — actual outcome value
- `pred_q10`, `pred_q25`, `pred_q50`, `pred_q75`, `pred_q90` — quantile predictions
- `over_prob` — model's over probability (optional, for Brier/ECE)
- `line` — prop line (optional, for Brier/ECE)
- `game_date` or `prediction_date` — date column (optional, for date range display)

### DB Path

Joins `daily_predictions` + `player_game_stats` tables to get the same columns from stored production predictions. Requires `--start` and `--end` dates.

## Metrics Computed

All metrics are computed per stat AND globally.

### Quantile Coverage (Q10–Q90)

For each quantile q, computes P(actual <= pred_q). Ideal: coverage = q.

- **Mean absolute gap**: Average |coverage - q| across all quantiles
- **Worst quantile**: Quantile with largest absolute gap
- Flags stats where gap exceeds `--tolerance`

### Bias

Uses pred_q50 (median) as point prediction.

- **Mean predicted** vs **mean actual**
- **Absolute bias**: mean_pred - mean_actual
- **Relative bias %**: bias / mean_actual * 100
- Flags stats where |relative bias| > 5%

### Interval Sharpness

- **80% width**: mean(pred_q90 - pred_q10)
- **50% width**: mean(pred_q75 - pred_q25)

Wider intervals = less precise predictions. Compare across stats to see which are more/less certain.

### Brier Score

`mean((over_prob - hit)^2)` where hit = 1 if actual > line, else 0.

Requires `over_prob` and `line` columns. Skipped gracefully if missing.

### Expected Calibration Error (ECE)

Bins predictions into 10 probability buckets, computes weighted average of |predicted_prob - actual_rate| per bin.

Requires `over_prob` and `line` columns. Skipped gracefully if missing.

### Reliability Curve Data

Per-bin data: (bin_start, bin_end, predicted_prob, actual_rate, count). Available in JSON export for plotting.

## Console Output

```
============================================================
PER-STAT CALIBRATION DIAGNOSTIC REPORT
============================================================
Data source: predictions.csv | 12,847 predictions
Date range: 2024-10-22 to 2025-01-15
PTS: 4,291 | REB: 4,278 | AST: 4,278
============================================================

--- QUANTILE COVERAGE ---
         Q10    Q25    Q50    Q75    Q90    Mean|Gap|  Worst
PTS    0.118  0.263  0.512  0.761  0.908   0.012     Q10 +0.018
REB    0.142  0.298  0.543  0.779  0.921   0.036     Q50 +0.043 ⚠
AST    0.095  0.244  0.498  0.748  0.892   0.006     Q10 -0.005
GLOBAL 0.118  0.268  0.518  0.763  0.907   0.018     Q50 +0.018

--- BIAS ---
       Mean Pred  Mean Actual  Bias   Rel Bias
PTS      22.4       21.8      +0.6    +2.8%
REB       7.1        6.8      +0.3    +4.4% ⚠
AST       4.9        4.8      +0.1    +2.1%

--- DIAGNOSIS ---
• REB: Q50 over-coverage (+0.043) — model over-predicts rebounds
• REB: Relative bias +4.4% — systematic upward shift
```

## JSON Export Structure

```json
{
  "data_source": "predictions.csv",
  "n_predictions": 900,
  "date_range": {"start": "2025-01-15", "end": "2025-01-15"},
  "stat_counts": {"pts": 300, "reb": 300, "ast": 300},
  "tolerance": 0.03,
  "quantile_coverage": {
    "pts": {"coverages": {...}, "gaps": {...}, "mean_abs_gap": 0.012, ...},
    "reb": {...},
    "ast": {...},
    "GLOBAL": {...}
  },
  "bias": {...},
  "sharpness": {...},
  "brier": {...},
  "ece": {...},
  "diagnoses": ["REB: Q50 over-coverage (+0.043) — model over-predicts reb"]
}
```

## Auto-Diagnosis Thresholds

| Metric | Threshold | Flag |
|--------|-----------|------|
| Quantile coverage gap | `--tolerance` (default 0.03) | Per-stat mean absolute gap |
| Relative bias | 5% | Systematic over/under-prediction |
| ECE | 0.05 | Probability calibration |

## Dependencies

- `numpy`, `pandas` — data manipulation
- `src.db.client` — DB connection (only for `--db` mode)
- `sqlalchemy` — SQL queries (only for `--db` mode)

## Related Files

- `src/models/analyze_calibration_drift.py` — Minutes-rate correlation analysis (different purpose)
- `src/backtesting/performance_metrics.py` — Backtest-level calibration metrics
- `src/models/calibration.py` — Training-time calibration evaluation
