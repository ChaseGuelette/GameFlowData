# Calibration Decisions

> Part of [[Decisions]]

## The Core Insight
The model's apparent "miscalibration" on Q10 is actually the betting edge. Sportsbooks set lines where the public bets (overs), creating systematic inflation. Our model under-predicts relative to the market — and that's where the money is.

## Decision: NEVER Deploy Global Offsets

### Evidence
| Test | Result |
|------|--------|
| Session 42 A/B | Offsets improved calibration, degraded ROI |
| run_20260218 retrain | Significantly hurt model performance |
| Mar 17 A/B backtest | ROI dropped 1.4-12.8pp across configs |
| Session 78 | 4th confirmation |

### Academic Support
- Hubacek 2022: Market inefficiencies in sports betting
- Dmochowski 2023: Systematic biases in prop markets

### What This Means
- Better calibration numbers != better edges
- Correcting Q10 under-coverage removes the under-betting edge
- The `--calibrate-only` mode exists but should almost never be used
- Future experiments: targeted single-stat, single-quantile only (never global)

## Decision: Full Retrains Are Risky
- run_20260218 was a full retrain that significantly hurt performance
- Always lock hyperparams from the current production model
- Validate everything with backtests before promoting

#calibration #decisions #critical
