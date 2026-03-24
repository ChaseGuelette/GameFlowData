# Calibration Guide

> Part of [[Models]]

## Philosophy
Better calibration numbers do NOT equal better edges. The model's Q10 "miscalibration" IS the under-betting edge — sportsbooks inflate lines due to public over-bias. Correcting the calibration removes the edge. This has been confirmed in 4 separate A/B backtests.

Academic support: Hubacek 2022, Dmochowski 2023.

## NEVER Deploy Global Offsets
This is the single most important rule in the entire system. Global conformal recalibration offsets have been tested 4 times and hurt ROI every time:
- Session 42: First A/B test — offsets improved calibration, degraded ROI
- run_20260218 retrain: Significantly hurt model performance
- Mar 17 A/B backtest: ROI dropped 1.4-12.8pp across configs
- Session 78: 4th confirmation

## Recalibration Triggers (Session 75, tightened)
| Trigger | Threshold |
|---------|-----------|
| ROI drops below | 8% over 14-day window |
| Any stat ECE exceeds | 0.06 |
| Model age exceeds | 3 weeks |

### Code Thresholds
| Metric | Threshold |
|--------|-----------|
| Quantile gap | 3% (was 5%) |
| ECE | 0.03 (was 0.05) |
| Edge gap | 8pp (was 10pp) |
| Bias | 4% (was 5%) |

## Structural "Issues" (Not Bugs)
- **AST Q10 combined gap (+7-10%)**: ~18% of games have 0 assists, creating a structural floor on Q10 coverage. Not fixable. Minimal betting impact.
- **PTS systematic under-prediction**: INTENTIONAL. This is where the edge lives.

## If Recalibration Is Needed
1. Run diagnostics: `python -m src.diagnostics.calibration_per_stat --db --start <date> --end <date>`
2. ALWAYS validate with backtests before deploying anything
3. Future experiments: targeted single-stat, single-quantile adjustments ONLY (never global)
4. Lock hyperparams from production when retraining
5. Full retrains are RISKY — run_20260218 significantly hurt performance

## Tools
- `src/diagnostics/calibration_per_stat.py` — Per-stat calibration diagnostic
- `src/backtesting/run_sweep.py` — Parameter sweep for validation
- `src/tools/compare_models.py` — Side-by-side model comparison

#calibration #model #critical
