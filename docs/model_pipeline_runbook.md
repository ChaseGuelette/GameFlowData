# Model pipeline runbook

This runbook covers the retained NBA model workflow. MLB work uses the YAML lifecycle documented in [`development_docs/mlb_model_lifecycle_usage_guide.md`](development_docs/mlb_model_lifecycle_usage_guide.md).

Training, sweeps, broad backfills, artifact promotion, deployment, and production DB changes are human-gated. Chase launches long jobs after reviewing a dry-run or preflight.

## Production artifacts

- NBA regular season: `src/models/artifacts/production/`
- NBA playoffs: `src/models/artifacts/production_playoffs/`
- MLB: `src/models/mlb/artifacts/production/`

Generated `run_*` directories and backtest outputs are local/archived evidence and are not committed. Directory names are not promotion evidence.

## Non-negotiable modeling rules

- Probabilities from samples use `(samples > line).mean()`; never substitute a Gaussian CDF.
- Never deploy global conformal recalibration offsets. Q10 behavior is edge-bearing and must not be blindly corrected.
- Preserve temporal integrity: training and inference features use only information available before the target game/decision time.
- Lock production hyperparameters for controlled retrains unless tuning is the explicit experiment.
- Evaluate with flat stakes first; Kelly remains a separate optional paper-only certification lane.
- Backtest artifacts must identify the exact model, quote source, decision-time policy, and evaluation window.

## Environment

Use the project virtual environment and a local database mirror for training/backtesting where supported. Do not print connection strings.

```powershell
Set-Location 'C:\Users\Chase\Projects\GameFlowData'
.\venv\Scripts\python.exe --version
```

## Train an NBA candidate

Entrypoint: `src/models/train_pipeline.py`.

Before launching, inspect the current CLI and production metadata. A typical locked-hyperparameter candidate command is:

```powershell
.\venv\Scripts\python.exe src\models\train_pipeline.py --train-seasons 22022 22023 --cal-season 22024 --hyperparams-path src\models\artifacts\production\best_hyperparams.json
```

Use current seasons/cutoffs appropriate to the intended evaluation window. Training writes an incomplete directory first and finalizes it only after all required artifacts are saved. Never promote an `_incomplete` run.

Do not run calibrate-only/global-offset workflows. A calibration report is diagnostic evidence, not authorization to shift production distributions.

## Backtest an exact candidate

Entrypoint: `src/backtesting/run_backtest.py`.

```powershell
.\venv\Scripts\python.exe src\backtesting\run_backtest.py --model-dir src\models\artifacts\run_YYYYMMDD_HHMMSS --start YYYY-MM-DD --end YYYY-MM-DD
```

Use a held-out window strictly after training/calibration data. Certify flat-stake ROI, Sharpe, drawdown, volume, dropout, and timing integrity before considering paper deployment. Old backtest outputs are not trustworthy merely because they exist; verify the current harness and quote-clean path.

## Daily inference

Production entrypoint: `src/orchestration/inference_job.py`. The persistent schedule is owned by `src/orchestration/scheduler.py`.

Useful manual shapes:

```powershell
.\venv\Scripts\python.exe src\orchestration\inference_job.py --dry-run
.\venv\Scripts\python.exe src\orchestration\inference_job.py --model-dir src\models\artifacts\production --skip-discord --skip-bets
```

A dry run may still require database reads. Confirm the command's current CLI before execution. Never run a second scheduler process just to trigger inference.

## Promotion gate

Promotion is a separate approved change. Before touching production artifact directories:

1. identify the exact candidate and immutable evidence;
2. verify artifact completeness and forbidden-file absence;
3. confirm point-in-time backtest integrity;
4. review flat-stake performance and risk metrics;
5. verify current production consumers load the candidate;
6. preserve rollback identity;
7. review the scoped artifact diff;
8. only then authorize commit/deploy separately.

For MLB artifact functionality, run:

```powershell
.\venv\Scripts\python.exe scripts\audit_mlb_model_artifacts.py --model-dir src\models\mlb\artifacts\production --json
```

That audit does not replace quote-clean CLV, timing, dropout, or paper evidence.

## Repository verification

```powershell
.\venv\Scripts\python.exe -m compileall -q src
.\venv\Scripts\python.exe -m pytest
```

Dashboard verification is separate and runs from `dashboard\` with `npm run lint` and `npm run build`.

## Canonical context

Use remote GBrain before recommending architecture, feature-family, calibration, promotion, or betting-policy changes. Read `operations/hard-facts`, `operations/critical-invariants`, relevant atomic lessons, canonical model decisions, and only then recent handoffs.
