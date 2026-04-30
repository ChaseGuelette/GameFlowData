> Part of [[Handoffs]]

**Date**: April 30, 2026 at 10:00 AM

## Summary

Short session investigating copula backtest readiness. A truncated Glob search initially led to the incorrect conclusion that April 28 training runs were missing — they are present and complete. Both the copula run (`mlb_run_20260428_164726`) and single-model run (`mlb_run_20260428_163823`) exist with all artifacts. Bash was non-functional throughout so no sweeps were executed. No code or brain files were modified.

## What Was Done

- Confirmed copula model artifacts ARE present at `src/models/mlb/artifacts/mlb_run_20260428_164726/`:
  - `ip_model/pitcher_k_model.joblib` ✓
  - `krate_model/pitcher_k_model.joblib` ✓
  - `pitcher_k_copula_params.json` ✓
  - `ip_feature_manifest.json` / `krate_feature_manifest.json` ✓
- Confirmed single-model run at `src/models/mlb/artifacts/mlb_run_20260428_163823/`
- Confirmed `production/` still has the old single model only — copula not yet promoted
- Bash tool was non-functional for the entire session — no sweeps were run
- Backtest is unblocked — no retraining needed

## Decisions Made

- No decisions made; session was diagnostic with Bash blocked.

## Blockers and Open Questions

- **Bash non-functional** — required a session restart to run sweep commands
- **Copula not yet promoted to production** — still using `pitcher_k_model.joblib`. Needs backtest comparison first.
- **Star-hitter filter threshold** — real kill zone is yes_price 65-71 (not ≥72). Filter still needs adjustment.

## Recommended Next Steps

1. **Run copula backtest sweep** (ready now — no retraining needed):
   ```bash
   python src/backtesting/mlb/run_mlb_sweep.py \
     --local --stats pitcher_strikeouts \
     --model-dir src/models/mlb/artifacts/mlb_run_20260428_164726 \
     --start 2025-04-01 --end 2025-09-28 \
     --tau 0.75 none --edge 0.10 0.12 0.14 \
     --z-max 0.25 0.50 --max-weight 0.8 --direction both
   ```
2. **Run single-model backtest for comparison** (can run in parallel):
   ```bash
   python src/backtesting/mlb/run_mlb_sweep.py \
     --local --stats pitcher_strikeouts \
     --model-dir src/models/mlb/artifacts/mlb_run_20260428_163823 \
     --start 2025-04-01 --end 2025-09-28 \
     --tau 0.75 none --edge 0.10 0.12 0.14 \
     --z-max 0.25 0.50 --max-weight 0.8 --direction both
   ```
3. **If copula wins**: copy `ip_model/`, `krate_model/`, `pitcher_k_copula_params.json`, feature manifests → `production/`
4. **If single model wins**: keep `production/pitcher_k_model.joblib` as-is
5. **Adjust star-hitter filter** to catch yes_price 65-71 instead of ≥72

## Files to Read on Resume

- [[handoff-053]] — Full copula architecture overhaul (7 files, +1,011 lines, code complete)
- [[MLB-Model-Architecture-Overhaul-Apr28]] — Architecture rationale and copula design
- [[Execution-Plan]] — Phase 1 MLB model steps
