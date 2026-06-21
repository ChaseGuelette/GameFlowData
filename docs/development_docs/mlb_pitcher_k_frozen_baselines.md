# MLB Pitcher K Frozen Baselines

Date: 2026-06-21
Status: baseline-restoration runbook / Chase-run validation required

## Purpose

Freeze the current `pitcher_strikeouts` baseline before any further feature-family or architecture work. This page separates what is already known from the manual training/sweep commands that still need to be run for decision-grade evidence.

## Relevant prior lessons/invariants

- Use empirical CDF from samples for probabilities: `(samples > line).mean()`; never Gaussian CDF smoothing.
- Do not globally recalibrate or conformal-shift away low-tail/Q10 behavior without ROI/Sharpe/drawdown proof.
- Feature selector output is diagnostic only; it is not an ablation or promotion gate.
- Correlated pitcher feature families must be validated with downstream force-include / force-exclude or named variant comparisons.
- Cheap baseline before architecture: do not revive survival/copula/decomposition work until a clean direct-model baseline fails a specific gate.
- Quote-clean replay and CLV/ranker diagnostics are required before promotion or paper/live staking decisions.

## Baseline artifacts currently known

| Label | Artifact | Train/cal shape | Current interpretation |
|---|---|---|---|
| Production file bundle | `src/models/mlb/artifacts/production/` | legacy production bundle with `pitcher_k_model.joblib` and `pitcher_k_feature_config.joblib` | Loader compatibility target; use for smoke/load checks, not enough by itself as a frozen experiment. |
| Phase 2 baseline | `src/models/mlb/artifacts/mlb_run_20260512_221929` | train 2023-2025, cal 2026 through 2026-05-11 | Strong trained direct quantile model; calibration gate passed in prior handoff, but cal window overlaps later comparison windows. |
| Phase 2 clean comparison baseline | `src/models/mlb/artifacts/mlb_run_20260513_111207` | train 2024-2025, cal 2026 through 2026-04-12 | Preferred baseline for 2026-04-13+ validation because calibration ends before the main comparison window. |
| IP/hook variants | `src/models/mlb/artifacts/ip_ablation_*` | train 2024-2025, cal 2026 through 2026-04-12 | Existing named variant artifacts for Phase 2/3B-style comparisons. |
| 2025 validation variants | `src/models/mlb/artifacts/validation_2025_*` | train 2024, cal 2025 through 2025-08-31 | Independent-year validation artifacts; use only when the sweep window and data coverage are explicitly matched. |

## Existing baseline result anchors

Known result anchors from local sweep summaries and prior docs:

| Window / mode | Config | Bets | ROI | Interpretation |
|---|---:|---:|---:|---|
| 2026-04-13 to 2026-05-10, legacy raw under-only | raw edge=0.05 | 131 | +21.72% | Strong Phase 2 baseline, but legacy line mode is hypothesis-generating only. |
| 2026-04-13 to 2026-05-10, legacy BL under-only | tau=0.90, z=0.25, max_weight=0.80, edge=0.02 | 110 | +34.68% | Historical headline benchmark; do not treat as forward bar without quote-clean/CLV. |
| 2026-03-25 to 2026-04-12, quote-clean pre-window | raw edge=0.05 | 144 | +11.13% | Directionally profitable quote-clean sanity check. |
| 2026-03-25 to 2026-04-12, quote-clean pre-window | tau=0.90, z=0.25, max_weight=0.80, edge=0.02 | 130 | +8.74% | More realistic working bar for future feature phases than the legacy +34.68% headline. |

Working interpretation: Phase 2 pitcher K is still the baseline to beat, but the decision-grade bar should be quote-clean + CLV/ranker quality, not raw legacy ROI.

## Slice 7 replay result — 2026-06-21

Chase reran the preferred 2026-04-13 to 2026-05-10 quote-clean baseline checks against `src/models/mlb/artifacts/mlb_run_20260513_111207`:

| Output dir | Predictions | Configs | Decision-grade bets | Result |
|---|---:|---:|---:|---|
| `backtest_results/mlb_pitcher_k_phase2_quote_clean_raw_under_20260413_20260510` | 752 | 6 | 0 | FAIL baseline freeze gate |
| `backtest_results/mlb_pitcher_k_phase2_quote_clean_bl_under_20260413_20260510` | 752 | 108 | 0 | FAIL baseline freeze gate |

Interpretation:

- The old Phase 2 clean comparison artifact still produced predictions, but no tested raw or BL quote-clean under config produced any bets.
- Follow-up debugging showed the zero-bet result was caused by the requested `--line-source mlb_player_props_clv_snapshots`: local `mlb_player_props_clv_snapshots` has 0 `pitcher_strikeouts` rows for 2026-04-13 to 2026-05-10, while `mlb_raw_player_props` has linked rows for that market/window.
- Therefore this result is a line-source coverage failure, not proof that the artifact/model edge disappeared.
- Quote-clean pitcher K replays should use `--line-source mlb_raw_player_props` unless/until dense CLV snapshots are populated for `pitcher_strikeouts`.
- CLV certification remains blocked on dense CLV coverage for pitcher K; ROI replay can proceed against raw props in quote-clean/as-of mode.

## Slice 7 fresh baseline note — 2026-06-21

Chase retrained a fresh `--ablation-variant none` baseline under the Slice 6 trainer lifecycle:

- Artifact: `src/models/mlb/artifacts/baselines/pitcher_strikeouts_phase2_slice7_none_20260621/mlb_run_20260621_170841`
- The `_incomplete` directory was finalized successfully to the path above.
- One-day debug on 2026-04-13 confirmed the issue is line source, not model loading:
  - Legacy/non-quote-clean raw props: 20 predictions, 277 precomputed line rows, 6 bets at edge 0.0.
  - Quote-clean with `mlb_player_props_clv_snapshots`: 20 predictions, 0 precomputed line rows, 0 bets.
  - Quote-clean with `mlb_raw_player_props`: 20 predictions, 277 precomputed line rows, 7 bets at edge 0.0.

Next gate is to rerun the full raw/BL quote-clean baseline checks for the fresh artifact with `--line-source mlb_raw_player_props`.

## Freeze criteria

A pitcher K baseline is frozen only when all of these are true:

1. Artifact loads through the current `MLBModelSuite` path.
2. `run_config.json`, `feature_manifest.json`, `calibration_report_combined.json`, `training_metadata.json`, and new `model_manifest.json` exist for new runs.
3. Calibration window ends before the validation window.
4. Raw under-only quote-clean sweep has at least one config with `n_bets >= 100`.
5. Focused BL under-only quote-clean sweep has at least one config with `n_bets >= 100`.
6. Dropout/CLV audit suite runs on decision-grade configs.
7. Ranker/Spearman or CLV quality diagnostics do not invert edge quality.
8. Over-only or both-direction sweep is checked before any claim that overs are safe.

## What Chase should run next

I did not run long training/backtests in the agent session. The safest order is below.

### 1. Smoke the current code path

Justification: confirms the refactored training entrypoint and quote-clean sweep CLI still load before spending time on long runs.

```powershell
.\venv\Scripts\python.exe -m py_compile src\models\mlb\mlb_train_pipeline.py src\models\mlb\training\base_orchestrator.py src\backtesting\mlb\run_mlb_sweep.py; .\venv\Scripts\python.exe src\models\mlb\mlb_train_pipeline.py --help; .\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --help
```

### 2. Replay the existing clean Phase 2 baseline in quote-clean raw-under mode

Justification: this is the cheapest decision-grade baseline check; it does not retrain and tests the preferred 2026-04-13 to 2026-05-10 validation window with the pre-window-calibrated artifact.

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_raw_player_props --book-routing-policy preferred_book_first --model-dir src\models\mlb\artifacts\mlb_run_20260513_111207 --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-05-10 --tau none --edge 0.02 0.05 0.08 0.10 0.12 0.15 --flat 100 --output-dir backtest_results\mlb_pitcher_k_phase2_quote_clean_raw_under_20260413_20260510
```

### 3. Replay the existing clean Phase 2 baseline in quote-clean focused-BL-under mode

Justification: tests whether the historical BL edge survives the current quote-clean line-selection path; use this as the main baseline to beat for future variants.

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_raw_player_props --book-routing-policy preferred_book_first --model-dir src\models\mlb\artifacts\mlb_run_20260513_111207 --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-05-10 --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --flat 100 --output-dir backtest_results\mlb_pitcher_k_phase2_quote_clean_bl_under_20260413_20260510
```

### 4. Run audit/CLV suite on decision-grade configs from steps 2-3

Justification: ROI without dropout/CLV/ranker quality is not enough to freeze or promote. Use only configs with at least 100 bets as decision-grade.

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --local --sweep-output-dir backtest_results\mlb_pitcher_k_phase2_quote_clean_bl_under_20260413_20260510 --output-dir backtest_results\mlb_pitcher_k_phase2_quote_clean_bl_under_20260413_20260510\audit_suite --model-dir src\models\mlb\artifacts\mlb_run_20260513_111207 --start 2026-04-13 --end 2026-05-10 --stats pitcher_strikeouts --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_raw_player_props --snapshots-table mlb_raw_player_props --batch-size 25
```

### 5. Fresh baseline retrain after Slice 6

Justification: now required for current baseline restoration because the old pre-window-calibrated Phase 2 artifact produced 0 quote-clean bets on the main validation window. This should not change model math; it refreshes the artifact under the current trainer and proves the Slice 6 artifact lifecycle during a real run.

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_train_pipeline.py --local --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --n-simulations 10000 --ablation-variant none --output-dir src\models\mlb\artifacts\baselines\pitcher_strikeouts_phase2_slice7_none_20260621
```

After step 5, repeat steps 2-4 with the new finalized artifact directory under `src\models\mlb\artifacts\baselines\pitcher_strikeouts_phase2_slice7_none_20260621\mlb_run_<timestamp>`.

## How to interpret outputs

- If quote-clean raw and BL under-only both clear +8% ROI with `n_bets >= 100`, treat Phase 2 as frozen baseline and proceed to controlled named variants.
- If only low-volume configs win, keep the baseline exploratory and do not compare new features against it as a hard bar.
- If quote-clean baseline fails while legacy remains strong, pause feature work and diagnose line-selection/CLV timing before training more models.
- If CLV/ranker diagnostics show edge ranking is inverted, keep paper-only and treat ROI as untrusted.

## Current recommendation

Do not run new feature-variant training yet. The old artifact failed the quote-clean baseline-freeze gate with 0 bets, and pitcher K has not been retrained for months. Run a fresh `--ablation-variant none` baseline first, then repeat quote-clean raw/BL validation before `hook_deep_start_l30`, `static_no_l30`, `ip_only`, or `ip_hook`.
