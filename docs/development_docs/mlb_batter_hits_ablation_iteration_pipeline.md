# MLB Batter Hits Ablation Iteration Pipeline

**Purpose:** A repeatable, faster iteration loop for MLB `batter_hits` model reruns and feature-family ablations. This is meant to find better models without spending 4-6 hours on every candidate before we know whether it deserves full certification.

**Default posture:** research / paper-only until quote-clean CLV, edge-ranking, timing, and book-routing gates pass. Do not promote from ROI alone.

---

## Relevant prior lessons and invariants

These guardrails apply to every rerun and ablation.

1. **Empirical probabilities only**
   - Use empirical sample probabilities such as `(samples > line).mean()`.
   - Never substitute Gaussian CDF probabilities.

2. **Feature selector is not an ablation**
   - A feature selector dropping or keeping a feature does not prove the feature family is bad or good.
   - For ablations, compare controlled force-include / force-exclude or explicit feature-family variants.

3. **Correlated feature families need family-level validation**
   - Correlated columns can substitute for each other.
   - Validate the family first, then prune within the family later.

4. **Cheap baseline before architecture**
   - Start with the smallest controlled experiment that can answer the question.
   - Do not launch broad architecture work when a fixed-config retrain + CLV/ranker comparison can answer the next gate.

5. **Quote-clean CLV before feature expansion or promotion**
   - Quote-clean ROI is only the first screen.
   - CLV, ranker quality, same-book/timing coverage, and book concentration decide whether the model is improving in a usable way.

6. **Positive ROI / positive mean CLV is not enough**
   - If edge magnitude does not rank CLV, raw edge may only be a binary bet/no-bet discriminator.
   - Edge/Kelly sizing remains blocked until ranker CI low and bucket monotonicity survive.

7. **Full dropout bucket audit is certification, not the inner loop**
   - The full audit suite's dropout/bucket phase is prediction-level and can query dense CLV snapshots for every prediction, not just placed bets.
   - Do not run several full dropout audits in parallel for initial ablation triage.

---

## High-level staged funnel

Use this funnel for each new ablation or model rerun.

### Stage 0 — Define the experiment

Write down before training:

- Model/stat: usually `batter_hits`.
- Training seasons.
- Calibration season and calibration cutoff.
- Evaluation window.
- Exact feature-family hypothesis.
- Variant name.
- Whether `prop_line` is included or excluded.
- Selector tolerance / forced include / forced exclude behavior.

For the current clean evaluation window starting `2026-04-13`, train/calibrate through `2026-04-12`:

```powershell
--train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12
```

Do not compare a model calibrated through the evaluation window against a leak-free model.

---

### Stage 1 — Train candidate artifact

Template:

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100 --feature-tolerance <TOLERANCE> <OPTIONAL_VARIANT_FLAGS> --output-dir src\models\mlb\artifacts\ranker_retrains\<VARIANT_NAME>
```

Examples of optional flags:

```text
--exclude-prop-line
```

After training, use the completed artifact directory printed by the trainer. Do not assume the requested `--output-dir` is the final artifact path; the trainer writes a timestamped completed run under it and may leave `_incomplete` directories during training.

Metadata sanity check:

```powershell
$run='<COMPLETED_MODEL_DIR>'; Get-Content "$run\run_config.json"; Get-Content "$run\training_metadata.json"
```

Minimum checks:

- `train_seasons` match the experiment.
- `cal_end_date` is before the sweep start.
- `exclude_prop_line` / feature-family flags match the intended variant.
- Completed run directory is not `_incomplete`.

---

### Stage 2 — Fast quote-clean sweep

The sweep is the first filter, not the final decision.

Default first-pass grid:

```text
--tau none --edge 0.10 0.12 0.15 --flat 100 --direction under
```

Use `preferred_book_first` if the latest baseline shows preferred-book routing is the operational policy. Use `lowest_vig` as a control when comparing against older baseline artifacts or when checking book concentration.

Preferred-book first-pass template:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --start 2026-04-13 --end 2026-05-17 --stats batter_hits --model-dir <COMPLETED_MODEL_DIR> --n-samples 5000 --tau none --edge 0.10 0.12 0.15 --z-max 0.25 --max-weight 0.50 --flat 100 --direction under --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --output-dir backtest_results\ranker_retrains\<VARIANT_NAME>_preferred_book_20260413_20260517
```

Lowest-vig control template:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --start 2026-04-13 --end 2026-05-17 --stats batter_hits --model-dir <COMPLETED_MODEL_DIR> --n-samples 5000 --tau none --edge 0.10 0.12 0.15 --z-max 0.25 --max-weight 0.50 --flat 100 --direction under --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy lowest_vig --output-dir backtest_results\ranker_retrains\<VARIANT_NAME>_lowest_vig_20260413_20260517
```

Only run the broader BL/tau grid when the current question is specifically BL/risk-control behavior:

```text
--tau none 0.50 0.90 --edge 0.10 0.12 0.15 --z-max 0.25 --max-weight 0.50
```

First-pass sweep interpretation:

- Treat configs with `<100` bets as exploratory only, even if ROI is huge.
- Prefer fixed, comparable configs over winner-picking from many cells.
- Do not promote from ROI/Sharpe alone.

---

### Stage 3 — Choose decision-grade configs

After the sweep, identify configs with at least 100 bets.

Quick summary:

```powershell
Import-Csv backtest_results\ranker_retrains\<SWEEP_DIR>\sweep_summary.csv | Format-Table config,bets,roi,profit,hit_rate,max_drawdown -AutoSize
```

Decision-grade rule:

```text
bets >= 100
```

For current `batter_hits` work, these are usually the no-BL configs:

```text
config_01_no_BL_edge0.1_kelly0.125
config_02_no_BL_edge0.12_kelly0.125
config_03_no_BL_edge0.15_kelly0.125
```

But do not hardcode blindly; verify `sweep_summary.csv`.

---

### Stage 4 — First-pass CLV-only audit

This is the normal ablation iteration path.

Use:

```text
--skip-dropout-audit
--bets-csv <only decision-grade configs>
```

Why:

- CLV analysis is bet-level.
- Dropout/bucket analysis is prediction-level and can be slow/local-DB I/O bound.
- For model selection, first ask whether placed bets have CLV and whether edge/model scores rank CLV.

Template for one sweep:

```powershell
$s='<SWEEP_DIR>'; $model='<COMPLETED_MODEL_DIR>'; .\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --local --skip-dropout-audit --sweep-output-dir "backtest_results\ranker_retrains\$s" --output-dir "backtest_results\ranker_retrains\$s\audit_suite" --model-dir $model --start 2026-04-13 --end 2026-05-17 --stats batter_hits --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --snapshots-table mlb_player_props_clv_snapshots --batch-size 25 --bets-csv "backtest_results\ranker_retrains\$s\config_01_no_BL_edge0.1_kelly0.125\bets.csv" --bets-csv "backtest_results\ranker_retrains\$s\config_02_no_BL_edge0.12_kelly0.125\bets.csv" --bets-csv "backtest_results\ranker_retrains\$s\config_03_no_BL_edge0.15_kelly0.125\bets.csv"
```

If only configs 01 and 02 have `>=100` bets, omit config 03.

Do not run four full audit suites in parallel. If running multiple CLV-only audits in parallel, two terminals is safer than four if local Postgres is already I/O-bound.

---

### Stage 5 — Ranker diagnostics

Run after the CLV-only audit writes `audit_suite\clv\...\clv_matches.csv`.

Template for one sweep:

```powershell
$s='<SWEEP_DIR>'; Get-ChildItem "backtest_results\ranker_retrains\$s\config_*" -Directory | ForEach-Object { $label=$_.Name; $clv="backtest_results\ranker_retrains\$s\audit_suite\clv\$label\clv_matches.csv"; $cand="$($_.FullName)\bookmaker_candidate_edges.csv"; if((Test-Path $clv) -and (Test-Path $cand)){ Write-Host "`n### RANKER $s / $label"; .\venv\Scripts\python.exe scripts\analyze_mlb_clv_ranking_diagnostics.py --clv-matches-csv $clv --candidate-edges-csv $cand --score-set all --bootstrap-samples 1000 --min-n 100 --output-dir "backtest_results\ranker_retrains\$s\audit_suite\ranker\$label" } }
```

Primary ranker signals to inspect:

- `raw_edge`
- `model_prob`
- `model_alpha`
- `execution_alpha`
- `selected_vs_candidate_mean_gap`

Interpretation:

- If only execution/reference-market signals rank CLV, the model is not yet producing stable model-alpha ranking.
- If raw edge fails ranking but mean CLV is positive, use flat threshold-only paper posture; do not use Kelly/edge sizing.
- If model-alpha improves across an ablation, that is stronger evidence than ROI alone.

---

### Stage 6 — Book sensitivity / deconcentration

Run for variants that survive initial CLV/ranker screening, or for baseline comparisons where book concentration is the active question.

Template:

```powershell
$s='<SWEEP_DIR>'; .\venv\Scripts\python.exe scripts\analyze_mlb_clv_book_sensitivity.py --audit-suite-dir "backtest_results\ranker_retrains\$s\audit_suite" --use-suite-selected --books espnbet prophetx draftkings fanduel betmgm hardrockbet --bootstrap-samples 1000 --min-bets 50 --output-dir "backtest_results\ranker_retrains\$s\audit_suite\book_sensitivity"
```

Interpretation:

- Reduced ESPNBet/ProphetX concentration helps, but it is not enough if edge-ranking fails.
- If the same opportunity has viable preferred-book alternatives, concentration is more likely routing/price-selection than model dependence on one book.

---

### Stage 7 — Compare candidate to baseline

Summaries:

```powershell
Get-ChildItem backtest_results\ranker_retrains -Recurse -Filter suite_manifest.csv | ForEach-Object { Write-Host "`n### $($_.FullName)"; Import-Csv $_.FullName | Format-Table label,gate_status,policy_recommendation,roi,total_bets,mean_clv_ci_low,edge_clv_ci_low,book_routing_policy,preferred_book_share,espnbet_or_prophetx_share -AutoSize }
```

```powershell
Get-ChildItem backtest_results\ranker_retrains -Recurse -Filter ranking_summary.csv | ForEach-Object { Write-Host "`n### $($_.FullName)"; Import-Csv $_.FullName | Sort-Object {[double]($_.spearman_ci_low)} -Descending | Select-Object -First 8 | Format-Table score,n,spearman,spearman_ci_low,spearman_ci_high,top_minus_bottom_clv -AutoSize }
```

Compare in this order:

1. Metadata apples-to-apples check.
2. Decision-grade bet counts.
3. Flat ROI / drawdown / hit rate.
4. Mean CLV and CLV CI low.
5. Edge/model ranker CI low and bucket monotonicity.
6. Model-alpha vs execution-alpha decomposition.
7. Book concentration / deconcentration.
8. Timing coverage and same-book CLV coverage if available.

Winner criteria:

- Prefer the model that improves CLV/ranker quality under fixed comparable configs.
- If no-prop-line is competitive, prefer it as the cleaner baseline.
- If a candidate wins ROI but loses CLV/ranker quality, do not promote it.
- If a candidate only wins through tiny configs with `<100` bets, treat it as exploratory.
- If no candidate improves model-alpha stability, do not start more feature work blindly; diagnose residuals/error buckets first.

---

### Stage 8 — Full certification audit for finalists only

Run this only after a candidate survives the faster CLV/ranker funnel.

Full audit template:

```powershell
$s='<SWEEP_DIR>'; $model='<COMPLETED_MODEL_DIR>'; .\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --local --sweep-output-dir "backtest_results\ranker_retrains\$s" --output-dir "backtest_results\ranker_retrains\$s\audit_suite_full" --model-dir $model --start 2026-04-13 --end 2026-05-17 --stats batter_hits --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --snapshots-table mlb_player_props_clv_snapshots --batch-size 25
```

Operational rules:

- Run full dropout/bucket audits serially or overnight.
- Do not launch four full audits in parallel unless intentionally stress-testing local DB.
- If it stalls in bucket analysis, it is likely local Postgres I/O/query shape, not CPU/RAM.
- Full audit output is certification evidence, not required for every rejected ablation.

---

## Recommended iteration loop per ablation

For most future ablations, the standard wall-clock path should be:

1. Train one candidate artifact.
2. Run one preferred routing quote-clean sweep with 3 no-BL edge thresholds.
3. Run CLV-only audit on configs with `bets >= 100`.
4. Run ranker diagnostics.
5. Compare to current baseline.
6. If promising, run book sensitivity.
7. If still promising, run lowest-vig control and/or independent-window validation.
8. If finalist, run full dropout certification serially.

Avoid this as the default:

```text
4 models x 2 routing policies x full BL grid x full dropout audits x all configs
```

That path is useful for establishing a baseline matrix, but too expensive for normal feature iteration.

---

## When to stop and change the question

Stop adding features and diagnose instead if:

- ROI is positive but mean CLV fails.
- Mean CLV is positive but all model/edge rankers fail.
- Only execution-alpha ranks CLV.
- Book sensitivity shows signal survives only in one post-hoc book/odds slice.
- Candidate-only bets are harmful while baseline-overlap bets drive the result.
- Feature-family changes mainly increase false-positive bet volume.

In those cases, the next step is residual/error-bucket diagnostics, not another broad feature-family ablation.

---

## Naming conventions

Use output names that encode the experiment:

```text
src\models\mlb\artifacts\ranker_retrains\<variant_name>
backtest_results\ranker_retrains\<variant_name>_<routing>_<start>_<end>
```

Examples:

```text
with_prop_line_tol002_preferred_book_20260413_20260517
no_prop_line_tol002_lowest_vig_20260413_20260517
weather_family_forced_preferred_book_20260413_20260517
lineup_contact_excluded_preferred_book_20260413_20260517
```

Keep enough detail in the name to avoid mixing variants during audit/ranker comparison.

---

## Minimum report format after each candidate

Every candidate comparison should answer:

1. What was tested?
2. Which baseline was it compared against?
3. Did metadata match the intended experiment?
4. Which fixed config was used for comparison?
5. Did it improve ROI with `>=100` bets?
6. Did it improve mean CLV / CLV CI low?
7. Did it improve model-alpha or edge-ranking CI low?
8. Did book concentration improve or worsen?
9. Is it rejected, shadow-paper, or finalist for full audit?
10. What is the next experiment and why?
