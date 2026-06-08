# 05 — Pitcher K Port and Baseline Restoration

## Goal

Port pitcher_strikeouts onto the rebuilt operational suite and restore/freeze a current baseline before new ablation/training work.

## Current pitcher-specific files

- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/mlb_quantile_trainer.py`
- `src/models/mlb/mlb_monte_carlo.py`
- `src/models/mlb/features/contracts.py`
- `src/models/mlb/features/pitcher_training_loader.py`
- `src/models/mlb/features/pitcher_inference_loader.py`
- `scripts/diagnose_pitcher_k_ip_variance.py`
- `docs/development_docs/mlb_pitcher_k_quote_clean_validation_scope.md`
- `docs/development_docs/mlb_pitcher_k_phase3a_lineup_contact_expansion.md`
- `docs/development_docs/mlb_pitcher_k_phase3b_pitcher_extremes_roadmap.md`
- `reports/mlb-pitcher-k-hook-ablation-hardening-2026-05-13.md`

## New docs/files

- Create: `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`
- Create: `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md`
- Create: `tests/test_mlb_pitcher_train_pipeline_variants.py`
- Create/update profile in `src/models/mlb/training/profiles.py`

## Pitcher stat profile

`pitcher_strikeouts` profile defaults:

- `stat_key`: `pitcher_strikeouts`
- `display_name`: `Pitcher Strikeouts`
- `model_type`: `quantile`
- `target_columns`: `actual_so`, `actual_ip` for optional IP-source variants
- `prop_line_feature`: `prop_line_pitcher_strikeouts`
- `default_direction`: `under` for first baseline restoration
- `default_quote_policy`: `slate_or_tminus`
- `default_line_source`: `mlb_player_props_clv_snapshots`
- `default_book_routing_policy`: `preferred_book_first`
- `min_decision_grade_bets`: initially 100 unless Chase changes it
- `locked_out_features`: `PITCHER_K_PHASE3A_REJECTED_FEATURES`

## Pitcher feature families

Add to contracts/profile:

- `market`
  - `prop_line_pitcher_strikeouts`
  - maybe `line_total` if confirmed useful/available
- `workload_leash`
  - `pitcher_avg_ip_l3`, `pitcher_avg_ip_l5`, `pitcher_avg_ip_szn`
  - `pitcher_min_ip_l5`, `pitcher_max_ip_l5`, `pitcher_median_ip_l5`, `pitcher_ip_range_l5`
  - `pitcher_short_start_rate_l5`, `pitcher_start_stability_l5`
  - `pitcher_avg_pitches_per_start_l5`, `pitcher_avg_pitches_thrown_l3`, `pitcher_avg_pitches_thrown_l5`
  - `pitcher_workload_spike_ratio`, `pitcher_recent_pitch_count_trend`, `rest_after_high_pitch_count`
- `team_hook`
  - `team_starter_avg_ip_l30`
  - `team_starter_short_hook_rate_l30`
  - `team_starter_deep_start_rate_l30`
  - `manager_starter_short_hook_rate_l30`
- `pitcher_stuff`
  - `pitcher_avg_whiff_pct_l5`, `pitcher_avg_csw_pct_l5`, `pitcher_avg_chase_pct_l5`, `pitcher_avg_zone_pct_l5`
  - `pitcher_avg_fastball_velo_l5`, `pitcher_std_whiff_pct_l3`
  - `pitcher_fastball_velo_delta_l3_vs_szn`
  - `pitcher_fip_szn`, `pitcher_k_pct_szn`
- `inning_fatigue`
  - `pitcher_velo_drop_late_l5`
  - `pitcher_avg_whiff_rate_late_l5`
  - `pitcher_avg_k_rate_early_l5`
  - `pitcher_avg_pitches_per_inning_l5`
  - `pitcher_avg_csw_rate_l5_inning`
  - `pitcher_deep_inning_pct_l5`
  - `pitcher_avg_k_first_5ip_l5`
- `opponent_contact`
  - `opp_team_avg_so_l10`, `opp_team_k_pct_l10`, `opp_team_whiff_pct_l10`
  - `opp_team_contact_rate_l10`, `opp_team_chase_pct_l10`, `opp_team_zone_contact_pct_l10`
- `environment`
  - `park_so_factor`, `is_home`, `line_total`, `air_density_idx`, `wind_out_mph`
- `phase3b_downside`
  - `manager_starter_short_hook_rate_l30`
  - `pitcher_pct_starts_under_5_ip_l10`
  - `pitcher_fastball_velo_delta_l3_vs_szn`
  - `team_bullpen_pitches_last_3d`
  - `pitcher_left_last_start_early_flag`
- `ip_feature_source`
  - `predicted_ip_q25`, `predicted_ip_q50`, `predicted_ip_spread`, `predicted_ip_q25_delta`

Default lockout:

- `PITCHER_K_PHASE3A_REJECTED_FEATURES` remains excluded unless a deliberate controlled experiment force-includes it.

## Baseline restoration process

### Step 1: Identify current candidate artifact

Use targeted artifact inventory only under:

- `src/models/mlb/artifacts/`
- `src/models/mlb/artifacts/ip_ablation_*`
- `src/models/mlb/artifacts/validation_*`

Read only:

- `run_config.json`
- `training_metadata.json`
- `feature_manifest.json`
- `calibration_report_combined.json`

### Step 2: Run quote-clean under-only sweep manually after approval

Profile defaults should produce a command equivalent to:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --model-dir <artifact> --start <start> --end <end> --stats pitcher_strikeouts --direction under --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --tau none --edge 0.10 0.12 0.15 --kelly 0.125 --flat 100 --output-dir <sweep_dir>
```

Exact windows should be chosen from current lane context and approved before running.

### Step 3: Run audit/ranker/book sensitivity

Use generic audit suite after CLV analyzer decoupling:

- quote-clean audit suite;
- ranker diagnostics for decision-grade configs;
- book sensitivity for decision-grade configs;
- summary markdown.

### Step 4: Freeze baseline doc

Update/create:

- `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`

Include:

- artifact path;
- git hash;
- training window;
- cal window;
- sweep window;
- line source;
- quote policy;
- book routing policy;
- config labels;
- bets, ROI, CLV CI, ranker CI, book concentration;
- pass/fail/shelf status;
- next allowed experiment.

## Done criteria

- Pitcher K can run through generic dry-run runner.
- Pitcher family controls exist and are tested.
- A current frozen baseline doc exists after Chase-approved runs.
- No live/Kelly promotion recommended.
- Baseline comparison is reproducible from commands in docs.
