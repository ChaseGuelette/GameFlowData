# MLB Lifecycle Feature Families

This is the human-readable inventory of feature families accepted by lifecycle YAML `model.feature_controls.families`. The canonical registries are:

- batter profiles: `src/models/mlb/features/contracts.py` (`BATTER_FORCE_FEATURE_FAMILIES`)
- pitcher strikeouts: `src/models/mlb/training/profiles.py` (`PITCHER_K_FORCE_FEATURE_FAMILIES`)

The config resolver rejects unknown family names. This guide is covered by a test that checks every registered profile, family, and member feature is present.

## `batter_hits`

Model type: compound binomial batter model. Valid families:

- `market`
- `recent_form`
- `contact_quality`
- `matchup_pitcher`
- `bullpen`
- `platoon`
- `environment`
- `opportunity`

## `batter_rbis`

Model type: negative-binomial batter model. It currently shares the complete batter family registry with `batter_hits`:

- `market`
- `recent_form`
- `contact_quality`
- `matchup_pitcher`
- `bullpen`
- `platoon`
- `environment`
- `opportunity`

## Shared batter family definitions

### `market`

- `prop_line_batter_hits`
- `line_total`

Caveat: the shared registry currently names the batter-hits prop-line feature. For `batter_rbis`, verify the resolved config and trainer behavior before forcing this family; prefer `base: no_prop_line` when testing non-market RBI families.

### `recent_form`

- `batter_avg_h_l5`
- `batter_avg_h_l10`
- `batter_avg_h_l20`
- `batter_avg_h_szn`
- `batter_h_l5_l10_ratio`
- `batter_std_h_l5`

### `contact_quality`

- `batter_avg_exit_velocity_l5`
- `batter_avg_exit_velocity_l10`
- `batter_avg_launch_angle_l5`
- `batter_barrel_pct_l5`
- `batter_barrel_pct_l10`
- `batter_hard_hit_pct_l5`
- `batter_xba_l5`
- `batter_xba_l10`
- `batter_xslg_l5`
- `batter_xwoba_l5`
- `batter_zone_pct_l5`
- `batter_chase_pct_l5`
- `batter_whiff_pct_l5`
- `batter_gb_pct_l10`
- `batter_fb_pct_l10`
- `batter_babip_szn`
- `batter_hard_pct_szn`

### `matchup_pitcher`

- `opp_pitcher_avg_era_l5`
- `opp_pitcher_avg_whip_l5`
- `opp_pitcher_avg_k_per_9_l5`
- `opp_pitcher_avg_bb_per_9_l5`
- `opp_pitcher_avg_h_allowed_l5`
- `opp_pitcher_avg_hr_allowed_l5`
- `opp_pitcher_xwoba_against_l5`
- `opp_pitcher_hard_hit_pct_against_l5`
- `opp_pitcher_avg_fastball_velo_l5`
- `opp_pitcher_days_rest`
- `opp_pitcher_babip_against_l5`
- `opp_pitcher_velo_drop_late_l5`
- `opp_pitcher_avg_pitches_per_inning_l5`
- `opp_pitcher_deep_inning_pct_l5`

### `bullpen`

- `opp_bullpen_ip_last_3d`
- `opp_bullpen_era_last_7d`
- `opp_relievers_available`
- `opp_bullpen_pitches_last_3d`

### `platoon`

- `is_same_hand`
- `batter_avg_h_vs_hand_l20`
- `batter_avg_ops_vs_hand_l20`

### `environment`

- `park_hits_factor`
- `air_density_idx`
- `wind_out_mph`
- `has_precip`
- `is_home`

### `opportunity`

- `lineup_position`
- `projected_ab`
- `batter_avg_ab_l5`
- `batter_avg_pa_l5`
- `batter_rest_days`
- `batter_games_last_7d`
- `batter_game_number`

## `pitcher_strikeouts`

Model type: direct quantile pitcher-strikeouts model. Valid family definitions follow.

### `market`

- `prop_line_pitcher_strikeouts`
- `line_total`

### `workload_leash`

- `pitcher_avg_ip_l3`
- `pitcher_avg_ip_l5`
- `pitcher_avg_ip_szn`
- `pitcher_min_ip_l5`
- `pitcher_max_ip_l5`
- `pitcher_median_ip_l5`
- `pitcher_ip_range_l5`
- `pitcher_short_start_rate_l5`
- `pitcher_start_stability_l5`
- `pitcher_avg_pitches_per_start_l5`
- `pitcher_avg_pitches_thrown_l3`
- `pitcher_avg_pitches_thrown_l5`
- `pitcher_workload_spike_ratio`
- `pitcher_recent_pitch_count_trend`
- `rest_after_high_pitch_count`

### `team_hook`

- `team_starter_avg_ip_l30`
- `team_starter_short_hook_rate_l30`
- `team_starter_deep_start_rate_l30`
- `manager_starter_short_hook_rate_l30`

### `pitcher_stuff`

- `pitcher_avg_whiff_pct_l5`
- `pitcher_avg_csw_pct_l5`
- `pitcher_avg_chase_pct_l5`
- `pitcher_avg_zone_pct_l5`
- `pitcher_avg_fastball_velo_l5`
- `pitcher_std_whiff_pct_l3`
- `pitcher_fastball_velo_delta_l3_vs_szn`
- `pitcher_fip_szn`
- `pitcher_k_pct_szn`

### `inning_fatigue`

- `pitcher_velo_drop_late_l5`
- `pitcher_avg_whiff_rate_late_l5`
- `pitcher_avg_k_rate_early_l5`
- `pitcher_avg_pitches_per_inning_l5`
- `pitcher_avg_csw_rate_l5_inning`
- `pitcher_deep_inning_pct_l5`
- `pitcher_avg_k_first_5ip_l5`

### `opponent_contact`

- `opp_team_avg_so_l10`
- `opp_team_k_pct_l10`
- `opp_team_whiff_pct_l10`
- `opp_team_contact_rate_l10`
- `opp_team_chase_pct_l10`
- `opp_team_zone_contact_pct_l10`

### `environment`

- `park_so_factor`
- `is_home`
- `line_total`
- `air_density_idx`
- `wind_out_mph`

### `phase3b_downside`

- `manager_starter_short_hook_rate_l30`
- `pitcher_pct_starts_under_5_ip_l10`
- `pitcher_fastball_velo_delta_l3_vs_szn`
- `team_bullpen_pitches_last_3d`
- `pitcher_left_last_start_early_flag`

### `ip_feature_source`

- `predicted_ip_q25`
- `predicted_ip_q50`
- `predicted_ip_spread`
- `predicted_ip_q25_delta`

## Using families in YAML

Force-include one or more registered families:

```yaml
model:
  feature_controls:
    mode: include
    families: [platoon, contact_quality]
    features: []
```

Force-exclude a family for a load-bearing ablation:

```yaml
model:
  feature_controls:
    mode: exclude
    families: [workload_leash]
    features: []
```

Use `features` for an exact single-feature test. An explicitly requested feature must exist in the trainer dataframe or the run fails. Family members absent from an optional/narrow dataframe may be ignored by the training control layer, so always inspect `resolved_config.yaml`, `run_config.json`, `training_metadata.json`, and the completed artifact feature manifest before interpreting an experiment.

Feature selection is not an ablation. Validate correlated groups through controlled family-level include/exclude runs, then evaluate quote-clean ROI, drawdown, CLV, ranker confidence, timing, and bet volume on the exact artifact.
