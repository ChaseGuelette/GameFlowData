# MLB Pitcher K Phase 3A Agreement Diagnostic

Generated: 2026-05-13T16:47:58

## Decision

Do not promote Phase 3A tuned or untuned. Keep Phase 2 clean as the current production winner pending future non-overlap validation.

## Compared artifacts

- Phase 2: `src\models\mlb\artifacts\mlb_run_20260513_111207`
- Phase 3A tuned: `src\models\mlb\artifacts\mlb_run_20260513_160159`
- Primary BL config: tau=0.9, z_max=0.25, max_weight=0.8, edge=0.02

## Performance context

| label | total_bets | wins | losses | pushes | hit_rate | roi | total_profit | total_staked | sharpe_ratio | max_drawdown |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Phase 2 raw under edge=0.05 | 131 | 79 | 52 | 0 | 0.6031 | 0.2172 | 9595.9718 | 44176.6536 | 2.5825 | 0.0925 |
| Phase 3A untuned raw under edge=0.05 | 138 | 80 | 58 | 0 | 0.5797 | 0.1539 | 8369.4438 | 54365.1233 | 1.8426 | 0.1311 |
| Phase 3A tuned raw/no-BL under edge=0.02 | 191 | 104 | 87 | 0 | 0.5445 | 0.0980 | 4761.3871 | 48583.8179 | 1.0381 | 0.1947 |
| Phase 2 BL under tau=.90 z=.25 mw=.80 edge=.02 | 110 | 70 | 40 | 0 | 0.6364 | 0.3468 | 7872.9756 | 22698.7822 | 3.9981 | 0.0448 |
| Phase 3A tuned same BL under | 99 | 56 | 43 | 0 | 0.5657 | 0.1508 | 3349.5730 | 22205.4848 | 1.7014 | 0.1004 |
| Phase 3A tuned best meaningful both-direction | 195 | 103 | 89 | 0 | 0.5365 | 0.0214 | 722.6976 | 33745.5117 | 0.2239 | 0.2480 |

## Phase 2 BL bet buckets vs Phase 3A tuned same BL config

| bucket | count | p2_wins | p2_losses | p2_hit_rate | p2_profit | p2_staked | p2_roi | avg_p2_edge | avg_p3_under_edge | avg_edge_drop | median_edge_drop | avg_p2_under_prob | avg_p3_under_prob |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| same_side_similar_edge | 54 | 29 | 25 | 0.5370 | 2271.8182 | 12748.7345 | 0.1782 | 0.0697 | 0.0924 | -0.0227 | -0.0134 | 0.5826 | 0.6053 |
| same_side_edge_dropped_below_threshold | 31 | 22 | 9 | 0.7097 | 2705.5995 | 4330.5805 | 0.6248 | 0.0433 | 0.0051 | 0.0383 | 0.0350 | 0.5390 | 0.5007 |
| same_side_lower_edge_still_cleared | 15 | 12 | 3 | 0.8000 | 2229.0228 | 4484.9207 | 0.4970 | 0.0891 | 0.0478 | 0.0413 | 0.0457 | 0.6137 | 0.5724 |
| flipped_or_direction_invalidated | 10 | 7 | 3 | 0.7000 | 666.5352 | 1134.5465 | 0.5875 | 0.0327 | -0.0503 | 0.0830 | 0.0783 | 0.5103 | 0.4272 |

## Phase 2 BL winners only

| bucket | count | p2_wins | p2_losses | p2_hit_rate | p2_profit | p2_staked | p2_roi | avg_p2_edge | avg_p3_under_edge | avg_edge_drop | median_edge_drop | avg_p2_under_prob | avg_p3_under_prob |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| same_side_similar_edge | 29 | 29 | 0 | 1.0000 | 7162.6138 | 7857.9389 | 0.9115 | 0.0763 | 0.1044 | -0.0281 | -0.0194 | 0.5957 | 0.6238 |
| same_side_edge_dropped_below_threshold | 22 | 22 | 0 | 1.0000 | 3540.0994 | 3496.0805 | 1.0126 | 0.0460 | 0.0053 | 0.0407 | 0.0354 | 0.5526 | 0.5119 |
| same_side_lower_edge_still_cleared | 12 | 12 | 0 | 1.0000 | 3189.9763 | 3523.9671 | 0.9052 | 0.0895 | 0.0496 | 0.0399 | 0.0417 | 0.6185 | 0.5787 |
| flipped_or_direction_invalidated | 7 | 7 | 0 | 1.0000 | 1001.2562 | 799.8255 | 1.2518 | 0.0352 | -0.0534 | 0.0886 | 0.0929 | 0.5170 | 0.4284 |

## Phase 3A added bets vs same Phase 2 BL config

| lineup_delta_bucket | count | wins | losses | hit_rate | profit | staked | roi | avg_edge | avg_model_prob | avg_lineup_minus_team_k_pct | avg_abs_lineup_minus_team_k_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| high_delta | 10 | 7 | 3 | 0.7000 | 650.2130 | 1278.1974 | 0.5087 | 0.0385 | 0.5222 | 0.0101 | 0.0397 |
| low_delta | 10 | 4 | 6 | 0.4000 | -718.9632 | 1460.0230 | -0.4924 | 0.0522 | 0.5704 | 0.0041 | 0.0145 |
| mid_delta | 10 | 4 | 6 | 0.4000 | -881.2836 | 1580.9076 | -0.5575 | 0.0323 | 0.5133 | -0.0046 | 0.0244 |

## Quantile shift toward sportsbook line

| subset | quantile | count_with_q | mean_shift_toward_line | median_shift_toward_line | pct_moved_toward_line | mean_p2_dist_to_line | mean_p3_dist_to_line |
| --- | --- | --- | --- | --- | --- | --- | --- |
| all_phase2_bl_bets | q10 | 110 | -0.0777 | -0.0721 | 0.2545 | 2.9193 | 2.9970 |
| all_phase2_bl_bets | q25 | 110 | 0.0099 | 0.0018 | 0.5000 | 1.8470 | 1.8371 |
| all_phase2_bl_bets | q50 | 110 | -0.0231 | 0.0383 | 0.5273 | 0.6360 | 0.6591 |
| all_phase2_bl_bets | q75 | 110 | -0.0440 | -0.0679 | 0.4273 | 1.0341 | 1.0781 |
| all_phase2_bl_bets | q90 | 110 | -0.1369 | -0.0663 | 0.4273 | 2.8959 | 3.0329 |
| phase2_bl_winners | q10 | 70 | -0.0597 | -0.0499 | 0.3286 | 2.9516 | 3.0113 |
| phase2_bl_winners | q25 | 70 | 0.0230 | 0.0155 | 0.5286 | 1.8871 | 1.8641 |
| phase2_bl_winners | q50 | 70 | -0.0113 | 0.0719 | 0.5429 | 0.6783 | 0.6897 |
| phase2_bl_winners | q75 | 70 | -0.0558 | -0.0926 | 0.3857 | 0.9931 | 1.0489 |
| phase2_bl_winners | q90 | 70 | -0.1485 | -0.0531 | 0.4571 | 2.8607 | 3.0092 |
| phase2_bl_losers | q10 | 40 | -0.1092 | -0.1034 | 0.1250 | 2.8627 | 2.9719 |
| phase2_bl_losers | q25 | 40 | -0.0130 | -0.0029 | 0.4500 | 1.7768 | 1.7899 |
| phase2_bl_losers | q50 | 40 | -0.0437 | 0.0038 | 0.5000 | 0.5619 | 0.6056 |
| phase2_bl_losers | q75 | 40 | -0.0234 | -0.0222 | 0.5000 | 1.1059 | 1.1293 |
| phase2_bl_losers | q90 | 40 | -0.1167 | -0.0857 | 0.3750 | 2.9576 | 3.0743 |

## Plots

- `docs\analysis\mlb_phase3a_agreement_20260513\plots\q10_phase3a_vs_phase2_scatter.png`
- `docs\analysis\mlb_phase3a_agreement_20260513\plots\q10_shift_toward_line_hist.png`
- `docs\analysis\mlb_phase3a_agreement_20260513\plots\q50_phase3a_vs_phase2_scatter.png`
- `docs\analysis\mlb_phase3a_agreement_20260513\plots\q50_shift_toward_line_hist.png`
- `docs\analysis\mlb_phase3a_agreement_20260513\plots\edge_drop_hist.png`

## Final causal form

Phase 3A lost because 46/110 (41.8%) of Phase 2 BL under bets were edge-compressed, including 34/70 (48.6%) of Phase 2 winners; 31/110 dropped below threshold and 10/110 flipped or were directionally invalidated. Mean under-edge drop was 0.0128, while the same BL config ROI fell from +34.68% to +15.08%; Phase 3A-only added bets returned -22.00%. winner Q50 distance-to-line changed from 0.678 to 0.690; 54.3% moved toward the line; winner Q10 distance-to-line changed from 2.952 to 3.011; 32.9% moved toward the line. Phase 3A added-bet ROI was +50.87% in high lineup/team-delta cases, -55.75% in mid-delta cases, and -49.24% in low-delta cases. The mechanism is primarily blanket feature dilution/edge compression, not blanket lineup anti-signal; lineup information may be conditionally useful only when it materially differs from team average; no Phase 3A artifact should be promoted.

## Saved CSV artifacts

- `docs\analysis\mlb_phase3a_agreement_20260513\performance_context.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\phase2_bl_vs_phase3a_tuned_paired_bets.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\phase2_bl_bucket_summary.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\phase2_bl_winner_bucket_summary.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\phase3a_added_bets.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\phase3a_added_bet_summary.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\quantile_shift_summary.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\regenerated_quantiles.csv`
- `docs\analysis\mlb_phase3a_agreement_20260513\regenerated_lineup_features.csv`