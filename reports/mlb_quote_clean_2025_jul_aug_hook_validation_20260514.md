# MLB Pitcher K Hook Quote-Clean Independent Window Check

Date run: 2026-05-14
Window: 2025-07-01 through 2025-08-31
Market/stat: pitcher_strikeouts
Quote methodology: quote-clean, latest snapshot <= 13:30 ET, fixed config only
Config: tau=0.75, z_max=0.25, max_weight=0.65, edge=0.02, flat $100

## Decision

Do not promote.

This independent July/August 2025 window supports a weak-to-moderate incremental hook signal, but the absolute ROI and smoothness are still too implausible to treat as production validation. The hook model again improves mainly by avoiding terrible static-only bets. Unlike April 2026, hook-only bets were strongly profitable in this larger independent window, so the feature is not dead; it deserves continued investigation under hardened methodology.

## Relevant prior lessons/invariants

- Empirical probabilities must remain empirical CDF from samples; no Gaussian CDF substitution.
- Feature selection is not an ablation; downstream paired ROI/drawdown/bet-volume comparison is the right decision surface.
- Correlated feature families need force/include-style downstream validation, not selector-only conclusions.
- Quote-clean results that look implausibly high/smooth should be treated as red flags, not promotion evidence.

## Overall fixed-config results

| Model | Bets | Hit rate | ROI | Profit | Staked | MaxDD / Sharpe source |
|---|---:|---:|---:|---:|---:|---|
| Static | 711 | 68.4% | +31.34% | +$22,282 | $71,100 | sweep console: Sharpe 4.45, MaxDD 3.1% |
| Hook `team_starter_deep_start_rate_l30` | 708 | 74.0% | +42.23% | +$29,896 | $70,800 | sweep console: Sharpe 6.33, MaxDD 2.2% |

Interpretation: hook wins by +10.89 pp ROI on this independent window, but both absolute returns are unrealistic and remain methodology-suspicious.

## Paired overlap analysis

Exact paired key: `game_date, player_id, game_id, stat, side, line`.

| Bucket | Bets | Hit rate | ROI | Profit | Interpretation |
|---|---:|---:|---:|---:|---|
| Static/Hook overlap | 622 | 73.8% | +41.77% | +$25,984 | Shared baseline/window signal dominates both models. |
| Static-only | 89 | 30.3% | -41.59% | -$3,702 | Static-only bets are extremely bad. Hook removes these. |
| Hook-only | 86 | 75.6% | +45.49% | +$3,912 | Unlike April, hook-only bets are not breakeven here; they are strongly positive. |

Prediction-key paired buckets:

| Bucket | Count | Static ROI | Hook ROI | Note |
|---|---:|---:|---:|---|
| Same side / same line | 622 | +41.77% | +41.77% | Identical bets/results. |
| Same side / different line | 0 | n/a | n/a | No line-only divergences. |
| Opposite side | 4 | -39.00% | +50.76% | Tiny sample; directionally favorable to hook but not meaningful alone. |

## Side splits

Static:
- Over: 172 bets, +49.19% ROI
- Under: 539 bets, +25.64% ROI

Hook:
- Over: 210 bets, +55.52% ROI
- Under: 498 bets, +36.62% ROI

Interpretation: this window is not solely an Under artifact. Both sides are implausibly profitable, especially Overs.

## Conclusion

This resolves the single-month ambiguity partly in favor of the hook feature, but not enough for deployment:

1. The April pattern repeats in the most important way: the hook removes a set of bad static-only bets.
2. The independent window is stronger than April because hook-only bets are positive, not breakeven.
3. However, the shared overlap is itself +41.77% ROI over 622 bets, and both models show Sharpe > 4 with sub-4% drawdown. That is too good to trust as market edge without additional methodology hardening.

Working classification: weak-to-moderate real signal candidate, not promotion-grade evidence.

## Outputs

- Static sweep: `backtest_results/quote_clean_2025_jul_aug_static_fixed_flat100_20260514`
- Hook sweep: `backtest_results/quote_clean_2025_jul_aug_hook_deep_start_l30_fixed_flat100_20260514`
- Paired summary JSON: `reports/mlb_quote_clean_2025_jul_aug_static_vs_hook_overlap_summary_20260514.json`
- Paired detail CSV: `reports/mlb_quote_clean_2025_jul_aug_static_vs_hook_overlap_20260514.csv`
