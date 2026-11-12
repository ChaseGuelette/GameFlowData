# NBA direct-total REB saved-window sweep

## Kick off immediately

- **You:** the frozen-direct and tuning-only sweep/analysis pairs are complete and verified; do not rerun those occupied outputs. The retuned/reselected candidate is separate and owner-running. Commands below remain reusable recipes with a new matching output path.
- **This chat:** implementation is integrated into main; return focus to error-guided improvement of the existing minutes/rate models. Full real-grid results remain owner-launched, not agent-certified performance.
- **Other chat:** no sweep writer needed. Production/paper policy is separate.

This offline diagnostic compares the completed direct-total rebound benchmark with a REB-only replay of the original minutes-times-rate control. It does not train, load model binaries, query a database, use the network, promote a model, or alter production policy.

The [first two completed sweep review](nba-direct-reb-fixed-vs-retuned-sweep-review-2026-11-12.md) verifies both 20-cell runs and analyses. Neither direct model establishes a betting upgrade; every raw cell loses, and tuning-only loses every cell. The logged console overlap did not indicate an incomplete run. Preserve the original control and the separate forecast-versus-betting interpretation.

## Inputs and semantics

- Benchmark: any completed additive-schema direct-total benchmark for this saved window, including `nba_direct_reb_benchmark_nov2_v1`, `nba_direct_reb_pinball_benchmark_nov2_v1`, or `nba_direct_reb_pinball_reselect_benchmark_nov2_v1`. Saved native quantiles and receipts are authoritative; feature count and training-source hashes are not frozen by this sweep.
- Window: October 21-November 2, 2026; the first scored date is October 22.
- Fixed settings inherited from the original sweep: seed 42, 5,000 samples, $2,000 bankroll, $10 flat stakes, minimum average minutes 10, and the original bookmaker list.
- Default run: two model lanes x ten settings = 20 cells. `--models direct` runs ten direct-only cells.
- The direct lane reconstructs samples from native saved quantiles in deterministic benchmark order and verifies saved sampled quantiles and unbounded over probabilities.
- The control lane uses original receipt-verified raw/fixed-BL preselection REB bookmaker edges. It never slices old selected bets or reconstructs control samples from five quantiles.
- Raw direct replay preserves incumbent betting semantics: empirical over probability is clipped to `[0.05, 0.90]`, and under is `1-over`. BL uses the incumbent `BlackLittermanBlender`. The benchmark's diagnostic Brier score used the unclipped empirical over probability.

## Owner commands

Dry-run (validates receipts/grid without creating output, sampling, or simulation):

```powershell
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -X utf8 scripts/sweep_nba_direct_reb.py --benchmark-dir C:/Users/Chase/Projects/GameFlowData/backtest_results/nba_direct_reb_benchmark_nov2_v1 --output-dir C:/Users/Chase/Projects/GameFlowData/backtest_results/nba_direct_reb_sweep_nov2_v1 --dry-run
```

Full paired 20-cell sweep (owner-launched):

```powershell
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -X utf8 scripts/sweep_nba_direct_reb.py --benchmark-dir C:/Users/Chase/Projects/GameFlowData/backtest_results/nba_direct_reb_benchmark_nov2_v1 --output-dir C:/Users/Chase/Projects/GameFlowData/backtest_results/nba_direct_reb_sweep_nov2_v1
```

Post-sweep analysis:

```powershell
C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -X utf8 scripts/analyze_nba_direct_reb_sweep.py --sweep-dir C:/Users/Chase/Projects/GameFlowData/backtest_results/nba_direct_reb_sweep_nov2_v1 --output-dir C:/Users/Chase/Projects/GameFlowData/backtest_results/nba_direct_reb_sweep_nov2_v1_analysis
```

## Evidence and limitations

The custom manifest records source hashes, planned/completed cells, per-cell file receipts, model lane, source treatment, and non-production `decision_grade=false`. The analyzer verifies receipts, counts, metrics, payoff accounting, and emits paired deltas plus side/date/book and common/direct-only/control-only wager summaries.

Cells are competing policies; never sum their profits. Best grid ROI is exploratory, especially below 100 bets. This saved discovery window is not an independent validation window, CLV study, ranker, architecture decision, or authorization for a production betting change.

## Delivery verification

Independent specification and quality recheck passed after five localized receipt/provenance fixes. Main-checkout verification passed exact-input dry-run (1,579 totals/1,033 quotes/20 paired cells), an actual saved October22 one-day raw/BL direct/control in-memory smoke, the synthetic public 20-cell sweep-to-analysis path, and the corruption/source-mutation/conflicting-provenance checks. All 25 existing simulator/Monte Carlo tests passed under the offline guard, as did Ruff/compilation; Pyright reported 0 errors and 7 intentional private-helper reuse warnings. No model binary loading, fitting, DB/network access, full real grid, commit or production mutation was performed by this implementation lane. Main dependency differences at integration were line-ending-only for metrics/BL/sampler; normal runtime code remained unchanged.
