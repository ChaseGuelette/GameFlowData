# MLB Pitcher K Ablation Iteration Pipeline

Date: 2026-06-21
Status: runbook / awaiting Chase-run baseline freeze

## Purpose

Define the pitcher `pitcher_strikeouts` ablation loop after the stat-suite rebuild. This is intentionally gated: first freeze the Phase 2 baseline, then run one controlled variant/family at a time with quote-clean replay, audit, and ranker checks.

## Operating principles

1. Baseline first. Do not train new variants until `docs/development_docs/mlb_pitcher_k_frozen_baselines.md` has a quote-clean decision-grade baseline.
2. Under-only is the primary pitcher K edge shape. Always run under-only before drawing conclusions from both-direction summaries.
3. 100+ bets is the minimum headline threshold; lower-volume winners are exploratory.
4. Feature selector pickup/drop is diagnostic only. Use true variant or force-include / force-exclude comparisons.
5. Avoid architecture escalation. Direct quantile baseline and named cheap variants come before survival/copula/decomposition.
6. Keep Phase 3A lineup/contact features locked out by default unless deliberately forced in a controlled experiment.
7. No live/Kelly promotion from this loop. Paper/live requires baseline freeze plus CLV/ranker/pass-through operational gates.

## Prerequisite baseline gate

Before running any variant, Chase should run the baseline commands in:

- `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`

As of the 2026-06-21 Slice 7 replay, the old Phase 2 comparison artifact (`src/models/mlb/artifacts/mlb_run_20260513_111207`) produced 752 predictions but 0 quote-clean bets across both raw and focused BL under-only grids when using `--line-source mlb_player_props_clv_snapshots`. Follow-up debugging showed local `mlb_player_props_clv_snapshots` has 0 `pitcher_strikeouts` rows for 2026-04-13 to 2026-05-10, while `mlb_raw_player_props` has linked rows. Therefore pitcher K quote-clean ROI replays should use `--line-source mlb_raw_player_props` unless/until dense CLV snapshots are populated for this market. A fresh `--ablation-variant none` baseline was trained at `src/models/mlb/artifacts/baselines/pitcher_strikeouts_phase2_slice7_none_20260621/mlb_run_20260621_170841`; validate that artifact with raw-props quote-clean before any variant run.

Baseline pass criteria:

- raw under-only quote-clean has `n_bets >= 100` and directionally positive ROI;
- focused BL under-only quote-clean has `n_bets >= 100` and directionally positive ROI;
- audit/CLV/ranker output does not show obvious line timing, dropout, or edge-ranking inversion;
- output directories are saved and named in this doc or the next handoff.

## Variant order

Run existing named variants before creating any new strategy modules. These are already supported by the pitcher trainer:

1. `static_no_l30` — removes L30 hook context; tests whether recent manager/team hook signals are helping or overfitting.
2. `hook_only` — force/includes hook family; tests whether hook context alone carries the useful short-outing signal.
3. `hook_deep_start_l30` — isolates deep-start hook proxy that previously had comparatively better behavior.
4. `ip_only` — tests predicted-IP feature source without hook features.
5. `ip_hook` — tests predicted-IP plus hook context together.

Do not start with `--copula`; prior context says heavier decomposition underperformed and cheap direct baselines/feature-source tests come first.

## Dry-run command pattern

Use dry-run first to inspect generated train/sweep/audit commands without spending compute.

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant hook_deep_start_l30 -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_hook_deep_start_quote_clean -DryRun
```

Justification: verifies labels, artifact root, sweep directory, quote-clean arguments, and audit command shape before long work.

## Actual variant run pattern

Only run after the baseline gate passes and the dry-run command looks right.

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant hook_deep_start_l30 -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_hook_deep_start_quote_clean
```

Justification: runs train -> quote-clean sweep -> audit/ranker path through the generic stat ablation runner with a single named variant.

Caveat: the current generic runner's default sweep is raw `--tau none`. For focused BL validation, run the direct BL sweep below against the produced model directory after training.

## Focused BL sweep for a produced variant artifact

Replace `<MODEL_DIR>` with the finalized artifact directory printed by the variant run.

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir <MODEL_DIR> --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-05-10 --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --flat 100 --output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260510
```

Justification: keeps BL comparison aligned with the frozen baseline rather than relying only on the wrapper's raw sweep.

## Audit/ranker command for the focused BL sweep

Replace `<MODEL_DIR>` and `<LABEL>` with the same values used above.

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --local --sweep-output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260510 --output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260510\audit_suite --model-dir <MODEL_DIR> --start 2026-04-13 --end 2026-05-10 --stats pitcher_strikeouts --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --snapshots-table mlb_player_props_clv_snapshots --batch-size 25
```

Justification: ROI alone is not the decision. Audit/CLV/ranker output decides whether the apparent edge is actually rankable and timing-clean.

## Recommended first run sequence

After baseline freeze, run these in order, one at a time:

### A. `hook_deep_start_l30`

Why first: prior Phase 3A diagnostics suggested broad lineup/contact compressed Phase 2 winners; deep-start/hook context is pitcher-side and narrower.

Dry-run:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant hook_deep_start_l30 -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_hook_deep_start_quote_clean -DryRun
```

Actual:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant hook_deep_start_l30 -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_hook_deep_start_quote_clean
```

### B. `static_no_l30`

Why second: tests whether L30 hook context is helping or if the model is better with more static features only.

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant static_no_l30 -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_static_no_l30_quote_clean -DryRun
```

### C. `ip_only`

Why third: tests the cheap predicted-IP feature-source premise before any survival/decomposition architecture.

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant ip_only -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_ip_only_quote_clean -DryRun
```

### D. `ip_hook`

Why fourth: tests whether predicted-IP and hook context combine constructively after each family is seen alone.

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant ip_hook -Start 2026-04-13 -End 2026-05-10 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.05,0.08,0.10,0.12 -FlatBet -LabelTag phase2_ip_hook_quote_clean -DryRun
```

## Result triage

Use this exact triage after each run:

| Result | Action |
|---|---|
| Beats frozen baseline on quote-clean raw and BL, `n_bets >= 100`, CLV/ranker not inverted | Confirm; run independent/pre-window check before paper. |
| ROI improves but volume < 100 | Shelf as exploratory; do not promote or use as baseline. |
| Selector picks up features but downstream ROI/CLV fails | Exclude; selector is not enough. |
| Legacy ROI wins but quote-clean fails | Exclude for promotion; investigate timing/line-selection. |
| Under wins, over fails | Keep under-only; do not enable both directions. |
| Baseline itself fails quote-clean | Stop variants and diagnose quote-clean coverage/CLV before more training. |

## Required summary after each actual run

Record this in the next handoff or a small report:

- variant and artifact directory;
- train/cal window;
- sweep window and quote policy;
- best raw under config with `n_bets >= 100`;
- best BL under config with `n_bets >= 100`;
- audit/CLV/ranker status;
- winner: baseline vs variant;
- deploy status: `confirm`, `shelf`, or `exclude`;
- next action.

## Non-goals

- Do not run broad historical rescrapes/backfills as part of this loop.
- Do not change DB schema.
- Do not enable live/Kelly trading.
- Do not add more feature families until existing named variants and baseline freeze are complete.
- Do not modify model math while running baseline restoration comparisons.
