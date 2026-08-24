# MLB Pitcher K Two-Track Ablation Plan

> **Additional supersession note (2026-07-27):** The current flat-first lifecycle also supersedes
> this plan's use of the frozen BL policy and audit/ranker sequence during feature-family discovery.
> Use identical raw/no-BL flat settings for the baseline artifact and every candidate; select model
> finalists first; then run broad BL policy selection and independent-window dropout/timing
> certification. Ranker diagnostics are optional Kelly work, not a flat-model gate. See
> `.hermes/plans/2026-07-27_204057-flat-first-model-selection-lifecycle.md`.

> **Superseded operating plan (2026-07-18):** Use
> [`mlb_pitcher_k_ablation_roadmap.md`](mlb_pitcher_k_ablation_roadmap.md) for current execution.
> This file is retained as the immediate predecessor and historical rationale.

Date: 2026-06-21
Status: planning / future-run runbook

## Purpose

Define the next pitcher `pitcher_strikeouts` model-improvement loop after the Slice 7 dense-CLV audit. The goal is to improve the model without waiting 30 calendar days by using controlled historical ablations and feature-family tests now, while keeping live/Kelly/Kalshi promotion blocked until forward paper evidence exists.

This plan complements:

- `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`
- `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md`
- `docs/development_docs/mlb_pitcher_k_phase3b_pitcher_extremes_roadmap.md`
- `handoffs/handoff-108`

## Current state from latest checkpoint

Pitcher K has a flat-paper candidate, not a live-money candidate:

- Window: `2026-04-13` through `2026-06-21`
- Candidate: BL under-only `config_01_tau0.5_edge0.02_kelly0.125`
- Config: `tau=0.5`, `z_max=0.25`, `max_weight=0.50`, `edge=0.02`, flat $100
- Result: 146 bets, 91-55, +17.19% ROI, Sharpe 2.34
- Mean implied-prob CLV: +1.2455%
- Dropout audit: PASS
- Blocker: edge magnitude does not rank CLV well enough; Spearman(edge, CLV) was weak and CI-low negative
- Decision: flat paper only; no Kelly, no edge-sized staking, no live/Kalshi

## Prior lessons and invariants

Apply these before interpreting any run:

1. Feature selector output is not an ablation. A feature selected/dropped by pinball selection is diagnostic only. Use force-include / force-exclude and downstream backtests.
2. Correlated feature families must be validated at the family level before pruning to individual features.
3. Cheap baseline before architecture. Do not jump to survival, copula, or new decomposition unless cheap controlled variants show a specific failure mode.
4. Q10 / low-tail behavior can be edge. Do not globally recalibrate or smooth it unless ROI, Sharpe, drawdown, and CLV improve.
5. Probabilities must remain empirical CDF from samples: `(samples > line).mean()`.
6. Under-only is the primary pitcher K edge shape until over/both-direction evidence independently clears gates.
7. No live/Kelly/Kalshi until forward paper evidence confirms ranking and operational stability.

## Two-track plan

### Track A — load-bearing base feature ablations

Question: what parts of the current baseline are actually carrying edge?

Run force-exclude family ablations against the same baseline window, same line source, same quote policy, and same BL/flat-stake settings. The output is a load-bearing map of the current model.

Families to test first:

| Family | CLI family name | Purpose |
|---|---|---|
| Market / prop-line | `market` | Separates model alpha from market anchoring/execution alpha. |
| Workload / leash | `workload_leash` | Tests whether short-outing / volume risk is the under edge. |
| Team / manager hook | `team_hook` | Tests team context and hook/leash context. |
| Pitcher stuff | `pitcher_stuff` | Tests skill/stuff dependence. |
| Inning fatigue | `inning_fatigue` | Tests TTOP / late-stuff degradation value. |
| Opponent contact / K context | `opponent_contact` | Tests matchup value vs market-anchor compression. |
| Environment | `environment` | Tests park/weather/home context. |

Primary command pattern, dry-run first:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode exclude -Families workload_leash -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag load_bearing_exclude_workload_leash_slice7 -DryRun
```

Actual run after dry-run review:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode exclude -Families workload_leash -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag load_bearing_exclude_workload_leash_slice7
```

Repeat by replacing `workload_leash` with:

```text
market
team_hook
pitcher_stuff
inning_fatigue
opponent_contact
environment
```

Important runner caveat:

- `scripts/run_pitcher_k_ablation.ps1` exposes `-Families`, `-Features`, `-Mode`, `-Variant`, `-Start`, `-End`, `-CalEndDate`, `-TrainSeasons`, `-FeatureTolerance`, `-Direction`, `-Edge`, `-Kelly`, `-FlatBet`, `-SkipTrain`, `-SkipSweep`, `-SkipAudit`, `-DryRun`.
- The underlying generic runner currently hardcodes the sweep/audit line source as `mlb_player_props_clv_snapshots` even though it has `LineSource` parameters internally. If future line-source routing matters, either patch the runner to honor `-LineSource` or run the direct `run_mlb_sweep.py` command below.

### Focused BL sweep for any Track A artifact

Use this after a training wrapper produces `<MODEL_DIR>`. This matches the current flat-paper candidate shape more closely than the wrapper's raw/no-BL sweep.

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir <MODEL_DIR> --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-06-21 --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --flat 100 --output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260621
```

If dense CLV source coverage regresses, use `--line-source mlb_raw_player_props` for ROI replay only and label CLV certification as blocked until dense snapshots are restored.

### Audit/ranker for any focused BL sweep

Use decision-grade configs only (`n_bets >= 100`). For early discovery, `--skip-dropout-audit` is acceptable. Run the full dropout audit only for finalists.

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --local --skip-dropout-audit --sweep-output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260621 --output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260621\audit_suite --model-dir <MODEL_DIR> --start 2026-04-13 --end 2026-06-21 --stats pitcher_strikeouts --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --snapshots-table mlb_player_props_clv_snapshots --batch-size 25
```

Ranker diagnostic after CLV matches exist:

```powershell
.\venv\Scripts\python.exe scripts\analyze_mlb_clv_ranking_diagnostics.py --clv-matches-csv <CLV_MATCHES_CSV> --candidate-edges-csv <BOOKMAKER_CANDIDATE_EDGES_CSV> --score-set all --bootstrap-samples 1000 --min-n 100 --output-dir <RANKER_OUTPUT_DIR>
```

Track A decision rules:

| Result | Interpretation | Action |
|---|---|---|
| Excluding family worsens ROI/CLV/ranker or drops many profitable baseline bets | Load-bearing | Keep family; consider more precise subfamily tests. |
| Excluding family improves CLV/ranker or removes bad bets without harming winners | Harmful/noisy | Prune or keep excluded in next baseline candidate. |
| Excluding family changes volume but not quality | Mixed | Shelf; test subfamilies or individual features. |
| Family selected by model but downstream quality fails | Not enough | Do not confirm from selector output alone. |
| Results depend only on low-volume configs | Exploratory | Shelf until `n_bets >= 100` or independent window confirms. |

## Track B — high-value feature-family tests

Question: which new or currently locked/optional feature family can improve ranker quality or low-tail under selection?

Start only after Track A establishes the baseline's load-bearing map, unless the feature is already implemented and cheap to force-include.

### Priority 1: pitcher-side downside / short-start risk

CLI family: `phase3b_downside`

Features:

- `manager_starter_short_hook_rate_l30`
- `pitcher_pct_starts_under_5_ip_l10`
- `pitcher_fastball_velo_delta_l3_vs_szn`
- `team_bullpen_pitches_last_3d`
- `pitcher_left_last_start_early_flag`

Why first:

- It aligns with the current under-only edge.
- It targets short-outing/downside risk rather than broad market-priced context.
- It is the recommended Phase 3B direction after the prior broad lineup/contact failure.

Dry-run:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode include -Families phase3b_downside -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag include_phase3b_downside_slice7 -DryRun
```

Actual:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode include -Families phase3b_downside -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag include_phase3b_downside_slice7
```

### Priority 2: single-feature isolates inside Phase 3B

Run isolates only if the full family is promising or ambiguous.

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode include -Features manager_starter_short_hook_rate_l30 -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag include_manager_short_hook_slice7 -DryRun
```

Other isolate candidates:

```text
pitcher_pct_starts_under_5_ip_l10
pitcher_fastball_velo_delta_l3_vs_szn
team_bullpen_pitches_last_3d
pitcher_left_last_start_early_flag
```

### Priority 3: named existing variants

Existing pitcher K variants supported by `-Variant`:

```text
static_no_l30
hook_only
ip_only
ip_hook
hook_avg_ip_l30
hook_short_hook_l30
hook_deep_start_l30
```

Example:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Variant hook_deep_start_l30 -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag variant_hook_deep_start_slice7 -DryRun
```

Use these to answer narrow questions:

- `static_no_l30`: is recent L30 hook context helping or overfitting?
- `hook_only`: does hook context alone carry useful signal?
- `hook_deep_start_l30`: is deep-start tendency the useful sub-signal?
- `ip_only`: does predicted-IP feature-source help without hook features?
- `ip_hook`: do predicted-IP and hook context combine constructively?

### Priority 4: market/reference ranker features

Do not start by training these into the model. First use saved sweep artifacts and ranker diagnostics to evaluate scores as filters/sizers:

- raw model probability
- implied probability
- raw edge
- BL edge
- reference-book edge
- execution-book vs reference-book discrepancy
- market tightness / hold proxy
- preferred-book availability

Goal: find a score that ranks CLV better than raw edge. If a score passes, use it first as a paper filter, not as permission for Kelly.

### Priority 5: broad lineup/contact context

Status: shelved by default.

Only revisit if Track A/paired diagnostics show opponent context is truly load-bearing and not just market-anchor compression. If revisited, require train/serve historical coverage checks before retraining.

## Required train/serve coverage gate for new families

Before interpreting a new family, verify both source-table coverage and actual feature-path variation across train/cal/eval rows. Do not treat selector drops as meaningful if training rows are default-only.

Use a targeted feature-path check modeled on `gameflow-model-evaluation`'s coverage gate:

```powershell
.\venv\Scripts\python.exe - <<'PY'
from src.db.client import get_engine
from src.models.mlb.mlb_feature_store import MLBFeatureStore
import pandas as pd

NEW_FEATURES = [
    'manager_starter_short_hook_rate_l30',
    'pitcher_pct_starts_under_5_ip_l10',
    'pitcher_fastball_velo_delta_l3_vs_szn',
    'team_bullpen_pitches_last_3d',
    'pitcher_left_last_start_early_flag',
]
DEFAULTS = {name: 0 for name in NEW_FEATURES}
fs = MLBFeatureStore(get_engine(local=True))
for season in [2024, 2025, 2026]:
    print('\n== season', season, '==')
    df = fs.enrich_with_matchup_features(fs.get_training_dataset([season]))
    print('rows', len(df), 'date_range', df['game_date'].min(), df['game_date'].max())
    for c in NEW_FEATURES:
        if c not in df.columns:
            print(c, 'MISSING')
            continue
        s = pd.to_numeric(df[c], errors='coerce')
        default = DEFAULTS.get(c, 0)
        non_default = int((s.fillna(default).round(10) != default).sum())
        print(c, 'nonnull', int(s.notna().sum()), 'nunique', int(s.nunique(dropna=True)), 'non_default', non_default)
PY
```

If Windows PowerShell rejects heredoc-style Python, save the snippet as a temporary `.hermes/tmp/check_pitcher_k_feature_variation.py` script and run:

```powershell
.\venv\Scripts\python.exe .hermes\tmp\check_pitcher_k_feature_variation.py
```

## Paired-bet diagnostic requirement

For every finalist or surprising failure, compare baseline vs candidate on identical bet keys and classify baseline bets into:

1. same-side similar edge
2. same-side lower edge but still cleared
3. same-side edge dropped below threshold
4. flipped or direction invalidated
5. candidate-only added bets

Report per bucket:

- count
- hit rate
- ROI
- profit/staked
- average baseline edge
- average candidate edge
- average edge delta
- CLV summary when available

This prevents the wrong lesson. A feature family may be conditionally useful even if the full bundle loses, or it may win only by adding noisy volume.

## Discovery split to avoid waiting 30 days

Use the historical dense-CLV window for discovery now:

- Full discovery window: `2026-04-13` through `2026-06-21`
- Optional split A: train/choose on `2026-04-13` through `2026-05-31`
- Optional split B: validate fixed winner on `2026-06-01` through `2026-06-21`

Do not call split-B proof production-grade if bet count is low. Use it to reduce variants before forward paper.

## Promotion gates

A variant can be marked `confirm_for_forward_paper` only if:

- same window/config comparison beats or cleanly improves the baseline;
- at least one headline config has `n_bets >= 100`;
- mean CLV is positive;
- CLV/ranker diagnostics are not inverted;
- paired-bet diagnostics explain the mechanism;
- quote-clean/dropout path is clean for finalist configs;
- no data coverage or train/serve default-only issue exists.

A variant cannot be marked live/Kelly/Kalshi-ready from this loop alone. Forward paper still needs +30 calendar days or +200 under-only bets, then Phase 1B CLV/ranker diagnostics.

## Output summary template after each run

```text
Pitcher K ablation result
- Track: A load-bearing / B feature-family
- Variant/family/features:
- Artifact dir:
- Sweep dir:
- Window:
- Cal cutoff:
- Line source / quote policy:
- Best raw under config, n>=100:
- Best BL under config, n>=100:
- Mean CLV:
- Spearman(score, CLV) and CI:
- Paired-bet mechanism:
- Triage: Confirm / Shelf / Exclude
- Deploy status: no live, no Kelly, no Kalshi
- Next action:
```

## Non-goals

- Do not run live/Kelly/Kalshi from this plan.
- Do not tune every ablation variant; fixed hyperparams keep family contribution apples-to-apples.
- Do not launch broad historical rescrapes/backfills as part of this loop without separate approval.
- Do not revive copula/survival/decomposition until cheap direct-model tests show a concrete need.
- Do not expand Phase 3B beyond its five-feature family during the first pass.
- Do not interpret selector output as the final decision.
