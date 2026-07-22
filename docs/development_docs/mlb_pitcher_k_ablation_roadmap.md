# MLB Pitcher K Ablation Roadmap

Date: 2026-07-18
Status: canonical active roadmap

This is the single operating plan for `pitcher_strikeouts` model ablations. Older Pitcher K planning documents remain historical evidence but are superseded where they conflict with this roadmap.

## Current certified state

### Baseline artifact

- Artifact: `src/models/mlb/artifacts/baselines/pitcher_strikeouts_phase2_slice7_none_20260621/mlb_run_20260621_170841`
- Train seasons: 2024-2025
- Calibration season/cutoff: 2026 through 2026-04-12
- Historical validation window: 2026-04-13 through 2026-06-21
- Quote source: `mlb_player_props_clv_snapshots`
- Quote policy: `slate_or_tminus`, T-60 fallback
- Routing: `preferred_book_first`
- Primary direction: under only

### Frozen flat-paper candidate

- BL: `tau=0.5`, `z_max=0.25`, `max_weight=0.50`
- Edge threshold: `0.02`
- Stake for validation/paper: flat $100
- Bets: 146
- Record: 91-55
- ROI: +17.19%
- Sharpe: 2.34
- Max drawdown: 5.32%
- Mean implied-probability CLV: +1.2455%
- Mean CLV CI: [+0.8298%, +1.7102%]
- Dropout/timing audit: PASS
- Cutoff violations: 0
- Commence violations: 0
- Edge-to-CLV Spearman: +0.0650; CI low: -0.1508

Interpretation: confirmed for flat-paper tracking. Edge is a threshold/filter only. Kelly, edge-sized staking, live betting, and Kalshi remain blocked because edge magnitude does not rank CLV reliably.

Primary evidence:

- `backtest_results/mlb_pitcher_k_fresh_slice7_denseclv_bl_under_20260413_20260621/audit_suite_full_config_01/suite_summary.md`
- `backtest_results/mlb_pitcher_k_fresh_slice7_denseclv_bl_under_20260413_20260621/audit_suite_full_config_01/dropout_audit/audit_summary.md`

## Non-negotiable methodology

1. Probabilities use empirical CDF: `(samples > line).mean()`.
2. Do not globally recalibrate or conformal-shift low-tail/Q10 behavior.
3. Feature selection is diagnostic, not an ablation. Use force-include/force-exclude or named variants.
4. Validate correlated features as families before isolating individual features.
5. Use fixed baseline hyperparameters during ablation discovery. Tuning every variant confounds family contribution with search luck and increases cost; tune only a surviving finalist.
6. Use quote-clean, game-specific, paired Over/Under lines with explicit decision-time policy.
7. Require at least 100 bets for a decision-grade headline configuration.
8. Evaluate ROI, Sharpe, drawdown, CLV, ranker confidence, side splits, and paired-bet behavior together.
9. Historical discovery may proceed now, but live/Kelly/Kalshi promotion requires forward paper evidence.
10. Do not escalate to survival/copula/decomposition until cheap direct-model experiments expose a specific unresolved failure mode.

## Gate 0 — data readiness for each evaluation window

The 2026-04-13 through 2026-06-21 dense-CLV window is certified. Before extending into July or later, verify both remote and local `mlb_player_props_clv_snapshots` for `market_key='pitcher_strikeouts'`:

- requested and inserted date range;
- rows by game date, `scrape_reason`, and `target_offset_minutes`;
- non-null `game_id` and `player_id` coverage;
- paired Over/Under availability;
- selected snapshot strictly before commence;
- local coverage matches remote coverage for the target window.

A collection gap is repaired in the existing dense snapshot table; do not create a pitcher-specific table. Use bounded date/ID probes before any scrape, linker continuation, or local sync. Do not launch a broad historical rescrape merely because some predictions have no executable quote.

### July readiness verification — 2026-07-18

Read-only local/remote verification found:

- Remote collection is active through 2026-07-18; local is current through 2026-07-16.
- Remote has 26,578 rows since 2026-06-22; local has 24,727 and is missing the 1,851 remote rows from July 17-18.
- Recent game linkage is 100% in both databases.
- Recent player linkage is 94.00% remote and 93.70% local.
- Recent paired Over/Under quote-group coverage is 87.17% remote and 87.46% local.
- Recent post-commence violations: 0 local and 0 remote.
- July collection contains only `scrape_reason=close_t_minus_30` with `target_offset_minutes=30`. It does not contain the T-60/T-15/T-5 or fixed/slate snapshots needed to reproduce the certified `slate_or_tminus` T-60 evaluation policy.
- The July 13-15 absence is present in both databases and aligns with the MLB All-Star break; it is not a local sync-only gap.

Decision:

- Ongoing collection and temporal integrity: PASS.
- July apples-to-apples dense validation against the certified Slice 7 policy: BLOCKED by single-offset T-30 coverage.
- Local forward-window replay: BLOCKED until a targeted remote-to-local sync includes July 17-18.
- ID-based analysis remains usable but incomplete until the recent ~6% player-unlinked rows are diagnosed/linked.

Before a July extension, choose and preregister one path:

1. Preferred apples-to-apples path: estimate the missing July T-60/T-15 grid scope and Odds API credit cost, then run a targeted resumable backfill, bounded linker, and targeted local sync after approval.
2. Cheaper separate-policy path: validate July as a new T-30-only forward policy. Keep it separate from the Slice 7 `slate_or_tminus` T-60 comparison and do not pool the results as one window.

Audit evidence is currently stored under `.hermes/tmp/audit_pitcher_k_clv_july2026.*`; promote a concise report if this July gate becomes part of finalist certification.

## Track A — load-bearing baseline map

Goal: determine which existing feature families carry the profitable under edge and which add noise.

Run one force-exclude family at a time against the same artifact lifecycle, window, quote policy, under direction, flat stake, and sweep grid.

Order and current status:

1. `workload_leash` — confirmed load-bearing; retain
2. `market` — confirmed load-bearing; retain
3. `team_hook` — confirmed load-bearing; retain; exclusion config #63 shelved as a flat-paper-only challenger
4. `pitcher_stuff` — confirmed load-bearing; retain; exclusion rejected after the full YAML lifecycle
5. `inning_fatigue` — next Track A family
6. `opponent_contact`
7. `environment`

Standard command shape:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode exclude -Families <FAMILY> -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag load_bearing_exclude_<FAMILY>_slice7
```

The wrapper's first sweep is raw/no-BL. Every plausible artifact must then receive the focused BL sweep and CLV/ranker checks described under “Finalist evaluation.”

Track A interpretation:

- Exclusion worsens ROI/CLV/ranker or removes profitable baseline bets: family is load-bearing; retain it.
- Exclusion improves CLV/ranker without damaging stable winners: family is noisy/harmful; confirm with paired-bet analysis and an independent window.
- Exclusion changes volume but not quality: shelf for narrower subfamily tests.
- Only low-volume cells win: exploratory, not confirmed.

Do not run all Track A families simultaneously. Review each completed result before launching the next so failed runner/data assumptions do not multiply across long jobs.

## Track B — targeted pitcher-side downside

Start after Track A establishes the load-bearing map, unless a Track A result directly invalidates the premise.

Primary family: `phase3b_downside`

Exactly five features:

- `manager_starter_short_hook_rate_l30`
- `pitcher_pct_starts_under_5_ip_l10`
- `pitcher_fastball_velo_delta_l3_vs_szn`
- `team_bullpen_pitches_last_3d`
- `pitcher_left_last_start_early_flag`

Why this family: it targets short-start and degraded-stuff downside aligned with the under edge, without repeating Phase 3A's broad lineup/contact anchoring failure.

Before training, verify real non-default feature variation in 2024, 2025, and the 2026 calibration/evaluation rows. Default-only training data invalidates the experiment.

Command:

```powershell
.\scripts\run_pitcher_k_ablation.ps1 -Mode include -Families phase3b_downside -Start 2026-04-13 -End 2026-06-21 -CalEndDate 2026-04-12 -TrainSeasons 2024,2025 -Direction under -Edge 0.02,0.03,0.04,0.05,0.06,0.08 -FlatBet -LabelTag include_phase3b_downside_slice7
```

Only isolate individual features if the full family is promising or ambiguous. Do not expand beyond these five features in the first pass.

## Track C — named mechanism variants

Use these after Track A, or earlier only when a Track A result raises the exact mechanism question:

1. `static_no_l30` — tests whether recent L30 hook context overfits.
2. `hook_only` — tests whether hook context carries useful signal alone.
3. `hook_deep_start_l30` — isolates the previously most plausible hook proxy.
4. `ip_only` — cheap predicted-IP feature-source test.
5. `ip_hook` — tests predicted IP plus hook context.

These are narrow mechanism tests, not architecture promotions. The prior IP/hook artifacts are historical references; retrained comparisons must use the same cutoff, window, quote path, and flat-stake grid as the frozen baseline.

## Shelved and excluded paths

### Shelf

- Broad lineup/contact features from Phase 3A. They compressed or flipped profitable Phase 2 under bets. Revisit only if later paired diagnostics show opponent context is load-bearing and historical train/serve coverage is valid.
- Umpire features. Historical assignment/tendency coverage was too sparse for the earlier proposal.
- Market/reference scores as model inputs. First test them from saved artifacts as ranking/filter candidates.
- IL/role/opener features. Potentially useful, but they need reliable source coverage and should follow successful cheaper downside work.

### Exclude from the active queue

- Global conformal recalibration.
- Broad 20-30 feature batches.
- Copula/survival/decomposition without a direct-model failure diagnosis.
- Hand-crafted lineup/contact interactions that repeat Phase 3A.
- Hyperparameter tuning per discovery ablation.
- Over/both-direction promotion without an independent over-only gate.

## Finalist evaluation

For every Track A/B/C candidate that is plausible after its raw wrapper sweep, run a focused BL quote-clean sweep against the exact newly trained artifact:

```powershell
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --book-routing-policy preferred_book_first --model-dir <MODEL_DIR> --stats pitcher_strikeouts --direction under --start 2026-04-13 --end 2026-06-21 --tau 0.5 0.75 0.9 --edge 0.02 0.03 0.04 0.05 0.06 0.08 --z-max 0.25 0.5 --max-weight 0.50 0.65 0.80 --flat 100 --output-dir backtest_results\ablations\<LABEL>_quote_clean_bl_under_20260413_20260621
```

Then run CLV-only audit/ranker diagnostics for decision-grade configurations first. Run the full dropout audit only for finalists because the historical baseline window is already certified and full dropout work is expensive.

Required paired-bet buckets versus baseline:

1. same-side, similar edge;
2. same-side, compressed but still clears;
3. same-side, dropped below threshold;
4. flipped or direction-invalidated;
5. candidate-only added bets.

For each bucket report count, hit rate, flat ROI, profit/staked, baseline and candidate edge, edge delta, and CLV where available.

## Validation windows

### Historical discovery

- Full Slice 7: 2026-04-13 through 2026-06-21.
- Optional discovery split: 2026-04-13 through 2026-05-31.
- Fixed-winner historical check: 2026-06-01 through 2026-06-21.

The split-B winner must be preselected from split A; do not fish for a different configuration on split B. Low split-B volume is supporting evidence only.

### Forward paper

A historical finalist may enter flat-paper tracking when it improves the baseline, has at least 100 historical bets, positive mean CLV, non-inverted ranker behavior, clean quote timing, valid feature coverage, and an understood paired-bet mechanism.

Live/Kelly/Kalshi remain blocked until at least one of:

- 30 calendar days of forward paper evidence; or
- 200 new under-only paper bets;

followed by Phase 1B CLV/ranker diagnostics. Edge-sized staking additionally requires positive ranker confidence-low and monotonic quality buckets.

## Triage contract

### Confirm for forward paper

- Same-window comparison improves or cleanly preserves the baseline.
- At least one decision-grade configuration has 100+ bets.
- Mean CLV is positive.
- Ranker is not inverted.
- Paired-bet mechanism is understood.
- Data and train/serve coverage gates pass.

### Shelf

- Plausible mechanism but mixed or underpowered evidence.
- Positive result depends on a narrow slice or low-volume cell.
- Family-level result is ambiguous and needs a predeclared isolate.

### Exclude from active queue

- Worsens CLV/ranker or compresses stable baseline winners.
- Depends on legacy/non-quote-clean lines.
- Uses default-only or temporally invalid features.
- Adds complexity without reproducible downstream benefit.

## Result record

After each run, record:

- track and family/variant;
- exact artifact directory;
- training seasons and calibration cutoff;
- evaluation window and quote policy;
- best raw and BL under configurations with 100+ bets;
- ROI, Sharpe, drawdown, CLV, and ranker CI;
- paired-bet mechanism;
- Confirm / Shelf / Exclude;
- paper/live status;
- next action.

## Historical documents retained as evidence

- `mlb_pitcher_k_phase3a_lineup_contact_expansion.md`: completed rejected experiment and train/serve coverage lesson.
- `mlb_pitcher_k_phase3b_pitcher_extremes_roadmap.md`: source for the five-feature downside thesis; its old baseline gate and tune-every-variant sequence are superseded.
- `mlb_pitcher_k_quote_clean_validation_scope.md`: historical validation requirements now satisfied for Slice 7.
- `mlb_pitcher_k_frozen_baselines.md`: baseline-restoration history; stale “dense CLV missing” and “do not run variants” conclusions are superseded.
- `mlb_pitcher_k_ablation_iteration_pipeline.md`: initial wrapper/runbook; stale pre-backfill blockers and old named-variant-first ordering are superseded.
- `mlb_pitcher_k_two_track_ablation_plan.md`: immediate predecessor consolidated into this roadmap.
