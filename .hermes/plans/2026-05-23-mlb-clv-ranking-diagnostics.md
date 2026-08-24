# MLB CLV Ranking Diagnostics Implementation Plan

> **Lane status update (2026-07-27):** This is retained as an optional Kelly/sizing diagnostic,
> not a prerequisite for feature-family discovery, BL policy selection, flat certification, or
> flat forward paper. Mean CLV may still be inspected as nonblocking finalist evidence, but ranker
> CI and edge-bucket monotonicity must not revoke flat approval. See
> `.hermes/plans/2026-07-27_204057-flat-first-model-selection-lifecycle.md`.

> For Hermes: Use subagent-driven-development or the GameFlow implementation-worker lane to implement this plan task-by-task after Chase approval.

Goal: Build a quote-clean MLB CLV ranking diagnostic that tests whether any current score ranks CLV well enough to become a quality filter, and creates a path for adding better composite scores if no current score passes.

Architecture: Add a new post-hoc diagnostics script that consumes existing audit/CLV artifacts without retraining or DB reads. Keep `scripts/analyze_mlb_batter_hits_clv.py` as the CLV join/source of truth, reuse its block-bootstrap primitives, and write score-specific outputs under a separate diagnostics directory. Candidate/market-agreement scores are derived by joining `clv_matches.csv` to candidate edge artifacts when present.

Tech Stack: Python, pandas, scipy/stats rank correlation, existing GameFlow CLV CSV artifacts, pytest.

---

## Relevant prior lessons / invariants

- `operations/critical-invariants`: probabilities must remain empirical CDF from samples; do not introduce Gaussian CDF probability logic; no global recalibration offsets.
- `lessons/quote-clean-clv-before-feature-work`: positive mean CLV is not enough if edge does not rank CLV; do not size by raw edge until ranking evidence passes.
- `lessons/feature-selector-is-not-an-ablation`: later feature-set comparisons need force-include / force-exclude family tests, not selector-only claims.
- `lessons/correlated-feature-family-validation`: validate feature families as families before pruning correlated proxies.
- `lessons/cheap-baseline-before-architecture`: test cheap post-hoc ranker transforms before retraining or architecture work.

---

## Current-code findings

Existing reusable pieces:

- `scripts/analyze_mlb_batter_hits_clv.py`
  - Produces `clv_matches.csv`, `clv_summary.csv`, CLV by book/band/edge-bin, timing stability, and phase decision.
  - Has reusable `block_bootstrap_ci()` and Spearman helper, but `spearman_edge_clv()` is hardcoded to `edge`.
  - `clv_matches.csv` carries original bet columns plus CLV fields, including `edge`, `model_prob`, `implied_prob`, `bookmaker_at_bet`, `line_at_bet`, `odds_at_bet`, `bet_implied_prob`, `close_implied_prob`, `same_book_clv_cents`, and `clv_implied_prob`.

- `scripts/analyze_mlb_clv_alt_book_candidates.py`
  - Produces `all_candidate_book_edges.csv` and `alt_book_candidate_summary.csv` from `clv_matches.csv` + `raw_snapshots_used.csv`.
  - Best source for market-agreement / candidate-rank / preferred-book survival features.

- `scripts/analyze_mlb_clv_book_sensitivity.py`
  - Good for post-ranker book concentration slices, but currently reuses CLV summary logic with raw-edge assumptions.

- `scripts/diagnose_mlb_clv_failure_modes.py`
  - Good final classifier, but not designed for alternative rankers.

- `scripts/analyze_mlb_batter_hits_residuals.py`
  - Useful after ranking diagnostics; not the primary CLV-ranker tool.

Current latest audit signal:

- `backtest_results/mlb_batter_hits_dense_quote_clean_preferred_book_tminus60_20260413_20260517/audit_suite/suite_summary.md`
- Both selected configs fail because raw edge ranking CI low is <= 0, despite positive mean CLV.
- Config 02 has positive ROI / CLV but raw edge Spearman CI low is negative, so it is flat-threshold candidate only.

---

## Build decision

Create a new script rather than overloading the Phase 1B CLV join script:

- New: `scripts/analyze_mlb_clv_ranking_diagnostics.py`
- New tests: `tests/test_analyze_mlb_clv_ranking_diagnostics.py`

Rationale:

- CLV matching and audit gating are already complicated and should stay stable.
- Ranking diagnostics are post-hoc over existing `clv_matches.csv` and optional candidate artifacts.
- The new script can iterate fast without risking the quote-clean matcher.

---

## Acceptance criteria

The diagnostic is useful only if it answers these questions per config:

1. Which candidate score has the highest robust CLV rank relationship?
2. Does any score have block-bootstrap Spearman CI low > 0?
3. Does the score separate top vs bottom buckets by mean CLV?
4. Are score buckets monotonic enough to support quality filtering?
5. Does the apparent ranker survive simple book/odds/line slices?
6. If no score passes, what composite score ingredients should be tested next?

Primary output schema:

```text
score_name,n,n_scored,spearman,ci_low,ci_high,n_blocks,monotonic_bins,top_decile_mean_clv,bottom_decile_mean_clv,top_minus_bottom_clv,pass
```

Recommended pass rule for a first version:

- `n_scored >= 100`
- `ci_low > 0`
- `top_minus_bottom_clv > 0`
- top bucket mean CLV > bottom bucket mean CLV
- monotonic bins true OR at least top quartile > bottom quartile by a meaningful margin

Keep this as a diagnostic flag, not an automatic promotion gate.

---

## Task 1: Add the new script skeleton

Objective: Create a standalone post-hoc diagnostics CLI with no DB access.

Files:

- Create: `scripts/analyze_mlb_clv_ranking_diagnostics.py`
- Create: `tests/test_analyze_mlb_clv_ranking_diagnostics.py`

CLI arguments:

```text
--clv-matches-csv PATH        required
--output-dir PATH             required
--candidate-edges-csv PATH    optional, e.g. all_candidate_book_edges.csv
--bets-csv PATH               optional, only for extra selected-candidate metadata if needed
--score-set default|all        default default
--bootstrap-samples INT       default 1000
--ci-level FLOAT              default 0.95
--min-n INT                   default 100
--random-seed INT             default 42
```

Initial outputs:

- `ranking_score_summary.csv`
- `ranking_score_bins.csv`
- `ranking_score_slice_summary.csv`
- `ranking_score_recommendation.md`

Validation command:

```text
.\venv\Scripts\python.exe -m pytest tests/test_analyze_mlb_clv_ranking_diagnostics.py -q
```

---

## Task 2: Implement score registry and safe transforms

Objective: Centralize all ranker definitions so adding a score does not change the ranking engine.

Required built-in score columns / transforms:

- `raw_edge`: `edge`
- `abs_edge`: `abs(edge)`
- `model_prob`: `model_prob`
- `implied_prob`: prefer `implied_prob`, fallback to `bet_implied_prob`
- `logit_edge`: `logit(model_prob) - logit(implied_prob_or_bet_implied_prob)`
- `model_prob_x_abs_edge`: `model_prob * abs(edge)`
- `edge_zscore`: z-score of edge within config/file
- `plus_odds_band_score`: ordinal from odds bands, diagnostic only
- `line_score`: numeric line, diagnostic only
- `odds_at_bet_score`: numeric odds at bet, diagnostic only

Safety details:

- Clip probabilities to `[1e-6, 1 - 1e-6]` before logit.
- Do not create any model probabilities from Gaussian assumptions.
- Drop rows with missing score or missing `clv_implied_prob` for that score only.
- Preserve row counts per score.

Tests:

- logit score is finite at 0/1-like probabilities due to clipping.
- missing optional columns skip the score instead of crashing.
- `implied_prob` fallback to `bet_implied_prob` works.

---

## Task 3: Reuse block-bootstrap Spearman for arbitrary score columns

Objective: Generalize `Spearman(score, clv_implied_prob)` while preserving block bootstrap by `game_date` / `game_id`.

Implementation approach:

- Import `block_bootstrap_ci` from `scripts.analyze_mlb_batter_hits_clv` if safe.
- Add local `spearman_score_clv(df, score_col)` function.
- Use same `_block_col` semantics: `game_date` if present, else `game_id`, else row-level only if explicitly unavoidable.

Tests:

- perfect increasing score gets positive Spearman.
- reversed score gets negative Spearman.
- blocks are resampled as blocks, not rows.

---

## Task 4: Add quantile/decile bucket diagnostics

Objective: Determine whether a score separates high-quality from low-quality bets.

For each score:

- bucket into deciles when `n >= 100`, else quintiles when smaller.
- write one row per score/bucket to `ranking_score_bins.csv`.
- include count, mean score, min/max score, mean CLV, CLV CI, same-book share, top bookmaker share if available.

Monotonicity logic:

- Primary: Spearman(bucket_index, bucket_mean_clv) > 0.
- Strict monotonic flag: every upper bucket >= previous bucket with tolerance 0.
- Practical monotonic flag: top bucket > bottom bucket and top quartile > bottom quartile.

Tests:

- synthetic monotonic data passes.
- noisy but top-heavy data gets practical monotonic but not strict monotonic.
- flat/noisy data fails.

---

## Task 5: Add optional candidate/market-agreement score enrichment

Objective: If `all_candidate_book_edges.csv` exists, derive scores that test whether market agreement beats raw edge.

Candidate-derived scores to implement when columns are available:

- `candidate_best_edge`: max candidate edge for the bet side/line.
- `candidate_mean_edge`: mean candidate edge across books for same bet/line.
- `candidate_edge_survival_count`: number of books where edge clears configured threshold or is > 0.
- `preferred_edge_survival`: share or boolean for preferred books with surviving edge.
- `selected_candidate_rank`: rank of selected book among candidate edges; invert so higher score means better rank.
- `selected_vs_candidate_best_gap`: selected edge minus best candidate edge.
- `selected_vs_candidate_mean_gap`: selected edge minus mean candidate edge.
- `book_outlier_penalty`: negative absolute selected-vs-mean gap.

Important implementation note:

- First inspect actual candidate-edge columns before final coding. The candidate script is strict but column names may differ from the conceptual labels above.
- Join key should be stable: prefer `bet_id`; fallback to `player_id,game_id,market_key,line_at_bet,bookmaker_at_bet` only if necessary.

Tests:

- candidate rows aggregate to one row per bet.
- selected rank is stable with ties.
- missing candidate file leaves candidate scores out cleanly.

---

## Task 6: Add price-quality / market-tightness proxies

Objective: Test whether edge only works when the selected price is not isolated/off-market.

Scores / features:

- `candidate_book_count`: number of candidate books with same line/side.
- `candidate_line_agreement_count`: number of books with same line.
- `candidate_edge_std`: dispersion of candidate edge across books.
- `candidate_implied_prob_std`: dispersion of implied probabilities.
- `market_tightness_score`: inverse of dispersion, e.g. `-candidate_implied_prob_std`.
- `quality_composite_v1`: standardized blend of logit_edge + market agreement - outlier penalty.

Keep composites clearly labeled experimental.

Tests:

- more agreement gives higher tightness score.
- isolated one-book candidate is penalized.

---

## Task 7: Add slice summaries for ranker robustness

Objective: Catch rankers that work only because of one book/odds/line bucket.

Slices:

- overall
- by selected bookmaker
- plus-money vs not plus-money
- odds bands, if odds exist
- line buckets, if line exists
- same-book CLV only vs consensus fallback
- date blocks / week buckets if `game_date` exists

Output: `ranking_score_slice_summary.csv` with the same score summary fields plus slice metadata.

First version can restrict to the top 3 scores by overall CI/estimate to avoid huge output.

Tests:

- slices are created only when columns exist.
- low-n slices are marked underpowered, not pass/fail.

---

## Task 8: Generate a concise recommendation markdown

Objective: Make the output paste-readable for model decisions.

Report sections:

1. Input files and row counts.
2. Top score table by `ci_low`, then by `spearman`.
3. Pass/fail candidate rankers.
4. Top-vs-bottom bucket spread.
5. Robustness warnings: low n, single-book concentration, missing candidate data.
6. Recommendation:
   - `no_ranker_found_flat_only`
   - `candidate_quality_filter_found_no_kelly_yet`
   - `candidate_ranker_underpowered_collect_more_data`
   - `candidate_market_agreement_score_promising`
7. Next suggested experiment.

---

## Task 9: Wire into the audit suite without changing existing gates

Objective: Run the new diagnostic as an optional post-audit artifact.

Files likely to modify after standalone script passes:

- `scripts/run_mlb_quote_clean_audit_suite.py`

Behavior:

- Add optional flag: `--run-ranking-diagnostics` default true if `clv_matches.csv` exists.
- Pass each CLV config dir's `clv_matches.csv` to the new script.
- If `all_candidate_book_edges.csv` exists for that config, pass it too.
- Summarize top ranker in `suite_summary.md`, but do not turn it into a hard production gate yet.

Tests:

- audit suite still passes existing tests without candidate artifacts.
- ranking diagnostics failure should not hide CLV/dropout failures; it should be surfaced as its own return code/status.

---

## Task 10: Use ranker result to guide parallel feature-set model tests

Objective: Prepare for Chase's note that once a good CLV ranker exists, multiple feature-set models can be trained in parallel and compared against that ranker.

Do not implement model training in this script.

Add a documented comparison contract:

Each feature-set sweep/audit should emit:

- same validation window
- same quote-clean line source
- same selected bet policy
- same ranking diagnostics output
- same candidate artifact if available

Model comparison table should include:

```text
model_variant,feature_family,n_bets,roi,mean_clv_ci_low,best_ranker,best_ranker_ci_low,top_bottom_clv_spread,book_concentration,max_drawdown,decision
```

Important: Feature-set tests remain force-include / force-exclude family experiments, not selector-only claims.

---

## Recommended first run after implementation

Use the current preferred-book audit config outputs first, no retrain:

```text
.\venv\Scripts\python.exe scripts\analyze_mlb_clv_ranking_diagnostics.py --clv-matches-csv backtest_results\mlb_batter_hits_dense_quote_clean_preferred_book_tminus60_20260413_20260517\audit_suite\clv\config_02_tau0.9_edge0.05_kelly0.125\clv_matches.csv --candidate-edges-csv backtest_results\mlb_batter_hits_dense_quote_clean_preferred_book_tminus60_20260413_20260517\audit_suite\clv\config_02_tau0.9_edge0.05_kelly0.125\all_candidate_book_edges.csv --output-dir backtest_results\mlb_batter_hits_dense_quote_clean_preferred_book_tminus60_20260413_20260517\audit_suite\ranking_diagnostics\config_02_tau0.9_edge0.05_kelly0.125
```

If the candidate edge CSV is located under a different audit-suite subdirectory, discover it by exact filename inside that audit suite before running; do not scan all `backtest_results` broadly.

---

## Interpretation rules

If a current score passes:

- Treat it as a quality-filter candidate, not immediate Kelly sizing.
- Re-run audit with score buckets/tiered flat staking.
- Test whether top buckets improve CLV and ROI without creating single-book concentration.

If no current score passes:

- Keep current model as flat-threshold candidate only.
- Build `quality_composite_v1` from logit edge + market agreement + outlier penalty.
- If composite still fails, then start feature-family model variants and judge them by CLV ranking, not just ROI.

If candidate/market-agreement scores pass but raw edge fails:

- The issue is likely not model discrimination alone; it is selection/price-quality.
- Focus next work on book routing, same-line agreement, and stale/off-market quote avoidance before retraining.

---

## Non-goals

- No DB reads in the ranking script.
- No model retraining.
- No production promotion.
- No Kelly sizing promotion from this first diagnostic.
- No Gaussian probability approximations.
- No global recalibration offsets.
