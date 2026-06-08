# MLB Pitcher K Architecture Gap Investigation

Date: 2026-06-07
Scope: compare `pitcher_strikeouts` lane against current `batter_hits` lane for training suites, sweep suites, CLV/ranker/book-comparison validation, and experiment scaffolding.

## Executive conclusion

Chase's suspicion is correct: `pitcher_strikeouts` has some core model/training pieces and partial quote-clean validation support, but it is materially behind `batter_hits` as an iteration system.

Do not continue directly into new pitcher K training/ablation as if the batter_hits process already exists for pitcher K. The next work should be rescaffolding first: create pitcher-K-specific wrappers, frozen baselines, artifact interpretation, residual/CLV/ranker/book sensitivity workflow, and tests proving the generic validation scripts truly handle `pitcher_strikeouts`.

## Evidence reviewed

### Canonical/GBrain context
- `models/mlb-model`: pitcher K is still described as quantile model; batter pipeline is described as more complete with NLL feature selection and newer validation flow.
- `handoffs/handoff-081`: batter_hits has quote-clean audit suite, CLV/ranker diagnostics, book comparison, and a roadmap before feature expansion.
- `handoffs/handoff-093`: batter_hits has family ablation wrapper, resume audit wrapper, ranker-gate pass workflow, independent-window next gate.
- `docs/development_docs/mlb_pitcher_k_quote_clean_validation_scope.md`: pitcher K quote-clean validation was explicitly marked required before future feature work.
- `docs/development_docs/mlb_pitcher_k_phase3b_pitcher_extremes_roadmap.md`: pitcher K feature work was explicitly validation-gated, and Phase 3B should wait for Phase 2 baseline validation.
- `reports/mlb-pitcher-k-hook-ablation-hardening-2026-05-13.md`: older pitcher K quote-clean hardening exists but predates dense CLV/ranker/preferred-book workflow maturity.

### Repo paths reviewed
Batter lane:
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `scripts/run_batter_hits_family_ablation.ps1`
- `scripts/resume_batter_hits_ablation_audit.ps1`
- `docs/development_docs/mlb_batter_hits_ablation_iteration_pipeline.md`
- `docs/development_docs/mlb_batter_hits_frozen_baselines.md`
- `scripts/run_mlb_quote_clean_audit_suite.py`
- `scripts/analyze_mlb_clv_ranking_diagnostics.py`
- `scripts/analyze_mlb_clv_book_sensitivity.py`

Pitcher lane:
- `src/models/mlb/mlb_train_pipeline.py`
- `src/models/mlb/features/contracts.py`
- `docs/development_docs/mlb_pitcher_k_quote_clean_validation_scope.md`
- `docs/development_docs/mlb_pitcher_k_phase3a_lineup_contact_expansion.md`
- `docs/development_docs/mlb_pitcher_k_phase3b_pitcher_extremes_roadmap.md`
- `reports/mlb-pitcher-k-hook-ablation-hardening-2026-05-13.md`
- `scripts/diagnose_pitcher_k_ip_variance.py`

Validation run during investigation:
- `./venv/Scripts/python.exe -m py_compile src/models/mlb/mlb_train_pipeline.py src/models/mlb/features/contracts.py src/backtesting/mlb/run_mlb_sweep.py` passed.
- `run_mlb_sweep.py --help` shows current quote-clean/dense CLV/preferred-book flags.
- `run_mlb_quote_clean_audit_suite.py --help` shows generic `--stats` support.
- `analyze_mlb_clv_ranking_diagnostics.py --help` and `analyze_mlb_clv_book_sensitivity.py --help` are stat-agnostic at the CLI level.

## Side-by-side architecture comparison

| Layer | batter_hits current state | pitcher_strikeouts current state | Gap |
|---|---|---|---|
| Model form | Dedicated Binomial model for hits-in-AB, plus AB model/compound path in newer artifacts | Quantile direct SO model; optional old copula/IP-rate path; IP-as-feature hard-coded variants | Pitcher model is simpler/older and architecture decisions are partially embedded as ad-hoc variants |
| Training CLI | `mlb_batter_train_pipeline.py --stat hits` supports `--exclude-prop-line`, force include/exclude families, exact feature force include/exclude, metadata | `mlb_train_pipeline.py` supports only hard-coded `--ablation-variant` choices; no generic feature-family CLI | Pitcher lacks batter-style general feature-family experimentation controls |
| Feature families | `BATTER_FORCE_FEATURE_FAMILIES`, normalized family names, fail-loud forced feature checks | `PITCHER_K_FEATURES` / `PITCHER_K_TRAINING_FEATURES`; hard-coded L30 hook and predicted-IP groups only | Need pitcher feature-family registry and generic force include/exclude scaffolding |
| Experiment metadata | Run config records variant, prop-line exclusion, forced families/features, comparison rules; feature experiment metadata saved | Run config records ablation variant/cutoffs; training metadata records forced features and locked-out features; less explicit comparison/promotion rules | Need richer pitcher experiment metadata matching batter lane |
| Frozen baselines | `docs/development_docs/mlb_batter_hits_frozen_baselines.md` with artifact paths, configs, ROI, CLV, ranker, book concentration | No equivalent finalized pitcher K frozen baseline doc under current dense/preferred-book workflow | Need pitcher K frozen baseline doc before iteration |
| End-to-end wrapper | `scripts/run_batter_hits_family_ablation.ps1`: train -> preferred_book sweep -> CLV audit -> ranker -> summary | No pitcher equivalent. Only `scripts/diagnose_pitcher_k_ip_variance.py` exists | Major scaffold gap |
| Resume audit wrapper | `scripts/resume_batter_hits_ablation_audit.ps1` resumes CLV/ranker for existing sweeps | No pitcher equivalent | Gap |
| Sweep suite | Generic `run_mlb_sweep.py` supports pitcher stats, quote-clean, dense CLV source, preferred_book_first | Same generic harness likely usable, but no pitcher-specific wrapper/grid defaults/frozen configs | Need pitcher wrapper to prevent ad-hoc command drift |
| CLV audit suite | Generic suite exists and batter wrappers use it with selected decision-grade configs | Generic suite has CLI `--stats pitcher_strikeouts`, but pitcher-specific E2E coverage is not proven by wrapper/tests | Need stat-specific smoke/regression for pitcher K artifact/sweep shape |
| Ranker diagnostics | Generic ranker exists; batter pipeline invokes it per decision-grade config | Generic ranker should work if `bets.csv`/candidate edges contain required columns | Need pitcher wrapper and sample artifact validation |
| Book sensitivity | Generic book sensitivity exists, but docs/scripts were built around batter artifacts | Should work post-CLV; no pitcher-specific commands/reports | Need pitcher book-comparison phase and output interpretation |
| Residual diagnostics | `scripts/analyze_mlb_batter_hits_residuals.py` exists and roadmap says residual diagnostics before feature expansion | No matching pitcher residual/error bucketing script found | Need pitcher residual diagnostic before feature expansion |
| Tests | Tests exist for generic sweep/audit/ranker and batter CLV; many CLV tests are batter_hits-labeled | No obvious pitcher-specific tests for audit suite/ranker/book flow | Need tests proving `pitcher_strikeouts` path |

## Important nuance

The generic backtest/audit infrastructure is not completely batter-only anymore. These pieces appear stat-capable:
- `src/backtesting/mlb/run_mlb_sweep.py` accepts `--stats pitcher_strikeouts` and dense CLV/preferred-book flags.
- `scripts/run_mlb_quote_clean_audit_suite.py` accepts `--stats` and repeatable `--bets-csv`.
- `scripts/analyze_mlb_clv_ranking_diagnostics.py` is post-hoc CSV based and stat-agnostic.
- `scripts/analyze_mlb_clv_book_sensitivity.py` reads audit-suite CLV matches and is mostly stat-agnostic.

But the pitcher lane lacks the glue and guardrails that make this safe and repeatable:
- no pitcher-specific PowerShell wrapper;
- no frozen baseline doc using current dense CLV/preferred-book workflow;
- no pitcher-specific residual script;
- no tested interpretation report template;
- no independent-window gate sequence;
- no generic pitcher feature-family force include/exclude controls.

So the issue is not that every validation script must be rebuilt from scratch. The issue is that pitcher K has not been upgraded into the batter_hits validation operating system.

## Existing pitcher K state that should not be ignored

Pitcher K is not a blank slate:
- `mlb_train_pipeline.py` already trains from explicit `PITCHER_K_TRAINING_FEATURES`.
- Rejected Phase 3A lineup/contact/umpire features are locked out through `PITCHER_K_PHASE3A_REJECTED_FEATURES`.
- Phase 3B added features are required by the trainer.
- Hard-coded ablation variants exist for L30 hook and predicted-IP feature-source experiments.
- Older docs show quote-clean validation was started and one hook/deep-start quote-clean run looked promising.

But those are not enough for current promotion-grade iteration.

## Why direct training now is risky

If we train new pitcher K artifacts immediately, we will likely create artifacts that are hard to compare because:
1. There is no current frozen baseline with dense CLV, ranker, and book sensitivity metrics.
2. The existing `--ablation-variant` choices are not enough for broader feature-family testing.
3. The old pitcher K docs use older quote-clean assumptions (`fixed_et`, raw table, lowest-vig) while batter_hits now uses dense CLV snapshots, `slate_or_tminus`, and preferred-book routing for the current operational candidate path.
4. Phase 3A already showed plausible-looking lineup/contact features can hurt the actual contrarian-under edge. We need residual diagnostics before new features.
5. Ranker/CLV gates, not training metrics, should decide whether an iteration is useful.

## Recommended rescaffolding phases before more training

### Phase A — Pitcher K baseline + validation doc scaffolding
Deliverables:
- `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`
- `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md`
- Update existing `.hermes/plans/mlb-pitcher-k-refresh-lane.md` to point at these docs once created.

Content should mirror batter_hits but pitcher-specific:
- baseline artifact(s);
- train/calibration/eval windows;
- quote decision policy;
- dense line source;
- book-routing policy;
- under-only vs over-only posture;
- decision-grade volume threshold;
- CLV/ranker/book gates;
- no live/Kelly until ranker passes.

### Phase B — Pitcher K end-to-end wrapper
Add a PowerShell wrapper analogous to batter_hits:
- `scripts/run_pitcher_k_ablation.ps1`

Initial capabilities:
- run one candidate end-to-end: train -> compact quote-clean sweep -> CLV-only audit -> ranker diagnostics -> small markdown summary;
- support existing hard-coded variants first: `none`, `static_no_l30`, `hook_only`, `hook_deep_start_l30`, `ip_only`, `ip_hook`;
- use pitcher defaults: `--stats pitcher_strikeouts`, `--direction under`, dense CLV snapshots, `preferred_book_first`, `slate_or_tminus`, flat $100 first-pass;
- auto-discover decision-grade configs >=100 bets;
- do not run full dropout audit in the inner loop unless explicitly requested.

Add companion:
- `scripts/resume_pitcher_k_ablation_audit.ps1`

### Phase C — Prove generic validation scripts on pitcher K
Before trusting the wrapper, add/extend tests or fixtures:
- `tests/test_run_mlb_quote_clean_audit_suite.py`: include a pitcher_strikeouts-shaped `bets.csv`/candidate file fixture if practical.
- `tests/test_analyze_mlb_batter_hits_clv.py`: either rename/generalize later or add tests proving `STAT_TO_MARKET_KEY` leaves `pitcher_strikeouts` untouched and CLV matching works for K lines (e.g. 5.5/6.5).
- `tests/test_mlb_sweep_results.py` / edge metadata tests: ensure `bookmaker_candidate_edges.csv` columns needed by ranker are saved for pitcher stats too.

### Phase D — Generic pitcher feature-family controls
Only after baseline validation scaffolding exists, add batter-style controls to `mlb_train_pipeline.py`:
- `--force-include-families`
- `--force-exclude-families`
- `--force-include-features`
- `--force-exclude-features`

Add a pitcher registry in `src/models/mlb/features/contracts.py`, e.g.:
- `market`: `prop_line_pitcher_strikeouts`
- `workload_leash`: IP, pitch count, starts, short-start/stability, hook/deep-start
- `pitcher_stuff`: whiff, CSW, chase, zone, velo, pitch mix
- `inning_fatigue`: late whiff/CSW/velo, early K, pitches per inning, deep-inning pct
- `opponent_contact`: opponent K/whiff/contact/chase/zone-contact
- `environment`: park, home, total, weather
- `phase3b_downside`: current Phase 3B five-feature set
- `ip_feature_source`: predicted IP features when present

Rules:
- fail loud on missing forced features;
- keep Phase 3A rejected lineup/contact/umpire locked out unless explicitly force-included in a controlled experiment;
- persist all include/exclude controls to run metadata;
- do not re-enable copula as default.

### Phase E — Pitcher residual/error diagnostics
Create pitcher-specific residual script before new feature families:
- `scripts/analyze_mlb_pitcher_k_residuals.py`

It should consume saved sweep outputs and/or predictions, bucket by:
- existing workload/leash proxies;
- pitcher-side downside features;
- opponent K/contact proxies;
- book/routing/line bands;
- under-only hit rate and edge error.

Purpose: decide what feature family deserves the next ablation instead of adding plausible features blindly.

## Suggested immediate implementation scope

Do not start by editing model math. Start by adding scaffolding only:

1. Write pitcher K docs:
   - `docs/development_docs/mlb_pitcher_k_frozen_baselines.md`
   - `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md`
2. Add wrappers:
   - `scripts/run_pitcher_k_ablation.ps1`
   - `scripts/resume_pitcher_k_ablation_audit.ps1`
3. Add/adjust tests proving CLV/audit/ranker works for pitcher_strikeouts artifacts.
4. Only then run the first frozen baseline replay.
5. Only after a baseline is accepted, implement generic pitcher feature-family force controls.

## Recommended worker spec scope

This is likely implementation-worker sized. Allowed edit scope for the first worker should be docs + wrappers + tests only, not model training logic:
- `docs/development_docs/mlb_pitcher_k_frozen_baselines.md` (new)
- `docs/development_docs/mlb_pitcher_k_ablation_iteration_pipeline.md` (new)
- `scripts/run_pitcher_k_ablation.ps1` (new)
- `scripts/resume_pitcher_k_ablation_audit.ps1` (new)
- focused tests for generic audit/ranker pitcher-shaped inputs

Non-goals for first scaffold pass:
- no DB writes;
- no model retraining;
- no new feature families;
- no copula/survival work;
- no promotion/live trading recommendation.

Validation for first scaffold pass:
- PowerShell wrapper `-DryRun` should print valid commands.
- `run_mlb_quote_clean_audit_suite.py --help` still passes.
- Existing focused tests for sweep/audit/ranker pass.
- New pitcher-shaped tests pass.

## Bottom line

Pitcher K should be treated as validation-infrastructure debt, not merely a stale model artifact. It has enough core modeling code to train, but not enough current scaffolding to iterate safely. Build the pitcher K version of the batter_hits validation lane before spending long runs on training/ablation.
