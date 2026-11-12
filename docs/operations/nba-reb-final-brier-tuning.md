# NBA REB final-Brier tuning (owner runbook)

## Purpose and boundaries

This opt-in mode keeps the existing minutes × rate architecture and canonical five-head quantile fitting, but selects one shared REB-rate configuration by empirical final-REB over-probability Brier score at real pregame lines. The default `pinball` tuning path is unchanged.

`22025` is an external **tuning-validation/development** cohort once this command is run. It is not independent calibration or validation afterward. The October 21–November 2, 2026 window was already used for discovery and must not be described or used as untouched confirmation. No ROI, bet selection, market-probability benchmark, model promotion, or production write occurs here.

The mode freezes the parent minutes model, PTS/AST suites, all ordered feature lists, and parent copula. It replaces only the REB rate suite, uses normal loadable decomposition artifacts, preserves ordinary per-component offset behavior, and passes no global combined-calibration offsets to Monte Carlo.

## Verified source boundary

The canonical training loader materializes `prop_line_reb` from `nba_player_prop_feature_lines` using market literal `player_rebounds` and policy `t60_v1`, with `source_snapshot_time <= decision_time` and `decision_time = commence_time - 60 minutes`. Runtime scoring rejects missing, non-finite, zero, or negative lines rather than treating the loader's zero placeholder as a quote. It also excludes missing/invalid outcomes, nonpositive-minute void/DNP outcomes, and exact pushes with explicit counts. Pregame eligibility matches the reference: finite `player_avg_min_l5 >= 10`; realized minutes are not filtered at 10. Missing frozen model features and nonfinite/incomplete sampled distributions fail rather than silently substituting inputs. Progress logs identify the scoring cohort and report each 1,000 rows.

A bounded local preflight sampled October 21–23, 2025 and found 200/200 finite positive lines satisfying the clock policy, with at least 201 rows available in the October 21–November 7 bound. Receipt:

`C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-reb-brier-corrected-preflight-20261112T173856Z-9c6411ee/receipt.json`

This is sampled readiness, not full-cohort certification. The old zero-row query used the wrong market literal `rebounds` and is not availability evidence. The training command performs full loaded-cohort eligibility/count/date checks before any trial fit.

## Preflight (no DB load, model load, or fit)

Run from `C:\Users\Chase\Projects\GameFlowData`. The source is integrated into main, uncommitted; the absolute parent path preserves the exact research-control identity.

```powershell
C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe -X utf8 -m src.models.train_pipeline --local --base-model-dir C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_feature_ablations\game_market_free\nba_run_20261108_150945 --retrain-stats reb --tune --tuning-objective final_brier --tuning-trials 20 --tuning-match-final-features --train-seasons 22023 22024 --cal-season 22025 --artifacts-dir C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_reb_final_brier --dry-run
```

Confirm the JSON plan says:

- mode `partial`, objective `final_brier`, trials `20`, samples `5000`, seed `42`;
- exactly REB retraining, no minutes retraining, feature reselection, forced features, per-quantile tuning, or fixed hyperparameter file;
- the manifest-backed parent identity resolves successfully;
- output is disjoint from the parent and remains uncreated during preflight.

Preflight validates configuration and files only. It does not certify database readiness or successful fitting.

## Owner-launched training command

Agents are not authorized to launch this fit/search. After reviewing preflight and local data readiness, the owner may remove `--dry-run`:

```powershell
C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe -X utf8 -m src.models.train_pipeline --local --base-model-dir C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_feature_ablations\game_market_free\nba_run_20261108_150945 --retrain-stats reb --tune --tuning-objective final_brier --tuning-trials 20 --tuning-match-final-features --train-seasons 22023 22024 --cal-season 22025 --artifacts-dir C:\Users\Chase\Projects\GameFlowData\src\models\artifacts\nba_reb_final_brier
```

Twenty trials is a conservative starting budget, not an optimality claim. Defaults are 5,000 samples and seed 42. Explicit overrides are bounded to 100–20,000 samples and at most 100 trials.

## Runtime gates and outputs

Before fitting, the mode requires a non-empty training population and non-empty later development population, proves both the parent fit end and current training end precede the development start, requires the parent's REB copula, and validates the full scoring cohort. Estimator early stopping remains the canonical chronological inner training split; external outcomes are used only for candidate scoring.

Each candidate is trained through `PlayerPropsModelPipeline.train_rate_models(..., stats=["reb"], chronological_dates=True)`. Scoring prepares the validated/cohort-ordered rows and canonical float32 feature matrices once per study, predicts frozen minutes quantiles once in bounded 256-row batches, and reuses those minutes outputs for the parent and every trial. Every candidate still gets fresh batched REB quantile inference and a fresh canonical `MonteCarloPredictor` RNG initialized with the same seed. Sampling remains row-ordered and chunk boundaries do not reset RNG state; empirical `(samples > line).mean()`, sampled final-REB pinball, sample count, copula, tails, offsets, monotonicity and variance handling are unchanged. Global combined offsets remain disabled.

The public `score_final_reb_brier(...)` signature and CLI are unchanged. For bounded repeated scoring or the parent-run real-model timing check, use `prepare_final_reb_brier_scorer(..., batch_size=256)` once and call `.score(pipeline)` for the parent/candidates. The cache is memory-only and bound to the exact frozen minutes object, cohort and ordered per-quantile feature contracts; incompatible minutes/features and duplicate player/game/stat identities fail closed. It stores prepared feature/quantile matrices only, not season-scale Monte Carlo samples or model artifacts. `None` and empty-dict copula calls retain the legacy scalar branch semantics because the canonical scalar and batch methods intentionally test those values differently.

The owner-running process that loaded the pre-optimization module continues with that old in-memory code. Do not restart, hot-patch or cancel it; these changes apply only to a later process after integration.

The completed ordinary artifact includes:

- `best_hyperparams.json` — selected REB configuration;
- `run_config.json` — effective objective, external development role, parent reference, every completed trial's Brier and sampled final-REB pinball, best-Brier and best-pinball identities, chronology, cohort counts, search space, RNG, and sampler settings;
- unchanged ordered frozen component feature evidence and the frozen parent `copula_params.json`;
- ordinary minutes/PTS/REB/AST model files and manifest for existing NBA consumers.

No market benchmark is reported because paired prices are unavailable; the tuner never substitutes 0.5. If every candidate trails the frozen parent, `candidate_beats_parent` is false and the artifact must not be described as an improvement. Later independent confirmation with genuinely unused data remains required before any promotion decision.

## Delivery checks

Main exact-command dry-run, public partial-orchestrator controlled probe and 43 existing offline tests passed. The new scorer passes Ruff, compilation and Pyright (0 errors/warnings). Under the same Python/project context, the integrated pipeline has the same 31 errors/4 warnings as its untouched base; no diagnostic messages were introduced. Do not label the existing pipeline globally type-clean.

Independent review accepted the frozen worker implementation. Coordinator integration additionally preserved the ordinary partial-mode sanity path, rejected direct full-mode calls, matched pregame eligibility, and rejected missing frozen features/invalid samples. Those focused corrections passed main controlled checks. Real parent manifest chronology and copula metadata were checked without loading model binaries. The separate canonical-trainer probe verifies estimator-boundary behavior; its final receipt is tracked in the implementation plan. No agent launched real training, a full development-cohort DB load or a replay.

The scoring-speedup controlled probe matched the immutable scalar helper exactly for per-row Monte Carlo samples, probabilities, outcomes, Brier and sampled pinball across the parent and two distinct candidates, with missing values, calibration offsets, shuffled input and chunk boundaries. Its instrumented 7-row/3-candidate run prepared each feature matrix once (minutes and REB), made three frozen-minutes batch calls once, made zero minutes calls during repeated scoring, and made three fresh REB batch calls per candidate. Main also passed the canonical trainer/Optuna winner/final-fit probe, 43 existing checks, Ruff/compile/Pyright and unchanged owner-command preflight. Coordinator corrected raw-versus-normalized sample-dictionary keys for numeric-string IDs and restored progress reporting at batch crossings; the exact ID parity reproducer passed.

### Bounded real-model measurement

Parent benchmarked two existing saved REB suites with the same frozen minutes model, 32 explicitly synthetic feature/outcome rows and 5,000 samples. This was inference only, not training or predictive-quality evaluation. Old/new probabilities, outcomes, Brier, pinball and candidate ordering matched exactly; saved artifact and learned-booster hashes were unchanged. Median cold scoring was about19x faster, including preparation; shared-cache scoring was about21x faster. Warm checks used16-row chunks, exercising real-model batch boundaries. These short paired timings ran under concurrent machine load and are not a guaranteed full-season or full-training runtime reduction. See [the measured receipt](evidence/nba-reb-brier-batch-benchmark-2026-11-12.json).

The already-running owner job was not accessed, interrupted or hot-patched and continues using its loaded scalar implementation. Future invocations use the optimized scorer with the same command. At a fixed trial count/seed, the objective and candidate training are unchanged; an explicitly configured wall-clock timeout can allow more trials after a speedup, so timeout-limited searches need not select the same winner.
