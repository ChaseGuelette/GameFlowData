# 01 — Target Architecture and Software Patterns

## Goal

Define the rebuild architecture and the software patterns/best practices we are following.

## Target architecture

```text
scripts/
  run_mlb_stat_ablation.ps1              # generic profile-driven operational runner
  run_batter_hits_family_ablation.ps1    # compatibility shim/profile wrapper
  run_pitcher_k_ablation.ps1             # thin pitcher profile wrapper
  resume_mlb_stat_ablation_audit.ps1     # generic resume/audit runner
  analyze_mlb_clv.py                     # generic CLV analyzer
  analyze_mlb_batter_hits_clv.py         # compatibility shim

src/models/mlb/training/
  profiles.py                            # stat profiles and experiment profiles
  base_orchestrator.py                   # shared lifecycle/artifacts
  feature_controls.py                    # include/exclude resolution helpers
  artifacts.py                           # run dir, manifests, metadata helpers
  strategies.py                          # protocol/base classes for stat strategies

src/models/mlb/
  mlb_train_pipeline.py                  # pitcher CLI wrapper around shared base + PitcherKStrategy
  mlb_batter_train_pipeline.py           # batter CLI wrapper around shared base + BatterStrategy

src/models/mlb/features/
  contracts.py                           # feature lists/families remain canonical
  pitcher_training_loader.py             # public loader protocol implementation
  batter_training_loader.py              # public loader protocol implementation
```

## Architectural boundaries

### Shared lifecycle layer

Owns:

- run directory creation and `_incomplete` finalization;
- run config writing;
- feature manifest writing;
- calibration report writing;
- training metadata writing;
- common forced feature validation helpers;
- command/profile metadata;
- consistency checks and manifests.

Does not own:

- model objective;
- probability distribution choice;
- stat-specific calibration metrics;
- feature SQL;
- betting promotion decisions.

### Stat strategy layer

Owns:

- target columns;
- model factory/training call;
- stat-specific calibration report;
- sanity checks;
- default model artifact names;
- optional stat-specific submodels, e.g. pitcher IP feature source or batter AB model.

### Stat profile layer

Owns declarative configuration:

- `stat_key`, e.g. `pitcher_strikeouts`, `batter_hits`;
- `train_short_stat`, e.g. `hits` for batter pipeline compatibility;
- default betting direction;
- default quote-clean policy;
- default line source;
- default book routing;
- training artifact pattern;
- model artifact names;
- feature-family registry name;
- locked-out features/families;
- validation gates and minimum decision-grade volume.

### Operational runner layer

Owns:

- building PowerShell-friendly commands;
- train/sweep/audit/ranker/book-sensitivity sequencing;
- dry-run output;
- finding the right artifact and sweep dirs;
- decision-grade config filtering;
- writing small run summary markdown.

Does not own:

- feature selection internals;
- CLV math;
- model math;
- DB writes.

## Patterns and best practices

### 1. Strategy pattern

Use for model-family-specific behavior:

- `PitcherKQuantileStrategy`
- `BatterHitsBinomialStrategy`
- future `BatterNegBinStrategy`

Why: model math/calibration differs; lifecycle does not.

### 2. Profile/config object pattern

Use typed profiles instead of hard-coded script branches.

Why: avoids cloning wrappers and makes defaults visible/reviewable.

### 3. Template method pattern

`BaseMLBTrainingOrchestrator.run()` should define lifecycle order and call stat-specific hooks:

1. load train/cal data;
2. enrich features;
3. resolve features;
4. train model;
5. calibrate/evaluate;
6. save artifacts;
7. finalize.

Why: preserves sequence while enabling stat-specific implementations.

### 4. Adapter/facade pattern

Keep existing public entrypoints as compatibility shims:

- `mlb_train_pipeline.py`
- `mlb_batter_train_pipeline.py`
- `run_batter_hits_family_ablation.ps1`
- `analyze_mlb_batter_hits_clv.py`

Why: reduce migration risk and avoid breaking daily jobs/scripts.

### 5. Characterization tests before extraction

Before moving logic, tests should lock current behavior:

- artifact filenames;
- run config fields;
- feature include/exclude semantics;
- CLV matching output shape;
- sweep artifact output shape;
- model suite loading behavior.

Why: refactor should be behavior-preserving.

### 6. DRY but not over-abstracted

Share repeated lifecycle and command assembly. Do not force pitcher K and batter_hits into the same model objective abstraction beyond a strategy interface.

Why: avoids both duplication and fake generic abstractions.

### 7. YAGNI

Build profiles for `batter_hits` and `pitcher_strikeouts` first. Do not design a full plugin registry for every possible stat until needed.

`batter_hrr` / hits+runs+RBIs should be treated as a likely separate validation track, not automatically folded into the batter_hits/pitcher K migration slice. HRR is Kalshi-oriented and does not have the same sportsbook prop-line/CLV surface as batter_hits, so its profile and gates may need a separate rubric after the generic runner/profile scaffolding exists.

### 8. Explicit manifests over filename inference

New training runs should write a manifest with:

- stat key;
- model type;
- model artifact files;
- training entrypoint/profile;
- feature controls;
- validation contract notes;
- compatibility version.

Existing `MLBModelSuite` should keep filename fallback for legacy artifacts.

### 9. Compatibility-first deprecation

No deletion in first pass. Add generic path, route new workflows through it, then later retire old wrappers after repeated use.

## Non-goals

- Do not change model math in the scaffold/refactor phase.
- Do not promote pitcher K or batter_hits artifacts.
- Do not run long training/backtests as part of code refactor validation.
- Do not change database schema.
- Do not remove legacy feature-store files in this rebuild.
- Do not revive global conformal recalibration offsets.

## Invariants

- Probabilities use empirical CDF from samples: `(samples > line).mean()`.
- Pitcher K Phase 3A rejected lineup/contact/umpire features remain locked out by default.
- Feature selector output alone is never an ablation/promotion result.
- Promotion uses quote-clean replay, CLV CI, ranker/Spearman CI, volume, drawdown, and book concentration.
- No live/Kelly promotion until ranker gates pass.
- No broad DB/backfill/DDL work in this rebuild unless separately approved.
