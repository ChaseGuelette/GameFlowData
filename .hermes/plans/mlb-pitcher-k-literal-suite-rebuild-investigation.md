# MLB Pitcher K Literal Training-Suite / File Architecture Investigation

Date: 2026-06-07
Scope: not model architecture/math. This is about the literal files we run: training entrypoints, wrappers, audit/ranker/book scripts, and how generalizable the batter_hits files are to pitcher_strikeouts without creating another duplicated suite.

## Executive answer

Yes: pitcher K is still in a detached/older training-suite shape.

The current batter_hits operational files are only partially generalizable:

- The generic backtest/audit/ranker/book-analysis core is mostly reusable.
- The two batter_hits PowerShell run files are not generalizable; they hard-code batter_hits, hits training, artifact names, and run labels.
- The CLV analyzer is mostly stat-agnostic internally but is still named/wrapped as batter_hits and called that way by the audit suite.
- The training entrypoints are duplicated by sport/stat family: `mlb_train_pipeline.py` for pitcher K and `mlb_batter_train_pipeline.py` for batters. They share the same high-level train lifecycle, but each owns its own run-dir lifecycle, metadata writers, selector logic, calibration, and CLI. That is real tech debt.

So the right move is not “copy the batter_hits files and rename them pitcher K.” That would reproduce the tech debt. The better move is a small rebuild/refactor of the training suite and operational runner layer before doing expensive pitcher K training.

## File-level findings

### 1. Pitcher K training entrypoint is its own detached orchestrator

File:
- `src/models/mlb/mlb_train_pipeline.py`

Main class:
- `MLBTrainingOrchestrator`, about 657 lines inside an 837-line file.

Current responsibilities in one class:
- DB engine and `MLBFeatureStore` construction
- training/calibration data loading through `PitcherTrainingLoader`
- matchup and interaction enrichment via feature-store private-ish methods
- hard-coded pitcher K ablation variants
- per-quantile feature selection
- optional IP-feature-source model
- optional older copula branch
- hyperparameter tuning
- quantile model training
- calibration offsets
- calibration report
- Monte Carlo sanity check
- artifact saving and run-dir finalization
- CLI parsing

This is a complete, separate training suite. It is not a thin stat config plugged into a shared MLB training runner.

### 2. Batter training entrypoint is also its own large orchestrator

File:
- `src/models/mlb/mlb_batter_train_pipeline.py`

Main class:
- `MLBBatterTrainingOrchestrator`, about 977 lines inside an 1116-line file.

It has the same lifecycle shape as pitcher K:
- construct engine/store/loader
- load train/cal data
- enrich features
- select features
- train model
- calibrate/evaluate
- save manifests/metadata
- finalize run directory
- CLI parse

But it has newer operational controls that pitcher K lacks:
- `--exclude-prop-line`
- `--force-include-families`
- `--force-exclude-families`
- `--force-include-features`
- `--force-exclude-features`
- richer run metadata and comparison rules
- feature experiment metadata

The duplicated lifecycle plus asymmetric feature controls is the root problem.

### 3. Feature stores have an active facade + legacy implementation split

Files:
- `src/models/mlb/mlb_feature_store.py` — thin pitcher facade, 38 lines
- `src/models/mlb/mlb_batter_feature_store.py` — thin batter facade, 48 lines
- `src/models/mlb/features/legacy_pitcher_feature_store.py` — 1595 lines
- `src/models/mlb/features/legacy_batter_feature_store.py` — 1334 lines

Both current facades explicitly say the implementation remains in legacy files while callers finish migrating to explicit loaders.

This matters because the training pipelines still call methods like:
- pitcher: `enrich_with_matchup_features`, `_add_interaction_features`
- batter: `enrich_with_matchup_features`, `_add_batter_interaction_features`

Those are not clean shared training interfaces. They are legacy-store method coupling. That supports your memory that the suite is detached/duplicated.

### 4. Training loaders exist but are very thin

Files:
- `src/models/mlb/features/pitcher_training_loader.py` — 15 lines
- `src/models/mlb/features/batter_training_loader.py` — 15 lines
- `src/models/mlb/features/pitcher_inference_loader.py` — 28 lines
- `src/models/mlb/features/batter_inference_loader.py` — 36 lines

These are good seeds for a cleaner interface, but today they mainly wrap legacy feature-store methods. The real orchestration still lives in the big training files.

### 5. Contracts are partly centralized, but pitcher feature-family experiment controls are missing

File:
- `src/models/mlb/features/contracts.py`

Good:
- Pitcher feature lists are centralized:
  - `PITCHER_K_FEATURES`
  - `PITCHER_K_TRAINING_FEATURES`
  - `PITCHER_K_PHASE3A_REJECTED_FEATURES`
  - `PITCHER_K_PHASE3B_ADDED_FEATURES`
- Batter feature maps/families are centralized:
  - `BATTER_FEATURE_MAP`
  - `features_for_batter_families(...)`
  - `normalize_feature_family_names(...)`
  - `normalize_feature_names(...)`

Gap:
- There is no pitcher equivalent of batter family include/exclude helpers.
- Pitcher ablations are hard-coded in `mlb_train_pipeline.py` through `ABLATION_VARIANTS`, `L30_HOOK_FEATURES`, `PREDICTED_IP_FEATURES`, and `SINGLE_HOOK_ABLATION_FEATURES`.

So if we add pitcher feature-family testing by copying batter wrapper patterns, we will likely make the duplicated suite worse unless we first add a proper pitcher contract/profile layer.

## How generalizable are the batter_hits files?

### Not generalizable as-is

#### `scripts/run_batter_hits_family_ablation.ps1`
Hard-coded batter assumptions:
- training command uses `src\models\mlb\mlb_batter_train_pipeline.py --stat hits`
- sweep command uses `--stats batter_hits`
- output labels are `batter_hits_${Base}_${Mode}_...`
- accepted feature families are batter-specific:
  - `market`, `recent_form`, `contact_quality`, `matchup_pitcher`, `bullpen`, `platoon`, `environment`, `opportunity`

This should not be copied to `run_pitcher_k_family_ablation.ps1` as a near-duplicate unless we accept new tech debt.

#### `scripts/resume_batter_hits_ablation_audit.ps1`
Hard-coded batter assumptions:
- searches artifact dirs using `mlb_run_batter_hits_*`
- assumes batter_hits run labels and sweep layout

Also not generalizable as-is.

### Mostly reusable, but with bad naming/coupling

#### `scripts/analyze_mlb_batter_hits_clv.py`
Despite the name, core CLV logic is mostly stat-agnostic if the input CSV has the expected columns.

Hard-coded pieces:
- module/report says batter_hits
- `STAT_TO_MARKET_KEY = {"batter_hrr": "batter_hits_runs_rbis"}` only remaps HRR; pitcher_strikeouts passthrough would work
- fixed excluded-book policy
- fixed Phase 1B decision wording/thresholds

This should probably be renamed or wrapped as generic, not copied.

#### `scripts/run_mlb_quote_clean_audit_suite.py`
Good:
- has `--stats STATS [STATS ...]`
- supports dense CLV snapshots and quote-clean settings
- discovers `bets.csv`

Coupling:
- still invokes `scripts/analyze_mlb_batter_hits_clv.py` internally.

It is generic at the CLI layer, but not cleanly generic internally.

### Actually stat-agnostic

#### `src/backtesting/mlb/run_mlb_sweep.py`
Designed to take arbitrary `--stats`, and its default stats already include pitcher_strikeouts. It loads models through `MLBModelSuite`, builds pitcher/batter predictions separately, then hands all stats into common edge/sweep code.

#### `scripts/analyze_mlb_clv_ranking_diagnostics.py`
Post-hoc CSV analyzer. No meaningful stat lock found. It needs `clv_matches.csv` and optional candidate edge columns.

#### `scripts/analyze_mlb_clv_book_sensitivity.py`
Post-hoc audit-suite analyzer. No meaningful stat lock found; it assumes `clv/<config>/clv_matches.csv` shape.

## Root cause

The debt is two-layered:

1. Training layer debt:
   - pitcher and batter have separate large orchestrators that duplicate lifecycle and artifact behavior.
   - batter has newer controls; pitcher does not.
   - feature-store facades are thin while real behavior lives in legacy stores.

2. Operational runner layer debt:
   - batter_hits has mature PowerShell wrappers, but they are stat-specific wrappers, not profiles over a generic runner.
   - CLV/audit tools are halfway generic but still named/coupled to batter_hits.

That means “rebuild the pitcher K training suite” should mean: create a shared MLB training/experiment runner architecture, then port pitcher K onto it. Not “create another pitcher-specific copy of the batter_hits scripts.”

## Recommended rebuild path, avoiding tech debt

### Phase 0 — Do not copy batter_hits wrappers

Avoid adding these as straight copies:
- `scripts/run_pitcher_k_family_ablation.ps1` copied from `run_batter_hits_family_ablation.ps1`
- `scripts/resume_pitcher_k_ablation_audit.ps1` copied from `resume_batter_hits_ablation_audit.ps1`

A thin stat wrapper is fine, but it should call a shared generic runner.

### Phase 1 — Build a stat-profile driven operational runner

Add a generic runner script, not another one-off:

- `scripts/run_mlb_model_ablation.ps1` or `scripts/run_mlb_stat_ablation.ps1`

Profile inputs:
- `-StatKey` e.g. `batter_hits`, `pitcher_strikeouts`
- `-TrainEntrypoint` initially one of:
  - `src\models\mlb\mlb_batter_train_pipeline.py --stat hits`
  - `src\models\mlb\mlb_train_pipeline.py`
- `-ArtifactPattern`
  - batter: `mlb_run_batter_hits_*`
  - pitcher: `mlb_run_*`
- `-Direction`
  - batter_hits: both/controlled by experiment
  - pitcher_strikeouts: probably under for first pass
- `-BookRoutingPolicy`
- `-LineSource`
- `-QuoteDecisionPolicy`
- `-AuditOnly` / `-SkipTrain` / `-SkipSweep` / `-DryRun`

Then keep stat-specific files as thin wrappers only:
- `scripts/run_batter_hits_family_ablation.ps1` can eventually call the generic runner.
- `scripts/run_pitcher_k_ablation.ps1` should be a small profile wrapper, not a full duplicate.

### Phase 2 — Rename/generalize CLV analyzer boundary

Current problem:
- `run_mlb_quote_clean_audit_suite.py` calls `analyze_mlb_batter_hits_clv.py`.

Better:
- either rename to `scripts/analyze_mlb_clv.py`, or
- add a generic adapter and leave the old batter filename for compatibility.

Do not fork `analyze_mlb_pitcher_k_clv.py` unless there is truly pitcher-specific CLV logic. Current evidence says there is not.

Suggested compatibility approach:
- Create `scripts/analyze_mlb_clv.py` with the shared logic.
- Make `scripts/analyze_mlb_batter_hits_clv.py` a compatibility wrapper/import.
- Update `run_mlb_quote_clean_audit_suite.py` to call the generic script.

### Phase 3 — Add a shared MLB training orchestrator base

Create something like:
- `src/models/mlb/training/base_orchestrator.py`

Move shared lifecycle code out of the two large files:
- run directory creation and `_incomplete` finalization
- `_save_run_config`
- `_save_feature_manifest`
- `_save_calibration_report`
- `_save_training_metadata`
- common train/cal loading shell
- common forced include/exclude validation helpers where applicable

Keep stat-specific strategy classes/hooks for:
- pitcher quantile target `actual_so`
- pitcher optional IP/copy/correlated-source experiments
- batter binomial hits/AB model
- batter negbin stats
- stat-specific calibration reports

Do not try to make pitcher K use batter_hits model code. Share the training lifecycle, not the model objective.

### Phase 4 — Add stat contracts/profiles

Add a profile/config layer, likely under:
- `src/models/mlb/training/profiles.py`

Profile fields:
- stat key: `pitcher_strikeouts`, `batter_hits`
- train entry short stat: `hits` for batter, none or `pitcher_strikeouts` for pitcher
- target column(s)
- prop-line feature column
- default direction
- artifact model names
- allowed feature families
- default line source and book policy
- rejected feature families/locked-out features
- promotion-gate labels

This lets the runner and docs stop hard-coding stat assumptions.

### Phase 5 — Pitcher feature-family controls after the shared interface exists

Only then add pitcher K family include/exclude controls.

Do it in `contracts.py` / profile config, not by adding more hard-coded cases inside `mlb_train_pipeline.py`.

Likely pitcher families:
- `market`
- `workload_leash`
- `pitcher_stuff`
- `inning_fatigue`
- `opponent_contact`
- `environment`
- `phase3b_downside`
- `ip_feature_source`

Rules:
- Phase 3A rejected lineup/contact/umpire remains locked out by default.
- Forced includes fail loudly if missing/non-numeric.
- Selector output is not an ablation result.
- Promotion uses CLV/ranker/book gates.

## Recommended immediate implementation scope

First implementation should be “scaffold/refactor”, not training:

1. Add generic operational runner:
   - `scripts/run_mlb_stat_ablation.ps1`
2. Add thin pitcher wrapper:
   - `scripts/run_pitcher_k_ablation.ps1`
3. Add thin resume wrapper/profile:
   - `scripts/resume_mlb_stat_ablation_audit.ps1` or stat-profile support in one resume script
4. Genericize CLV analyzer name/boundary:
   - `scripts/analyze_mlb_clv.py`
   - keep old `analyze_mlb_batter_hits_clv.py` compatible
5. Add tests proving generic CLV/audit/ranker works with `pitcher_strikeouts` shaped rows.
6. Only after that, refactor training orchestrator base/profiles.

If we want the least disruption:
- Phase A: generic scripts + tests only.
- Phase B: shared training base/profile refactor.
- Phase C: pitcher feature-family controls.
- Phase D: run first current pitcher K frozen baseline.

## What not to do

Do not:
- copy batter_hits PowerShell wrappers and do a search/replace;
- fork `analyze_mlb_batter_hits_clv.py` into a pitcher-specific CLV analyzer unless a real stat-specific CLV rule appears;
- add more hard-coded pitcher variants to `mlb_train_pipeline.py` as the main experiment mechanism;
- train several new pitcher K artifacts before the runner/audit path is profile-driven and repeatable.

## Bottom line

The batter_hits files are reusable as patterns, not reusable as literal run files.

The reusable core is:
- `run_mlb_sweep.py`
- `run_mlb_quote_clean_audit_suite.py`, after CLV analyzer decoupling
- `analyze_mlb_clv_ranking_diagnostics.py`
- `analyze_mlb_clv_book_sensitivity.py`

The tech-debt danger is:
- `mlb_train_pipeline.py` and `mlb_batter_train_pipeline.py` are duplicated orchestration suites.
- Batter wrappers are stat-specific and should not be cloned.

Best next step: rebuild the operational suite around stat profiles, then use that to port pitcher K cleanly. After that, refactor the training entrypoints into a shared orchestrator + stat-specific strategy layer.
