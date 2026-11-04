
# Layer 4 — Training and artifact management

- Revision: 2026-11-03 / v1. Status: training/artifact design, not a trained model or launch authorization.
- Goal: fit comparable game-distribution candidates from frozen datasets and produce artifacts that cannot silently change their features, time boundaries or probability semantics.
- Architecture: model adapters implement one typed prediction contract; a trainer owns chronology and manifests; release approval is separate from fitting.

[Previous: features](03-feature-builders-and-feature-store.md) | [Shared contracts](../architecture-overview.md#shared-contracts) | [Next: prediction](05-prediction-and-betting-decisions.md)

## 1. Existing code: reuse patterns, not NBA behavior

The inspected `src/models/quantile_trainer.py` contains `QuantileModelConfig`, `QuantileModelSuite.train(X, y, feature_names_per_quantile, sample_weight=None)`, `predict_quantiles`, `save` and `load`. It uses separate `XGBRegressor(objective='reg:quantileerror', quantile_alpha=q)` estimators and cumulative-max noncrossing repair.

It also **computes conformal offsets during training and applies them during prediction**. Do not import this suite unchanged, subclass it with a fragile post-hoc offset reset, or fix the protected NBA implementation as a side effect. The proposed NCAAB adapter uses XGBoost directly and has no conformal-offset capability. The existing player-prop pipeline owns minutes/rate decomposition and is not a game-model interface.

`src/models/nba_artifacts.py` supplies useful manifest-last, digest inventory and `_incomplete` finalization patterns. Its validator requires NBA stats, minutes/rate architecture and NBA-specific files. Do not relax it to accept NCAAB; create the domain-specific bundle owner below.

## 2. Proposed files

| File | Classes / functions | Responsibility |
|---|---|---|
| `src/ncaab/models/base.py` | `GameDistributionModel`, `ModelFactory`, `ModelCapabilities` | Common adapter contract and supported-family registry |
| `src/ncaab/models/boosted.py` | `BoostedQuantileGameModel` | Margin and total quantile estimators; no calibration offsets |
| `src/ncaab/models/state.py` | `HierarchicalStateEstimator`, `StateParameterBundle` | Phase-2 fitted team-state parameters and forward filtering |
| `src/ncaab/models/neural.py` | `TeamFeatureMLP`, `RosterEncoder`, `NeuralMatchupModel` | Phase-2 optional neural backend; lazy optional imports |
| `src/ncaab/training/splits.py` | `SplitSpec`, `ChronologicalSplitBuilder` | Event/information cutoffs, outer folds and inner validation |
| `src/ncaab/training/trainer.py` | `NcaabTrainer`, `TrainingRunResult` | Dataset checks, fitting, diagnostics and finalization |
| `src/ncaab/training/artifacts.py` | `ArtifactManifest`, `ArtifactInspector`, `ArtifactWriter`, `load_model_bundle` | Immutable identities, digest checks, schema and forbidden-content checks |
| `src/ncaab/training/experiments.py` | `ExperimentSpec`, `ExperimentPlanner` | Small explicit candidate matrix; never an automatic full Cartesian search |
| `configs/ncaab/research.yaml` | Expanded research configuration | Frozen variants/features/history windows and resource limits |
| `requirements-ncaab.txt` | Initial dependency overlay | Existing runtime requirements plus declared PyArrow support |

Initial versions instantiate only the boosted backend. Phase-2 files are scoped extensions, not reasons to add heavyweight dependencies before the baseline. A dedicated `requirements-ncaab-neural.txt` and tested/pinned CPU PyTorch build are prerequisites for the neural phase; do not assume the workstation's GPU is supported. Fully Bayesian MCMC tooling is not a baseline dependency.

## 3. Model API

```python
class GameDistributionModel(Protocol):
    capabilities: ModelCapabilities
    def fit(self, train: TrainingDataset, validation: TrainingDataset,
            *, settings: ModelSettings) -> TrainingDiagnostics: ...
    def predict_quantiles(self, features: FeatureBatch) -> QuantilePrediction: ...
    def save_weights(self, directory: Path) -> list[ArtifactFile]: ...
    @classmethod
    def load_weights(cls, directory: Path, manifest: ArtifactManifest) -> GameDistributionModel: ...

class NcaabTrainer:
    def plan(self, experiment: ExperimentSpec) -> TrainingPlan: ...
    def fit(self, plan: TrainingPlan, *, execute: bool) -> TrainingRunResult: ...

class ArtifactInspector:
    def inspect(self, directory: Path, *, required_schema: str | None = None) -> ArtifactInspection: ...
```

`TrainingDataset` has an ordered numeric feature matrix, separate margin/total targets, row/context metadata, provenance, and optional roster tensors. `QuantilePrediction` is keyed by game/decision with margin/total arrays in the declared quantile order; it does not include betting prices. `ModelCapabilities` declares supported targets, optional roster/state requirements, and `supports_joint=False` for the initial independent marginal heads.

A factory accepts only a registered architecture ID. An unavailable backend produces a specific unsupported-dependency error, not a fallback to another model. Artifacts from other sports or unknown schema versions fail closed.

## 4. Initial boosted-tree configuration

Initial variants:

- `gbt_independent_v1` -> schema `team_box_v1`.
- `gbt_market_aware_v1` -> schema `team_box_market_v1`.

Both use the same eligible game/decision rows, target definitions, split policy and fixed estimator settings. The independent variant receives no market values. Its ability to operate on additional quote-missing games is reported separately, not used to quietly change the head-to-head training cohort.

Targets are final full-game home points minus away points (`margin`) and their sum (`total`), including supported overtime. Cancelled/unfinished/invalid results are not numerical zero labels.

Proposed research-control settings, not tuned or profitability-certified:

| Setting | Initial value |
|---|---|
| Quantiles | 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99 |
| Estimator | One XGBoost quantile regressor per target/quantile |
| Objective / tree method | `reg:quantileerror` / `hist` |
| Estimators / depth / learning rate | 400 / 3 / 0.05 |
| Subsample / column sample | 0.8 / 0.9 |
| Minimum child weight / L2 | 20 / 10 |
| Seed / worker ceiling | 42 / 4, configurable but frozen across a comparison |
| Feature selection / tuning | Disabled for initial discovery and family ablations |
| Early stopping | Off for fixed boosted baseline; inner validation remains diagnostic |
| Conformal offsets / BL / Kelly | Absent from this model path |

Use exact ordered arrays and validate the feature contract before calling XGBoost. Do not disable caller-level schema checks to work around dataframe/index compatibility. Persist XGBoost native model files, not an arbitrary executable pickle bundle.

Quantile predictions must be finite. Apply the declared `cumulative_max_v1` monotonic repair, recording raw crossing frequency and repair magnitude. This is not calibration. Nonphysical total outputs are blocked for investigation, not repaired with global offsets. Layer 5 owns conversion from quantiles to samples and probabilities.

## 5. Chronological experiment design

Collection depth is not the training window. Initial ending-year pool is 2022-2026; add earlier warm-up only if needed. Proposed folds:

| Purpose | Evaluation season | Two-season fit pool | Three-season fit pool |
|---|---:|---|---|
| Discovery/selection | 2025 | 2023, 2024 | 2022, 2023, 2024 |
| Independent confirmation | 2026 | 2024, 2025 | 2023, 2024, 2025 |

For each fit pool, reserve the last 20% of distinct chronological game dates as inner validation; do not split mirrored rows or games across partitions. Feature transforms are fitted only on the training portion. The boosted baseline reports inner validation without using the outer test; neural early stopping later uses the same inner boundary. Log actual row/date ranges rather than assuming every season is complete.

Use discovery to choose the history/feature/architecture policy, freeze it, then evaluate the independent season without choosing a new winner from that season. If that holdout has already been used to redesign the system, label it selection-contaminated and require another independent/forward window. No broad hyperparameter search per feature family.

A replay artifact may be physically trained today to simulate a historical fit. Keep actual `created_at` truthful while separately recording the simulated fit/selection information cutoffs. Do not require a retrospectively trained artifact to have existed historically, and do not backdate its creation. A prospective paper release must actually exist and be approved before its decisions.

Any generated state/roster feature with learned parameters must be prequential/cross-fitted within the training data. The model may not use full-period smoothed states, full-season player aggregates, closing lines or target-game participation. A training row's observed future label is allowed only in the separate target array and only inside the fit boundary.

## 6. State-model extension

First state challenger: a hierarchical dynamic estimator with league-level shrinkage and evolving team offense/defense/pace. Use an empirical-Bayes/MAP parameter fit with NumPy/SciPy and forward filtering as the inexpensive implementation. Label that approximation accurately; it is not a full joint posterior over all unknowns. A fully Bayesian backend remains a distinct later architecture.

The state specification includes:

- Offensive and defensive latent strengths with an identified league baseline/centering constraint.
- A pace state and home/neutral context; one shared pace observation per game, not duplicated independent pace evidence for both teams.
- Positive process/observation variances and uncertainty that evolves with elapsed time and games.
- Versioned season reset/carryover policy, estimated only on earlier data; unknown roster continuity must not become an invented continuity percentage.
- Observation inputs derived from prior box-score possessions/efficiencies, with explicit approximation metadata.

`fit_parameters(history, fit_context)` emits `StateParameterBundle`; `filter_before(game, context)` emits means/SDs; `update_after_game(result)` updates a forward state only after eligible observations. No daily MCMC/full refit is required to update inputs.

The layer-3 provider verifies parameter cutoffs, state revision and game ordering. A tree consuming these summaries still owns the final outcome distribution; do not convert state uncertainty directly into Gaussian cover probabilities. The state artifact can feed independent and aware branches only if its own inputs are market-independent.

## 7. Neural extension

First compare `TeamFeatureMLP` on the same numeric feature schema as the trees. Then add the roster representation; this separates predictor-family value from new input representation value.

Proposed roster network:

1. Player tensor `[batch, team_side, player, feature]`, boolean masks and expected-minute weights from layer 3.
2. Shared player MLP, initially two 64-unit layers with ELU.
3. Masked expected-minute-weighted mean and masked max pooling per team. Padding must never affect outputs; no order-dependent team representation.
4. Matchup input concatenates the two pooled representations, their difference/product, and game context. Optional state summaries are a separately tested family.
5. A compact matchup head emits `[batch, target, quantile]` outputs. Use the same named quantile grid and downstream monotonic/sampling contract.

Train with mean pinball loss across targets/quantiles, train-fitted scaling, Adam and bounded epoch/batch resources declared in the experiment config. Proposed initial neural controls are learning rate 0.001, batch size 64, maximum 50 epochs and inner-validation patience 5. These are starting controls, not optimized settings. Save CPU-loadable state dictionaries and all network dimensions; use the pinned backend's weights-only loading and `numpy.load(..., allow_pickle=False)` for numeric state bundles. Do not execute arbitrary serialized model classes or import a class path supplied by a manifest.

Optional dependencies are imported only when that architecture is selected. No player-ID memorization as the sole representation; evaluate unseen players and season transitions explicitly. Missing/ambiguous roster history blocks or labels the roster experiment instead of silently falling back to the tree model.

Temporal encoders, set attention, integrated Bayesian game prediction, market-residual targets and joint score/possession simulation remain later adapter variants. They must satisfy the same artifact, chronology and output capability contracts. Do not build an unrestricted architecture zoo before a controlled comparison exists.

## 8. Artifact bundle contract

Artifact type: `ncaab_game_distribution_bundle`; manifest schema version 1. Proposed layout beneath `data_root/ncaab/artifacts/<run_id>/`:

```text
artifact_manifest.json
run_config.json
feature_schema.json
split_manifest.json
source_manifest.json
training_diagnostics.json
validation_metrics.json
models/margin/q01.ubj ... q99.ubj
models/total/q01.ubj ... q99.ubj
optional/preprocessing.json
optional/state_parameters.npz
optional/neural_state.pt
```

The optional paths exist only for a declared capability. The manifest lists every required file/hash/size; no silent optional fallback. It records architecture/variant/targets, ordered features and hash, quantiles, units, sampler/repair policy, fit and selection cutoffs, data mode/grade, source/dataset/config hashes, actual code revision and dirty-state evidence, Python/library versions, seed/resources and diagnostics.

Write into `<run_id>_incomplete`, verify every file and a load/predict round trip, write the manifest last, then atomically finalize without overwriting an existing bundle. Only after success may `ncaab_model_runs` become complete. Do not commit large NCAAB training outputs into Git or reuse a mutable global `production` directory as identity.

Reject nonzero calibration-offset fields/files, unknown target units, missing features, digest tampering, incomplete directories, unsupported architectures and mismatched metadata. Never loosen NBA artifact validation to admit this bundle.

## 9. Registration and release lifecycle

Training can produce a local artifact without mutating the canonical registry. Registration is a separate explicit operation that verifies the durable ref and digest. `ncaab_model_runs` records identity; `ncaab_model_releases` records reviewed research/paper stage and policy.

Stages are `research_evaluated`, `paper_approved`, `retired`; there is no automatic live-betting stage. Paper approval requires an explicit owner decision, functioning current inputs, artifact integrity and a documented evidence classification. It does not assert long-run profitability. A release pointer is resolved once per run and pinned in saved outputs.

## 10. CLI, implementation and tests

Planned commands only:

```powershell
.\venv\Scripts\python.exe -m src.ncaab.cli train --config configs/ncaab/research.yaml --fold discovery --variant gbt_independent_v1 --train-window-seasons 2 --dry-run
.\venv\Scripts\python.exe -m src.ncaab.cli validate-artifact --artifact-dir C:\GameFlowDataArtifacts\ncaab\artifacts\example-run
```

The artifact path above is an illustrative future caller-supplied path, not an existing artifact. Training defaults to planning and requires `--execute`; Chase launches long fits. No dataset downloads or schema migrations happen implicitly during training.

Implement split/dataset validation, boosted adapter without offsets, manifest writer/inspector, round-trip prediction, explicit experiment planning, then registry/release integration. State and neural implementations follow as separate bounded phases.

Proposed tests: `tests/ncaab/test_training_splits.py`, `test_boosted_model.py`, `test_artifact_contract.py`, `test_training_lifecycle.py`, `test_neural_contract.py`, `test_state_model_contract.py`.

Acceptance: exact feature order, no future fit/selection information, paired variant cohort equality, no auto-pruned columns, no conformal behavior, monotonic finite outputs, deterministic bounded fixtures, tamper/legacy rejection, no half-written loadable artifacts, CPU round trip, and distinct actual creation versus historical fit cutoffs. Test tiny controlled models only in the future implementation suite; this design task launches none.
