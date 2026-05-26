# MLB Batter Hits Force Feature-Family Infrastructure Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Add explicit force-include / force-exclude infrastructure for MLB batter model feature-family experiments so ranker-driven retrains can validate feature families as families, not as isolated selector artifacts.

**Architecture:** Define a canonical batter feature-family registry near the existing feature contracts, wire the MLB batter trainer to accept family-level and individual feature overrides, and persist all experiment metadata into artifacts. The default training path must remain behavior-preserving when no new flags are provided.

**Tech Stack:** Python, argparse, pandas, pytest, GameFlowData MLB batter training pipeline, existing quote-clean sweep/audit/ranking diagnostics.

---

## Context

We are working on MLB batter_hits ranking diagnostics. The current ranker finding is that `selected_vs_candidate_mean_gap` / `execution_alpha` ranks CLV better than raw model edge or model probability. That enables controlled model/feature experiments, but the experiment infrastructure needs to validate correlated feature families properly.

Important correction from Chase:

- Prop-line vs no-prop-line has already been tested before.
- Props were fine.
- A fresh rerun can be used as a baseline, but `prop_line_batter_hits` should not be framed as an unresolved blocker by default.

Current trainer support:

- `--exclude-prop-line`
- `--feature-tolerance`

Missing support:

- `--force-include-families`
- `--force-exclude-families`
- `--force-include-features`
- `--force-exclude-features`
- artifact metadata explaining which features were forced, excluded, missing, and selected.

## Relevant prior lessons / invariants

- Feature selector is not an ablation: selector output is diagnostic only.
- Correlated feature family validation: force-include / force-exclude whole families before pruning individual members.
- Cheap baseline before architecture: build minimal trainer infrastructure before changing model architecture.
- Quote-clean CLV before feature work: evaluate variants via quote-clean replay, CLV, ranking diagnostics, drawdown, and volume.
- Empirical CDF only for Monte Carlo probabilities; no Gaussian CDF shortcuts.
- Q10 miscalibration is edge-bearing; do not globally correct it.

## Non-goals

- Do not change model architecture.
- Do not promote or copy artifacts to production.
- Do not change quote-clean replay behavior.
- Do not add SQL migrations or DB writes.
- Do not implement Kalshi-specific ranker changes in this plan.
- Do not run long retrains automatically as part of implementation validation.

## Design decision

Build both family-level and individual-feature controls, but make family-level the primary workflow.

Primary workflow:

```text
--force-include-families contact_quality,matchup_pitcher
--force-exclude-families market
```

Debug/surgical workflow:

```text
--force-include-features batter_xba_l10
--force-exclude-features prop_line_batter_hits
```

Why family-first:

- Feature families contain correlated/substitutable columns.
- Individual feature selection can drop a member while the family still adds downstream value.
- Families map directly to baseball hypotheses and CLV/ranker gates.

Why still support individual features:

- isolate suspicious/leaky/bad-default columns;
- prune winning families after the coarse test;
- reproduce exact one-off diagnostics;
- support rapid debugging without editing contracts.

---

## Target files

Modify:

- `src/models/mlb/features/contracts.py`
- `src/models/mlb/mlb_batter_train_pipeline.py`
- `tests/test_mlb_batter_train_pipeline_variants.py`

Optional docs update after implementation:

- `docs/development_docs/kalshi_sportsbook_reference_ranker_notes_2026-05-24.md`
- `.hermes/plans/2026-05-25-mlb-batter-hits-force-feature-family-infrastructure.md`

---

## Feature family registry

Add a batter-specific feature-family registry in `src/models/mlb/features/contracts.py` after `BATTER_FEATURE_MAP`.

Initial registry:

```python
BATTER_FORCE_FEATURE_FAMILIES: dict[str, tuple[str, ...]] = {
    "market": (
        "prop_line_batter_hits",
        "line_total",
    ),
    "recent_form": (
        "batter_avg_h_l5",
        "batter_avg_h_l10",
        "batter_avg_h_l20",
        "batter_avg_h_szn",
        "batter_h_l5_l10_ratio",
        "batter_std_h_l5",
    ),
    "contact_quality": (
        "batter_avg_exit_velocity_l5",
        "batter_avg_exit_velocity_l10",
        "batter_avg_launch_angle_l5",
        "batter_barrel_pct_l5",
        "batter_barrel_pct_l10",
        "batter_hard_hit_pct_l5",
        "batter_xba_l5",
        "batter_xba_l10",
        "batter_xslg_l5",
        "batter_xwoba_l5",
        "batter_zone_pct_l5",
        "batter_chase_pct_l5",
        "batter_whiff_pct_l5",
        "batter_gb_pct_l10",
        "batter_fb_pct_l10",
        "batter_babip_szn",
        "batter_hard_pct_szn",
    ),
    "matchup_pitcher": (
        "opp_pitcher_avg_era_l5",
        "opp_pitcher_avg_whip_l5",
        "opp_pitcher_avg_k_per_9_l5",
        "opp_pitcher_avg_bb_per_9_l5",
        "opp_pitcher_avg_h_allowed_l5",
        "opp_pitcher_avg_hr_allowed_l5",
        "opp_pitcher_xwoba_against_l5",
        "opp_pitcher_hard_hit_pct_against_l5",
        "opp_pitcher_avg_fastball_velo_l5",
        "opp_pitcher_days_rest",
        "opp_pitcher_babip_against_l5",
        "opp_pitcher_velo_drop_late_l5",
        "opp_pitcher_avg_pitches_per_inning_l5",
        "opp_pitcher_deep_inning_pct_l5",
    ),
    "bullpen": (
        "opp_bullpen_ip_last_3d",
        "opp_bullpen_era_last_7d",
        "opp_relievers_available",
        "opp_bullpen_pitches_last_3d",
    ),
    "platoon": (
        "is_same_hand",
        "batter_avg_h_vs_hand_l20",
        "batter_avg_ops_vs_hand_l20",
    ),
    "environment": (
        "park_hits_factor",
        "air_density_idx",
        "wind_out_mph",
        "has_precip",
        "is_home",
    ),
    "opportunity": (
        "lineup_position",
        "projected_ab",
        "batter_avg_ab_l5",
        "batter_avg_pa_l5",
        "batter_rest_days",
        "batter_games_last_7d",
        "batter_game_number",
    ),
}
```

Add helpers:

```python
def normalize_feature_family_names(names: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize CLI family names and fail on unknown names."""
    if not names:
        return []
    normalized: list[str] = []
    unknown: list[str] = []
    for raw in names:
        for part in str(raw).split(","):
            name = part.strip().lower().replace("-", "_")
            if not name:
                continue
            if name not in BATTER_FORCE_FEATURE_FAMILIES:
                unknown.append(name)
            elif name not in normalized:
                normalized.append(name)
    if unknown:
        valid = ", ".join(sorted(BATTER_FORCE_FEATURE_FAMILIES))
        raise ValueError(f"Unknown batter feature family/families: {unknown}. Valid: {valid}")
    return normalized


def normalize_feature_names(names: list[str] | tuple[str, ...] | None) -> list[str]:
    """Normalize comma-separated CLI feature names while preserving order."""
    if not names:
        return []
    normalized: list[str] = []
    for raw in names:
        for part in str(raw).split(","):
            name = part.strip()
            if name and name not in normalized:
                normalized.append(name)
    return normalized


def features_for_batter_families(families: list[str] | tuple[str, ...]) -> list[str]:
    """Expand family names to a de-duped feature list, preserving registry order."""
    expanded: list[str] = []
    for family in families:
        for feature in BATTER_FORCE_FEATURE_FAMILIES[family]:
            if feature not in expanded:
                expanded.append(feature)
    return expanded
```

---

## Candidate/required feature behavior

The trainer should resolve three things for a given `train_df.dtypes`:

1. `candidates_for_selector`
2. `required_features`
3. `excluded_features`

Rules:

1. Start with the current numeric candidates.
2. Existing structural exclusions remain:
   - `game_id`, `player_id`, `game_date`, `season`, `team_id`, `opp_team_id`, `actual`, `player_name`, `actual_at_bats`.
3. If `--exclude-prop-line` is set, exclude all `prop_line_*` columns. This legacy behavior remains.
4. Apply `--force-exclude-families` and `--force-exclude-features` next.
5. Expand `--force-include-families` and `--force-include-features` into `required_features`.
6. Required features must:
   - exist in the training dataframe;
   - be numeric;
   - not be structurally excluded;
   - not be excluded by any exclude flag.
7. Required features are removed from selector candidates so the selector does not duplicate them.
8. Final selected features are:

```python
selected_features = required_features + [f for f in selector_selected_features if f not in required_features]
```

9. If include and exclude conflict, fail loudly. Do not silently let include win.
10. If required features are missing or non-numeric, fail loudly by default.

---

## Task 1: Add feature-family registry tests

**Objective:** Lock family names, expansion, normalization, and unknown-family errors before implementation.

**Files:**

- Modify: `tests/test_mlb_batter_train_pipeline_variants.py`
- Modify later: `src/models/mlb/features/contracts.py`

**Step 1: Add failing tests**

Append tests similar to:

```python
import pytest
from src.models.mlb.features.contracts import (
    BATTER_FORCE_FEATURE_FAMILIES,
    features_for_batter_families,
    normalize_feature_family_names,
    normalize_feature_names,
)


def test_batter_force_feature_family_registry_contains_core_families():
    assert "contact_quality" in BATTER_FORCE_FEATURE_FAMILIES
    assert "matchup_pitcher" in BATTER_FORCE_FEATURE_FAMILIES
    assert "bullpen" in BATTER_FORCE_FEATURE_FAMILIES
    assert "environment" in BATTER_FORCE_FEATURE_FAMILIES
    assert "opportunity" in BATTER_FORCE_FEATURE_FAMILIES
    assert "market" in BATTER_FORCE_FEATURE_FAMILIES


def test_feature_family_expansion_preserves_order_and_dedupes():
    expanded = features_for_batter_families(["market", "environment"])
    assert expanded[:2] == ["prop_line_batter_hits", "line_total"]
    assert "park_hits_factor" in expanded
    assert len(expanded) == len(set(expanded))


def test_normalize_feature_family_names_accepts_commas_and_hyphens():
    assert normalize_feature_family_names(["contact-quality,matchup_pitcher"]) == [
        "contact_quality",
        "matchup_pitcher",
    ]


def test_normalize_feature_family_names_rejects_unknown_family():
    with pytest.raises(ValueError, match="Unknown batter feature family"):
        normalize_feature_family_names(["made_up_family"])


def test_normalize_feature_names_accepts_comma_lists():
    assert normalize_feature_names(["a,b", "c"]) == ["a", "b", "c"]
```

**Step 2: Run test to verify failure**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q
```

Expected: FAIL because registry/helper functions do not exist yet.

---

## Task 2: Implement feature-family registry and helpers

**Objective:** Add the canonical family registry and normalization helpers in the contracts module.

**Files:**

- Modify: `src/models/mlb/features/contracts.py`

**Step 1: Add registry and helper functions**

Use the registry and helper code from the “Feature family registry” section above.

**Step 2: Run tests**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q
```

Expected: Existing prop-line tests pass plus the new registry tests pass.

---

## Task 3: Add orchestrator constructor fields and CLI args

**Objective:** Wire new flags into `MLBBatterTrainingOrchestrator` without changing default behavior.

**Files:**

- Modify: `src/models/mlb/mlb_batter_train_pipeline.py`
- Test: `tests/test_mlb_batter_train_pipeline_variants.py`

**Step 1: Add failing tests**

Update `_make_orchestrator` to accept `**kwargs`:

```python
def _make_orchestrator(monkeypatch, tmp_path, exclude_prop_line=False, **kwargs):
    monkeypatch.setattr(pipeline, "get_engine", lambda local=False: _DummyEngine())
    return pipeline.MLBBatterTrainingOrchestrator(
        stat="hits",
        base_artifacts_dir=str(tmp_path),
        exclude_prop_line=exclude_prop_line,
        **kwargs,
    )
```

Add test:

```python
def test_orchestrator_normalizes_force_feature_controls(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["contact-quality,matchup_pitcher"],
        force_exclude_families=["market"],
        force_include_features=["batter_xba_l10"],
        force_exclude_features=["prop_line_batter_hits"],
    )

    assert orchestrator.force_include_families == ["contact_quality", "matchup_pitcher"]
    assert orchestrator.force_exclude_families == ["market"]
    assert orchestrator.force_include_features == ["batter_xba_l10"]
    assert orchestrator.force_exclude_features == ["prop_line_batter_hits"]
```

**Step 2: Implement constructor params**

In `MLBBatterTrainingOrchestrator.__init__`, add params:

```python
force_include_families: list[str] | None = None,
force_exclude_families: list[str] | None = None,
force_include_features: list[str] | None = None,
force_exclude_features: list[str] | None = None,
```

Import helpers from contracts:

```python
from src.models.mlb.features.contracts import (
    normalize_feature_family_names,
    normalize_feature_names,
    features_for_batter_families,
)
```

Set fields:

```python
self.force_include_families = normalize_feature_family_names(force_include_families)
self.force_exclude_families = normalize_feature_family_names(force_exclude_families)
self.force_include_features = normalize_feature_names(force_include_features)
self.force_exclude_features = normalize_feature_names(force_exclude_features)
```

**Step 3: Add argparse flags**

Add to the bottom CLI parser:

```python
parser.add_argument(
    "--force-include-families",
    nargs="*",
    default=None,
    help="Comma/space separated batter feature families to force include after selector filtering.",
)
parser.add_argument(
    "--force-exclude-families",
    nargs="*",
    default=None,
    help="Comma/space separated batter feature families to exclude from selector candidates.",
)
parser.add_argument(
    "--force-include-features",
    nargs="*",
    default=None,
    help="Comma/space separated exact feature names to force include.",
)
parser.add_argument(
    "--force-exclude-features",
    nargs="*",
    default=None,
    help="Comma/space separated exact feature names to exclude.",
)
```

Pass them into the orchestrator.

**Step 4: Run tests**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q
```

Expected: PASS.

---

## Task 4: Implement feature candidate/required resolution

**Objective:** Centralize feature controls so all training branches can use the same logic.

**Files:**

- Modify: `src/models/mlb/mlb_batter_train_pipeline.py`
- Test: `tests/test_mlb_batter_train_pipeline_variants.py`

**Step 1: Add tests**

Add tests:

```python
def test_force_exclude_family_removes_candidates(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_exclude_families=["contact_quality"],
    )

    candidates = orchestrator._numeric_model_feature_candidates(
        {
            "actual": "int64",
            "batter_avg_h_l5": "float64",
            "batter_xba_l10": "float64",
            "batter_barrel_pct_l10": "float64",
            "prop_line_batter_hits": "float64",
        }
    )

    assert "batter_avg_h_l5" in candidates
    assert "prop_line_batter_hits" in candidates
    assert "batter_xba_l10" not in candidates
    assert "batter_barrel_pct_l10" not in candidates


def test_force_include_family_returns_required_features(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["contact_quality"],
    )

    candidates, required = orchestrator._resolve_selector_candidates_and_required_features(
        {
            "actual": "int64",
            "batter_avg_h_l5": "float64",
            "batter_xba_l10": "float64",
            "batter_barrel_pct_l10": "float64",
        }
    )

    assert "batter_avg_h_l5" in candidates
    assert "batter_xba_l10" not in candidates
    assert "batter_barrel_pct_l10" not in candidates
    assert required == ["batter_barrel_pct_l10", "batter_xba_l10"] or set(required) == {
        "batter_xba_l10",
        "batter_barrel_pct_l10",
    }


def test_include_exclude_conflict_fails_loudly(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_features=["batter_xba_l10"],
        force_exclude_features=["batter_xba_l10"],
    )

    with pytest.raises(ValueError, match="both included and excluded"):
        orchestrator._resolve_selector_candidates_and_required_features(
            {"actual": "int64", "batter_xba_l10": "float64"}
        )


def test_missing_forced_feature_fails_loudly(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_features=["missing_feature"],
    )

    with pytest.raises(ValueError, match="Forced feature.*missing"):
        orchestrator._resolve_selector_candidates_and_required_features(
            {"actual": "int64", "batter_avg_h_l5": "float64"}
        )
```

**Step 2: Implement helper method**

Add to `MLBBatterTrainingOrchestrator`:

```python
_NUMERIC_DTYPES = {"float64", "float32", "int64", "int32"}
_STRUCTURAL_EXCLUDED_FEATURES = {
    "game_id", "player_id", "game_date", "season", "team_id",
    "opp_team_id", "actual", "player_name", "actual_at_bats",
}

def _dtype_items(self, dtypes) -> list[tuple[str, str]]:
    return [(str(c), str(dtype)) for c, dtype in (dtypes.items() if hasattr(dtypes, "items") else [])]


def _excluded_feature_set(self, dtypes, extra_excluded: set[str] | None = None) -> set[str]:
    items = self._dtype_items(dtypes)
    excluded = set(self._STRUCTURAL_EXCLUDED_FEATURES)
    if extra_excluded:
        excluded.update(extra_excluded)
    if self.exclude_prop_line:
        excluded.update({c for c, _ in items if c.startswith("prop_line_")})
    excluded.update(features_for_batter_families(self.force_exclude_families))
    excluded.update(self.force_exclude_features)
    return excluded


def _forced_feature_set(self) -> list[str]:
    forced: list[str] = []
    for feature in features_for_batter_families(self.force_include_families):
        if feature not in forced:
            forced.append(feature)
    for feature in self.force_include_features:
        if feature not in forced:
            forced.append(feature)
    return forced


def _resolve_selector_candidates_and_required_features(
    self,
    dtypes,
    extra_excluded: set[str] | None = None,
) -> tuple[list[str], list[str]]:
    items = self._dtype_items(dtypes)
    dtype_by_name = {name: dtype for name, dtype in items}
    excluded = self._excluded_feature_set(dtypes, extra_excluded=extra_excluded)
    forced = self._forced_feature_set()

    conflicts = [feature for feature in forced if feature in excluded]
    if conflicts:
        raise ValueError(f"Features cannot be both included and excluded: {conflicts}")

    missing = [feature for feature in forced if feature not in dtype_by_name]
    if missing:
        raise ValueError(f"Forced feature(s) missing from training dataframe: {missing}")

    non_numeric = [feature for feature in forced if dtype_by_name.get(feature) not in self._NUMERIC_DTYPES]
    if non_numeric:
        raise ValueError(f"Forced feature(s) are not numeric: {non_numeric}")

    required = [feature for feature in forced if feature not in excluded]
    candidates = [
        name for name, dtype in items
        if name not in excluded
        and name not in required
        and dtype in self._NUMERIC_DTYPES
    ]
    return candidates, required
```

Keep `_numeric_model_feature_candidates` as compatibility wrapper:

```python
def _numeric_model_feature_candidates(self, dtypes, extra_excluded=None) -> list[str]:
    candidates, _required = self._resolve_selector_candidates_and_required_features(
        dtypes,
        extra_excluded=extra_excluded,
    )
    return candidates
```

**Step 3: Run tests**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q
```

Expected: PASS.

---

## Task 5: Apply required features in training paths

**Objective:** Ensure force-included features bypass selector pruning in binomial and NegBin paths.

**Files:**

- Modify: `src/models/mlb/mlb_batter_train_pipeline.py`
- Test: `tests/test_mlb_batter_train_pipeline_variants.py`

**Step 1: Add small pure helper test**

Add a method or static helper:

```python
def _merge_required_and_selected_features(required: list[str], selected: list[str]) -> list[str]:
    merged: list[str] = []
    for feature in [*required, *selected]:
        if feature not in merged:
            merged.append(feature)
    return merged
```

Test:

```python
def test_required_features_precede_selector_features_and_dedupe():
    assert pipeline.MLBBatterTrainingOrchestrator._merge_required_and_selected_features(
        ["b", "a"],
        ["a", "c"],
    ) == ["b", "a", "c"]
```

**Step 2: Update binomial pipeline**

Where binomial candidates are built, replace:

```python
candidates = self._numeric_model_feature_candidates(...)
selected_features = selector.select_features_binomial_nll(...)
```

with:

```python
candidates, required_features = self._resolve_selector_candidates_and_required_features(...)
selector_selected = selector.select_features_binomial_nll(..., candidates, ...)
selected_features = self._merge_required_and_selected_features(required_features, selector_selected)
```

If the binomial path trains multiple feature sets/models, apply the same rule to the final feature set used by the model.

**Step 3: Update NegBin pipeline**

Replace candidate construction around lines currently near Step 3:

```python
candidates = self._numeric_model_feature_candidates(
    train_df.dtypes,
    extra_excluded=extra_excluded,
)
...
selected_features = selector.select_features_nll(...)
```

with:

```python
candidates, required_features = self._resolve_selector_candidates_and_required_features(
    train_df.dtypes,
    extra_excluded=extra_excluded,
)
...
selector_selected = selector.select_features_nll(...)
selected_features = self._merge_required_and_selected_features(required_features, selector_selected)
```

Preserve the existing `projected_ab` exposure exclusion for exposure stats. If `projected_ab` is structurally required as exposure, do not allow it as a learned feature for exposure stats.

**Step 4: Run focused tests**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py tests/test_mlb_batter_train_pipeline.py tests/test_mlb_batter_feature_store.py -q
```

Expected: PASS.

---

## Task 6: Persist experiment metadata

**Objective:** Make every artifact self-describing so downstream audit/ranker comparisons can identify forced-family variants.

**Files:**

- Modify: `src/models/mlb/mlb_batter_train_pipeline.py`
- Test: `tests/test_mlb_batter_train_pipeline_variants.py`

**Step 1: Add test for run_config metadata**

```python
def test_force_feature_metadata_written_to_run_config(monkeypatch, tmp_path):
    orchestrator = _make_orchestrator(
        monkeypatch,
        tmp_path,
        force_include_families=["contact_quality"],
        force_exclude_families=["market"],
        force_include_features=["batter_xba_l10"],
        force_exclude_features=["prop_line_batter_hits"],
    )

    orchestrator._save_run_config([2024, 2025], 2026, "2026-04-12")

    config = json.loads((orchestrator.run_dir / "run_config.json").read_text())
    assert config["force_include_families"] == ["contact_quality"]
    assert config["force_exclude_families"] == ["market"]
    assert config["force_include_features"] == ["batter_xba_l10"]
    assert config["force_exclude_features"] == ["prop_line_batter_hits"]
```

**Step 2: Update `_save_run_config` and `_save_training_metadata`**

Add fields:

```python
"force_include_families": self.force_include_families,
"force_exclude_families": self.force_exclude_families,
"force_include_features": self.force_include_features,
"force_exclude_features": self.force_exclude_features,
"force_feature_experiment": bool(
    self.force_include_families
    or self.force_exclude_families
    or self.force_include_features
    or self.force_exclude_features
),
```

Also add a clear preregistered rule:

```python
"force_feature_comparison_rule": (
    "Feature-family experiments must be compared via quote-clean replay, CLV CI, "
    "ranker/Spearman CI, drawdown, volume, and book concentration. Selector output alone "
    "is not an ablation or promotion gate."
),
```

**Step 3: Include selected required features in feature manifest**

Option A: extend `_save_feature_manifest` to accept metadata.

Preferred minimal option:

- Save selected features exactly as before.
- Add a new file, `feature_experiment_metadata.json`, after model training.

Example method:

```python
def _save_feature_experiment_metadata(self, selected_features: dict[str, list[str]], required_features: list[str] | None = None):
    metadata = {
        "force_include_families": self.force_include_families,
        "force_exclude_families": self.force_exclude_families,
        "force_include_features": self.force_include_features,
        "force_exclude_features": self.force_exclude_features,
        "required_features": required_features or [],
        "selected_features": selected_features,
    }
    with open(self.run_dir / "feature_experiment_metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)
```

Call it where `_save_feature_manifest` is called.

**Step 4: Run tests**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q
```

Expected: PASS.

---

## Task 7: Add CLI smoke tests

**Objective:** Verify new args are accepted by argparse and instantiated correctly.

**Files:**

- Modify: `tests/test_mlb_batter_train_pipeline_variants.py`
- Modify: `src/models/mlb/mlb_batter_train_pipeline.py`

**Step 1: Add parser-access helper if needed**

Currently the parser is built inside `if __name__ == "__main__"`. If tests cannot reach it cleanly, extract parser creation to:

```python
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train MLB Batter Prop Models")
    ...
    return parser
```

Then main block calls:

```python
parser = build_arg_parser()
args = parser.parse_args()
```

**Step 2: Add parser test**

```python
def test_parser_accepts_force_feature_controls():
    parser = pipeline.build_arg_parser()
    args = parser.parse_args([
        "--stat", "hits",
        "--force-include-families", "contact_quality,matchup_pitcher",
        "--force-exclude-families", "market",
        "--force-include-features", "batter_xba_l10",
        "--force-exclude-features", "prop_line_batter_hits",
    ])

    assert args.force_include_families == ["contact_quality,matchup_pitcher"]
    assert args.force_exclude_families == ["market"]
    assert args.force_include_features == ["batter_xba_l10"]
    assert args.force_exclude_features == ["prop_line_batter_hits"]
```

**Step 3: Run tests**

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py -q
```

Expected: PASS.

---

## Task 8: Full focused validation

**Objective:** Verify the trainer patch is syntactically valid and does not regress current MLB batter paths.

Run:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py tests/test_mlb_batter_feature_store.py tests/test_mlb_batter_train_pipeline.py tests/test_mlb_feature_contracts.py -q
```

If `tests/test_mlb_batter_train_pipeline.py` does not exist in this checkout, run the available subset:

```powershell
.\venv\Scripts\python.exe -m pytest tests/test_mlb_batter_train_pipeline_variants.py tests/test_mlb_batter_feature_store.py tests/test_mlb_feature_contracts.py -q
```

Run compile check:

```powershell
.\venv\Scripts\python.exe -m py_compile src\models\mlb\mlb_batter_train_pipeline.py src\models\mlb\features\contracts.py
```

Run diff hygiene:

```powershell
git diff --check -- src/models/mlb/mlb_batter_train_pipeline.py src/models/mlb/features/contracts.py tests/test_mlb_batter_train_pipeline_variants.py .hermes/plans/2026-05-25-mlb-batter-hits-force-feature-family-infrastructure.md
```

Expected: all pass.

---

## Example commands after implementation

Baseline rerun with prop-line, default selector tolerance:

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100 --feature-tolerance 0.02 --output-dir src\models\mlb\artifacts\ranker_retrains\with_prop_line_tol002
```

Force include contact-quality family:

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100 --feature-tolerance 0.02 --force-include-families contact_quality --output-dir src\models\mlb\artifacts\ranker_retrains\force_include_contact_quality_tol002
```

Force include matchup/bullpen families:

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100 --feature-tolerance 0.02 --force-include-families matchup_pitcher,bullpen --output-dir src\models\mlb\artifacts\ranker_retrains\force_include_matchup_bullpen_tol002
```

Force include environment/opportunity families:

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100 --feature-tolerance 0.02 --force-include-families environment,opportunity --output-dir src\models\mlb\artifacts\ranker_retrains\force_include_environment_opportunity_tol002
```

Clean model-skill check excluding market-derived family:

```powershell
.\venv\Scripts\python.exe src\models\mlb\mlb_batter_train_pipeline.py --local --stat hits --train-seasons 2024 2025 --cal-season 2026 --cal-end-date 2026-04-12 --tune --tuning-trials 100 --feature-tolerance 0.02 --force-exclude-families market --output-dir src\models\mlb\artifacts\ranker_retrains\force_exclude_market_tol002
```

---

## Post-training evaluation standard

Every feature-family retrain must be evaluated with the same quote-clean replay/audit/ranking path.

Primary comparison metrics:

- flat ROI;
- mean CLV;
- CLV CI low;
- `selected_vs_candidate_mean_gap` Spearman CI low;
- `execution_alpha` Spearman CI low when available;
- monotonic score buckets;
- top-minus-bottom CLV;
- drawdown;
- bet volume;
- same-book CLV coverage;
- ESPNBet / ProphetX concentration;
- selected book concentration.

A family advances only if it improves at least one promotion-relevant target without materially harming the rest.

Do not promote from:

- selector output alone;
- raw ROI alone;
- mean CLV without timing/same-book/book-concentration checks;
- top-bucket result without sufficient volume and monotonicity.

---

## Suggested experiment order after baseline

1. Baseline rerun with prop-line, default tolerance.
2. Force include `contact_quality`.
3. Force include `matchup_pitcher,bullpen`.
4. Force include `environment,opportunity`.
5. Force include `platoon` only if residual diagnostics show hand/split clustering.
6. Force exclude `market` only as a clean model-skill sanity check, not because prop-line is presumed bad.
7. Only combine winning families after individual family runs show stable CLV/ranker improvement.

---

## Completion checklist

- [ ] Registry and helpers exist in `src/models/mlb/features/contracts.py`.
- [ ] CLI accepts family and feature include/exclude controls.
- [ ] Default trainer behavior is unchanged with no new flags.
- [ ] Include/exclude conflicts fail loudly.
- [ ] Missing/non-numeric forced features fail loudly.
- [ ] Required forced features bypass selector pruning and appear in final selected features.
- [ ] Artifact metadata records force-family experiment details.
- [ ] Focused tests pass.
- [ ] Py compile passes.
- [ ] `git diff --check` passes.

---

## Commit suggestion

After implementation and validation:

```powershell
git add src\models\mlb\features\contracts.py src\models\mlb\mlb_batter_train_pipeline.py tests\test_mlb_batter_train_pipeline_variants.py .hermes\plans\2026-05-25-mlb-batter-hits-force-feature-family-infrastructure.md; git commit -m "feat: add MLB batter force feature-family controls"
```
