"""NBA artifact identity, validation, and non-overwriting finalization helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

ARTIFACT_MANIFEST = "artifact_manifest.json"
MANIFEST_SCHEMA_VERSION = 1
MODEL_ARTIFACT_TYPE = "nba_training_bundle"
DIAGNOSTIC_ARTIFACT_TYPE = "nba_calibration_diagnostic"
SUPPORTED_STATS = ("pts", "reb", "ast")
_SAFE_FILENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_BUNDLE_FILES = {
    "minutes_model.joblib", "feature_config.joblib", "selected_features.json",
    "run_config.json", "best_hyperparams.json", "copula_params.json",
    "calibration_report.json", "calibration_report_combined.json",
    "combined_calibration_offsets.json", "correlation_analysis.json",
    "CALIBRATION_FAILED.txt", "CALIBRATION_WARNING.txt",
    *(f"{stat}_rate_model.joblib" for stat in SUPPORTED_STATS),
}
_DIAGNOSTIC_FILES = {"calibration_report_combined.json", "combined_calibration_offsets.json"}


@dataclass(frozen=True)
class ArtifactInspection:
    path: Path
    state: str
    identity: str
    manifest: dict[str, Any] | None
    file_hashes: dict[str, dict[str, Any]]

    def to_plan_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path.resolve(strict=False)),
            "state": self.state,
            "identity": self.identity,
            "file_hashes": self.file_hashes,
            "manifest_schema_version": self.manifest.get("schema_version") if self.manifest else None,
            "artifact_type": self.manifest.get("artifact_type") if self.manifest else "legacy_unverified",
            "legacy": self.state == "legacy_unverified",
            "identity_verified": self.state == "verified_manifest",
            "model_load_verified": False,
            "decision_grade": False,
            "historical_policy_verified": False,
        }


def canonical_json_bytes(value: Any) -> bytes:
    """Return strict canonical JSON bytes while preserving list order."""
    _reject_unsupported_metadata(value, "metadata")
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    if type(chunk_size) is not int or chunk_size <= 0:
        raise ValueError("Hash chunk size must be a positive integer")
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def create_exclusive_incomplete_dir(base_dir: str | Path, final_name: str) -> tuple[Path, Path]:
    """Create a new incomplete directory and reject either-name collisions."""
    if (not isinstance(final_name, str) or not _SAFE_FILENAME.fullmatch(final_name)
            or final_name.endswith("_incomplete")):
        raise ValueError("Final artifact name must be a safe completed directory name")
    base = Path(base_dir)
    final_dir = base / final_name
    incomplete_dir = base / f"{final_name}_incomplete"
    if os.path.lexists(final_dir) or os.path.lexists(incomplete_dir):
        raise FileExistsError(f"Artifact destination collision: {final_dir} or {incomplete_dir}")
    base.mkdir(parents=True, exist_ok=True)
    incomplete_dir.mkdir(exist_ok=False)
    if os.path.lexists(final_dir):
        raise FileExistsError(f"Artifact destination collision: {final_dir}")
    return incomplete_dir, final_dir


def build_training_manifest(
    bundle_dir: str | Path,
    *,
    mode: str,
    requested_stats: Iterable[str],
    available_stats: Iterable[str],
    effective_features: Mapping[str, Any],
    sanity: Mapping[str, Any],
    train_settings: Mapping[str, Any],
    calibration_settings: Mapping[str, Any],
    data_provenance: Mapping[str, Any],
    parent: Mapping[str, Any] | None = None,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a complete model manifest from actual files without writing it."""
    path = Path(bundle_dir)
    requested = _validate_stats(requested_stats, "requested_stats")
    available = _validate_stats(available_stats, "available_stats")
    if not set(requested).issubset(available):
        raise ValueError("requested_stats must be a subset of available_stats")
    _validate_sanity(sanity, available)

    selected_features_path = path / "selected_features.json"
    selected_features = _load_json_file(selected_features_path, "selected feature evidence")
    if canonical_json_bytes(selected_features) != canonical_json_bytes(dict(effective_features)):
        raise ValueError("Effective features do not match selected_features.json")

    inventory = _inventory_files(path)
    _require_model_files(inventory, requested, available, manifested=True)
    if available != _infer_available_stats(inventory):
        raise ValueError("Available stats must match actual supported model files in canonical order")
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "artifact_type": MODEL_ARTIFACT_TYPE,
        "architecture": "minutes_x_rate",
        "mode": mode,
        "completion": {"complete": True, "sanity": dict(sanity)},
        "requested_stats": requested,
        "available_stats": available,
        "features": {
            "ordered": dict(effective_features),
            "canonical_sha256": canonical_sha256(dict(effective_features)),
            "evidence_file": "selected_features.json",
            "evidence_sha256": inventory["selected_features.json"]["sha256"],
        },
        "configuration": _configuration_identity(path, inventory),
        "files": inventory,
        "training": dict(train_settings),
        "calibration": dict(calibration_settings),
        "sample_weighting": "none",
        "parent": dict(parent) if parent is not None else None,
        "source": dict(source) if source is not None else source_git_state(path),
        "data_provenance": dict(data_provenance),
        "diagnostics": {
            "deployable": False,
            "files": sorted(
                name for name in inventory
                if name.startswith("calibration_report") or name == "combined_calibration_offsets.json"
            ),
            "global_conformal_offsets": "diagnostic_only_not_loaded_by_canonical_replay",
        },
        "created_at": datetime.now(UTC).isoformat(),
    }
    manifest["manifest_identity"] = _manifest_identity(manifest)
    _validate_training_metadata(path, manifest)
    _validate_feature_identity(path, manifest, inventory)
    return manifest


def write_manifest_last(bundle_dir: str | Path, manifest: Mapping[str, Any]) -> Path:
    """Write a new manifest exclusively, then validate it against bundle bytes."""
    path = Path(bundle_dir)
    manifest_path = path / ARTIFACT_MANIFEST
    if manifest_path.exists():
        raise FileExistsError(f"Manifest already exists: {manifest_path}")
    canonical_json_bytes(dict(manifest))
    payload = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
    with manifest_path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    _validate_artifact_bundle(
        path,
        requested_stats=list(manifest.get("requested_stats", [])),
        allow_legacy=False,
        allow_incomplete=True,
    )
    return manifest_path


def finalize_training_bundle(
    incomplete_dir: str | Path,
    final_dir: str | Path,
    manifest: Mapping[str, Any],
) -> ArtifactInspection:
    """Write/verify the manifest last and rename without replacing a destination."""
    incomplete = Path(incomplete_dir)
    final = Path(final_dir)
    _validate_finalize_paths(incomplete, final)
    write_manifest_last(incomplete, manifest)
    inspection = _validate_artifact_bundle(
        incomplete,
        requested_stats=list(manifest.get("requested_stats", [])),
        allow_legacy=False,
        allow_incomplete=True,
    )
    _rename_no_replace(incomplete, final)
    return ArtifactInspection(final, inspection.state, inspection.identity, inspection.manifest, inspection.file_hashes)


def validate_artifact_bundle(
    bundle_dir: str | Path,
    requested_stats: Iterable[str] | None = None,
    *,
    allow_legacy: bool = False,
) -> ArtifactInspection:
    """Validate a model bundle without deserializing model files."""
    return _validate_artifact_bundle(
        bundle_dir,
        requested_stats=requested_stats,
        allow_legacy=allow_legacy,
        allow_incomplete=False,
    )


def _validate_artifact_bundle(
    bundle_dir: str | Path,
    requested_stats: Iterable[str] | None,
    *,
    allow_legacy: bool,
    allow_incomplete: bool,
) -> ArtifactInspection:
    given = Path(bundle_dir).expanduser()
    if given.name.endswith("_incomplete") and not allow_incomplete:
        raise ValueError(f"Incomplete model directory cannot be used: {given}")
    path = given.resolve(strict=False)
    if not path.exists() or not path.is_dir():
        raise ValueError(f"Model directory does not exist or is not a directory: {path}")
    if path.name.endswith("_incomplete") and not allow_incomplete:
        raise ValueError(f"Incomplete model directory cannot be used: {path}")
    manifest_path = path / ARTIFACT_MANIFEST
    requested = _validate_stats(() if requested_stats is None else requested_stats, "requested_stats")
    if manifest_path.is_symlink():
        raise ValueError("Artifact manifest symlinks are not allowed")
    if not os.path.lexists(manifest_path):
        if not allow_legacy:
            raise ValueError(f"Model directory requires {ARTIFACT_MANIFEST}: {path}")
        inventory = _inventory_files(path)
        inferred = _infer_available_stats(inventory)
        _require_model_files(inventory, requested, inferred)
        identity = canonical_sha256({"state": "legacy_unverified", "files": inventory})
        return ArtifactInspection(path, "legacy_unverified", identity, None, inventory)

    manifest = _load_json_file(manifest_path, "artifact manifest")
    if not isinstance(manifest, dict):
        raise ValueError("Artifact manifest root must be an object")
    if type(manifest.get("schema_version")) is not int or manifest["schema_version"] != MANIFEST_SCHEMA_VERSION:
        raise ValueError("Unsupported artifact manifest schema version")
    if manifest.get("artifact_type") != MODEL_ARTIFACT_TYPE:
        raise ValueError("Artifact is not an NBA training bundle")
    if manifest.get("architecture") != "minutes_x_rate":
        raise ValueError("Unsupported NBA artifact architecture")
    completion = manifest.get("completion")
    if not isinstance(completion, dict) or completion.get("complete") is not True:
        raise ValueError("Artifact manifest does not claim successful completion")
    declared_requested = _validate_stats(manifest.get("requested_stats"), "manifest requested_stats")
    declared_available = _validate_stats(manifest.get("available_stats"), "manifest available_stats")
    _validate_sanity(completion.get("sanity"), declared_available)
    if not set(declared_requested).issubset(declared_available):
        raise ValueError("Manifest requested_stats are outside available_stats")
    if requested and not set(requested).issubset(declared_available):
        raise ValueError("Requested replay stats are unavailable in the artifact")

    actual_inventory = _inventory_files(path)
    declared_inventory = manifest.get("files")
    if not isinstance(declared_inventory, dict) or not declared_inventory:
        raise ValueError("Artifact manifest has no file digest inventory")
    for name in declared_inventory:
        _validate_safe_filename(name)
    if set(actual_inventory) != set(declared_inventory):
        raise ValueError("Artifact file inventory does not match manifest")
    for name, actual in actual_inventory.items():
        _validate_safe_filename(name)
        declared = declared_inventory.get(name)
        if not isinstance(declared, dict):
            raise ValueError(f"Missing digest entry for {name}")
        if (type(declared.get("size")) is not int or declared["size"] <= 0
                or not _is_digest(declared.get("sha256")) or declared != actual):
            raise ValueError(f"Artifact content mismatch for {name}")

    inferred_available = _infer_available_stats(actual_inventory)
    if inferred_available != declared_available:
        raise ValueError("Manifest available_stats do not match saved model files")
    _require_model_files(actual_inventory, requested or declared_requested, declared_available, manifested=True)
    _validate_feature_identity(path, manifest, actual_inventory)
    _validate_configuration_identity(path, manifest, actual_inventory)
    _validate_training_metadata(path, manifest)
    if manifest.get("sample_weighting") != "none":
        raise ValueError("Unsupported sample weighting metadata for this orchestrator")
    expected_identity = _manifest_identity(manifest)
    if manifest.get("manifest_identity") != expected_identity:
        raise ValueError("Artifact manifest self-identity mismatch")
    return ArtifactInspection(path, "verified_manifest", expected_identity, manifest, actual_inventory)


def resolve_artifact_for_execution(
    value: str | Path,
    requested_stats: Iterable[str],
    *,
    allow_legacy: bool = False,
    explicit: bool = False,
) -> ArtifactInspection:
    """Resolve an explicit bundle or the newest valid discovered run."""
    if Path(value).name.endswith("_incomplete"):
        raise ValueError(f"Incomplete model directory cannot be used: {value}")
    base = Path(value).expanduser().resolve(strict=False)
    if (explicit
            or base.name.startswith(("nba_run_", "nba_calibration_"))
            or any(os.path.lexists(base / name) for name in (ARTIFACT_MANIFEST, *_BUNDLE_FILES))):
        return validate_artifact_bundle(base, requested_stats, allow_legacy=allow_legacy)
    if not base.exists() or not base.is_dir():
        raise ValueError(f"Artifacts directory not found: {base}")
    candidates = sorted(
        (
            child for child in base.iterdir()
            if child.is_dir() and child.name.startswith("nba_run_") and not child.name.endswith("_incomplete")
        ),
        key=lambda child: child.name,
        reverse=True,
    )
    failures: list[str] = []
    for candidate in candidates:
        try:
            return validate_artifact_bundle(candidate, requested_stats, allow_legacy=allow_legacy)
        except (OSError, ValueError) as exc:
            failures.append(f"{candidate.name}: {exc}")
    detail = "; ".join(failures[:3]) if failures else "no completed run directories found"
    raise ValueError(f"No valid NBA model artifact is available in {base}: {detail}")


def build_diagnostic_manifest(
    bundle_dir: str | Path,
    *,
    source_model: ArtifactInspection,
    config: Mapping[str, Any],
    completed: bool,
) -> dict[str, Any]:
    """Build a non-deployable calibration diagnostic manifest."""
    path = Path(bundle_dir)
    if completed is not True:
        raise ValueError("Incomplete diagnostics cannot be finalized")
    inventory = _inventory_files(path)
    required = {"calibration_report_combined.json", "combined_calibration_offsets.json"}
    missing = sorted(required - set(inventory))
    if missing:
        raise ValueError(f"Diagnostic bundle is missing outputs: {', '.join(missing)}")
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "artifact_type": DIAGNOSTIC_ARTIFACT_TYPE,
        "mode": "calibration-only",
        "completion": {"complete": True},
        "deployable": False,
        "source_model": {
            "path": str(source_model.path),
            "identity": source_model.identity,
            "state": source_model.state,
        },
        "configuration": dict(config),
        "files": inventory,
        "created_at": datetime.now(UTC).isoformat(),
    }
    manifest["manifest_identity"] = _manifest_identity(manifest)
    _validate_diagnostic_manifest(path, manifest)
    return manifest


def finalize_diagnostic_bundle(
    incomplete_dir: str | Path,
    final_dir: str | Path,
    manifest: Mapping[str, Any],
) -> Path:
    incomplete = Path(incomplete_dir)
    final = Path(final_dir)
    _validate_finalize_paths(incomplete, final)
    _validate_diagnostic_manifest(incomplete, manifest)
    manifest_path = incomplete / ARTIFACT_MANIFEST
    if manifest_path.exists():
        raise FileExistsError(f"Manifest already exists: {manifest_path}")
    with manifest_path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    stored = _load_json_file(manifest_path, "diagnostic manifest")
    _validate_diagnostic_manifest(incomplete, stored)
    _rename_no_replace(incomplete, final)
    return final


def source_git_state(path: str | Path) -> dict[str, Any]:
    """Return honest source revision state, or null values when unavailable."""
    cwd = Path(path).resolve(strict=False)
    try:
        root = subprocess.run(
            ["git", "-C", str(cwd), "rev-parse", "--show-toplevel"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        commit = subprocess.run(
            ["git", "-C", root, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "-C", root, "status", "--porcelain"], check=True, capture_output=True, text=True
            ).stdout.strip()
        )
        return {"git_commit": commit, "dirty": dirty}
    except (OSError, subprocess.SubprocessError):
        return {"git_commit": None, "dirty": None}


def observed_dataframe_provenance(frame: Any) -> dict[str, Any]:
    """Record only observed row/date facts from a dataframe-like object."""
    result: dict[str, Any] = {"row_count": int(len(frame))}
    columns = getattr(frame, "columns", ())
    if "game_date" not in columns or len(frame) == 0:
        result["game_date_min"] = None
        result["game_date_max"] = None
        return result
    values = frame["game_date"].dropna()
    result["game_date_min"] = _date_text(values.min()) if len(values) else None
    result["game_date_max"] = _date_text(values.max()) if len(values) else None
    return result


def _date_text(value: Any) -> str:
    if isinstance(value, date | datetime):
        return value.isoformat()
    return str(value)


def _configuration_identity(path: Path, inventory: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in ("run_config.json", "best_hyperparams.json", "copula_params.json"):
        if name in inventory:
            value = _load_json_file(path / name, name)
            result[name] = {
                "canonical_sha256": canonical_sha256(value),
                "file_sha256": inventory[name]["sha256"],
            }
    return result


def _validate_configuration_identity(path: Path, manifest: Mapping[str, Any], inventory: Mapping[str, Any]) -> None:
    declared = manifest.get("configuration")
    if not isinstance(declared, dict):
        raise ValueError("Manifest configuration identity is malformed")
    if declared != _configuration_identity(path, inventory):
        raise ValueError("Manifest configuration identities do not match saved evidence")


def _validate_feature_identity(path: Path, manifest: Mapping[str, Any], inventory: Mapping[str, Any]) -> None:
    features = manifest.get("features")
    if not isinstance(features, dict):
        raise ValueError("Manifest feature identity is malformed")
    evidence_file = features.get("evidence_file")
    if evidence_file != "selected_features.json" or evidence_file not in inventory:
        raise ValueError("Manifest feature evidence is missing")
    saved = _load_json_file(path / evidence_file, "selected feature evidence")
    expected_keys = {"minutes_features", *(f"{stat}_rate_features" for stat in manifest["available_stats"])}
    if not isinstance(saved, dict) or set(saved) != expected_keys:
        raise ValueError("Selected feature evidence must describe exactly the available models")
    for model, values in saved.items():
        lists = values.values() if isinstance(values, dict) and values else [values]
        if isinstance(values, dict):
            for quantile in values:
                try:
                    valid = 0 < float(quantile) < 1
                except (ValueError, TypeError):
                    valid = False
                if not valid:
                    raise ValueError(f"Invalid feature quantile in {model}")
        for ordered in lists:
            if (not isinstance(ordered, list) or not ordered
                    or any(not isinstance(item, str) or not item for item in ordered)
                    or len(set(ordered)) != len(ordered)):
                raise ValueError(f"Malformed ordered feature list for {model}")
    if features.get("ordered") != saved:
        raise ValueError("Manifest ordered features differ from saved evidence")
    if features.get("canonical_sha256") != canonical_sha256(saved):
        raise ValueError("Manifest feature canonical identity mismatch")
    if features.get("evidence_sha256") != inventory[evidence_file]["sha256"]:
        raise ValueError("Manifest feature file identity mismatch")


def _manifest_identity(manifest: Mapping[str, Any]) -> str:
    content = dict(manifest)
    content.pop("manifest_identity", None)
    return canonical_sha256(content)


def _inventory_files(path: Path) -> dict[str, dict[str, Any]]:
    inventory: dict[str, dict[str, Any]] = {}
    for child in sorted(path.iterdir(), key=lambda item: item.name):
        if child.name == ARTIFACT_MANIFEST:
            continue
        _validate_safe_filename(child.name)
        if child.is_symlink():
            raise ValueError(f"Artifact symlinks are not allowed: {child.name}")
        if not child.is_file():
            raise ValueError(f"Artifact bundle entries must be regular files: {child.name}")
        if child.stat().st_size <= 0:
            raise ValueError(f"Artifact file is empty: {child.name}")
        if child.suffix == ".json":
            _load_json_file(child, child.name)
        inventory[child.name] = {"size": child.stat().st_size, "sha256": sha256_file(child)}
    return inventory


def _validate_safe_filename(name: str) -> None:
    if not isinstance(name, str) or not _SAFE_FILENAME.fullmatch(name) or name in {".", ".."}:
        raise ValueError(f"Unsafe artifact inventory filename: {name!r}")
    candidate = Path(name)
    if candidate.is_absolute() or len(candidate.parts) != 1:
        raise ValueError(f"Unsafe artifact inventory path: {name!r}")
    if name not in _BUNDLE_FILES:
        raise ValueError(f"Unexpected artifact bundle filename: {name!r}")


def _infer_available_stats(inventory: Mapping[str, Any]) -> list[str]:
    return [stat for stat in SUPPORTED_STATS if f"{stat}_rate_model.joblib" in inventory]


def _require_model_files(
    inventory: Mapping[str, Any], requested_stats: Iterable[str], available_stats: Iterable[str],
    *, manifested: bool = False,
) -> None:
    if not available_stats:
        raise ValueError("Model bundle must contain at least one supported stat-rate model")
    required = {"minutes_model.joblib", "feature_config.joblib"}
    if manifested:
        required.update({"selected_features.json", "run_config.json"})
    required.update(f"{stat}_rate_model.joblib" for stat in set(requested_stats) | set(available_stats))
    missing = sorted(required - set(inventory))
    if missing:
        raise ValueError(f"Model directory is missing required artifacts: {', '.join(missing)}")


def _validate_stats(values: Any, label: str) -> list[str]:
    if not isinstance(values, list | tuple) or any(not isinstance(value, str) for value in values):
        raise ValueError(f"{label} must be a list")
    result = list(values)
    if len(set(result)) != len(result):
        raise ValueError(f"{label} must not contain duplicates")
    unsupported = sorted(set(result) - set(SUPPORTED_STATS))
    if unsupported:
        raise ValueError(f"{label} contains unsupported stats: {', '.join(unsupported)}")
    return result


def _load_json_file(path: Path, label: str) -> Any:
    if not path.is_file():
        raise ValueError(f"Missing {label}: {path}")
    try:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle, object_pairs_hook=_unique_json_object)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Unreadable {label}: {path}") from exc
    _reject_unsupported_metadata(value, label)
    return value


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON mapping key: {key}")
        result[key] = value
    return result


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _validate_sanity(sanity: Any, available: list[str]) -> None:
    if not isinstance(sanity, Mapping) or sanity.get("passed") is not True:
        raise ValueError("Successful sanity evidence is required for completion")
    if sanity.get("predictor") != "canonical_no_global_offsets":
        raise ValueError("Sanity must represent canonical no-global-offset inference")
    checked = _validate_stats(sanity.get("checked_stats"), "sanity checked_stats")
    if not checked or set(checked) != set(available):
        raise ValueError("Sanity must check every available model stat")
    if (type(sanity.get("player_id")) is not int
            or not isinstance(sanity.get("game_id"), str) or not sanity["game_id"]
            or not isinstance(sanity.get("game_date"), str) or not sanity["game_date"]):
        raise ValueError("Sanity must identify the actual inference sample")
    predictions = sanity.get("predictions")
    if not isinstance(predictions, dict) or set(predictions) != set(checked):
        raise ValueError("Sanity prediction evidence is missing")
    for stat, prediction in predictions.items():
        if not isinstance(prediction, dict) or set(prediction) != {"mean", "median", "q10", "q90"}:
            raise ValueError(f"Malformed sanity prediction evidence for {stat}")
        if any(type(value) not in (int, float) or not math.isfinite(value) for value in prediction.values()):
            raise ValueError(f"Nonfinite sanity prediction evidence for {stat}")
        if prediction["mean"] < 0 or prediction["q10"] > prediction["q90"] or prediction["median"] > 100:
            raise ValueError(f"Sanity prediction bounds failed for {stat}")


def _validate_training_metadata(path: Path, manifest: Mapping[str, Any]) -> None:
    required = {"mode", "training", "calibration", "data_provenance", "source", "parent", "created_at", "diagnostics"}
    if not required.issubset(manifest) or manifest["mode"] not in ("full", "partial"):
        raise ValueError("Missing or unsupported training manifest metadata")
    training, calibration = manifest["training"], manifest["calibration"]
    if not isinstance(training, dict) or not isinstance(calibration, dict):
        raise ValueError("Training and calibration settings must be objects")
    seasons = training.get("train_seasons")
    if not isinstance(seasons, list) or not seasons or any(not isinstance(s, str) or not s for s in seasons):
        raise ValueError("Training seasons must be an ordered nonempty string list")
    config = _load_json_file(path / "run_config.json", "run config")
    if not isinstance(config, dict):
        raise ValueError("Saved run config must be an object")
    comparisons = {
        "train_seasons": seasons,
        "calibration_season": calibration.get("season"),
        "cal_end_date": calibration.get("end_date_exclusive"),
        "calibration_tolerance": calibration.get("tolerance"),
        "calibration_hard_fail": calibration.get("hard_fail"),
    }
    if any(key not in config or config[key] != value for key, value in comparisons.items()):
        raise ValueError("Declared train/calibration settings differ from run_config.json")
    if not isinstance(calibration.get("season"), str) or not calibration["season"]:
        raise ValueError("Missing calibration season")
    if "end_date_exclusive" not in calibration:
        raise ValueError("Missing calibration end-date setting")
    end = calibration["end_date_exclusive"]
    if end is not None:
        if not isinstance(end, str):
            raise ValueError("Calibration end date must be ISO text or null")
        date.fromisoformat(end)
    for key in ("tolerance", "hard_fail"):
        value = calibration.get(key)
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"Malformed calibration {key}")
    for key in ("tune_hyperparams", "tuning_per_quantile"):
        if type(training.get(key)) is not bool:
            raise ValueError(f"Malformed training setting: {key}")
    if type(training.get("tuning_trials")) is not int or training["tuning_trials"] <= 0:
        raise ValueError("Malformed tuning_trials")
    timeout = training.get("tuning_timeout")
    if "tuning_timeout" not in training or (timeout is not None and (type(timeout) is not int or timeout <= 0)):
        raise ValueError("Malformed tuning_timeout")
    tolerance = training.get("feature_tolerance")
    if isinstance(tolerance, bool) or not isinstance(tolerance, int | float) or not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError("Malformed feature_tolerance")
    source = manifest["source"]
    if not isinstance(source, dict) or set(source) != {"git_commit", "dirty"}:
        raise ValueError("Missing source Git provenance (null values are allowed)")
    if source["git_commit"] is not None and (
        not isinstance(source["git_commit"], str) or re.fullmatch(r"[0-9a-f]{40,64}", source["git_commit"]) is None
    ):
        raise ValueError("Malformed source Git commit")
    if source["dirty"] is not None and type(source["dirty"]) is not bool:
        raise ValueError("Malformed source dirty state")
    parent = manifest["parent"]
    if manifest["mode"] == "partial":
        _validate_parent_identity(parent)
        if config.get("mode") != "partial_retrain":
            raise ValueError("Partial manifest requires partial run config")
        if "retrain_stats" not in config:
            raise ValueError("Partial run config must identify the requested stat set")
        requested_retrains = config["retrain_stats"]
        if requested_retrains is None:
            requested_retrains = []
        requested_retrains = _validate_stats(requested_retrains, "retrain_stats")
        if manifest["requested_stats"] != requested_retrains:
            raise ValueError("Partial requested stats differ from run_config.json retrain_stats")
    elif parent is not None or config.get("mode") == "partial_retrain":
        raise ValueError("Full manifest cannot claim a partial parent/config")
    provenance = manifest["data_provenance"]
    if not isinstance(provenance, dict) or set(provenance) != {"training", "calibration"}:
        raise ValueError("Missing observed data provenance")
    for label, observed in provenance.items():
        if (not isinstance(observed, dict) or type(observed.get("row_count")) is not int
                or observed["row_count"] < 0 or not {"game_date_min", "game_date_max"}.issubset(observed)):
            raise ValueError(f"Malformed observed {label} provenance")
        for key in ("game_date_min", "game_date_max"):
            if observed[key] is not None:
                if not isinstance(observed[key], str):
                    raise ValueError("Observed dates must be ISO text or null")
                datetime.fromisoformat(observed[key])
    diagnostics = manifest["diagnostics"]
    expected_diagnostics = sorted(name for name in manifest["files"] if name.startswith("calibration_report")
                                  or name == "combined_calibration_offsets.json")
    if (not isinstance(diagnostics, dict) or diagnostics.get("deployable") is not False
            or diagnostics.get("files") != expected_diagnostics
            or diagnostics.get("global_conformal_offsets") != "diagnostic_only_not_loaded_by_canonical_replay"):
        raise ValueError("Diagnostic outputs must remain explicitly non-deployable")
    if not isinstance(manifest["created_at"], str):
        raise ValueError("Manifest creation time must be ISO text")
    datetime.fromisoformat(manifest["created_at"])


def _validate_parent_identity(parent: Any) -> None:
    if (not isinstance(parent, dict) or not _is_digest(parent.get("identity"))
            or parent.get("state") not in ("verified_manifest", "legacy_unverified")
            or not isinstance(parent.get("path"), str) or not parent["path"]):
        raise ValueError("Missing or malformed source/parent artifact identity")


def _validate_diagnostic_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    canonical_json_bytes(dict(manifest))
    completion = manifest.get("completion")
    if (type(manifest.get("schema_version")) is not int or manifest["schema_version"] != MANIFEST_SCHEMA_VERSION
            or manifest.get("artifact_type") != DIAGNOSTIC_ARTIFACT_TYPE
            or manifest.get("mode") != "calibration-only" or manifest.get("deployable") is not False
            or not isinstance(completion, dict) or completion.get("complete") is not True):
        raise ValueError("Diagnostic manifest must be complete and explicitly non-deployable")
    _validate_parent_identity(manifest.get("source_model"))
    if not isinstance(manifest.get("configuration"), dict):
        raise ValueError("Missing diagnostic configuration")
    if not isinstance(manifest.get("created_at"), str):
        raise ValueError("Diagnostic creation time must be ISO text")
    datetime.fromisoformat(manifest["created_at"])
    inventory = _inventory_files(path)
    if set(inventory) != _DIAGNOSTIC_FILES or manifest.get("files") != inventory:
        raise ValueError("Diagnostic bundle must contain exactly the report and offset outputs")
    reports = _load_json_file(path / "calibration_report_combined.json", "combined report")
    offsets = _load_json_file(path / "combined_calibration_offsets.json", "diagnostic offsets")
    if (not isinstance(reports, dict) or not reports or not isinstance(offsets, dict)
            or set(reports) != set(offsets) or not set(reports).issubset(SUPPORTED_STATS)):
        raise ValueError("Diagnostic outputs lack successful supported-stat evaluations")
    for stat, rows in reports.items():
        if (not isinstance(rows, list) or not rows or any(not isinstance(row, dict) for row in rows)
                or not isinstance(offsets[stat], dict) or not offsets[stat]):
            raise ValueError(f"Malformed diagnostic outputs for {stat}")
        quantiles = (0.1, 0.25, 0.5, 0.75, 0.9)
        if len(rows) != len(quantiles) or set(offsets[stat]) != {str(q) for q in quantiles}:
            raise ValueError(f"Diagnostic quantile evidence is incomplete for {stat}")
        for row, quantile in zip(rows, quantiles, strict=True):
            fields = ("quantile", "coverage", "target", "gap", "conformal_offset")
            if (any(type(row.get(key)) not in (int, float) for key in fields)
                    or row["quantile"] != quantile or row["target"] != quantile
                    or not 0 <= row["coverage"] <= 1
                    or not math.isclose(row["gap"], row["coverage"] - quantile, abs_tol=1e-12)
                    or type(offsets[stat][str(quantile)]) not in (int, float)
                    or offsets[stat][str(quantile)] != round(row["conformal_offset"], 4)):
                raise ValueError(f"Diagnostic report/offset evidence is inconsistent for {stat}")
    if manifest.get("manifest_identity") != _manifest_identity(manifest):
        raise ValueError("Diagnostic manifest self-identity mismatch")


def _validate_finalize_paths(incomplete: Path, final: Path) -> None:
    if (incomplete.is_symlink() or not incomplete.is_dir() or not incomplete.name.endswith("_incomplete")
            or final.name.endswith("_incomplete") or incomplete.name != f"{final.name}_incomplete"
            or incomplete.parent.resolve() != final.parent.resolve()):
        raise ValueError("Finalization requires matching sibling incomplete/final directories")
    if os.path.lexists(final):
        raise FileExistsError(f"Final artifact destination already exists: {final}")


def _rename_no_replace(source: Path, destination: Path) -> None:
    """Use an OS non-replacing rename, including protection against collision races."""
    if os.path.lexists(destination):
        raise FileExistsError(f"Final artifact destination already exists: {destination}")
    if os.name == "nt":
        # Windows rename fails if the destination already exists (unlike POSIX rename).
        os.rename(source, destination)
    elif sys.platform.startswith("linux"):
        import ctypes
        import errno

        libc = ctypes.CDLL(None, use_errno=True)
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise OSError(errno.ENOTSUP, "Atomic non-replacing rename is unavailable")
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        if rename(-100, os.fsencode(source), -100, os.fsencode(destination), 1) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), str(destination))
    else:
        raise OSError("Atomic non-replacing artifact rename is unsupported on this platform")


def _reject_unsupported_metadata(value: Any, label: str) -> None:
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{label} contains a nonfinite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _reject_unsupported_metadata(item, f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{label} contains a non-string mapping key")
            _reject_unsupported_metadata(item, f"{label}.{key}")
        return
    raise ValueError(f"{label} contains unsupported metadata type: {type(value).__name__}")
