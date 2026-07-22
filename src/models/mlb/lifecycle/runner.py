"""Resumable stage runner for YAML-driven MLB model lifecycles."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import subprocess
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from src.models.mlb.lifecycle.adapters import (
    build_audit_command,
    build_ranker_command,
    build_sweep_command,
    build_training_command,
    repo_root,
)
from src.models.mlb.lifecycle.config import ResolvedLifecycleConfig, resolve_lifecycle_config
from src.models.mlb.lifecycle.decision import (
    DecisionRecord,
    evaluate_decision,
    recommend_staking,
)

STAGES = (
    "validate",
    "train_or_attach",
    "artifact_identity",
    "sweep",
    "audit",
    "ranker",
    "decision",
    "staking_policy",
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity(payload: Any, files: list[Path] | None = None) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode()
    )
    for path in sorted(files or [], key=lambda item: str(item)):
        digest.update(str(path).encode())
        digest.update((_sha256_file(path) or "MISSING").encode())
    return digest.hexdigest()


def _decision_grade_bets(sweep_dir: Path, minimum_bets: int) -> list[Path]:
    selected: list[Path] = []
    candidates = [sweep_dir / "bets.csv", *sorted(sweep_dir.glob("config_*/bets.csv"))]
    for bets_csv in candidates:
        if not bets_csv.exists():
            continue
        metrics = _read_json(bets_csv.parent / "metrics.json", {})
        count = metrics.get("total_bets", metrics.get("bets")) if isinstance(metrics, dict) else None
        try:
            total = int(count) if count is not None else -1
        except (TypeError, ValueError):
            total = -1
        if total < 0:
            with bets_csv.open("r", encoding="utf-8", newline="") as handle:
                total = max(sum(1 for _ in csv.reader(handle)) - 1, 0)
        if total >= minimum_bets:
            selected.append(bets_csv)
    return selected


def _float_matches(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def _normalized_sweep_config(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "tau": payload.get("tau"),
        "z_max": payload.get("z_max", 1.0),
        "max_weight": payload.get("max_weight", 0.50),
        "edge_threshold": payload.get("edge_threshold"),
        "kelly_fraction": payload.get("kelly_fraction"),
    }


def _selector_matches(selector: dict[str, Any], config: dict[str, Any]) -> bool:
    return all(_float_matches(selector.get(key), config.get(key)) for key in selector)


def _sorted_sweep_config_dirs(sweep_dir: Path) -> list[Path]:
    def sort_key(path: Path) -> tuple[int, str]:
        match = re.match(r"^config_(\d+)(?:_|$)", path.name)
        if match is None:
            raise RuntimeError(f"Sweep config directory has no numeric index: {path}")
        return int(match.group(1)), path.name

    return sorted(
        (path for path in sweep_dir.glob("config_*") if path.is_dir()),
        key=sort_key,
    )


def _flatten_selection_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    flattened = {
        key: value
        for key, value in payload.items()
        if key != "config" and not isinstance(value, dict)
    }
    for section in ("betting", "risk"):
        nested = payload.get(section)
        if isinstance(nested, dict):
            flattened.update(nested)
    return flattened


def _metric_values_match(left: Any, right: Any) -> bool:
    try:
        left_number = float(left)
        right_number = float(right)
    except (TypeError, ValueError):
        return left == right
    if math.isnan(left_number) and math.isnan(right_number):
        return True
    return left_number == right_number or math.isclose(
        left_number, right_number, rel_tol=1e-12, abs_tol=1e-12
    )


def _validated_selection_metrics(
    result: dict[str, Any],
    persisted: dict[str, Any],
    config_dir: Path,
) -> dict[str, Any]:
    result_config = _normalized_sweep_config(result.get("config", {}))
    persisted_config_payload = persisted.get("config")
    if not isinstance(persisted_config_payload, dict):
        raise RuntimeError(f"Sweep config metrics are missing embedded config identity: {config_dir}")
    persisted_config = _normalized_sweep_config(persisted_config_payload)
    if not _selector_matches(result_config, persisted_config):
        raise RuntimeError(
            f"Sweep config metrics identity conflicts with sweep_results.json: {config_dir}"
        )

    result_metrics_payload = result.get("metrics")
    if not isinstance(result_metrics_payload, dict):
        raise RuntimeError(f"Sweep result metrics are malformed: {config_dir}")
    result_metrics = _flatten_selection_metrics(result_metrics_payload)
    persisted_metrics = _flatten_selection_metrics(persisted)
    for key in ("total_bets", "roi", "max_drawdown", "sharpe_ratio"):
        if key not in result_metrics or key not in persisted_metrics:
            continue
        if not _metric_values_match(result_metrics[key], persisted_metrics[key]):
            raise RuntimeError(
                f"Sweep config has conflicting persisted metric {key!r}: {config_dir}"
            )
    return result_metrics


def _select_audit_configs(
    resolved: ResolvedLifecycleConfig,
    sweep_dir: Path,
) -> dict[str, Any]:
    """Select the bounded certification subset from persisted sweep payloads."""

    results_path = sweep_dir / "sweep_results.json"
    payload = _required_json(results_path, "audit selection")
    results = payload.get("results")
    config_dirs = _sorted_sweep_config_dirs(sweep_dir)
    if not isinstance(results, list) or not results or len(results) != len(config_dirs):
        raise RuntimeError(
            "audit selection requires sweep_results.json results to map one-to-one to sorted config directories"
        )

    candidates: list[dict[str, Any]] = []
    minimum_bets = int(resolved.audit.minimum_bets or 0)
    for config_dir, result in zip(config_dirs, results, strict=True):
        if not isinstance(result, dict) or not isinstance(result.get("config"), dict):
            raise RuntimeError(f"audit selection found malformed sweep result for {config_dir}")
        config = _normalized_sweep_config(result["config"])
        persisted_metrics = _read_json(config_dir / "metrics.json", {})
        if not isinstance(persisted_metrics, dict):
            raise RuntimeError(f"audit selection found malformed metrics for {config_dir}")
        metrics = _validated_selection_metrics(result, persisted_metrics, config_dir)
        try:
            total_bets = int(metrics.get("total_bets", metrics.get("bets", 0)) or 0)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"audit selection has invalid total_bets for {config_dir}") from exc
        metrics["total_bets"] = total_bets
        candidates.append(
            {
                "config_dir": str(config_dir),
                "bets_csv": str(config_dir / "bets.csv"),
                "candidate_edges_csv": str(config_dir / "bookmaker_candidate_edges.csv"),
                "config": config,
                "metrics": metrics,
            }
        )

    selection = resolved.audit.selection
    selected: list[dict[str, Any]] = []
    if selection.policy == "explicit":
        for selector_model in selection.configs:
            selector = selector_model.model_dump()
            matches = [item for item in candidates if _selector_matches(selector, item["config"])]
            if len(matches) != 1:
                qualifier = "missing" if not matches else "ambiguous"
                raise RuntimeError(f"Explicit audit selector is {qualifier}: {selector}")
            match = matches[0]
            if match["metrics"]["total_bets"] < minimum_bets:
                raise RuntimeError(
                    f"Explicit audit selector is underpowered: {selector}; "
                    f"total_bets={match['metrics']['total_bets']} < {minimum_bets}"
                )
            selected.append({**match, "reason": "explicit_preregistered_selector"})
    else:
        eligible: list[dict[str, Any]] = []
        for item in candidates:
            metrics = item["metrics"]
            if metrics["total_bets"] < minimum_bets:
                continue
            try:
                rank_value = float(metrics.get(selection.rank_by, math.nan))
                roi = float(metrics.get("roi", math.nan))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(rank_value) or not math.isfinite(roi):
                continue
            if selection.policy == "risk_filtered_top_n":
                try:
                    drawdown = float(metrics.get("max_drawdown", math.inf))
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(drawdown):
                    continue
                if drawdown > resolved.decision.max_drawdown:
                    continue
                if resolved.decision.require_positive_roi and roi <= 0:
                    continue
            eligible.append(item)

        def rank_key(item: dict[str, Any]) -> tuple[float, float, int, str]:
            metrics = item["metrics"]
            try:
                primary = float(metrics.get(selection.rank_by, -math.inf))
            except (TypeError, ValueError):
                primary = -math.inf
            try:
                roi = float(metrics.get("roi", -math.inf))
            except (TypeError, ValueError):
                roi = -math.inf
            if not math.isfinite(primary):
                primary = -math.inf
            if not math.isfinite(roi):
                roi = -math.inf
            return (-primary, -roi, -int(metrics["total_bets"]), str(item["config_dir"]))

        controls = sorted((item for item in eligible if item["config"]["tau"] is None), key=rank_key)
        bl_configs = sorted((item for item in eligible if item["config"]["tau"] is not None), key=rank_key)
        if selection.include_no_bl_control:
            if not controls:
                raise RuntimeError(
                    "Audit selection requested a no-BL control but found no eligible no-BL control"
                )
            selected.append({**controls[0], "reason": "best_eligible_no_bl_control"})
        remaining = selection.max_configs - len(selected)
        pool = bl_configs if selection.include_no_bl_control else sorted(eligible, key=rank_key)
        for item in pool[:remaining]:
            selected.append({**item, "reason": f"top_{selection.rank_by}"})

    if not selected:
        raise RuntimeError(
            f"Audit selection policy {selection.policy!r} produced no valid selected configs"
        )
    if len(selected) > selection.max_configs:
        raise RuntimeError("Audit selection exceeded configured max_configs")
    selected_dirs = [item["config_dir"] for item in selected]
    if len(set(selected_dirs)) != len(selected_dirs):
        raise RuntimeError("Audit selection produced duplicate configurations")
    for item in selected:
        _required_file(Path(item["bets_csv"]), "audit selection")
        _required_file(
            Path(item["candidate_edges_csv"]), "audit selection candidate-edge"
        )

    return {
        "status": "selected",
        "policy": selection.policy,
        "max_configs": selection.max_configs,
        "include_no_bl_control": selection.include_no_bl_control,
        "rank_by": selection.rank_by,
        "minimum_bets": minimum_bets,
        "selected": selected,
        "selected_count": len(selected),
        "sweep_results_json": str(results_path),
    }


def _planned_audit_selection(resolved: ResolvedLifecycleConfig, sweep_dir: Path) -> dict[str, Any]:
    selection = resolved.audit.selection
    count = len(selection.configs) if selection.policy == "explicit" else selection.max_configs
    planned = []
    for index in range(count):
        selector = selection.configs[index].model_dump() if selection.policy == "explicit" else None
        config_dir = sweep_dir / f"<selected_config_{index + 1}>"
        planned.append(
            {
                "config_dir": str(config_dir),
                "bets_csv": str(config_dir / "bets.csv"),
                "candidate_edges_csv": str(config_dir / "bookmaker_candidate_edges.csv"),
                "config": selector,
                "metrics": None,
                "reason": "planned_explicit_selector" if selector else "planned_post_sweep_selection",
            }
        )
    return {
        "status": "planned",
        "policy": selection.policy,
        "max_configs": selection.max_configs,
        "include_no_bl_control": selection.include_no_bl_control,
        "rank_by": selection.rank_by,
        "minimum_bets": int(resolved.audit.minimum_bets or 0),
        "selected": planned,
        "selected_count": len(planned),
        "sweep_results_json": str(sweep_dir / "sweep_results.json"),
    }


def _required_file(path: Path, stage: str) -> Path:
    if not path.is_file() or path.stat().st_size == 0:
        raise RuntimeError(f"{stage} required output is missing or empty: {path}")
    return path


def _required_json(path: Path, stage: str) -> dict[str, Any]:
    _required_file(path, stage)
    payload = _read_json(path, None)
    if not isinstance(payload, dict):
        raise RuntimeError(f"{stage} required output is not a JSON object: {path}")
    return payload


def _required_csv(path: Path, stage: str) -> list[dict[str, str]]:
    _required_file(path, stage)
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, csv.Error) as exc:
        raise RuntimeError(f"{stage} required output is unreadable: {path}") from exc
    if not rows:
        raise RuntimeError(f"{stage} required output has no data rows: {path}")
    return rows


def _verify_sweep_outputs(sweep_dir: Path) -> dict[str, Any]:
    stage = "sweep"
    summary_path = sweep_dir / "sweep_summary.csv"
    results_path = sweep_dir / "sweep_results.json"
    summary_rows = _required_csv(summary_path, stage)
    required_summary_columns = {"edge_threshold", "total_bets", "roi", "max_drawdown"}
    if not required_summary_columns.issubset(summary_rows[0]):
        missing = sorted(required_summary_columns - set(summary_rows[0]))
        raise RuntimeError(f"{stage} summary is missing columns {missing}: {summary_path}")
    payload = _required_json(results_path, stage)
    if not isinstance(payload.get("sweep_metadata"), dict):
        raise RuntimeError(f"{stage} required sweep_metadata is missing: {results_path}")
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        raise RuntimeError(f"{stage} required output has no results: {results_path}")
    if len(summary_rows) != len(results):
        raise RuntimeError(
            f"{stage} output count mismatch: {len(summary_rows)} summary rows, "
            f"{len(results)} JSON results"
        )
    config_dirs = _sorted_sweep_config_dirs(sweep_dir)
    if len(config_dirs) != len(results):
        raise RuntimeError(
            f"{stage} output count mismatch: expected {len(results)} config directories, "
            f"found {len(config_dirs)}"
        )
    for summary_row, config_dir, result in zip(summary_rows, config_dirs, results, strict=True):
        if not isinstance(result, dict) or not isinstance(result.get("config"), dict):
            raise RuntimeError(f"{stage} result config is malformed: {results_path}")
        if not isinstance(result.get("metrics"), dict):
            raise RuntimeError(f"{stage} result metrics are malformed: {results_path}")
        result_config = result["config"]
        for key in ("tau", "z_max", "max_weight", "edge_threshold", "kelly_fraction"):
            summary_value = summary_row.get(key)
            result_value = result_config.get(key)
            if summary_value in {None, ""}:
                continue
            try:
                if not math.isclose(float(summary_value), float(result_value), abs_tol=1e-12):
                    raise ValueError
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"{stage} config mismatch for {key}: summary={summary_value!r}, "
                    f"result={result_value!r}"
                ) from exc
        encoded_values = {
            "edge_threshold": re.search(r"(?:^|_)edge(-?\d+(?:\.\d+)?)", config_dir.name),
            "kelly_fraction": re.search(r"(?:^|_)kelly(-?\d+(?:\.\d+)?)", config_dir.name),
            "tau": re.search(r"(?:^|_)tau(-?\d+(?:\.\d+)?)", config_dir.name),
        }
        for key, match in encoded_values.items():
            result_value = result_config.get(key)
            if match is None:
                if key == "tau" and "no_BL" in config_dir.name and result_value is None:
                    continue
                continue
            try:
                if not math.isclose(float(match.group(1)), float(result_value), abs_tol=1e-12):
                    raise ValueError
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"{stage} config directory mismatch for {key}: "
                    f"directory={match.group(1)!r}, result={result_value!r}"
                ) from exc
        metrics = _required_json(config_dir / "metrics.json", stage)
        result_metrics = _validated_selection_metrics(result, metrics, config_dir)
        try:
            flattened_persisted = _flatten_selection_metrics(metrics)
            persisted_bets = int(
                flattened_persisted.get(
                    "total_bets", flattened_persisted.get("bets", 0)
                )
                or 0
            )
            result_bets = int(result_metrics.get("total_bets", result_metrics.get("bets", 0)) or 0)
            requires_bets = max(persisted_bets, result_bets) > 0
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"{stage} metrics have invalid total_bets: {config_dir / 'metrics.json'}"
            ) from exc
        if requires_bets:
            _required_csv(config_dir / "bets.csv", stage)
    return {
        "sweep_summary_csv": str(summary_path),
        "sweep_results_json": str(results_path),
        "config_count": len(results),
    }


def _verify_audit_outputs(
    audit_dir: Path,
    bets_csvs: list[Path],
    *,
    full_audit: bool,
    require_timing_stability: bool = False,
) -> dict[str, Any]:
    stage = "audit"
    manifest_path = audit_dir / "suite_manifest.json"
    csv_path = audit_dir / "suite_manifest.csv"
    summary_path = audit_dir / "suite_summary.md"
    manifest = _required_json(manifest_path, stage)
    rows = _required_csv(csv_path, stage)
    _required_file(summary_path, stage)
    items = manifest.get("items")
    if not isinstance(items, list) or not items:
        raise RuntimeError(f"{stage} required output has no manifest items: {manifest_path}")
    labels = {str(item.get("label")) for item in items if isinstance(item, dict)}
    expected_labels = {bets.parent.name if bets.name == "bets.csv" else bets.stem for bets in bets_csvs}
    csv_labels = {row.get("label", "") for row in rows}
    if labels != expected_labels or csv_labels != expected_labels:
        raise RuntimeError(
            f"{stage} manifest labels do not match requested configurations: "
            f"expected {sorted(expected_labels)}, found {sorted(labels)}"
        )
    items_by_label = {
        str(item["label"]): item
        for item in items
        if isinstance(item, dict) and item.get("label") is not None
    }
    rows_by_label = {row.get("label", ""): row for row in rows}
    for label in sorted(expected_labels):
        item = items_by_label[label]
        if item.get("clv_returncode") != 0 or item.get("diagnosis_returncode") != 0:
            raise RuntimeError(f"{stage} child command failed for {label}")
        _required_csv(audit_dir / "clv" / label / "clv_matches.csv", stage)
        timing_rows = _required_csv(
            audit_dir / "clv" / label / "clv_timing_stability.csv", stage
        )
        diagnosis = _required_json(
            audit_dir / "diagnosis" / label / "clv_failure_modes.json", stage
        )
        timing = diagnosis.get("timing_stability")
        if not isinstance(timing, dict) or timing.get("status") not in {"PASS", "FAIL"}:
            raise RuntimeError(
                f"{stage} required timing-stability attestation is missing for {label}"
            )
        timing_status = timing["status"]
        required_horizons = {"+15m", "+30m", "+60m"}
        if set(timing.get("required_horizons") or []) != required_horizons:
            raise RuntimeError(f"{stage} timing required horizons are invalid for {label}")
        if set(item.get("timing_required_horizons") or []) != required_horizons:
            raise RuntimeError(f"{stage} JSON timing horizons are invalid for {label}")
        coverage = timing.get("coverage_pct")
        if not isinstance(coverage, dict) or any(
            isinstance(coverage.get(horizon), bool)
            or not isinstance(coverage.get(horizon), int | float)
            or coverage[horizon] < 0
            for horizon in required_horizons
        ):
            raise RuntimeError(f"{stage} timing coverage is invalid for {label}")
        aliases = {
            "+15m": {"+15m", "15m", "+15", "15"},
            "+30m": {"+30m", "30m", "+30", "30"},
            "+60m": {"+60m", "60m", "+60", "60"},
        }
        for horizon, horizon_aliases in aliases.items():
            scored = False
            for timing_row in timing_rows:
                observed = str(timing_row.get("horizon") or "").lower().replace(" ", "")
                value = timing_row.get("horizon_clv_implied_prob")
                if observed not in horizon_aliases or value in {None, ""}:
                    continue
                try:
                    scored = math.isfinite(float(value))
                except (TypeError, ValueError):
                    scored = False
                if scored:
                    break
            if require_timing_stability and not scored:
                raise RuntimeError(f"{stage} timing {horizon} has no scored evidence for {label}")
        if require_timing_stability and timing_status != "PASS":
            raise RuntimeError(f"{stage} timing stability failed for finalist {label}")
        if item.get("timing_stability_status") != timing_status:
            raise RuntimeError(f"{stage} JSON timing attestation mismatch for {label}")
        if rows_by_label[label].get("timing_stability_status") != timing_status:
            raise RuntimeError(f"{stage} CSV timing attestation mismatch for {label}")
    metadata = manifest.get("metadata")
    if not isinstance(metadata, dict):
        raise RuntimeError(f"{stage} manifest metadata is missing: {manifest_path}")
    if full_audit:
        if (
            metadata.get("audit_mode") != "full"
            or metadata.get("dropout_audit_ran") is not True
            or metadata.get("dropout_returncode") != 0
            or metadata.get("full_audit_complete") is not True
        ):
            raise RuntimeError(f"{stage} required full/dropout audit attestation is incomplete")
        dropout_dir = audit_dir / "dropout_audit"
        dropout_summary_path = dropout_dir / "audit_summary.json"
        dropout_output_paths = [
            dropout_summary_path,
            dropout_dir / "audit_summary.md",
            dropout_dir / "dropout_summary_by_bucket.csv",
            dropout_dir / "dropout_rows.csv",
            dropout_dir / "selected_clean_quotes.csv",
            dropout_dir / "dropout_by_date.csv",
            dropout_dir / "dropout_by_game.csv",
            dropout_dir / "dropout_by_bookmaker.csv",
        ]
        dropout_summary = _required_json(dropout_summary_path, stage)
        dropout_decision = dropout_summary.get("decision")
        if dropout_decision not in {"PASS", "WARN", "FAIL"}:
            raise RuntimeError(f"{stage} dropout decision is invalid: {dropout_summary_path}")
        if metadata.get("dropout_decision") != dropout_decision:
            raise RuntimeError(f"{stage} dropout decision attestation does not match its summary")
        if metadata.get("full_audit_passed") is not (dropout_decision == "PASS"):
            raise RuntimeError(f"{stage} full-audit PASS attestation is inconsistent")
        if metadata.get("dropout_summary_path") != str(dropout_summary_path):
            raise RuntimeError(f"{stage} dropout summary path attestation is inconsistent")
        if metadata.get("dropout_output_paths") != [str(path) for path in dropout_output_paths]:
            raise RuntimeError(f"{stage} dropout output-bundle attestation is inconsistent")
        _required_csv(dropout_dir / "dropout_summary_by_bucket.csv", stage)
        _required_csv(dropout_dir / "dropout_rows.csv", stage)
        _required_file(dropout_dir / "selected_clean_quotes.csv", stage)
        _required_file(dropout_dir / "audit_summary.md", stage)
        _required_file(dropout_dir / "dropout_by_date.csv", stage)
        _required_file(dropout_dir / "dropout_by_game.csv", stage)
        _required_file(dropout_dir / "dropout_by_bookmaker.csv", stage)
    evidence = {
        "suite_manifest_json": str(manifest_path),
        "suite_manifest_csv": str(csv_path),
        "suite_summary_md": str(summary_path),
        "item_count": len(items),
        "audit_mode": metadata.get("audit_mode"),
        "full_audit_complete": metadata.get("full_audit_complete"),
    }
    if full_audit:
        evidence["dropout_decision"] = metadata.get("dropout_decision")
        evidence["dropout_output_paths"] = metadata.get("dropout_output_paths")
    return evidence


def _verify_ranker_output(output_dir: Path) -> dict[str, Any]:
    summary_path = output_dir / "ranking_score_summary.csv"
    rows = _required_csv(summary_path, "ranker")
    required_columns = {"score_name", "ci_low", "monotonic_bins", "pass"}
    if not required_columns.issubset(rows[0]):
        missing = sorted(required_columns - set(rows[0]))
        raise RuntimeError(
            f"ranker required output is missing columns {missing}: {summary_path}"
        )
    return {"ranking_score_summary_csv": str(summary_path), "row_count": len(rows)}


def _load_artifact_metadata(artifact_dir: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for name in ("model_manifest.json", "run_config.json", "training_metadata.json"):
        data = _read_json(artifact_dir / name, {})
        if isinstance(data, dict) and data:
            metadata[name] = data
    return metadata


def build_artifact_identity(
    resolved: ResolvedLifecycleConfig,
    artifact_dir: Path,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    expected_files = list(resolved.profile_obj.model_artifact_names)
    missing = [name for name in expected_files if not (artifact_dir / name).exists()]
    manifest = _read_json(artifact_dir / "feature_manifest.json", {})
    model_feature_counts = {
        str(name): len(features)
        for name, features in manifest.items()
        if isinstance(features, list)
    } if isinstance(manifest, dict) else {}
    metadata = _load_artifact_metadata(artifact_dir)
    reasons: list[str] = []
    if not artifact_dir.exists() and not dry_run:
        reasons.append(f"Artifact directory does not exist: {artifact_dir}")
    if missing and not dry_run:
        reasons.append("Missing required artifact files: " + ", ".join(missing))
    expected_stat = resolved.profile_obj.train_short_stat or resolved.profile_obj.stat_key
    accepted_model_types = {
        "negative_binomial": {"negative_binomial", "negbin"},
        "compound_binomial": {"compound_binomial", "binomial"},
        "quantile": {"quantile", "quantile_regression"},
    }.get(resolved.profile_obj.model_type, {resolved.profile_obj.model_type})
    requested_families = set(resolved.feature_controls.requested_families)
    requested_features = set(resolved.feature_controls.requested_features)
    expected_exclude_prop_line = resolved.model.base == "no_prop_line"
    stat_evidence_found = False
    profile_evidence_found = False
    model_type_evidence_found = False
    base_evidence_found = False
    calibration_cutoff_evidence_found = False
    calibration_season_evidence_found = False
    training_seasons_evidence_found = False
    feature_control_evidence_found = not (requested_families or requested_features)
    variant_evidence_found = resolved.model.variant is None
    for metadata_name, exposed in metadata.items():
        actual_stat = (
            exposed.get("stat")
            or exposed.get("stat_key")
            or exposed.get("profile")
            or exposed.get("profile_name")
        )
        if actual_stat:
            stat_evidence_found = True
        if actual_stat and actual_stat not in {expected_stat, resolved.profile_obj.stat_key}:
            reasons.append(
                f"Artifact stat mismatch in {metadata_name}: expected {expected_stat}, "
                f"found {actual_stat}"
            )
        profile_name = exposed.get("profile_name") or exposed.get("profile")
        if profile_name:
            profile_evidence_found = True
        if profile_name and str(profile_name) != resolved.profile_obj.stat_key:
            reasons.append(
                f"Artifact profile mismatch in {metadata_name}: expected "
                f"{resolved.profile_obj.stat_key}, found {profile_name}"
            )
        actual_model_type = exposed.get("model_type")
        if actual_model_type:
            model_type_evidence_found = True
        if actual_model_type and str(actual_model_type) not in accepted_model_types:
            reasons.append(
                f"Artifact model type mismatch in {metadata_name}: expected "
                f"{resolved.profile_obj.model_type}, found {actual_model_type}"
            )
        actual_cal_end = exposed.get("cal_end_date") or exposed.get("calibration_end")
        if actual_cal_end is not None:
            calibration_cutoff_evidence_found = True
        if actual_cal_end and str(actual_cal_end) != str(resolved.training.calibration_end):
            reasons.append(
                f"Artifact calibration cutoff mismatch in {metadata_name}: expected "
                f"{resolved.training.calibration_end}, found {actual_cal_end}"
            )
        actual_cal_season = exposed.get("cal_season") or exposed.get("calibration_season")
        if actual_cal_season is not None:
            calibration_season_evidence_found = True
            if int(actual_cal_season) != resolved.training.calibration_season:
                reasons.append(
                    f"Artifact calibration season mismatch in {metadata_name}: expected "
                    f"{resolved.training.calibration_season}, found {actual_cal_season}"
                )
        actual_train_seasons = exposed.get("train_seasons") or exposed.get("training_seasons")
        if actual_train_seasons is not None:
            training_seasons_evidence_found = True
            if [int(value) for value in actual_train_seasons] != resolved.training.seasons:
                reasons.append(
                    f"Artifact training seasons mismatch in {metadata_name}: expected "
                    f"{resolved.training.seasons}, found {actual_train_seasons}"
                )

        actual_exclude_prop_line: bool | None = None
        if "exclude_prop_line" in exposed:
            actual_exclude_prop_line = bool(exposed["exclude_prop_line"])
        elif exposed.get("variant") in {"no_prop_line", "with_prop_line"}:
            actual_exclude_prop_line = exposed["variant"] == "no_prop_line"
        elif "force_exclude_features" in exposed and resolved.profile_obj.prop_line_feature:
            actual_exclude_prop_line = resolved.profile_obj.prop_line_feature in set(
                exposed.get("force_exclude_features") or []
            )
        if actual_exclude_prop_line is not None:
            base_evidence_found = True
            if actual_exclude_prop_line != expected_exclude_prop_line:
                reasons.append(
                    f"Artifact model base mismatch in {metadata_name}: expected "
                    f"{resolved.model.base}"
                )

        if resolved.model.variant is not None:
            actual_variant = exposed.get("ablation_variant")
            if actual_variant is not None:
                variant_evidence_found = True
                if str(actual_variant) != resolved.model.variant:
                    reasons.append(
                        f"Artifact ablation variant mismatch in {metadata_name}: expected "
                        f"{resolved.model.variant}, found {actual_variant}"
                    )

        control_prefix = resolved.feature_controls.mode
        opposite_prefix = "exclude" if control_prefix == "include" else "include"
        family_key = f"force_{control_prefix}_families"
        feature_key = f"force_{control_prefix}_features"
        opposite_family_key = f"force_{opposite_prefix}_families"
        opposite_feature_key = f"force_{opposite_prefix}_features"
        if any(
            key in exposed
            for key in (family_key, feature_key, opposite_family_key, opposite_feature_key)
        ):
            feature_control_evidence_found = True
            actual_families = set(exposed.get(family_key) or [])
            actual_features = set(exposed.get(feature_key) or [])
            opposite_families = set(exposed.get(opposite_family_key) or [])
            opposite_features = set(exposed.get(opposite_feature_key) or [])
            prop_line_exclusion = {
                resolved.profile_obj.prop_line_feature
            } if expected_exclude_prop_line and resolved.profile_obj.prop_line_feature else set()
            selected_prop_line_exception = prop_line_exclusion if control_prefix == "exclude" else set()
            opposite_prop_line_exception = prop_line_exclusion if opposite_prefix == "exclude" else set()

            if actual_families != requested_families:
                reasons.append(
                    f"Artifact forced-{control_prefix} family mismatch in {metadata_name}: requested "
                    f"{sorted(requested_families)}, found {sorted(actual_families)}"
                )
            if actual_features not in (
                requested_features,
                requested_features | selected_prop_line_exception,
            ):
                reasons.append(
                    f"Artifact forced-{control_prefix} feature mismatch in {metadata_name}: requested "
                    f"{sorted(requested_features)}, found {sorted(actual_features)}"
                )
            if opposite_families:
                reasons.append(
                    f"Artifact opposite forced-{opposite_prefix} family mismatch in "
                    f"{metadata_name}: expected [], found {sorted(opposite_families)}"
                )
            if not opposite_features.issubset(opposite_prop_line_exception):
                reasons.append(
                    f"Artifact opposite forced-{opposite_prefix} feature mismatch in "
                    f"{metadata_name}: expected at most {sorted(opposite_prop_line_exception)}, "
                    f"found {sorted(opposite_features)}"
                )
    artifact_material_present = bool(metadata) or any(
        (artifact_dir / name).exists() for name in expected_files
    )
    if artifact_material_present:
        if not metadata:
            reasons.append("Artifact identity metadata is unavailable")
        else:
            if not stat_evidence_found:
                reasons.append("Artifact stat evidence is unavailable")
            if not profile_evidence_found:
                reasons.append("Artifact profile evidence is unavailable")
            if not model_type_evidence_found:
                reasons.append("Artifact model type evidence is unavailable")
            if not base_evidence_found:
                reasons.append("Artifact model base evidence is unavailable")
            if not calibration_cutoff_evidence_found:
                reasons.append("Artifact calibration cutoff evidence is unavailable")
            if not calibration_season_evidence_found:
                reasons.append("Artifact calibration season evidence is unavailable")
            if not training_seasons_evidence_found:
                reasons.append("Artifact training season evidence is unavailable")
            if not feature_control_evidence_found:
                reasons.append("Artifact feature-control evidence is unavailable")
            if not variant_evidence_found:
                reasons.append("Artifact ablation variant evidence is unavailable")
    artifact_files = {
        name: {
            "sha256": _sha256_file(artifact_dir / name),
            "size_bytes": (artifact_dir / name).stat().st_size if (artifact_dir / name).is_file() else None,
        }
        for name in expected_files
    }
    supporting_files = {
        name: _sha256_file(artifact_dir / name)
        for name in (
            "run_config.json",
            "training_metadata.json",
            "feature_manifest.json",
            "model_manifest.json",
        )
        if (artifact_dir / name).is_file()
    }
    identity_sha256 = _identity(
        {
            "profile": resolved.profile_obj.stat_key,
            "model_type": resolved.profile_obj.model_type,
            "artifact_files": artifact_files,
            "supporting_files": supporting_files,
            "metadata": metadata,
            "requested_families": resolved.feature_controls.requested_families,
            "calibration_end": resolved.training.calibration_end,
        }
    )
    identity_status = "FAIL" if reasons else (
        "PLANNED" if dry_run and (not artifact_dir.exists() or bool(missing)) else "PASS"
    )
    return {
        "status": identity_status,
        "profile": resolved.profile_obj.stat_key,
        "model_type": resolved.profile_obj.model_type,
        "artifact_dir": str(artifact_dir),
        "attached": resolved.attached_artifact,
        "requested_families": resolved.feature_controls.requested_families,
        "family_features": resolved.feature_controls.family_features,
        "forced_unique_feature_count": resolved.feature_controls.resolved_feature_count,
        "forced_unique_features": resolved.feature_controls.resolved_features,
        "expected_artifact_files": expected_files,
        "artifact_files": artifact_files,
        "supporting_files": supporting_files,
        "identity_sha256": identity_sha256,
        "missing_artifact_files": missing,
        "model_feature_counts": model_feature_counts,
        "metadata": metadata,
        "reasons": reasons,
        "dry_run": dry_run,
    }


class LifecycleRunner:
    """Execute or dry-run one resolved lifecycle with durable stage state."""

    def __init__(
        self,
        config: str | Path | ResolvedLifecycleConfig,
        *,
        dry_run: bool = False,
        from_stage: str | None = None,
        force_stage: str | None = None,
        run_command: Any = None,
    ) -> None:
        self.resolved = (
            config if isinstance(config, ResolvedLifecycleConfig) else resolve_lifecycle_config(config)
        )
        if from_stage and from_stage not in STAGES:
            raise ValueError(f"Unknown --from-stage {from_stage!r}. Valid: {', '.join(STAGES)}")
        if force_stage and force_stage not in STAGES:
            raise ValueError(f"Unknown --force-stage {force_stage!r}. Valid: {', '.join(STAGES)}")
        self.dry_run = dry_run
        self.from_stage = from_stage
        self.force_stage = force_stage
        self.run_root = (
            Path(f"{self.resolved.run_root}_dry_run") if dry_run else self.resolved.run_root
        )
        self.audit_dir = self.run_root / "audit"
        self.ranker_dir = self.run_root / "ranker"
        self.status_path = self.run_root / "stage_status.json"
        self.commands_path = self.run_root / "commands.json"
        state_source = (
            self.resolved.run_root / "stage_status.json" if dry_run else self.status_path
        )
        command_source = (
            self.resolved.run_root / "commands.json" if dry_run else self.commands_path
        )
        self.statuses: dict[str, Any] = _read_json(state_source, {})
        self.commands: dict[str, Any] = _read_json(command_source, {})
        if force_stage:
            forced_index = STAGES.index(force_stage)
            for stage in STAGES[forced_index:]:
                self.statuses.pop(stage, None)
        self._run_command = run_command or self._subprocess_run
        self.artifact_dir = self.resolved.artifact_dir
        self.config_identity = _identity(self.resolved.to_resolved_dict())
        self.artifact_identity = ""

    @staticmethod
    def _subprocess_run(argv: list[str]) -> int:
        return subprocess.run(argv, cwd=repo_root(), check=False).returncode

    def _record(self, stage: str, status: str, **extra: Any) -> None:
        previous = self.statuses.get(stage, {})
        started = extra.pop("started_at", previous.get("started_at"))
        self.statuses[stage] = {
            "status": status,
            "started_at": started,
            "updated_at": _now(),
            **extra,
        }
        _write_json(self.status_path, self.statuses)

    def _record_command(self, stage: str, argv: list[str] | list[list[str]]) -> None:
        self.commands[stage] = argv
        _write_json(self.commands_path, self.commands)

    def _run_argv(
        self,
        stage: str,
        argv: list[str],
        *,
        input_identity: str,
        verify_outputs: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._record_command(stage, argv)
        if self.dry_run:
            self._record(stage, "skipped", reason="dry_run", command=argv, input_identity=input_identity)
            return
        for downstream in STAGES[STAGES.index(stage) + 1 :]:
            self.statuses.pop(downstream, None)
        self._record(stage, "running", started_at=_now(), command=argv, input_identity=input_identity)
        code = int(self._run_command(argv))
        if code != 0:
            self._record(stage, "failed", exit_code=code, command=argv, input_identity=input_identity)
            raise RuntimeError(f"Lifecycle stage {stage!r} failed with exit code {code}")
        try:
            outputs = verify_outputs() if verify_outputs else {}
        except (OSError, RuntimeError, ValueError) as exc:
            self._record(
                stage,
                "failed",
                exit_code=code,
                command=argv,
                input_identity=input_identity,
                output_error=str(exc),
            )
            raise RuntimeError(
                f"Lifecycle stage {stage!r} required output verification failed: {exc}"
            ) from exc
        self._record(
            stage,
            "completed",
            exit_code=code,
            command=argv,
            input_identity=input_identity,
            outputs=outputs,
        )

    def _before_from_stage(self, stage: str) -> bool:
        if not self.from_stage:
            return False
        return STAGES.index(stage) < STAGES.index(self.from_stage)

    def _completed_with_output(
        self, stage: str, expected: Path | None, *, input_identity: str
    ) -> bool:
        if self.force_stage == stage:
            return False
        state = self.statuses.get(stage, {})
        return (
            state.get("status") == "completed"
            and state.get("input_identity") == input_identity
            and (expected is None or expected.exists())
        )

    def _sweep_stage_input_identity(self) -> str:
        return _identity(
            {"config": self.config_identity, "artifact": self.artifact_identity}
        )

    def _sweep_evidence_identity(self) -> str:
        files = [
            self.resolved.sweep_dir / "sweep_summary.csv",
            self.resolved.sweep_dir / "sweep_results.json",
        ]
        files.extend(sorted(self.resolved.sweep_dir.glob("config_*/bets.csv")))
        files.extend(sorted(self.resolved.sweep_dir.glob("config_*/metrics.json")))
        return _identity(
            {"config": self.config_identity, "artifact": self.artifact_identity}, files
        )

    def _audit_input_identity(self, selection_manifest: dict[str, Any]) -> str:
        selected_files = [self.run_root / "audit_selection.json"]
        for item in selection_manifest["selected"]:
            selected_files.extend([Path(item["bets_csv"]), Path(item["candidate_edges_csv"])])
        return _identity(
            {
                "config": self.config_identity,
                "artifact": self.artifact_identity,
                "sweep": self._sweep_evidence_identity(),
            },
            selected_files,
        )

    def _ranker_input_identity(
        self, matches: list[Path], candidate_edges: list[Path]
    ) -> str:
        evidence_paths: list[Path] = [
            self.run_root / "audit_selection.json",
            self.audit_dir / "suite_manifest.csv",
            *candidate_edges,
        ]
        for match in matches:
            label = match.parent.name
            evidence_paths.extend(
                [
                    match,
                    match.parent / "clv_timing_stability.csv",
                    self.audit_dir / "diagnosis" / label / "clv_failure_modes.json",
                ]
            )
        return _identity(
            {"config": self.config_identity, "artifact": self.artifact_identity},
            evidence_paths,
        )

    def _resolve_trained_artifact(self) -> Path:
        root = self.resolved.artifact_dir
        if all((root / name).exists() for name in self.resolved.profile_obj.model_artifact_names):
            return root
        candidates = [
            path
            for path in root.glob(f"{self.resolved.profile_obj.artifact_prefix}*")
            if path.is_dir() and not path.name.endswith("_incomplete")
        ]
        if not candidates:
            raise FileNotFoundError(
                f"No completed {self.resolved.profile_obj.artifact_prefix}* artifact under {root}"
            )
        return max(candidates, key=lambda path: path.stat().st_mtime)

    def _verify_training_outputs(self) -> dict[str, Any]:
        self.artifact_dir = self._resolve_trained_artifact()
        missing = [
            name
            for name in self.resolved.profile_obj.model_artifact_names
            if not (self.artifact_dir / name).is_file()
        ]
        if missing or self.artifact_dir.name.endswith("_incomplete"):
            raise RuntimeError(
                "train_or_attach required output is incomplete: "
                + ", ".join(missing or [self.artifact_dir.name])
            )
        return {
            "artifact_dir": str(self.artifact_dir),
            "artifact_files": list(self.resolved.profile_obj.model_artifact_names),
        }

    @staticmethod
    def _outputs_are_valid(verifier: Callable[[], dict[str, Any]]) -> bool:
        try:
            verifier()
        except (OSError, RuntimeError, ValueError):
            return False
        return True

    def _write_static_manifests(self) -> None:
        self.run_root.mkdir(parents=True, exist_ok=True)
        resolved_dict = self.resolved.to_resolved_dict()
        (self.run_root / "resolved_config.yaml").write_text(
            yaml.safe_dump(resolved_dict, sort_keys=False), encoding="utf-8"
        )
        manifest_path = self.run_root / "run_manifest.json"
        existing_manifest = _read_json(manifest_path, {})
        manifest = {
            "experiment": self.resolved.experiment_name,
            "profile": self.resolved.profile_obj.stat_key,
            "purpose": self.resolved.purpose,
            "created_at": existing_manifest.get("created_at", _now()),
            "updated_at": _now(),
            "dry_run": self.dry_run,
            "stages": list(STAGES),
            "report_only": True,
            "live_actions_performed": False,
        }
        _write_json(manifest_path, manifest)

    def run(self) -> dict[str, Any]:
        self._write_static_manifests()
        self._record(
            "validate", "completed", started_at=_now(), input_identity=self.config_identity
        )

        if self.resolved.attached_artifact:
            self.artifact_dir = self.resolved.artifact_dir
            if not self.artifact_dir.exists() and not self.dry_run:
                raise FileNotFoundError(f"Attached artifact does not exist: {self.artifact_dir}")
            self._record(
                "train_or_attach",
                "completed",
                reason="attached",
                artifact_dir=str(self.artifact_dir),
                input_identity=self.config_identity,
            )
        elif self.statuses.get("train_or_attach", {}).get("status") == "completed":
            state = self.statuses["train_or_attach"]
            recorded = Path(state.get("artifact_dir", self.resolved.artifact_dir))
            if (
                state.get("input_identity") == self.config_identity
                and self.force_stage != "train_or_attach"
                and recorded.exists()
                and not recorded.name.endswith("_incomplete")
            ):
                self.artifact_dir = recorded
            else:
                state["status"] = "pending"
                if self._before_from_stage("train_or_attach"):
                    raise RuntimeError(
                        "Cannot resume from a downstream stage: the recorded trained artifact "
                        "is missing, incomplete, or has a stale config identity"
                    )
        elif self._before_from_stage("train_or_attach"):
            raise RuntimeError(
                "Cannot resume from a downstream stage without a completed train_or_attach record"
            )
        if (
            not self.resolved.attached_artifact
            and not self._before_from_stage("train_or_attach")
            and self.statuses.get("train_or_attach", {}).get("status") != "completed"
        ):
            command = build_training_command(self.resolved.profile_obj, self.resolved)
            self._run_argv(
                "train_or_attach",
                command,
                input_identity=self.config_identity,
                verify_outputs=self._verify_training_outputs,
            )

        identity = build_artifact_identity(self.resolved, self.artifact_dir, dry_run=self.dry_run)
        self.artifact_identity = str(identity["identity_sha256"])
        _write_json(self.run_root / "artifact_identity.json", identity)
        if identity["status"] == "FAIL":
            self._record("artifact_identity", "failed", reasons=identity["reasons"])
            raise RuntimeError("Artifact identity failed: " + "; ".join(identity["reasons"]))
        if self.resolved.attached_sweep:
            expected_sweep_artifact = self.resolved.model.sweep_artifact_identity_sha256
            if not expected_sweep_artifact:
                raise RuntimeError(
                    "Attached sweeps require model.sweep_artifact_identity_sha256 to prove "
                    "artifact/sweep correspondence"
                )
            if expected_sweep_artifact != self.artifact_identity:
                raise RuntimeError(
                    "Attached sweep artifact identity mismatch: expected "
                    f"{expected_sweep_artifact}, found {self.artifact_identity}"
                )
        self._record(
            "artifact_identity",
            "completed",
            artifact_dir=str(self.artifact_dir),
            input_identity=_identity(
                {"config": self.config_identity, "artifact": self.artifact_identity}
            ),
        )

        sweep_summary = self.resolved.sweep_dir / "sweep_summary.csv"
        if self.resolved.attached_sweep:
            sweep_outputs = _verify_sweep_outputs(self.resolved.sweep_dir)
            self._record(
                "sweep",
                "completed",
                reason="attached",
                sweep_dir=str(self.resolved.sweep_dir),
                input_identity=self._sweep_evidence_identity(),
                outputs=sweep_outputs,
            )
        elif self._before_from_stage("sweep"):
            if self.dry_run:
                self._record("sweep", "skipped", reason="before_from_stage")
            elif not self._completed_with_output(
                "sweep", sweep_summary, input_identity=self._sweep_stage_input_identity()
            ):
                raise RuntimeError(
                    "sweep required output/state is not valid for --from-stage resume"
                )
            else:
                _verify_sweep_outputs(self.resolved.sweep_dir)
        elif not (
            self._completed_with_output(
                "sweep", sweep_summary, input_identity=self._sweep_stage_input_identity()
            )
            and self._outputs_are_valid(
                lambda: _verify_sweep_outputs(self.resolved.sweep_dir)
            )
        ):
            command = build_sweep_command(
                self.resolved,
                artifact_dir=self.artifact_dir,
                output_dir=self.resolved.sweep_dir,
            )
            self._run_argv(
                "sweep",
                command,
                input_identity=self._sweep_stage_input_identity(),
                verify_outputs=lambda: _verify_sweep_outputs(self.resolved.sweep_dir),
            )

        if (self.resolved.sweep_dir / "sweep_results.json").is_file():
            selection_manifest = _select_audit_configs(self.resolved, self.resolved.sweep_dir)
        elif self.dry_run:
            selection_manifest = _planned_audit_selection(self.resolved, self.resolved.sweep_dir)
        else:
            raise RuntimeError("Audit selection cannot run without persisted sweep_results.json")
        _write_json(self.run_root / "audit_selection.json", selection_manifest)
        bets_csvs = [Path(item["bets_csv"]) for item in selection_manifest["selected"]]
        if not bets_csvs:
            raise RuntimeError("Audit selection produced no bets paths; refusing audit auto-discovery")
        audit_manifest = self.audit_dir / "suite_manifest.csv"
        audit_input_identity = self._audit_input_identity(selection_manifest)
        if self._before_from_stage("audit"):
            if self.dry_run:
                self._record("audit", "skipped", reason="before_from_stage")
            elif not self._completed_with_output(
                "audit", audit_manifest, input_identity=audit_input_identity
            ):
                raise RuntimeError(
                    "audit required output/state is not valid for --from-stage resume"
                )
            else:
                try:
                    _verify_audit_outputs(
                        self.audit_dir,
                        bets_csvs,
                        full_audit=self.resolved.audit.mode == "full",
                        require_timing_stability=(
                            self.resolved.purpose == "finalist_certification"
                        ),
                    )
                except (OSError, RuntimeError, ValueError) as exc:
                    raise RuntimeError(f"audit required output verification failed: {exc}") from exc
        elif not (
            self._completed_with_output(
                "audit", audit_manifest, input_identity=audit_input_identity
            )
            and self._outputs_are_valid(
                lambda: _verify_audit_outputs(
                    self.audit_dir,
                    bets_csvs,
                    full_audit=self.resolved.audit.mode == "full",
                    require_timing_stability=(
                        self.resolved.purpose == "finalist_certification"
                    ),
                )
            )
        ):
            command = build_audit_command(
                self.resolved,
                sweep_output_dir=self.resolved.sweep_dir,
                output_dir=self.audit_dir,
                artifact_dir=self.artifact_dir,
                bets_csvs=bets_csvs,
            )
            self._run_argv(
                "audit",
                command,
                input_identity=audit_input_identity,
                verify_outputs=lambda: _verify_audit_outputs(
                    self.audit_dir,
                    bets_csvs,
                    full_audit=self.resolved.audit.mode == "full",
                    require_timing_stability=(
                        self.resolved.purpose == "finalist_certification"
                    ),
                ),
            )

        ranker_commands: list[list[str]] = []
        matches: list[Path] = []
        candidate_edges_paths: list[Path] = []
        for selected_item in selection_manifest["selected"]:
            label = Path(selected_item["config_dir"]).name
            matches_csv = self.audit_dir / "clv" / label / "clv_matches.csv"
            candidate_edges_csv = Path(selected_item["candidate_edges_csv"])
            if not self.dry_run:
                _required_file(matches_csv, "ranker")
                _required_file(candidate_edges_csv, "ranker")
            matches.append(matches_csv)
            candidate_edges_paths.append(candidate_edges_csv)
            ranker_commands.append(
                build_ranker_command(
                    clv_matches_csv=matches_csv,
                    candidate_edges_csv=candidate_edges_csv,
                    output_dir=self.ranker_dir / label,
                    bootstrap_samples=self.resolved.audit.bootstrap_samples,
                    minimum_bets=int(self.resolved.audit.minimum_bets or 0),
                )
            )

        self._record_command("ranker", ranker_commands)
        ranker_outputs_valid = bool(ranker_commands) and all(
            self._outputs_are_valid(
                lambda command=command: _verify_ranker_output(
                    Path(command[command.index("--output-dir") + 1])
                )
            )
            for command in ranker_commands
        )
        ranker_input_identity = self._ranker_input_identity(matches, candidate_edges_paths)
        if self._before_from_stage("ranker"):
            if self.dry_run:
                self._record("ranker", "skipped", reason="before_from_stage")
            elif (
                self.statuses.get("ranker", {}).get("status") != "completed"
                or self.statuses["ranker"].get("input_identity") != ranker_input_identity
                or not ranker_outputs_valid
            ):
                raise RuntimeError(
                    "ranker required output/state is not valid for --from-stage resume"
                )
        elif (
            self.statuses.get("ranker", {}).get("status") == "completed"
            and self.statuses["ranker"].get("input_identity") == ranker_input_identity
            and ranker_outputs_valid
            and self.force_stage != "ranker"
        ):
            pass
        elif self.dry_run:
            self._record(
                "ranker",
                "skipped",
                reason="dry_run",
                commands=ranker_commands,
                input_identity=ranker_input_identity,
            )
        elif ranker_commands:
            self._record(
                "ranker",
                "running",
                started_at=_now(),
                commands=ranker_commands,
                input_identity=ranker_input_identity,
            )
            ranker_outputs: list[dict[str, Any]] = []
            for command in ranker_commands:
                code = int(self._run_command(command))
                if code != 0:
                    self._record(
                        "ranker",
                        "failed",
                        exit_code=code,
                        commands=ranker_commands,
                        input_identity=ranker_input_identity,
                    )
                    raise RuntimeError(f"Lifecycle stage 'ranker' failed with exit code {code}")
                try:
                    output_dir = Path(command[command.index("--output-dir") + 1])
                    ranker_outputs.append(_verify_ranker_output(output_dir))
                except (OSError, RuntimeError, ValueError) as exc:
                    self._record(
                        "ranker",
                        "failed",
                        exit_code=code,
                        commands=ranker_commands,
                        input_identity=ranker_input_identity,
                        output_error=str(exc),
                    )
                    raise RuntimeError(
                        "Lifecycle stage 'ranker' required output verification failed: "
                        f"{exc}"
                    ) from exc
            self._record(
                "ranker",
                "completed",
                exit_code=0,
                commands=ranker_commands,
                input_identity=ranker_input_identity,
                outputs=ranker_outputs,
            )
        else:
            self._record("ranker", "skipped", reason="no_clv_matches")

        if self.dry_run:
            decision = DecisionRecord(
                classification="Shelf",
                posture="live_blocked",
                reasons=["Dry-run only; no evidence was executed"],
                evidence={"dry_run": True},
            )
            self._record_command(
                "decision",
                [
                    "report-only-decision",
                    "--sweep-summary-csv",
                    str(sweep_summary),
                    "--suite-manifest-csv",
                    str(audit_manifest),
                    "--suite-manifest-json",
                    str(self.audit_dir / "suite_manifest.json"),
                    "--ranking-root",
                    str(self.ranker_dir),
                ],
            )
        else:
            decision = evaluate_decision(
                self.resolved,
                sweep_summary_csv=sweep_summary,
                suite_manifest_csv=audit_manifest,
                suite_manifest_json=self.audit_dir / "suite_manifest.json",
                ranking_root=self.ranker_dir,
            )
        _write_json(self.run_root / "promotion_decision.json", decision.to_dict())
        markdown = [
            f"# {self.resolved.experiment_name} decision",
            "",
            f"- Classification: **{decision.classification}**",
            f"- Posture: **{decision.posture}**",
            "- Report only: no deployment or live-trading action was performed.",
            "",
            "## Reasons",
            *(f"- {reason}" for reason in decision.reasons),
            "",
        ]
        (self.run_root / "promotion_decision.md").write_text("\n".join(markdown), encoding="utf-8")
        decision_evidence_paths = [
            self.run_root / "audit_selection.json",
            sweep_summary,
            sweep_summary.with_name("sweep_results.json"),
            audit_manifest,
            self.audit_dir / "suite_manifest.json",
            *sorted(self.audit_dir.glob("clv/*/clv_matches.csv")),
            *sorted(self.audit_dir.glob("clv/*/clv_timing_stability.csv")),
            *sorted(self.audit_dir.glob("diagnosis/*/clv_failure_modes.json")),
            *sorted(self.audit_dir.glob("dropout_audit/*")),
            *sorted(self.ranker_dir.glob("*/ranking_score_summary.csv")),
        ]
        self._record(
            "decision",
            "completed",
            classification=decision.classification,
            posture=decision.posture,
            input_identity=_identity(
                {"config": self.config_identity, "artifact": self.artifact_identity},
                [path for path in decision_evidence_paths if path.is_file()],
            ),
            evidence_files=[str(path) for path in decision_evidence_paths if path.is_file()],
        )
        staking = recommend_staking(self.resolved, decision, dry_run=self.dry_run)
        staking_payload = staking.to_dict()
        decision_output_paths = [
            self.run_root / "promotion_decision.json",
            self.run_root / "promotion_decision.md",
            *[path for path in decision_evidence_paths if path.is_file()],
        ]
        staking_identity = _identity(
            {"config": self.config_identity, "artifact": self.artifact_identity},
            decision_output_paths,
        )
        self._record_command(
            "staking_policy",
            [
                "report-only-staking-policy",
                "--decision-json",
                str(self.run_root / "promotion_decision.json"),
                "--output-json",
                str(self.run_root / "staking_recommendation.json"),
            ],
        )
        staking_payload["input_identity"] = staking_identity
        _write_json(self.run_root / "staking_recommendation.json", staking_payload)
        staking_markdown = [
            f"# {self.resolved.experiment_name} staking recommendation",
            "",
            f"- Recommendation: **{staking.recommendation}**",
            f"- Decision: **{staking.decision_classification}** / **{staking.decision_posture}**",
            "- Report only: no deployment, live-trading, or Kelly action was performed.",
            "",
            "## Reasons",
            *(f"- {reason}" for reason in staking.reasons),
            "",
        ]
        (self.run_root / "staking_recommendation.md").write_text(
            "\n".join(staking_markdown), encoding="utf-8"
        )
        self._record(
            "staking_policy",
            "completed",
            recommendation=staking.recommendation,
            input_identity=staking_identity,
            output_files=[
                str(self.run_root / "staking_recommendation.json"),
                str(self.run_root / "staking_recommendation.md"),
            ],
        )
        return {
            "run_root": str(self.run_root),
            "artifact_dir": str(self.artifact_dir),
            "sweep_dir": str(self.resolved.sweep_dir),
            "decision": decision.to_dict(),
            "staking": staking_payload,
            "statuses": self.statuses,
        }

    def status(self) -> dict[str, Any]:
        return _read_json(self.status_path, {})
