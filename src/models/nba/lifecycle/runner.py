"""Durable, report-only orchestration over the accepted NBA evidence contracts.

Hashes and before/after observations establish consistency, not authenticity or
hostile-filesystem locking. Child evidence is never overwritten or deleted.
"""
from __future__ import annotations

import csv
import importlib.metadata
import io
import json
import os
import re
import subprocess
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

import yaml

from src.models.nba.lifecycle import STAGES
from src.models.nba.lifecycle.adapters import build_audit_command, build_sweep_command, build_training_command
from src.models.nba.lifecycle.config import ResolvedConfig, disjoint, exact_date, read_json, repo_root
from src.models.nba.lifecycle.decision import build_decision, decision_markdown
from src.models.nba_artifacts import canonical_sha256, sha256_file, validate_artifact_bundle

LIFECYCLE_FILES = (
    "src/models/nba/__init__.py", "src/models/nba/lifecycle/__init__.py",
    "src/models/nba/lifecycle/config.py", "src/models/nba/lifecycle/adapters.py",
    "src/models/nba/lifecycle/decision.py", "src/models/nba/lifecycle/runner.py",
    "scripts/run_nba_model_lifecycle.py", "src/models/nba_artifacts.py",
    "src/models/training_preflight.py", "src/backtesting/__init__.py",
    "src/models/train_pipeline.py", "src/backtesting/nba/evidence.py",
    "src/models/features/nba/contracts.py", "src/models/hyperparameter_tuner.py",
    "src/processing/feature_selection.py",
)
TABLE_COLUMNS = {
    "concentration": "cell_id dimension value missing bet_count finite_stakes count_denominator stake_denominator count_share stake_share empty_reason".split(),
    "coverage": "cell_id population category value count denominator ratio reason".split(),
    "timing_violations": "cell_id population row check left_field left_value right_field right_value reason".split(),
    "unmeasurable": "cell_id population row check reason".split(),
}
AUDIT_OUTPUTS = {"audit_summary.json", "audit_summary.md", *(name + ".csv" for name in TABLE_COLUMNS)}
STATUSES = {"completed", "failed", "blocked", "unsupported", "not_requested", "planned"}


class LifecycleError(RuntimeError):
    """Only fixed reason codes cross the operator boundary."""

    def __init__(self, reason: str, status: str = "failed"):
        self.reason, self.status = reason, status
        super().__init__(reason)


def need(condition: bool, reason: str, status: str = "failed") -> None:
    if not condition:
        raise LifecycleError(reason, status)


def same(left, right, reason: str) -> None:
    need(canonical_sha256(left) == canonical_sha256(right), reason)


def owner_path(path: Path) -> None:
    """Ordinary owner-run guard, not a native/adversarial path-lock framework."""
    for candidate in (path, *path.parents):
        need(not candidate.is_symlink(), "linked_lifecycle_path")
        if candidate.exists():
            need(not getattr(candidate.lstat(), "st_reparse_tag", 0), "linked_lifecycle_path")


def atomic_json(path: Path, value: dict | list) -> None:
    atomic_text(path, json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False) + "\n")


def atomic_text(path: Path, text: str) -> None:
    owner_path(path)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def file_receipt(path: Path) -> dict:
    need(path.is_file() and not path.is_symlink(), "missing_or_linked_file")
    before = path.stat()
    receipt = {"sha256": sha256_file(path), "bytes": before.st_size}
    after = path.stat()
    need((before.st_size, before.st_mtime_ns, before.st_ino) ==
         (after.st_size, after.st_mtime_ns, after.st_ino), "file_changed_during_read")
    return receipt


def export_inventory(root: Path) -> dict:
    owner_path(root)
    need(root.is_dir(), "missing_export")
    names = {"audit_manifest.json", *AUDIT_OUTPUTS}
    need({item.name for item in root.iterdir()} == names, "audit_export_layout")
    return {name: file_receipt(root / name) for name in sorted(names)}


def verify_audit_export(output_dir: Path, *, sweep_dir: Path, checks: list[str], child_exit_code: int) -> dict:
    """Thin export envelope verifier; public audit_sweep owns all audit math."""
    from src.backtesting.nba.evidence import audit_sweep

    need(not output_dir.name.endswith("_incomplete") and
         not output_dir.with_name(output_dir.name + "_incomplete").exists(), "incomplete_audit_export")
    before = export_inventory(output_dir)
    auditor = file_receipt(repo_root() / "src/backtesting/nba/evidence.py")
    manifest: dict = read_json(output_dir / "audit_manifest.json")
    summary = read_json(output_dir / "audit_summary.json")
    expected_keys = {"audit_schema_version", "report_state", "decision_grade", "historical_readiness",
                     "requested_checks", "source_files", "checks", "outputs", "audit_source"}
    need(type(manifest) is dict and set(manifest) == expected_keys, "audit_manifest_schema")
    same(manifest["audit_schema_version"], 1, "audit_manifest_schema")
    need(manifest["report_state"] == "complete" and manifest["decision_grade"] is False
         and manifest["historical_readiness"] == "unverified", "audit_manifest_posture")
    same(manifest["requested_checks"], checks, "audit_requested_checks")
    need(type(manifest["outputs"]) is dict and set(manifest["outputs"]) == AUDIT_OUTPUTS,
         "audit_output_inventory")
    for name in AUDIT_OUTPUTS:
        same(manifest["outputs"][name], before[name], "audit_output_digest")
    same(manifest["audit_source"], {"module": "src/backtesting/nba/evidence.py", "sha256": auditor["sha256"]},
         "auditor_source_drift")
    # This is the authoritative public semantic recomputation, not an export helper.
    report = audit_sweep(sweep_dir, checks=checks)
    same(summary, report, "audit_report_recomputation")
    same(manifest["source_files"], report["source_files"], "audit_source_receipts")
    same(manifest["checks"], report["checks"], "audit_check_statuses")
    same(manifest["report_state"], report["report_state"], "audit_report_state")
    same(report["requested_checks"], checks, "audit_requested_checks")
    for table, columns in TABLE_COLUMNS.items():
        with (output_dir / (table + ".csv")).open(encoding="utf-8", newline="") as handle:
            actual = list(csv.reader(handle, strict=True))
        expected = io.StringIO(newline="")
        writer = csv.DictWriter(expected, fieldnames=columns)
        writer.writeheader()
        writer.writerows(report["tables"][table])
        expected.seek(0)
        same(actual, list(csv.reader(expected, strict=True)), "audit_csv_semantics")
    expected_text = ("NBA saved-evidence audit (not historical acceptance)\n\n"
                     + "\n".join(f"{key}: {value['status']} ({value['reason']})"
                                 for key, value in report["checks"].items()))
    expected_text += ("\n\nDecision grade: false. Report completion is serialization only.\n"
                      + "\n".join(report["limitations"]) + "\n")
    need((output_dir / "audit_summary.md").read_text(encoding="utf-8") == expected_text,
         "audit_markdown_semantics")
    same(before, export_inventory(output_dir), "audit_export_changed")
    same(auditor, file_receipt(repo_root() / "src/backtesting/nba/evidence.py"), "auditor_source_drift")
    requested = {key: report["checks"][key]["status"] for key in checks}
    # Never recompute precedence locally; retain the API's authoritative exit.
    report_exit = report["exit_code"]
    need(type(report_exit) is int and report_exit in (0, 1, 2), "audit_report_exit")
    conclusion = "FAIL" if report_exit == 1 else "BLOCKED" if report_exit == 2 else "PASS"
    return {"path": str(output_dir), "sweep_dir": str(sweep_dir), "export_verified": True,
            "report_state": report["report_state"], "audit_conclusion": conclusion,
            "requested_check_statuses": requested, "issues": report["issues"],
            "exit_code": child_exit_code, "report_exit_code": report_exit,
            "report_identity": canonical_sha256(report), "export_files": before,
            "source_request_identity": report.get("source_request_identity"),
            "source_input_identity": report.get("source_input_identity"),
            "audit_source": manifest["audit_source"]}


def inspect_artifact(resolved: ResolvedConfig, path: Path) -> dict:
    inspection = validate_artifact_bundle(path, requested_stats=resolved.config.evaluation.stats, allow_legacy=False)
    manifest = inspection.manifest
    if manifest is None:
        raise LifecycleError("unverified_artifact")
    need(inspection.state == "verified_manifest", "unverified_artifact")
    try:
        cutoff = exact_date(manifest["calibration"]["end_date_exclusive"])
    except (ValueError, TypeError, KeyError):
        raise LifecycleError("actual_calibration_cutoff_missing", "blocked") from None
    need(resolved.config.evaluation.start >= cutoff, "actual_calibration_overlap", "blocked")
    actual_training = manifest["training"]
    need(type(actual_training.get("train_seasons")) is list and bool(actual_training["train_seasons"]),
         "actual_training_metadata_missing", "blocked")
    training = resolved.config.training
    if training is not None:
        need(manifest["mode"] == "full", "training_not_full")
        same(actual_training, {"train_seasons": training.train_seasons, "tune_hyperparams": training.tune,
                               "tuning_trials": training.tuning_trials, "tuning_timeout": training.tuning_timeout,
                               "tuning_per_quantile": False, "feature_tolerance": float(training.feature_tolerance)},
             "requested_training_metadata_mismatch")
        same(manifest["calibration"]["season"], training.calibration_season, "calibration_season_mismatch")
        same(cutoff.isoformat(), training.calibration_end.isoformat(), "calibration_end_mismatch")
        if resolved.hyperparams_path is not None:
            hp = read_json(resolved.hyperparams_path)
            receipt = {"sha256": sha256_file(resolved.hyperparams_path), "canonical_sha256": canonical_sha256(hp)}
            same(receipt, resolved.hyperparams_receipt, "hyperparameters_changed")
            same(canonical_sha256(read_json(path / "best_hyperparams.json")), receipt["canonical_sha256"],
                 "saved_hyperparameters_mismatch")
    return {"artifact": inspection.to_plan_dict(), "manifest_file": file_receipt(path / "artifact_manifest.json"),
            "actual_training": actual_training, "actual_calibration": manifest["calibration"],
            "data_provenance": manifest["data_provenance"], "hyperparams_input": resolved.hyperparams_receipt,
            "runtime_setting_application": "unverified_unsigned_manifest",
            "historical_readiness": "unverified",
            "policy": resolved.historical_attachment or "unresolved_until_slice8"}


def execute_child(argv: list[str]) -> int:
    # Do not relay or persist arbitrary DB/provider stdout, stderr or environments.
    result = subprocess.run(argv, cwd=repo_root(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return result.returncode


@contextmanager
def run_lock(root: Path):
    path = root / ".lifecycle.lock"
    owner_path(path)
    try:
        handle = path.open("x", encoding="utf-8")
    except FileExistsError:
        raise LifecycleError("lifecycle_writer_lock_present", "blocked") from None
    try:
        with handle:
            handle.write(str(os.getpid()) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        yield
    finally:
        path.unlink()


def allocate_attempt(root: Path, kind: str) -> Path:
    parent = root / kind
    owner_path(parent)
    parent.mkdir(exist_ok=True)
    numbers = []
    for path in parent.iterdir():
        match = re.fullmatch(r"attempt-([0-9]{4,})", path.name)
        if match is None:
            raise LifecycleError("unexpected_attempt_entry")
        numbers.append(int(match[1]))
    candidate = parent / f"attempt-{max(numbers, default=0) + 1:04d}"
    candidate.mkdir(exist_ok=False)
    return candidate


def owned_state(root: Path) -> dict:
    owner_path(root)
    state: dict = read_json(root / "stage_status.json")
    need(type(state) is dict and set(state) == {"schema_version", "config_identity", "stages", "decision_current"},
         "corrupt_lifecycle_state")
    same(state["schema_version"], 1, "corrupt_lifecycle_state")
    need(type(state["decision_current"]) is bool and type(state["stages"]) is dict
         and set(state["stages"]).issubset(STAGES), "corrupt_lifecycle_state")
    for record in state["stages"].values():
        if type(record) is not dict:
            raise LifecycleError("corrupt_stage_record")
        need(record.get("status") in STATUSES, "corrupt_stage_record")
        if record["status"] != "planned":
            need(all(key in record for key in ("input", "output", "input_identity", "output_identity")),
                 "corrupt_stage_record")
        if record.get("output_identity") is not None:
            same(record["output_identity"], canonical_sha256(record["output"]), "inconsistent_stage_output")
        if record.get("input_identity") is not None:
            same(record["input_identity"], canonical_sha256(record["input"]), "inconsistent_stage_input")
    manifest = read_json(root / "run_manifest.json")
    same(manifest["config_identity"], state["config_identity"], "inconsistent_config_identity")
    resolved = read_json(root / "resolved_config.json")
    same(canonical_sha256(resolved), state["config_identity"], "inconsistent_resolved_config")
    artifact_stage = state["stages"].get("artifact_identity", {})
    if artifact_stage.get("status") == "completed":
        same(read_json(root / "artifact_identity.json"), artifact_stage["output"], "inconsistent_artifact_receipt")
    return state


def status(run_root: str | Path) -> tuple[dict, int]:
    """Read lifecycle-owned files only. Recorded stages are never live-verified."""
    root = Path(run_root).resolve(strict=False)
    if not root.is_dir() or not (root / "stage_status.json").is_file():
        return {"state": "missing", "recorded_evidence_verified": False, "decision_current": False}, 2
    try:
        state = owned_state(root)
        current = False
        classification = None
        if state["decision_current"] and not (root / ".lifecycle.lock").exists():
            record = state["stages"].get("decision", {})
            predecessors = {key: value for key, value in state["stages"].items() if key != "decision"}
            need(record.get("status") == "completed", "inconsistent_decision")
            same(record["input"]["stages_identity"], canonical_sha256(predecessors), "stale_decision")
            decision = read_json(root / "decision.json")
            same(record["output"]["decision"], decision, "inconsistent_decision")
            same(decision["stage_identity"], canonical_sha256(predecessors), "stale_decision")
            same(record["output"]["files"], {name: file_receipt(root / name)
                                            for name in ("decision.json", "decision.md")}, "inconsistent_decision")
            config = read_json(root / "resolved_config.json")["config"]
            same(decision, build_decision(purpose=config["experiment"]["purpose"], stages=predecessors,
                                          requested_checks=config["audit"]["checks"]), "inconsistent_decision")
            current, classification = True, decision["classification"]
        return {"state": "recorded", "recorded_evidence_verified": False,
                "stages": {name: item["status"] for name, item in state["stages"].items()},
                "decision_current": current, "classification": classification}, 0
    except Exception:
        return {"state": "corrupt", "recorded_evidence_verified": False, "decision_current": False}, 1


class LifecycleRunner:
    def __init__(self, resolved: ResolvedConfig, *, command_runner: Callable = execute_child):
        self.resolved = resolved
        self.root = resolved.run_root
        self.command_runner = command_runner
        self.state: dict = {"schema_version": 1, "config_identity": resolved.config_identity,
                            "stages": {}, "decision_current": False}
        self.commands: list = []
        self.active_child: dict | None = None

    def persist(self) -> None:
        atomic_json(self.root / "stage_status.json", self.state)

    def invalidate(self, stage: str) -> None:
        self.state["decision_current"] = False
        for name in STAGES[STAGES.index(stage):]:
            self.state["stages"].pop(name, None)
        self.persist()
        for name in ("decision.json", "decision.md"):
            path = self.root / name
            owner_path(path)
            if path.exists():
                path.unlink()

    def initialize(self) -> None:
        if (self.root / "stage_status.json").exists():
            try:
                self.state = owned_state(self.root)
            except Exception:
                # Keep corrupt state/evidence intact but withdraw the owned decision.
                for name in ("decision.json", "decision.md"):
                    path = self.root / name
                    owner_path(path)
                    if path.exists():
                        path.unlink()
                raise LifecycleError("corrupt_existing_lifecycle_state") from None
            if self.state["config_identity"] != self.resolved.config_identity:
                self.invalidate("validate")
                raise LifecycleError("existing_run_config_conflict", "blocked")
            self.commands = read_json(self.root / "commands.json")
            need(type(self.commands) is list, "corrupt_command_history")
            return
        need(not any(item.name != ".lifecycle.lock" for item in self.root.iterdir()), "unowned_run_root", "blocked")
        atomic_json(self.root / "resolved_config.json", self.resolved.to_dict())
        atomic_text(self.root / "resolved_config.yaml", yaml.safe_dump(self.resolved.to_dict(), sort_keys=False))
        atomic_json(self.root / "run_manifest.json", {"schema_version": 1, "config_identity": self.resolved.config_identity,
                                                    "historical_readiness": "unverified", "decision_grade": False})
        atomic_json(self.root / "commands.json", [])
        self.persist()

    def sources(self) -> dict:
        return {name: sha256_file(repo_root() / name) for name in LIFECYCLE_FILES}

    def input_for(self, stage: str) -> dict:
        predecessors = STAGES[:STAGES.index(stage)]
        values = {"schema_version": 1, "stage": stage, "config_identity": self.resolved.config_identity,
                  "sources": self.sources(), "predecessors": {name: self.state["stages"].get(name, {}).get("output_identity")
                                                               for name in predecessors}}
        if self.resolved.historical_attachment is not None:
            from src.backtesting.nba.historical_execution import HISTORICAL_SOURCE_FILES, recheck_attachment

            receipt = self.resolved.historical_attachment
            recheck_attachment(receipt)
            values['historical_attachment'] = receipt
            values['historical_sources'] = {name: sha256_file(repo_root() / name) for name in HISTORICAL_SOURCE_FILES}
        if stage == "train_or_attach" and self.resolved.artifact_path is not None:
            values["attached_artifact"] = inspect_artifact(self.resolved, self.resolved.artifact_path)
        if stage in ("artifact_identity", "sweep", "audit"):
            values["pinned_artifact"] = inspect_artifact(self.resolved, self.artifact_path())
        if stage in ("train_or_attach", "artifact_identity", "sweep", "audit"):
            from src.backtesting.nba.sweep_config import DEPENDENCIES, SOURCE_FILES

            values["sweep_sources"] = {name: sha256_file(repo_root() / name) for name in SOURCE_FILES}
            versions = {}
            for module, distribution in DEPENDENCIES.items():
                try:
                    versions[module] = importlib.metadata.version(distribution)
                except importlib.metadata.PackageNotFoundError:
                    if module != "psycopg2":
                        raise
                    versions[module] = importlib.metadata.version("psycopg2")
            values["dependencies"] = versions
        if stage == "decision":
            values["stages_identity"] = canonical_sha256({name: value for name, value in self.state["stages"].items()
                                                        if name != "decision"})
        return values

    def child(self, stage: str, argv: list[str], attempt: Path) -> int:
        self.active_child = {"attempt": str(attempt), "exit_code": None}
        self.commands.append({"stage": stage, "attempt": str(attempt), "argv": argv, "exit_code": None})
        atomic_json(self.root / "commands.json", self.commands)
        result = self.command_runner(argv)
        code = result if type(result) is int else result.returncode
        need(type(code) is int, "invalid_child_exit_code")
        self.active_child["exit_code"] = code
        self.commands[-1]["exit_code"] = code
        atomic_json(self.root / "commands.json", self.commands)
        return code

    def artifact_path(self) -> Path:
        return Path(self.state["stages"]["train_or_attach"]["output"]["artifact"]["path"])

    def expected_sweep_plan(self, output: Path) -> dict:
        from src.backtesting.nba.sweep_config import build_plan

        config = self.resolved.sweep_config(artifact_dir=self.artifact_path(), output_dir=output)
        return build_plan(config, check_output_collision=False)

    def verify_sweep(self, output: Path, expected: dict) -> dict:
        from src.backtesting.nba.evidence import inspect_sweep

        need(not output.with_name(output.name + "_incomplete").exists(), "incomplete_sweep")
        before = file_receipt(output / "sweep_manifest.json")
        report = inspect_sweep(output)
        code = report["exit_code"]
        need(code == 0 and report["checks"]["integrity"]["status"] == "PASS"
             and report["report_state"] == "complete", "sweep_integrity_not_pass", "blocked" if code == 2 else "failed")
        manifest = read_json(output / "sweep_manifest.json")
        need(manifest["state"] == "complete" and manifest["historical_readiness"] == "unverified"
             and manifest["decision_grade"] is False, "sweep_posture")
        same(manifest["plan"], expected, "sweep_command_plan_mismatch")
        same(before, file_receipt(output / "sweep_manifest.json"), "sweep_changed")
        return {"path": str(output), "plan": expected, "manifest_file": before,
                "inspection_identity": canonical_sha256(report), "source_files": report["source_files"],
                "input_identity": manifest["input_identity"], "exit_code": 0, "integrity": "PASS"}

    def verify(self, stage: str, output: dict) -> dict:
        if stage == "validate":
            return {"resolved_config_identity": canonical_sha256(read_json(self.root / "resolved_config.json"))}
        if stage == "train_or_attach":
            path = Path(output["artifact"]["path"])
            if self.resolved.config.artifact.mode == "train":
                attempt = self.checked_attempt(output["attempt"], "training")
                entries = list(attempt.iterdir())
                need(len(entries) == 1 and entries[0] == path and path.is_dir(), "ambiguous_training_output")
            else:
                need(path == self.resolved.artifact_path, "attached_path_mismatch")
            return {**inspect_artifact(self.resolved, path), "attempt": output.get("attempt"), "exit_code": 0}
        if stage == "artifact_identity":
            value = inspect_artifact(self.resolved, self.artifact_path())
            same(read_json(self.root / "artifact_identity.json"), value, "artifact_identity_file_mismatch")
            return value
        if stage == "sweep":
            attempt = self.checked_attempt(output["attempt"], "sweep")
            need(Path(output["path"]) == attempt / "output" and {item.name for item in attempt.iterdir()} == {"output"},
                 "sweep_attempt_layout")
            value = self.verify_sweep(attempt / "output", self.expected_sweep_plan(attempt / "output"))
            return {**value, "attempt": str(attempt)}
        if stage == "audit":
            attempt = self.checked_attempt(output["attempt"], "audit")
            need(Path(output["path"]) == attempt / "output" and {item.name for item in attempt.iterdir()} == {"output"},
                 "audit_attempt_layout")
            value = verify_audit_export(attempt / "output", sweep_dir=self.sweep_path(),
                                        checks=self.resolved.config.audit.checks, child_exit_code=output["exit_code"])
            return {**value, "attempt": str(attempt)}
        if stage == "ranker":
            return {"reason": "nba_ranker_not_scoped_in_slice7"}
        if stage == "decision":
            predecessors = {name: value for name, value in self.state["stages"].items() if name != "decision"}
            value = build_decision(purpose=self.resolved.config.experiment.purpose, stages=predecessors,
                                   requested_checks=self.resolved.config.audit.checks)
            same(read_json(self.root / "decision.json"), value, "decision_file_mismatch")
            need((self.root / "decision.md").read_text(encoding="utf-8") == decision_markdown(value), "decision_text_mismatch")
            return {"decision": value, "files": {name: file_receipt(self.root / name)
                                                  for name in ("decision.json", "decision.md")}}
        raise LifecycleError("unknown_stage")

    def checked_attempt(self, token: str, kind: str) -> Path:
        path = Path(token)
        owner_path(path)
        need(path.parent == self.root / kind and re.fullmatch(r"attempt-[0-9]{4,}", path.name) is not None,
             "attempt_path_mismatch")
        return path

    def sweep_path(self) -> Path:
        return Path(self.state["stages"]["sweep"]["output"]["path"])

    def perform(self, stage: str) -> tuple[str, dict]:
        if stage == "validate":
            return "completed", {"resolved_config_identity": self.resolved.config_identity}
        if stage == "train_or_attach":
            if self.resolved.artifact_path is not None:
                path, attempt = self.resolved.artifact_path, None
            else:
                attempt = allocate_attempt(self.root, "training")
                need(not list(attempt.iterdir()), "training_attempt_not_empty")
                code = self.child(stage, build_training_command(self.resolved, artifact_root=attempt), attempt)
                need(code == 0, "training_child_failed")
                entries = list(attempt.iterdir())
                need(len(entries) == 1 and entries[0].is_dir(), "ambiguous_training_output")
                path = entries[0]
            return "completed", {**inspect_artifact(self.resolved, path),
                                 "attempt": str(attempt) if attempt else None, "exit_code": 0}
        if stage == "artifact_identity":
            value = inspect_artifact(self.resolved, self.artifact_path())
            atomic_json(self.root / "artifact_identity.json", value)
            return "completed", value
        if stage == "sweep":
            attempt = allocate_attempt(self.root, "sweep")
            output = attempt / "output"
            expected = self.expected_sweep_plan(output)
            argv = build_sweep_command(self.resolved, artifact_dir=self.artifact_path(), output_dir=output)
            code = self.child(stage, argv, attempt)
            need(code == 0, "sweep_child_failed")
            return "completed", {**self.verify_sweep(output, expected), "attempt": str(attempt)}
        if stage == "audit":
            attempt = allocate_attempt(self.root, "audit")
            output = attempt / "output"
            code = self.child(stage, build_audit_command(self.resolved, sweep_dir=self.sweep_path(), output_dir=output), attempt)
            value = verify_audit_export(output, sweep_dir=self.sweep_path(), checks=self.resolved.config.audit.checks,
                                        child_exit_code=code)
            value["attempt"] = str(attempt)
            if code != value["report_exit_code"] or code not in (0, 1, 2):
                return "failed", value
            if value["report_exit_code"] == 1:
                return "failed", value
            if code != 0 or value["report_state"] != "complete" or any(
                    item != "PASS" for item in value["requested_check_statuses"].values()):
                return "blocked", value
            return "completed", value
        if stage == "ranker":
            return "not_requested", {"reason": "nba_ranker_not_scoped_in_slice7"}
        if stage == "decision":
            predecessors = {name: value for name, value in self.state["stages"].items() if name != "decision"}
            value = build_decision(purpose=self.resolved.config.experiment.purpose, stages=predecessors,
                                   requested_checks=self.resolved.config.audit.checks)
            atomic_json(self.root / "decision.json", value)
            atomic_text(self.root / "decision.md", decision_markdown(value))
            return "completed", {"decision": value, "files": {name: file_receipt(self.root / name)
                                                               for name in ("decision.json", "decision.md")}}
        raise LifecycleError("unknown_stage")

    def record(self, stage: str, state: str, inputs: dict, output: dict) -> None:
        self.state["stages"][stage] = {"status": state, "input": inputs, "input_identity": canonical_sha256(inputs),
                                      "output": output, "output_identity": canonical_sha256(output)}
        if stage == "decision":
            self.state["decision_current"] = state == "completed"
        self.persist()

    def run(self, *, from_stage: str | None = None, force_stage: str | None = None) -> tuple[dict, int]:
        need(from_stage is None or from_stage in STAGES, "invalid_from_stage", "blocked")
        need(force_stage is None or force_stage in STAGES, "invalid_force_stage", "blocked")
        if from_stage and force_stage:
            need(STAGES.index(force_stage) >= STAGES.index(from_stage), "force_precedes_from_stage", "blocked")
        owner_path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)
        with run_lock(self.root):
            self.initialize()
            if force_stage:
                self.invalidate(force_stage)
            blocked = False
            for stage in STAGES:
                self.active_child = None
                before_from = from_stage is not None and STAGES.index(stage) < STAGES.index(from_stage)
                inputs = {"config_identity": self.resolved.config_identity, "stage": stage}
                try:
                    if blocked and stage not in ("ranker", "decision"):
                        self.invalidate(stage)
                        self.record(stage, "blocked", inputs, {"reason": "required_predecessor_not_pass"})
                        continue
                    inputs = self.input_for(stage)
                    record = self.state["stages"].get(stage, {})
                    reusable = record.get("status") == "completed" or (
                        stage == "ranker" and record.get("status") == "not_requested")
                    if reusable and record.get("input_identity") == canonical_sha256(inputs):
                        try:
                            verified = self.verify(stage, record["output"])
                            same(record["output_identity"], canonical_sha256(verified), "stage_output_identity_mismatch")
                            continue
                        except Exception as exc:
                            # A changed saved output is invalid evidence, not an implicit repair.
                            self.invalidate(stage)
                            if isinstance(exc, LifecycleError):
                                raise
                            raise LifecycleError("saved_output_verification_failed") from None
                    if before_from:
                        self.invalidate(stage)
                        raise LifecycleError("unverified_resume_predecessor", "blocked")
                    self.invalidate(stage)

                    disposition, output = self.perform(stage)
                    # Re-observe explicit source/dependency/artifact inputs after child execution.
                    same(inputs, self.input_for(stage), "stage_inputs_changed")
                    self.record(stage, disposition, inputs, output)
                    if disposition in ("failed", "blocked", "unsupported"):
                        blocked = True
                except Exception as exc:
                    self.invalidate(stage)
                    failure = exc if isinstance(exc, LifecycleError) else LifecycleError("execution_or_verification_error")
                    output = {"reason": failure.reason, **(self.active_child or {})}
                    if stage == "audit":
                        output["export_verified"] = False
                    self.record(stage, failure.status, inputs, output)
                    blocked = True
                    if before_from:
                        # Never execute successors after a refused predecessor verification.
                        break
            return self.result()

    def result(self) -> tuple[dict, int]:
        stages = self.state["stages"]
        decision = stages.get("decision", {}).get("output", {}).get("decision")
        failed = any(item["status"] == "failed" for item in stages.values())
        completed = decision is not None and decision["classification"] == "controlled_mechanics_complete"
        code = 1 if failed else 0 if completed else 2
        return {"state": "failed" if failed else "complete" if completed else "blocked",
                "stages": {name: item["status"] for name, item in stages.items()},
                "classification": decision["classification"] if decision else None,
                "decision_current": self.state["decision_current"], "historical_readiness": "unverified",
                "decision_grade": False}, code


def dry_run(resolved: ResolvedConfig) -> tuple[dict, int]:
    """Write planning files only; train paths are explicitly unresolved plans."""
    root = resolved.run_root.with_name(resolved.run_root.name + "_dry_run")
    owner_path(root)
    artifact = resolved.artifact_path
    inspection = inspect_artifact(resolved, artifact) if artifact is not None else None
    planned_artifact = artifact or resolved.run_root / "training" / "attempt-0001" / "PLANNED_TRAINING_BUNDLE"
    commands = []
    if artifact is None:
        commands.append({"stage": "train_or_attach", "argv": build_training_command(
            resolved, artifact_root=resolved.run_root / "training" / "attempt-0001")})
    sweep = resolved.run_root / "sweep" / "attempt-0001" / "output"
    commands += [{"stage": "sweep", "argv": build_sweep_command(resolved, artifact_dir=planned_artifact, output_dir=sweep)},
                 {"stage": "audit", "argv": build_audit_command(resolved, sweep_dir=sweep,
                                                                    output_dir=resolved.run_root / "audit" / "attempt-0001" / "output")}]
    if root.exists():
        owner_path(root)
        expected = {"resolved_config.json", "resolved_config.yaml", "run_manifest.json", "commands.json", "stage_status.json"}
        need({path.name for path in root.iterdir()}.issubset(expected) and (root / "run_manifest.json").is_file(),
             "unowned_dry_run_root", "blocked")
        same(read_json(root / "run_manifest.json")["config_identity"], resolved.config_identity, "dry_run_config_conflict")
    root.mkdir(parents=True, exist_ok=True)
    atomic_json(root / "resolved_config.json", resolved.to_dict())
    atomic_text(root / "resolved_config.yaml", yaml.safe_dump(resolved.to_dict(), sort_keys=False))
    atomic_json(root / "run_manifest.json", {"schema_version": 1, "config_identity": resolved.config_identity,
                                           "mode": "dry_run", "artifact_inspection": inspection,
                                           "artifact_path_unresolved": artifact is None})
    atomic_json(root / "commands.json", commands)
    state = {"schema_version": 1, "config_identity": resolved.config_identity,
             "stages": {name: {"status": "planned"} for name in STAGES}, "decision_current": False}
    atomic_json(root / "stage_status.json", state)
    return {"state": "planned", "stages": {name: "planned" for name in STAGES}, "decision_current": False,
            "artifact_path_unresolved": artifact is None, "historical_readiness": "unverified", "decision_grade": False}, 0


def run_lifecycle(resolved: ResolvedConfig, *, dry_run_only: bool = False, from_stage: str | None = None,
                  force_stage: str | None = None, command_runner: Callable = execute_child) -> tuple[dict, int]:
    need(not dry_run_only or (from_stage is None and force_stage is None), "dry_run_resume_conflict", "blocked")
    # Re-check containment at the write boundary, without adversarial locking.
    for target in (resolved.artifact_path, resolved.hyperparams_path):
        if target is not None:
            need(disjoint(resolved.run_root, target.resolve(strict=False)), "output_input_collision", "blocked")
    if dry_run_only:
        return dry_run(resolved)
    return LifecycleRunner(resolved, command_runner=command_runner).run(from_stage=from_stage, force_stage=force_stage)
