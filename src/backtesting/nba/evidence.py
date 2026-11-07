"""Artifact-only NBA sweep v1 audits; no replay, model loading or source lookup.

PASS means saved-output consistency, never historical availability, truthful
execution, settlement correctness or promotion. Hashes are unsigned commitments.
Only an explicit export may resolve the recorded model path, for containment.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import stat
import sys
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from itertools import product
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, cast

from src.backtesting.nba.historical_execution import (
    BOUNDED_HISTORICAL_SOURCE_FILES,
    HISTORICAL_SOURCE_FILES,
    source_policy,
    validate_saved_attachment,
    validate_window,
)
from src.config.combo_config import COMBO_DEFINITIONS, STAT_TO_MARKET
from src.models.nba_artifacts import (
    _rename_no_replace,
    canonical_sha256,
    create_exclusive_incomplete_dir,
    sha256_file,
)

JSON_LIMIT = 16 * 1024 * 1024
CSV_LIMIT = 1024 * 1024 * 1024
ROW_LIMIT = 1_000_000
CELL_LIMIT = 64
DEFAULT_CHECKS = ("integrity", "concentration", "coverage", "timing")
CAPABILITIES = {
    "clv": ("UNSUPPORTED", "closing_snapshot_histories_required"),
    "intraday_stability": ("UNSUPPORTED", "observed_horizon_quotes_and_decisions_required"),
    "source_verified_dropout": ("BLOCKED", "source_verified_opportunity_universe_required"),
    "historical_readiness": ("BLOCKED", "source_clock_policy_and_loader_query_evidence_required"),
    "promotion": ("BLOCKED", "independent_historical_and_model_acceptance_required"),
    "input_content_reconstruction": ("BLOCKED", "not_persisted"),
    "execution_setting_application": ("BLOCKED", "no_runtime_receipt_proof"),
}
READINESS = {
    "historical_readiness": "unverified", "decision_grade": False,
    "unresolved": ["historical timing", "source policy", "loader completeness", "query tie ordering"],
}
FIXED_KEYS = set("model_dir start end stats bookmakers n_samples seed min_minutes_avg starting_bankroll "
                 "flat_bet_size edge_thresholds output_dir allowed_bets bl_taus bl_z_max bl_max_weight "
                 "max_cells local diagnostic_history market_stats".split())
CELL_KEYS = set("edge_threshold tau z_max max_weight raw_control".split())
PLAN_KEYS = set("schema_version fixed_config artifact predictor_settings dependencies source ordered_features "
                "source_policy cells cell_count acquisition_settings acquisition_identity request_identity".split()) | set(READINESS)
ACQUISITION_KEYS = "model_dir start end stats market_stats bookmakers n_samples seed min_minutes_avg local".split()
BOOKMAKERS = set("draftkings fanduel betmgm betrivers bovada williamhill_us betonlineag unibet_us mybookieag "
                "pointsbetus fanatics barstool wynnbet ballybet betopenly betparx espnbet fliff hardrockbet "
                "novig prophetx rebet windcreek pinnacle".split())
PROVENANCE_SUFFIXES = ("snapshot_time", "market_last_update", "bookmaker_last_update", "decision_time", "bookmaker")
PREDICTION_COLUMNS = "player_id game_id stat game_date player_name team_id game_time pred_mean pred_std pred_median pred_q10 pred_q25 pred_q50 pred_q75 pred_q90".split()
SELECTED_COLUMNS = "selected_side selected_line selected_price max_edge".split()
BL_COLUMNS = "model_over model_under market_over market_under confidence posterior_over posterior_under bl_enabled".split()
QUOTE_COLUMNS = "player_id game_id market_key line bookmaker over_odds under_odds".split()
QUOTE_METADATA = [f"{side}_{suffix}" for side in ("over", "under", "selected") for suffix in PROVENANCE_SUFFIXES]
EDGE_COLUMNS = PREDICTION_COLUMNS + "line bookmaker over_odds under_odds sample_status quote_status over_prob under_prob implied_over implied_under over_edge under_edge combo_rejection_reason combo_component_evidence market_key bet_decision_time quote_decision_policy".split() + QUOTE_METADATA + BL_COLUMNS
BET_COLUMNS = "player_id game_id game_date stat side line odds stake model_prob implied_prob edge bookmaker selected_snapshot_time over_snapshot_time under_snapshot_time selected_market_last_update selected_bookmaker_last_update selected_decision_time selected_line selected_price selected_side selected_bookmaker quote_decision_policy over_market_last_update under_market_last_update over_bookmaker_last_update under_bookmaker_last_update over_bookmaker under_bookmaker posterior_prob actual outcome profit".split()
GENERATED_COLUMNS = set("sample_status quote_status over_prob under_prob implied_over implied_under over_edge under_edge actual".split() + SELECTED_COLUMNS + BL_COLUMNS)
BETTING_KEYS = "total_bets wins losses pushes total_staked total_profit roi return_on_capital hit_rate".split()
RISK_KEYS = "sharpe_ratio max_drawdown win_streak loss_streak".split()
SUMMARY_COLUMNS = ["cell_id", "edge_threshold", "tau", "z_max", "max_weight", "raw_control", "input_identity", "request_identity", "predictions", "candidate_edges", *BETTING_KEYS, *RISK_KEYS, "overall_calibration_gap", "metrics_json", "historical_readiness", "decision_grade"]
CELL_FILES = {"config.json", "metrics.json", "predictions.csv", "bets.csv", "all_bookmaker_edges.csv"}
ENCODING = {"json_nonfinite": {"$nonfinite": "float.hex()"}, "nested_csv_values": "typed_value JSON", "csv_nan": "empty field"}
TABLE_COLUMNS = {
    "concentration": "cell_id dimension value missing bet_count finite_stakes count_denominator stake_denominator count_share stake_share empty_reason".split(),
    "coverage": "cell_id population category value count denominator ratio reason".split(),
    "timing_violations": "cell_id population row check left_field left_value right_field right_value reason".split(),
    "unmeasurable": "cell_id population row check reason".split(),
}
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")


class EvidenceError(Exception):
    """Only static reason codes cross the public exception boundary."""

    def __init__(self, reason: str, status: str = "FAIL"):
        self.reason, self.status = reason, status
        super().__init__(reason)


def _need(condition: bool, reason: str) -> None:
    if not condition:
        raise EvidenceError(reason)


def _same(left: Any, right: Any, reason: str) -> None:
    _need(canonical_sha256(left) == canonical_sha256(right), reason)


def _keys(value: Any, names: set | list, reason: str) -> None:
    _need(type(value) is dict and set(value) == set(names), reason)


def _digest(value: Any) -> None:
    _need(type(value) is str and DIGEST.fullmatch(value) is not None, "digest_shape")


def _integer(value: Any, low: int = 0, high: int | None = None) -> None:
    _need(type(value) is int and value >= low and (high is None or value <= high), "integer_domain")


def _number(value: Any, low: float | None = None, high: float | None = None, *, positive: bool = False) -> None:
    _need(type(value) in (int, float) and math.isfinite(value), "numeric_domain")
    _need((low is None or value >= low) and (high is None or value <= high) and (not positive or value > 0), "numeric_domain")


def _unique(value: Any, *, empty: bool = False) -> None:
    _need(type(value) is list and (empty or bool(value)), "list_domain")
    hashes = [canonical_sha256(item) for item in value]
    _need(len(hashes) == len(set(hashes)), "duplicate_list_value")


def _day(value: Any) -> date:
    _need(type(value) is str and DATE.fullmatch(value) is not None, "date_domain")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise EvidenceError("date_domain") from None


def _decimal(value: str, *, blank: bool = False) -> Decimal | None:
    if blank and value == "":
        return None
    _need(type(value) is str and NUMBER.fullmatch(value) is not None, "invalid_numeric_token")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise EvidenceError("invalid_numeric_token") from None
    _need(result.is_finite(), "invalid_numeric_token")
    return result


def _pairs(items: list) -> dict:
    result = {}
    for key, value in items:
        _need(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def _bad_constant(value: str) -> None:
    raise EvidenceError("nonfinite_json")


def _json(text: str) -> Any:
    try:
        value = json.loads(text, object_pairs_hook=_pairs, parse_constant=_bad_constant)
        # Reject overflow such as 1e999 as well as explicit NaN/Infinity.
        canonical_sha256(value)
        return value
    except EvidenceError:
        raise
    except (ValueError, TypeError, RecursionError, OverflowError):
        raise EvidenceError("malformed_json") from None


def _aware(value: str) -> tuple[datetime | None, str | None]:
    if value == "":
        return None, "missing_timestamp"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None, "naive_timestamp"
        return parsed, None
    except (ValueError, OverflowError):
        return None, "unparseable_or_ambiguous_timestamp"


def _timestamp_field(name: str) -> bool:
    return name == "bet_decision_time" or any(name.endswith("_" + suffix) for suffix in PROVENANCE_SUFFIXES[:-1])


def _typed(value: Any) -> None:
    """Check the producer's inert grammar, with bounded depth/node work.

    No NumPy/pandas imports, dtype construction or arbitrary reconstruction.
    Array hashes cannot prove omitted contents; dtype descriptors remain data.
    """
    reason = "nested_csv_encoding"

    def integer(v: Any, low: int, high: int) -> bool:
        return type(v) is int and low <= v <= high

    def hex_bytes(v: Any, size: int | None = None) -> None:
        _need(type(v) is str and re.fullmatch(r"(?:[0-9a-f]{2})*", v) is not None
              and (size is None or len(v) == size * 2), reason)

    def hex_float(v: Any) -> None:
        _need(type(v) is str and len(v) <= 64, reason)
        try:
            _need(float.fromhex(v).hex() == v, reason)
        except (ValueError, OverflowError):
            raise EvidenceError(reason) from None

    def dtype(v: Any) -> tuple[str, int]:
        _need(type(v) is str and len(v) <= 128, reason)
        match = re.fullmatch(r"[<>=|]([biufcOSUVMm])(\d*)(?:\[(?:\d+)?(?:Y|M|W|D|h|m|s|ms|us|ns|ps|fs|as)\])?", v)
        _need(match is not None, reason)
        kind, width = match.groups()
        _need(bool(width) or kind == "O", reason)
        size = int(width) if width else 0
        _need(size <= CSV_LIMIT and (kind not in "Mm" or size == 8), reason)
        return kind, size * 4 if kind == "U" else size

    pending: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > 64 or nodes > 100_000:
            raise EvidenceError("resource_limit", "BLOCKED")
        _need(type(item) is list and bool(item) and type(item[0]) is str, reason)
        item = cast(list[Any], item)
        tag = item[0]
        children: list[Any] = []
        if tag in ("null", "pandas.NA", "pandas.NaT"):
            _need(len(item) == 1, reason)
        elif tag in ("list", "tuple", "mapping"):
            _need(len(item) == 2 and type(item[1]) is list, reason)
            if tag == "mapping":
                for pair in item[1]:
                    _need(type(pair) is list and len(pair) == 2, reason)
                    children.extend(pair)
            else:
                children = list(item[1])
        elif tag in ("bool", "int", "float", "str", "bytes", "date"):
            _need(len(item) == 2, reason)
            v = item[1]
            _need(type(v) is (bool if tag == "bool" else str), reason)
            if tag == "int":
                _need(re.fullmatch(r"(?:0|-?[1-9][0-9]*)", v) is not None, reason)
            elif tag == "float":
                hex_float(v)
            elif tag == "bytes":
                hex_bytes(v)
            elif tag == "date":
                _day(v)
        elif tag in ("datetime", "time", "timestamp"):
            _need(len(item) == (6 if tag == "timestamp" else 4), reason)
            _need(type(item[1]) is str, reason)
            fold = item[3] if tag == "timestamp" else item[2]
            zone = item[2] if tag == "timestamp" else item[3]
            _need(integer(fold, 0, 1) and (zone is None or type(zone) is str and bool(zone)), reason)
            try:
                if tag == "time":
                    time.fromisoformat(item[1])
                else:
                    _need("T" in item[1], reason)
                    datetime.fromisoformat(item[1])
            except ValueError:
                raise EvidenceError(reason) from None
            if tag == "timestamp":
                _need(item[4] in ("s", "ms", "us", "ns"), reason)
                hex_bytes(item[5], 8)
        elif tag == "timedelta":
            _need(len(item) == 4 and integer(item[1], -999999999, 999999999)
                  and integer(item[2], 0, 86399) and integer(item[3], 0, 999999), reason)
        elif tag == "decimal":
            _need(len(item) == 4 and integer(item[1], 0, 1) and type(item[2]) is list
                  and all(integer(d, 0, 9) for d in item[2]), reason)
            _need(type(item[3]) is int or type(item[3]) is str and item[3] in ("F", "n", "N"), reason)
            _need(bool(item[2]) if type(item[3]) is int else item[3] != "F" or item[2] == [0], reason)
        elif tag == "complex":
            _need(len(item) == 3, reason)
            for v in item[1:]:
                hex_float(v)
        elif tag in ("numpy_scalar", "pandas.Timedelta"):
            _need(len(item) == 3, reason)
            kind, size = dtype(item[1])
            _need(kind != "O" and (tag != "pandas.Timedelta" or kind == "m"), reason)
            hex_bytes(item[2], size)
        elif tag == "ndarray":
            _need(len(item) == 5 and type(item[3]) is list and len(item[3]) <= 64, reason)
            kind, size = dtype(item[1])
            children = [item[2]]
            _need(type(item[2]) is list and len(item[2]) == 2 and item[2][0] == "list", reason)
            count = 1
            for length in item[3]:
                _need(integer(length, 0, CSV_LIMIT), reason)
                count *= length
                _need(count <= CSV_LIMIT, reason)
            if type(item[4]) is dict:
                _keys(item[4], {"bytes_sha256", "byte_count"}, reason)
                _digest(item[4]["bytes_sha256"])
                _need(kind != "O" and type(item[4]["byte_count"]) is int and item[4]["byte_count"] == count * size, reason)
            else:
                _need(kind in ("O", "V") and type(item[4]) is list and len(item[4]) == count, reason)
                children.extend(list(item[4]))
        elif tag == "masked":
            _need(len(item) == 4, reason)
            _need(all(type(v) is list and v and v[0] == "ndarray" for v in item[1:3]), reason)
            _need(len(item[1]) == len(item[2]) == 5 and item[1][3] == item[2][3], reason)
            children = item[1:]
        elif tag == "dataframe":
            _need(len(item) == 7 and type(item[2]) is list and all(type(v) is str and bool(v) for v in item[2])
                  and type(item[5]) is list, reason)
            for index in (1, 3, 4):
                _need(type(item[index]) is list and len(item[index]) == 2 and item[index][0] == "list"
                      and type(item[index][1]) is list, reason)
            _need(len(item[1][1]) == len(item[2]) and len(item[4][1]) == len(item[5]), reason)
            _need(type(item[6]) is list and bool(item[6]) and item[6][0] == "mapping", reason)
            children = [item[1], item[3], item[4], item[6]]
            for row in item[5]:
                _need(type(row) is list and len(row) == len(item[2]), reason)
                children.extend(row)
        else:
            raise EvidenceError(reason)
        if nodes + len(pending) + len(children) > 100_000:
            raise EvidenceError("resource_limit", "BLOCKED")
        pending.extend((child, depth + 1) for child in children)


class _HashReader(io.RawIOBase):
    def __init__(self, handle: Any, limit: int):
        self.handle, self.limit = handle, limit
        self.digest, self.size = hashlib.sha256(), 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        count = self.handle.readinto(buffer)
        if count:
            self.size += count
            if self.size > self.limit:
                raise EvidenceError("resource_limit", "BLOCKED")
            self.digest.update(memoryview(buffer)[:count])
        return count


def _identity(path: Path) -> dict:
    resolved = path.resolve(strict=True)
    info, literal = resolved.stat(), path.lstat()
    return {"resolved_path": str(resolved), "device": info.st_dev, "inode": info.st_ino,
            "bytes": info.st_size, "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns,
            "literal_inode": literal.st_ino, "literal_mtime_ns": literal.st_mtime_ns}


def _csv_records(text: Any):
    previous = csv.field_size_limit()
    try:
        csv.field_size_limit(CSV_LIMIT)
        yield from csv.reader(text, strict=True)
    finally:
        csv.field_size_limit(previous)


@dataclass
class _Reader:
    root: Path
    files: dict = field(default_factory=dict)
    directories: dict = field(default_factory=dict)

    def contained(self, relative: str, *, directory: bool = False) -> Path:
        path = self.root / relative
        _need(path.resolve(strict=False).is_relative_to(self.root), "path_escape")
        _need(path.exists(), "missing_required_file")
        mode = path.stat().st_mode
        _need(stat.S_ISDIR(mode) if directory else stat.S_ISREG(mode), "nonregular_required_path")
        return path

    def listing(self, relative: str) -> dict:
        path = self.contained(relative, directory=True)
        result = {}
        with os.scandir(path) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                linked = entry.is_symlink() or bool(getattr(info, "st_reparse_tag", 0))
                kind = "link" if linked else "directory" if stat.S_ISDIR(info.st_mode) else "file" if stat.S_ISREG(info.st_mode) else "special"
                result[entry.name] = {"type": kind, "inode": info.st_ino}
        self.directories[relative] = result
        return result

    def read(self, relative: str, receipt: dict | None = None, columns: list | None = None) -> Any:
        path = self.contained(relative)
        before = _identity(path)
        limit = JSON_LIMIT if relative.endswith(".json") else CSV_LIMIT
        if before["bytes"] > limit:
            raise EvidenceError("resource_limit", "BLOCKED")
        with path.open("rb") as raw:
            hashed = _HashReader(raw, limit)
            with io.TextIOWrapper(io.BufferedReader(hashed), encoding="utf-8", errors="strict", newline="") as text:
                if relative.endswith(".json"):
                    value = _json(text.read())
                else:
                    parsed = _csv_records(text)
                    header = next(parsed, None)
                    _need(header is not None and bool(header) and all(header) and len(header) == len(set(header)), "duplicate_or_missing_csv_header")
                    if columns is not None:
                        _need(header == columns, "output_schema")
                    value = []
                    for row in parsed:
                        if len(value) >= ROW_LIMIT:
                            raise EvidenceError("resource_limit", "BLOCKED")
                        _need(len(row) == len(header), "csv_row_width")
                        value.append(dict(zip(header, row, strict=True)))
        observed = {**before, "sha256": hashed.digest.hexdigest()}
        self.files[relative] = observed
        if _identity(path) != before:
            raise EvidenceError("source_changed_during_audit", "BLOCKED")
        if receipt is not None:
            expected_keys = {"sha256"} if relative.endswith(".json") else {"sha256", "rows", "columns"}
            _keys(receipt, expected_keys, "file_receipt")
            _digest(receipt["sha256"])
            _need(receipt["sha256"] == observed["sha256"], "file_hash")
            if not relative.endswith(".json"):
                _integer(receipt["rows"])
                _need(receipt["rows"] == len(value) and receipt["columns"] == header, "file_count_or_header")
        return value

    def stable(self) -> None:
        try:
            for relative, observed in self.files.items():
                path = self.contained(relative)
                before = _identity(path)
                _need(before == {k: v for k, v in observed.items() if k != "sha256"}, "source_changed_during_audit")
                limit = JSON_LIMIT if relative.endswith(".json") else CSV_LIMIT
                with path.open("rb") as handle:
                    hashed = _HashReader(handle, limit)
                    buffer = bytearray(1024 * 1024)
                    while hashed.readinto(buffer):
                        pass
                _need(hashed.digest.hexdigest() == observed["sha256"] and _identity(path) == before, "source_changed_during_audit")
            for relative, observed in list(self.directories.items()):
                _need(self.listing(relative) == observed, "source_changed_during_audit")
        except (OSError, EvidenceError):
            raise EvidenceError("source_changed_during_audit", "BLOCKED") from None


def _readiness(value: dict) -> None:
    for key, expected in READINESS.items():
        _same(value[key], expected, "readiness")


def _saved_plan(plan: Any) -> dict:
    """Independent v1 data adapter. Never normalize values before commitments."""
    _keys(plan, PLAN_KEYS, "plan_keys")
    if type(plan["cells"]) is list and len(plan["cells"]) > CELL_LIMIT:
        raise EvidenceError("resource_limit", "BLOCKED")
    _need(type(plan['schema_version']) is int and plan['schema_version'] in (1, 2), 'plan_version')
    _readiness(plan)
    fixed = plan["fixed_config"]
    historical = plan['schema_version'] == 2
    bounded = historical and 'acquisition_limits' in fixed
    _keys(fixed, FIXED_KEYS | ({'historical'} if historical else set())
          | ({'acquisition_limits'} if bounded else set()), "fixed_config_keys")
    if bounded:
        from src.models.features.nba.acquisition import AcquisitionLimits

        try:
            AcquisitionLimits(**fixed['acquisition_limits']).check_span(
                date.fromisoformat(fixed['start']), date.fromisoformat(fixed['end']))
        except (TypeError, ValueError):
            raise EvidenceError('acquisition_limits') from None
    if historical:
        try:
            validate_saved_attachment(fixed['historical'])
        except (ValueError, RuntimeError, KeyError, TypeError):
            raise EvidenceError('historical_attachment') from None
    for key in ("model_dir", "output_dir"):
        _need(type(fixed[key]) is str and bool(fixed[key]) and "\x00" not in fixed[key], "recorded_path_token")
    start, end = _day(fixed["start"]), _day(fixed["end"])
    _need(start <= end, "date_range")
    if historical:
        try:
            validate_window(fixed['historical'], start, end)
        except (ValueError, RuntimeError, KeyError, TypeError):
            raise EvidenceError('historical_window') from None
        _need(fixed['local'] is True, 'historical_local_target')
    for key, allowed in (("stats", {"pts", "reb", "ast"}), ("bookmakers", BOOKMAKERS)):
        _unique(fixed[key])
        _need(all(type(item) is str and item in allowed for item in fixed[key]), "literal_config_token")
    markets = fixed["stats"] + [key for key, definition in COMBO_DEFINITIONS.items() if set(definition["components"]).issubset(fixed["stats"])]
    _same(fixed["market_stats"], markets, "market_stats")
    for key, low, high in (("n_samples", 1, None), ("seed", 0, 2**32 - 1), ("max_cells", 1, None)):
        _integer(fixed[key], low, high)
    for key in ("min_minutes_avg", "starting_bankroll", "flat_bet_size"):
        _number(fixed[key], 0, positive=key != "min_minutes_avg")
    for key, high, positive in (("edge_thresholds", 1, False), ("bl_taus", None, False), ("bl_z_max", None, True), ("bl_max_weight", 1, False)):
        _unique(fixed[key], empty=key == "bl_taus")
        numeric = set()
        for item in fixed[key]:
            _number(item, 0, high, positive=positive)
            _need(item not in numeric, "duplicate_grid_value")
            numeric.add(item)
    for key in ("local", "diagnostic_history"):
        _need(type(fixed[key]) is bool, "boolean_config")
    _need(fixed["diagnostic_history"] is True, "diagnostic_acknowledgement")
    if fixed["allowed_bets"] is not None:
        _unique(fixed["allowed_bets"])
        for pair in fixed["allowed_bets"]:
            _need(type(pair) is list and len(pair) == 2 and pair[0] in markets and pair[1] in ("over", "under"), "allowed_bets")
    count = len(fixed["edge_thresholds"]) * (1 + len(fixed["bl_taus"]) * len(fixed["bl_z_max"]) * len(fixed["bl_max_weight"]))
    if count > CELL_LIMIT:
        raise EvidenceError("resource_limit", "BLOCKED")
    _integer(plan["cell_count"], 1)
    _need(count <= fixed["max_cells"] and plan["cell_count"] == count, "cell_grid_count")
    cells = []
    for edge in fixed["edge_thresholds"]:
        for tau, z_max, weight in [(None, None, None), *product(fixed["bl_taus"], fixed["bl_z_max"], fixed["bl_max_weight"])]:
            cell = {"edge_threshold": edge, "tau": tau, "z_max": z_max, "max_weight": weight, "raw_control": tau is None}
            cells.append({"cell_id": "cell_" + canonical_sha256(cell), **cell})
    _same(plan["cells"], cells, "cell_grid_identity")
    acquisition_keys = [*ACQUISITION_KEYS, *(['acquisition_limits'] if bounded else [])]
    _same(plan["acquisition_settings"], {key: fixed[key] for key in acquisition_keys}, "acquisition_settings")
    _saved_acquisition_metadata(plan, fixed)
    acquisition = {"settings": plan["acquisition_settings"], "artifact": plan["artifact"], "predictor": plan["predictor_settings"],
                   "features": plan["ordered_features"], "source": plan["source"], "policy": plan["source_policy"]}
    _same(plan["acquisition_identity"], canonical_sha256(acquisition), "acquisition_identity")
    _same(plan["request_identity"], canonical_sha256({k: v for k, v in plan.items() if k != "request_identity"}), "request_identity")
    return fixed


def _saved_acquisition_metadata(plan: dict, fixed: dict) -> None:
    artifact = plan["artifact"]
    _keys(artifact, "path state identity file_hashes manifest_schema_version artifact_type legacy identity_verified model_load_verified decision_grade historical_policy_verified".split(), "artifact_keys")
    _need(type(artifact["path"]) is str and bool(artifact["path"]) and "\x00" not in artifact["path"], "artifact_path")
    _digest(artifact["identity"])
    _same({k: artifact[k] for k in ("state", "legacy", "identity_verified", "model_load_verified", "decision_grade", "historical_policy_verified", "manifest_schema_version")},
          {"state": "verified_manifest", "legacy": False, "identity_verified": True, "model_load_verified": False,
           "decision_grade": False, "historical_policy_verified": False, "manifest_schema_version": 1}, "artifact_metadata")
    _need(type(artifact["artifact_type"]) is str and bool(artifact["artifact_type"]), "artifact_type")
    _need(type(artifact["file_hashes"]) is dict and bool(artifact["file_hashes"]), "artifact_file_hashes")
    for name, receipt in artifact["file_hashes"].items():
        _need(type(name) is str and bool(name), "artifact_file_token")
        _need(type(receipt) is dict and "sha256" in receipt, "artifact_file_receipt")
        _digest(receipt["sha256"])
        if "bytes" in receipt:
            _integer(receipt["bytes"])
    predictor = plan["predictor_settings"]
    _keys(predictor, "n_samples random_state use_correlated_sampling correlation_config variance_inflation tail_adjustment blowout_config copula_params combined_calibration_offsets".split(), "predictor_keys")
    _same(predictor["n_samples"], fixed["n_samples"], "predictor_settings")
    _same(predictor["random_state"], fixed["seed"], "predictor_settings")
    _same(predictor["use_correlated_sampling"], True, "predictor_settings")
    _need(predictor["combined_calibration_offsets"] is None, "global_offsets")
    for key in ("correlation_config", "variance_inflation", "tail_adjustment", "blowout_config"):
        _need(type(predictor[key]) is dict and bool(predictor[key]), "predictor_metadata")
    if predictor["copula_params"] is not None:
        _need(type(predictor["copula_params"]) is dict and bool(predictor["copula_params"]), "copula_metadata")
        for key, value in predictor["copula_params"].items():
            _need(key in ("pts", "reb", "ast"), "copula_metadata")
            _number(value, -1, 1)
    features = plan["ordered_features"]
    _need(type(features) is dict and {"minutes_features", *(f"{key}_rate_features" for key in fixed["stats"])}.issubset(features)
          and set(features).issubset({"minutes_features", "pts_rate_features", "reb_rate_features", "ast_rate_features"}), "ordered_features")
    for quantiles in features.values():
        # The producer accepts a shared ordered list or per-quantile lists.
        if type(quantiles) is dict:
            _keys(quantiles, {"0.1", "0.25", "0.5", "0.75", "0.9"}, "feature_quantiles")
            lists = quantiles.values()
        else:
            lists = [quantiles]
        for names in lists:
            _unique(names)
            _need(all(type(name) is str and bool(name) for name in names), "feature_names")
    if plan['schema_version'] == 2:
        _same(plan['source_policy'], source_policy(fixed['historical']), 'source_policy')
    else:
        _keys(plan["source_policy"], {"source_id", "policy_id", "watermark"}, "source_policy")
        _same(plan["source_policy"], {"source_id": "unknown", "policy_id": "unknown", "watermark": None}, "source_policy")
    dependencies = plan["dependencies"]
    _keys(dependencies, {"numpy", "pandas", "scipy", "sqlalchemy", "joblib", "xgboost", "sklearn", "psycopg2"}, "dependencies")
    for receipt in dependencies.values():
        _keys(receipt, {"distribution", "version"}, "dependency_receipt")
        _need(all(type(item) is str and bool(item) for item in receipt.values()), "dependency_receipt")
    source = plan["source"]
    _keys(source, {"base", "files"}, "source_keys")
    _keys(source["base"], {"git_commit", "dirty"}, "source_base")
    _need(source["base"]["git_commit"] is None or (type(source["base"]["git_commit"]) is str and re.fullmatch(r"[0-9a-f]{40}", source["base"]["git_commit"]) is not None), "source_commit")
    _need(source["base"]["dirty"] is None or type(source["base"]["dirty"]) is bool, "source_dirty")
    expected = {"src/backtesting/nba/" + name for name in ("__init__.py", "sweep_config.py", "sweep_runner.py", "run_nba_sweep.py")}
    expected.update("src/backtesting/" + name for name in ("backtest_harness.py", "bet_simulator.py", "performance_metrics.py"))
    expected.update("src/models/" + name for name in ("nba_artifacts.py", "monte_carlo.py", "quantile_trainer.py", "feature_store.py", "black_litterman.py"))
    expected.add("src/config/combo_config.py")
    # Older saved sweeps predate the compact reader; new producer plans include
    # both its implementation and the parser dependency in their fingerprint.
    if "src/backtesting/nba/compact_quotes.py" in source["files"]:
        expected.update(("src/backtesting/nba/compact_quotes.py", "src/processing/nba_prop_snapshots.py"))
    if plan['schema_version'] == 2:
        expected.update(HISTORICAL_SOURCE_FILES)
        if 'acquisition_limits' in fixed:
            expected.update(BOUNDED_HISTORICAL_SOURCE_FILES)
    _keys(source["files"], expected, "source_file_inventory")
    for digest in source["files"].values():
        _digest(digest)


def _schemas(value: Any) -> dict:
    _keys(value, {"version", "extension_policy", "input_columns", "prediction_renames", "quote_renames", "files"}, "schema_keys")
    _same(value["version"], 1, "output_schema_version")
    _same(value["extension_policy"], "required columns then sorted observed input fields; collisions use prediction__/quote__; ambiguous names fail", "extension_policy")
    _keys(value["input_columns"], {"predictions", "quotes"}, "input_columns")
    for columns in value["input_columns"].values():
        _unique(columns, empty=True)
        _need(all(type(c) is str for c in columns) and columns == sorted(columns), "input_columns")
    pred_columns, quote_columns = value["input_columns"]["predictions"], value["input_columns"]["quotes"]
    pr = {c: "prediction__" + c for c in pred_columns if c in GENERATED_COLUMNS or c in set(QUOTE_COLUMNS) - {"player_id", "game_id", "stat"}}
    pred = {pr.get(c, c) for c in pred_columns}
    qr = {c: "quote__" + c for c in quote_columns if c == "stat" or (c not in ("player_id", "game_id", "stat") and (c in pred or c in GENERATED_COLUMNS))}
    quote = {qr.get(c, c) for c in quote_columns}
    _need(len(pred) == len(pred_columns) and len(quote) == len(quote_columns) and not (pred - {"player_id", "game_id", "stat"}) & quote, "schema_namespace_collision")
    _same(value["prediction_renames"], pr, "prediction_renames")
    _same(value["quote_renames"], qr, "quote_renames")
    edges = EDGE_COLUMNS + sorted((pred | quote) - set(EDGE_COLUMNS))
    files = {"predictions.csv": edges + [c for c in [*SELECTED_COLUMNS, "actual"] if c not in edges], "bets.csv": BET_COLUMNS, "all_bookmaker_edges.csv": edges}
    _same(value["files"], files, "minimum_output_schema")
    return files


def _dates(manifest: dict, fixed: dict) -> dict:
    start, end = _day(fixed["start"]), _day(fixed["end"])
    length = (end - start).days + 1
    # Date arrays themselves are bounded by the JSON limit; avoid constructing
    # an unbounded secondary list from an adversarial multi-millennial range.
    _need(type(manifest["expected_dates"]) is list and len(manifest["expected_dates"]) == length, "expected_dates")
    expected = [(start + timedelta(days=i)).isoformat() for i in range(length)]
    _same(manifest["expected_dates"], expected, "expected_dates")
    acquired = manifest["acquired_dates"]
    _unique(acquired, empty=True)
    _need(all(type(day) is str and day in set(expected) for day in acquired) and acquired == sorted(acquired), "acquired_dates")
    _same(manifest["date_outcome_basis"], "game-date loader observation, not verified source absence", "date_outcome_basis")
    _same(manifest["empty_reason"], "no_game_dates" if not acquired else None, "manifest_empty_reason")
    observed = manifest["observed"]
    _need(type(observed) is list and len(observed) == length, "observed_dates")
    result = {}
    for record in observed:
        _keys(record, {"date", "reason", "counts"}, "observed_record")
        day = record["date"]
        _need(type(day) is str and day in expected and day not in result, "observed_dates")
        _need(record["reason"] in ("no_game", "no_features", "filtered", "no_predictions", "no_quotes", "acquired"), "date_reason")
        counts = record["counts"]
        _keys(counts, {"features", "eligible", "predictions", "quotes", "sample_bundles"}, "acquisition_counts")
        for count in counts.values():
            _integer(count)
        _need(counts["eligible"] <= counts["features"], "acquisition_counts")
        if day not in acquired:
            _need(record["reason"] == "no_game" and not any(counts.values()), "no_game_counts")
        else:
            reason = "no_features" if counts["features"] == 0 else "filtered" if counts["eligible"] == 0 else "no_predictions" if counts["predictions"] == 0 else "no_quotes" if counts["quotes"] == 0 else "acquired"
            _need(record["reason"] == reason, "date_reason_counts")
            if reason in ("no_features", "filtered"):
                _need(counts["predictions"] == counts["sample_bundles"] == 0, "acquisition_counts")
        result[day] = record
    _need(list(result) == expected, "observed_dates")
    components = manifest["input_component_digests"]
    names = "artifact source acquisition_identity acquisition_settings source_policy predictor_settings ordered_features rng_state_after expected_dates date_outcome_basis acquired_dates dates actuals voids empty_reason output_schemas".split()
    if manifest['schema_version'] == 2:
        names.append('quote_ledger_json')
    _keys(components, names, "input_component_inventory")
    for digest in components.values():
        _digest(digest)
    # These exact scalar/sequence encodings survive JSON export. Mapping
    # insertion order, raw frames, samples and RNG state do not: do not guess it.
    reconstructable = {
        "acquisition_identity": ["str", manifest["plan"]["acquisition_identity"]],
        "date_outcome_basis": ["str", manifest["date_outcome_basis"]],
        "empty_reason": ["null"] if manifest["empty_reason"] is None else ["str", manifest["empty_reason"]],
        **{name: ["tuple", [["date", day] for day in manifest[name]]]
           for name in ("expected_dates", "acquired_dates")},
    }
    for name, encoded in reconstructable.items():
        _same(components[name], canonical_sha256(encoded), "input_component_digest")
    _need(type(manifest["sample_inventory"]) is list, "sample_inventory")
    inventory_counts = Counter()
    sample_keys = set()
    for item in manifest["sample_inventory"]:
        _keys(item, {"date", "key", "content_digest", "dtype", "shape", "evidence_type"}, "sample_inventory")
        _need(item["date"] in result, "sample_inventory_date")
        _typed(item["key"])
        _need(item["key"][0] == "tuple" and len(item["key"][1]) == 3, "sample_key")
        signature = (item["date"], canonical_sha256(item["key"]))
        _need(signature not in sample_keys, "duplicate_sample_key")
        sample_keys.add(signature)
        _digest(item["content_digest"])
        _need(item["dtype"] is None or type(item["dtype"]) is str, "sample_dtype")
        _need(type(item["evidence_type"]) is str and bool(item["evidence_type"]), "sample_type")
        if item["shape"] is not None:
            _need(type(item["shape"]) is list, "sample_shape")
            for size in item["shape"]:
                _integer(size)
        inventory_counts[item["date"]] += 1
    for day, record in result.items():
        _need(inventory_counts[day] == record["counts"]["sample_bundles"], "sample_inventory_count")
    return result


def _effective(plan: dict, cell: dict, manifest: dict, bets: int) -> dict:
    bl = None if cell["raw_control"] else {"tau": cell["tau"], "max_weight": cell["max_weight"], "min_prob": 0.01, "max_prob": 0.99, "z_max": cell["z_max"]}
    return {**plan["fixed_config"], **cell, "bl_config": bl, "stat_config": None, "bl_sizing_blender": None,
            "max_bet_pct": None, "kelly_fraction": 0.125, "kelly_used": False, "use_bl_for_sizing": False,
            "sizing_mode": "flat", "min_odds": -200, "max_odds": 200, "request_identity": plan["request_identity"],
            "input_identity": manifest["input_identity"], "empty_reason": "no_bets" if bets == 0 else None,
            **READINESS, "output_schema_identity": canonical_sha256(manifest["output_schemas"])}


def _metric_number(value: Any) -> None:
    if type(value) is dict:
        _keys(value, {"$nonfinite"}, "metric_nonfinite_tag")
        _need(value["$nonfinite"] in ("nan", "inf", "-inf"), "metric_nonfinite_tag")
    else:
        _number(value)


def _metrics(value: Any) -> dict:
    _keys(value, {"betting", "risk", "calibration", "by_stat", "by_edge_bucket", "config"}, "metrics_keys")
    _keys(value["betting"], BETTING_KEYS, "betting_metrics")
    _keys(value["risk"], RISK_KEYS, "risk_metrics")
    for section in ("betting", "risk"):
        for key, number in value[section].items():
            if key in ("total_bets", "wins", "losses", "pushes", "win_streak", "loss_streak"):
                _integer(number)
            else:
                _metric_number(number)
    _keys(value["calibration"], {"results", "overall_gap"}, "calibration_metrics")
    _metric_number(value["calibration"]["overall_gap"])
    _need(type(value["calibration"]["results"]) is list, "calibration_results")
    for item in value["calibration"]["results"]:
        _keys(item, {"quantile", "target", "actual", "gap", "n"}, "calibration_result")
        _integer(item["n"])
        for key in ("quantile", "target", "actual", "gap"):
            _metric_number(item[key])
    for key in ("by_stat", "by_edge_bucket"):
        _need(type(value[key]) is dict, "metric_breakdown")
    return {k: v for k, v in value.items() if k != "config"}


def _summary_token(token: str, expected: Any) -> None:
    if expected is None:
        _need(token == "", "summary_value")
    elif type(expected) is bool:
        _need(token == ("True" if expected else "False"), "summary_boolean")
    elif type(expected) in (int, float):
        _need(_decimal(token) == Decimal(str(expected)), "summary_numeric")
    elif type(expected) is dict:
        # Nonfinite scalar summary cells use the producer's typed mapping JSON.
        _need(set(expected) == {"$nonfinite"}, "summary_value")
        _same(_json(token), ["mapping", [[["str", "$nonfinite"], ["str", expected["$nonfinite"]]]]], "summary_nonfinite")
    else:
        _need(type(expected) is str and token == expected, "summary_value")


def _summary(row: dict, plan: dict, cell: dict, manifest: dict, metrics: dict, counts: dict) -> None:
    expected = {**cell, "input_identity": manifest["input_identity"], "request_identity": plan["request_identity"],
                "predictions": counts["selected"], "candidate_edges": counts["candidates"], **metrics["betting"], **metrics["risk"],
                "overall_calibration_gap": metrics["calibration"]["overall_gap"], "historical_readiness": "unverified", "decision_grade": False}
    for key, value in expected.items():
        _summary_token(row[key], value)
    _same(_json(row["metrics_json"]), metrics, "summary_metrics_json")


def _core(row: dict, kind: str) -> tuple:
    prefix = (row["game_date"], row["player_id"], row["game_id"], row["stat"])
    if kind == "candidate":
        return (*prefix, row["bookmaker"], *(_decimal(row[key]) for key in ("line", "over_odds", "under_odds")))
    side, line, price = ("side", "line", "odds") if kind == "bet" else ("selected_side", "selected_line", "selected_price")
    return (*prefix, row[side], _decimal(row[line]), _decimal(row[price]))


def _metadata_equal(left: str, right: str, field_name: str) -> tuple[bool, bool]:
    if left == "" or right == "":
        return left == right, left == right  # copying two blanks is provable; availability is separate
    if _timestamp_field(field_name):
        a, ar = _aware(left)
        b, br = _aware(right)
        if ar or br:
            return True, False  # cannot contradict/confirm clock equivalence
        return a == b, True
    return left == right, True


def _provenance(parent: dict, child: dict, transition: str) -> tuple[bool, bool, tuple]:
    expected = {}
    for key in child:
        if key.startswith(("over_", "under_")) and (_timestamp_field(key) or key.endswith("_bookmaker")):
            if key in parent:
                expected[key] = parent[key]
        if key in ("quote_decision_policy", "bet_decision_time") and key in parent:
            expected[key] = parent[key]
    if transition == "candidate_to_selected":
        side = child["selected_side"]
        for suffix in PROVENANCE_SUFFIXES:
            if parent.get("selected_side") == side:
                token = parent.get("selected_" + suffix, "")
            else:
                token = parent.get(side + "_" + suffix, parent.get("bet_decision_time", "") if suffix == "decision_time" else "")
            expected["selected_" + suffix] = token
    else:
        # The incumbent simulator's _selected_quote prefers a non-null side
        # value, then same-side selected metadata, then its explicit default.
        # This is saved-field projection, not a new quote-selection policy.
        side = child["side"]
        for suffix in PROVENANCE_SUFFIXES:
            key = "selected_" + suffix
            if key in child and key in parent:
                default = parent.get("bookmaker", "") if suffix == "bookmaker" else (
                    parent.get("bet_decision_time", "") if suffix == "decision_time" else ""
                )
                selected = parent.get(key, "") if parent.get("selected_side") == side else ""
                expected[key] = parent.get(side + "_" + suffix, "") or selected or default
    proven = True
    projection = []
    for key, token in expected.items():
        equal, known = _metadata_equal(token, child.get(key, ""), key)
        if not equal:
            return False, known, ()
        proven &= known
        instant, error = _aware(token) if _timestamp_field(key) and token else (None, None)
        projection.append((key, instant.astimezone(UTC).isoformat() if instant and not error else token))
    return True, proven, tuple(projection)


def _injective(adjacency: list[list[int]]) -> list[int] | None:
    """Augmenting paths, iterative to avoid recursion limits on duplicate rows."""
    owner, assigned = {}, {}
    for child in range(len(adjacency)):
        queue, previous, seen = deque([child]), {}, {child}
        endpoint = None
        while queue and endpoint is None:
            current = queue.popleft()
            for parent in adjacency[current]:
                if parent not in owner:
                    endpoint = (current, parent)
                    break
                other = owner[parent]
                if other not in seen:
                    seen.add(other)
                    previous[other] = (current, parent)
                    queue.append(other)
        if endpoint is None:
            return None
        current, parent = endpoint
        while True:
            owner[parent], assigned[current] = current, parent
            if current == child:
                break
            current, parent = previous[current]
    return [assigned[i] for i in range(len(adjacency))]


def _relationship(parents: list[dict], children: list[dict], transition: str) -> dict:
    groups = defaultdict(list)
    for index, parent in enumerate(parents):
        # Placeholder no-quote candidates cannot be actionable parents.
        if transition == "candidate_to_selected" and any(parent[k] == "" for k in ("line", "over_odds", "under_odds")):
            continue
        groups[_core(parent, "candidate" if transition == "candidate_to_selected" else "selected")].append(index)
    adjacency, structural, availability, ambiguous = [], [], False, False
    for child in children:
        signature = _core(child, "candidate" if transition == "candidate_to_selected" else "bet")
        possible = groups.get(signature, [])
        _need(bool(possible), "correspondence_core")
        structural.append(possible)
        choices, projections = [], set()
        for index in possible:
            compatible, proven, projection = _provenance(parents[index], child, transition)
            if compatible:
                choices.append(index)
                projections.add(projection)
                availability |= not proven
        _need(bool(choices), "correspondence_provenance")
        ambiguous |= len(projections) > 1
        adjacency.append(choices)
    _need(_injective(structural) is not None and _injective(adjacency) is not None, "correspondence_multiplicity")
    return {"status": "BLOCKED" if availability or ambiguous else "PASS", "reason": "ambiguous_or_naive_provenance" if availability or ambiguous else None,
            "matched_occurrences": len(children), "parent_occurrences": len(parents), "source_row_identity": "not_claimed",
            "structural_containment": "PASS", "provenance_availability": "BLOCKED" if any(not row.get("selected_snapshot_time") or not row.get("selected_decision_time") for row in children) else "observed_only"}


def _row_validation(rows: list[dict], kind: str, fixed: dict, dates: dict) -> None:
    owner = set()
    numeric = {"line", "over_odds", "under_odds", "selected_line", "selected_price", "odds"}
    literal = {"player_id", "game_id", "stat", "bookmaker", "selected_bookmaker", "over_bookmaker", "under_bookmaker", "quote_decision_policy", "player_name", "team_id", "market_key", "game_date", "game_time"}
    for row in rows:
        _day(row["game_date"])
        _need(row["game_date"] in dates and row["player_id"] != "" and row["game_id"] != "" and row["stat"] in fixed["market_stats"], "row_identity")
        if "market_key" in row and row["market_key"]:
            _need(row["market_key"] == STAT_TO_MARKET[row["stat"]], "row_market")
        if row.get("bookmaker", ""):
            _need(row["bookmaker"] in fixed["bookmakers"], "row_bookmaker")
        if row.get("selected_bookmaker", ""):
            _need(row["selected_bookmaker"] in fixed["bookmakers"], "row_bookmaker")
        if row.get("bookmaker", "") and row.get("selected_bookmaker", ""):
            _need(row["bookmaker"] == row["selected_bookmaker"], "conflicting_bookmaker")
        if kind == "bets":
            # evaluate_predictions assigns this even when either field is blank.
            _need(row["bookmaker"] == row["selected_bookmaker"], "bet_bookmaker_projection")
        for key, token in row.items():
            if key in numeric:
                _decimal(token, blank=kind == "candidates" or key.startswith("selected_"))
            elif token and key not in literal and not _timestamp_field(key) and token[0] in "[{":
                nested = _json(token)
                # The harness already JSON-encodes rejected combo context as a
                # string; the CSV writer does not typed-encode strings again.
                if key.endswith("combo_component_evidence") and type(nested) is dict:
                    pass
                else:
                    _typed(nested)
        _need(row.get("bl_enabled", "") in ("", "True", "False"), "row_boolean")
        if kind == "candidates":
            continue
        key = (row["game_date"], row["player_id"], row["game_id"])
        _need(key not in owner, "multiple_selected_or_bet_per_player_game")
        owner.add(key)
        side = row["side"] if kind == "bets" else row["selected_side"]
        _need(side in ("over", "under") and row["selected_side"] == side, "selected_side")
        _need(_decimal(row["selected_line"]) == _decimal(row["line"]), "selected_line")
        price = row["odds"] if kind == "bets" else row[side + "_odds"]
        _need(_decimal(row["selected_price"]) == _decimal(price), "selected_price")
        if kind == "bets":
            stake = _decimal(row["stake"])
            _need(stake > 0, "invalid_actionable_stake")
            # The simulator caps a flat wager to available bankroll. Saved
            # fields alone do not independently prove why a smaller stake arose.
            _need(stake <= Decimal(str(fixed["flat_bet_size"])), "flat_stake")
            if fixed["allowed_bets"] is not None:
                _need([row["stat"], side] in fixed["allowed_bets"], "bet_direction")


def _concentration(cell_id: str, bets: list[dict]) -> list:
    total = sum((_decimal(row["stake"]) for row in bets), Decimal(0))
    result = []
    for dimension, key in (("side", "side"), ("stat", "stat"), ("bookmaker", "selected_bookmaker"), ("game", "game_id"), ("player", "player_id")):
        groups = defaultdict(list)
        for row in bets:
            token = row[key] or (row["bookmaker"] if dimension == "bookmaker" else "")
            groups[token].append(_decimal(row["stake"]))
        if not groups:
            groups[""] = []
        for token, stakes in sorted(groups.items()):
            amount = sum(stakes, Decimal(0))
            result.append({"cell_id": cell_id, "dimension": dimension, "value": token if token else None,
                           "missing": token == "", "bet_count": len(stakes), "finite_stakes": str(amount),
                           "count_denominator": len(bets), "stake_denominator": str(total),
                           "count_share": len(stakes) / len(bets) if bets else None,
                           "stake_share": float(amount / total) if total else None,
                           "empty_reason": "no_bets" if not bets else None})
    return result


def _coverage(cell_id: str, frames: dict, dates: dict, relationships: dict) -> list:
    """Each row carries its population, denominator and evidence limitation."""
    result = []

    def add(population: str, category: str, value: Any, count: int, denominator: int | None = None, reason: str | None = None) -> None:
        result.append({"cell_id": cell_id, "population": population, "category": category, "value": value,
                       "count": count, "denominator": denominator, "ratio": count / denominator if denominator else None, "reason": reason})

    for key in ("features", "eligible", "predictions", "quotes", "sample_bundles"):
        add("manifest_acquisition", "count", key, sum(item["counts"][key] for item in dates.values()), reason="loader_observation_not_source_completeness")
    candidates, selected, bets = frames["candidates"], frames["selected"], frames["bets"]
    opportunities = len({tuple(row[k] for k in ("game_date", "player_id", "game_id", "stat")) for row in candidates})
    add("saved_candidates", "distinct_opportunities", None, opportunities, reason="saved_keys_not_full_acquisition_universe")
    add("saved_candidates", "rows", None, len(candidates), reason="book_line_multiplicity_and_no_quote_placeholders")
    add("saved_candidates", "quoted_rows", None, sum(row["quote_status"] == "quoted" for row in candidates), reason="not_distinct_players_or_opportunities")
    add("saved_selected", "rows", None, len(selected), reason="selected_predictions_not_acquisition_predictions")
    add("saved_bets", "rows", None, len(bets), reason="placed_saved_rows_not_recomputed_settlement")
    for transition, denominator in (("candidate_to_selected", len(candidates)), ("selected_to_bet", len(selected))):
        match = relationships[transition]
        add("saved_transitions", transition, None, match["matched_occurrences"], denominator, "occurrence_consuming_containment; lost_at_stage_reasons_unmeasurable")
    reasons = Counter(item["reason"] for item in dates.values())
    for reason in ("no_game", "no_features", "filtered", "no_predictions", "no_quotes", "acquired"):
        add("manifest_dates", "reason", reason, reasons[reason], len(dates), "loader_observed_only")
    for key in ("sample_status", "quote_status", "combo_rejection_reason"):
        counts = Counter(row[key] if row[key] else None for row in candidates)
        for token, count in sorted(counts.items(), key=lambda item: (item[0] is not None, item[0] or "")):
            add("saved_candidates", key, token, count, len(candidates), "unknown_not_inferred" if token is None else "saved_reason_only")
    return result


def _timing(cell_id: str, rows: list[dict]) -> tuple[dict, list, list]:
    violations, missing, counts = [], [], {}
    rules = (("quote_before_decision", "selected_snapshot_time", "selected_decision_time", False),
             ("decision_before_commence", "selected_decision_time", "commence_time", True),
             ("quote_before_commence", "selected_snapshot_time", "commence_time", True))
    for check, left, right, strict in rules:
        evaluated = 0
        for index, row in enumerate(rows, 1):
            a, ar = _aware(row.get(left, ""))
            b, br = _aware(row.get(right, ""))
            if ar or br:
                missing.append({"cell_id": cell_id, "population": "saved_selected", "row": index, "check": check,
                                "reason": (left + ":" + ar) if ar else (right + ":" + br)})
                continue
            evaluated += 1
            if not (a < b if strict else a <= b):
                violations.append({"cell_id": cell_id, "population": "saved_selected", "row": index, "check": check,
                                   "left_field": left, "left_value": row[left], "right_field": right, "right_value": row[right], "reason": "ordering_violation"})
        counts[check] = {"population": len(rows), "evaluated": evaluated, "missing": len(rows) - evaluated,
                         "violations": sum(item["check"] == check for item in violations)}
        if not rows:
            missing.append({"cell_id": cell_id, "population": "saved_selected", "row": None, "check": check, "reason": "zero_evaluable_population"})
    status = "FAIL" if violations else "BLOCKED" if missing else "PASS"
    return {"status": status, "reason": "ordering_violation" if violations else "missing_required_observations" if missing else "saved_ordering_mechanics_only",
            "counts": counts, "source_clock_mapping": "unknown; not source_availability_certification"}, violations, missing


def _historical_ledger(reader: _Reader, manifest: dict, dates: dict) -> None:
    ledger = reader.read('historical_quote_ledger.json', manifest['outputs']['historical_quote_ledger.json'])
    _keys(ledger, {'schema_version', 'policy_id', 'dates'}, 'historical_ledger')
    _same(ledger['schema_version'], 1, 'historical_ledger_version')
    _same(ledger['policy_id'], manifest['plan']['source_policy']['policy_id'], 'historical_ledger_policy')
    _keys(ledger['dates'], manifest['acquired_dates'], 'historical_ledger_dates')
    for day, entry in ledger['dates'].items():
        _keys(entry, {'rows', 'summary'}, 'historical_ledger_entry')
        rows, summary = entry['rows'], entry['summary']
        _need(type(rows) is list and len(rows) <= ROW_LIMIT, 'historical_ledger_rows')
        _keys(summary, {'status', 'decision_grade', 'historical_readiness', 'input_occurrences',
                       'eligible_pairs', 'eligible_occurrences', 'rejected_occurrences', 'duplicate_occurrences',
                       'blocked_occurrences', 'policy_content_id', 'selection_status'}, 'historical_ledger_summary')
        _need(summary['decision_grade'] is False and summary['blocked_occurrences'] == 0,
              'historical_ledger_posture')
        _same(summary['policy_content_id'], ledger['policy_id'], 'historical_ledger_policy')
        _same(summary['input_occurrences'], len(rows), 'historical_occurrence_count')
        _same(summary['eligible_pairs'], dates[day]['counts']['quotes'], 'historical_pair_count')
        counts = Counter()
        for index, row in enumerate(rows):
            _keys(row, {'occurrence_index', 'raw_record', 'raw_game_id', 'game_id', 'game_id_mapping',
                        'policy_content_id', 'context_identity', 'source_ref', 'disposition', 'selector_disposition',
                        'adapter_reasons', 'quote_reasons', 'observation_reasons', 'observation', 'semantic_quote_id',
                        'pair_id', 'canonical_source_ref', 'blocked', 'adapter_schema_version', 'quote_schema_version',
                        'game_date'}, 'historical_ledger_row')
            _need(row['blocked'] is False and row['occurrence_index'] == index
                  and row['game_date'] == day and row['policy_content_id'] == ledger['policy_id'],
                  'historical_ledger_occurrence')
            _need(row['disposition'] in ('eligible_not_selected', 'rejected', 'duplicate'), 'historical_disposition')
            counts[row['disposition']] += 1
        for disposition, name in (('eligible_not_selected', 'eligible_occurrences'),
                                  ('rejected', 'rejected_occurrences'), ('duplicate', 'duplicate_occurrences')):
            _same(summary[name], counts[disposition], 'historical_ledger_accounting')
    encoded = json.dumps(ledger, sort_keys=True, separators=(',', ':'), allow_nan=False)
    _same(manifest['input_component_digests']['quote_ledger_json'], canonical_sha256(['str', encoded]),
          'historical_ledger_input_binding')


def _inspect(reader: _Reader, report: dict, requested: tuple) -> str:
    root_entries = reader.listing(".")
    report["ignored_root_entries"] = [{"name": name, "type": info["type"]} for name, info in root_entries.items() if name not in ("sweep_manifest.json", "sweep_summary.csv", "cells", 'historical_quote_ledger.json')]
    manifest = reader.read("sweep_manifest.json")
    _need(type(manifest) is dict, "manifest_type")
    _need(type(manifest.get("schema_version")) is int, "manifest_version_type")
    if manifest["schema_version"] not in (1, 2):
        raise EvidenceError("unsupported_format", "BLOCKED")
    state = manifest.get("state")
    _need(type(state) is str and state in ("complete", "incomplete", "failed"), "producer_state")
    if state == "failed" or (state == "complete" and (manifest.get("failure") is not None or manifest.get("active_cell") is not None)):
        raise EvidenceError("producer_failed")
    if reader.root.name.endswith("_incomplete") or state == "incomplete":
        if state == "complete":
            raise EvidenceError("producer_failed")
        raise EvidenceError("producer_incomplete", "BLOCKED")
    if manifest.get("encoding") != ENCODING:
        raise EvidenceError("unsupported_format", "BLOCKED")
    required = {"schema_version", "state", "plan", "completed_cells", "input_identity", "outputs", "failure", "encoding", "model_deserialized", "expected_dates", "acquired_dates", "date_outcome_basis", "output_schemas", "empty_reason", "observed", "input_component_digests", "sample_inventory", "active_cell", *READINESS}
    _keys(manifest, required, "manifest_keys")
    _readiness(manifest)
    _need(type(manifest["model_deserialized"]) is bool, "manifest_boolean")
    _digest(manifest["input_identity"])
    plan = manifest["plan"]
    fixed = _saved_plan(plan)
    _same(manifest['schema_version'], plan['schema_version'], 'manifest_plan_version')
    schemas = _schemas(manifest["output_schemas"])
    dates = _dates(manifest, fixed)
    receipts = manifest["completed_cells"]
    _need(type(receipts) is list, "completed_cells")
    if len(receipts) > CELL_LIMIT:
        raise EvidenceError("resource_limit", "BLOCKED")
    ids = [cell["cell_id"] for cell in plan["cells"]]
    receipt_map = {}
    for receipt in receipts:
        _keys(receipt, {"cell_id", "input_identity", "output_schema_identity", "effective_config_identity", "files"}, "cell_receipt")
        cell_id = receipt["cell_id"]
        _need(type(cell_id) is str and re.fullmatch(r"cell_[0-9a-f]{64}", cell_id) is not None, "cell_id")
        _need(cell_id in ids and cell_id not in receipt_map, "cell_set")
        _keys(receipt["files"], CELL_FILES, "cell_file_inventory")
        for key in ("input_identity", "output_schema_identity", "effective_config_identity"):
            _digest(receipt[key])
        receipt_map[cell_id] = receipt
    _need(set(receipt_map) == set(ids), "cell_set")
    _need(set(reader.listing("cells")) == set(ids), "layout")
    _keys(manifest["outputs"], {'sweep_summary.csv'} | (
        {'historical_quote_ledger.json'} if manifest['schema_version'] == 2 else set()), "summary_receipt")
    if manifest['schema_version'] == 2:
        _historical_ledger(reader, manifest, dates)
    summary = reader.read("sweep_summary.csv", manifest["outputs"]["sweep_summary.csv"], SUMMARY_COLUMNS)
    _need(len(summary) == len(ids), "summary_cell_set")
    summary_map = {}
    for row in summary:
        _need(row["cell_id"] in ids and row["cell_id"] not in summary_map, "summary_cell_set")
        summary_map[row["cell_id"]] = row
    integrity_status = "PASS"
    timing_results = []
    for cell in plan["cells"]:
        cell_id = cell["cell_id"]
        prefix = "cells/" + cell_id + "/"
        _need(set(reader.listing("cells/" + cell_id)) == CELL_FILES, "layout")
        receipt = receipt_map[cell_id]
        config = reader.read(prefix + "config.json", receipt["files"]["config.json"])
        metrics = reader.read(prefix + "metrics.json", receipt["files"]["metrics.json"])
        frames = {kind: reader.read(prefix + name, receipt["files"][name], schemas[name]) for kind, name in (("candidates", "all_bookmaker_edges.csv"), ("selected", "predictions.csv"), ("bets", "bets.csv"))}
        expected = _effective(plan, cell, manifest, len(frames["bets"]))
        _same(config, expected, "effective_config")
        _same(receipt["effective_config_identity"], canonical_sha256(expected), "effective_config_identity")
        _same(receipt["input_identity"], manifest["input_identity"], "input_identity")
        _same(receipt["output_schema_identity"], expected["output_schema_identity"], "output_schema_identity")
        saved_metrics = _metrics(metrics)
        _same(metrics["config"], expected, "metrics_config")
        _need(saved_metrics["betting"]["total_bets"] == len(frames["bets"]), "metrics_bet_count")
        counts = {key: len(rows) for key, rows in frames.items()}
        _summary(summary_map[cell_id], plan, cell, manifest, saved_metrics, counts)
        for kind, rows in frames.items():
            _row_validation(rows, kind, fixed, dates)
        candidate_counts = Counter(row["game_date"] for row in frames["candidates"])
        for day, observed in dates.items():
            saved = [row for row in frames["candidates"] if row["game_date"] == day]
            distinct = len({(row["player_id"], row["game_id"], row["stat"]) for row in saved})
            acquired_predictions = observed["counts"]["predictions"]
            _need(candidate_counts[day] >= acquired_predictions and distinct <= acquired_predictions, "date_candidate_counts")
            _need(bool(candidate_counts[day]) == bool(acquired_predictions), "date_candidate_counts")
            if observed["reason"] == "no_quotes":
                _need(all(row["quote_status"] == "no_quote" for row in saved), "date_quote_counts")
        relationships = {
            "candidate_to_selected": _relationship(frames["candidates"], frames["selected"], "candidate_to_selected"),
            "selected_to_bet": _relationship(frames["selected"], frames["bets"], "selected_to_bet"),
        }
        if any(value["status"] == "BLOCKED" for value in relationships.values()):
            integrity_status = "BLOCKED"
            for transition, relationship in relationships.items():
                if relationship["status"] == "BLOCKED":
                    report["issues"].append({"status": "BLOCKED", "reason": relationship["reason"], "cell_id": cell_id,
                                             "relationship": transition, "structural_containment": "PASS"})
        report["cells"].append({"cell_id": cell_id, "counts": counts, "relationships": relationships})
        if "concentration" in requested:
            report["tables"]["concentration"].extend(_concentration(cell_id, frames["bets"]))
        if "coverage" in requested:
            report["tables"]["coverage"].extend(_coverage(cell_id, frames, dates, relationships))
        if "timing" in requested:
            timing, violations, missing = _timing(cell_id, frames["selected"])
            timing_results.append(timing)
            report["cells"][-1]["timing"] = timing
            report["tables"]["timing_violations"].extend(violations)
            report["tables"]["unmeasurable"].extend(missing)
    report["checks"]["integrity"] = {"status": integrity_status, "reason": "saved_output_consistency_only" if integrity_status == "PASS" else "relationship_provenance_unmeasurable"}
    for check in ("concentration", "coverage"):
        if check in requested:
            report["checks"][check] = {"status": "PASS", "reason": "saved_evidence_descriptive_only"}
    if "timing" in requested:
        status = "FAIL" if any(item["status"] == "FAIL" for item in timing_results) else "BLOCKED" if any(item["status"] == "BLOCKED" for item in timing_results) else "PASS"
        report["checks"]["timing"] = {"status": status, "reason": "ordering_violation" if status == "FAIL" else "missing_required_observations" if status == "BLOCKED" else "saved_ordering_mechanics_only"}
    report["source_request_identity"] = plan["request_identity"]
    report["source_input_identity"] = manifest["input_identity"]
    return fixed["model_dir"]


def _report(requested: tuple) -> dict:
    checks = {name: {"status": "NOT_REQUESTED", "reason": None} for name in DEFAULT_CHECKS}
    checks.update({name: {"status": status, "reason": reason, "requested": name in requested} for name, (status, reason) in CAPABILITIES.items()})
    return {"audit_schema_version": 1, "report_state": "complete", "historical_readiness": "unverified", "decision_grade": False,
            "requested_checks": list(requested), "checks": checks, "issues": [], "cells": [], "tables": {key: [] for key in TABLE_COLUMNS},
            "source_files": {}, "source_directories": {}, "ignored_root_entries": [], "limitations": [
                "Unsigned hashes establish consistency, not authentic execution or an immutable snapshot.",
                "Observed read stability is not a lock and cannot rule out transient mutation.",
                "No model, original path, raw input, probability, selection policy or settlement is re-executed.",
                "Saved candidate quotes, selected predictions and bets have different denominators; no full source universe or quote-clean dropout is measured.",
                "Missing provenance, source clocks and closing histories cannot be synthesized; no ranking or promotion is performed.",
            ]}


def _exit_code(report: dict) -> int:
    statuses = [report["checks"][key]["status"] for key in report["requested_checks"]]
    if report["report_state"] == "failed" or any(item["status"] == "FAIL" for item in report["issues"]) or "FAIL" in statuses:
        return 1
    if any(item["status"] == "BLOCKED" for item in report["issues"]) or any(status in ("BLOCKED", "UNSUPPORTED") for status in statuses):
        return 2
    return 0


def inspect_sweep(path: str | Path) -> dict:
    """Inspect supported saved evidence; never follow recorded acquisition paths."""
    return audit_sweep(path, checks=("integrity",))


def audit_sweep(path: str | Path, checks: Any = DEFAULT_CHECKS) -> dict:
    """Return a JSON-compatible report, with mandatory integrity and stable reads.

    Expected invalid evidence is represented in the report, not raised. No files
    are written. Resource limits are fixed, not caller-overridable parser knobs.
    """
    report = _report(("integrity",))
    reader = None
    try:
        _need(isinstance(checks, tuple | list) and bool(checks) and all(type(key) is str for key in checks), "invalid_checks")
        _need(len(checks) == len(set(checks)) and set(checks).issubset({*DEFAULT_CHECKS, *CAPABILITIES}), "invalid_checks")
        requested = tuple(dict.fromkeys(("integrity", *checks)))
        report = _report(requested)
        root = Path(path).resolve(strict=True)
        _need(root.is_dir(), "sweep_directory")
        reader = _Reader(root)
        _inspect(reader, report, requested)
    except EvidenceError as exc:
        report["checks"]["integrity"] = {"status": exc.status, "reason": exc.reason}
        report["issues"].append({"status": exc.status, "reason": exc.reason})
        if exc.reason == "invalid_checks":
            report["report_state"] = "failed"
    except (OSError, UnicodeError, csv.Error, ValueError, TypeError, KeyError, IndexError, RecursionError, OverflowError):
        report["checks"]["integrity"] = {"status": "FAIL", "reason": "execution_or_malformed_evidence"}
        report["issues"].append({"status": "FAIL", "reason": "execution_or_malformed_evidence"})
    if reader is not None:
        try:
            reader.stable()
        except EvidenceError as exc:
            if report["checks"]["integrity"]["status"] != "FAIL":
                report["checks"]["integrity"] = {"status": exc.status, "reason": exc.reason}
            report["issues"].append({"status": exc.status, "reason": exc.reason})
        report["source_files"] = reader.files
        report["source_directories"] = reader.directories
    if report["checks"]["integrity"]["status"] != "PASS":
        report["cells"] = []
        # Negative observations are still evidence, not trusted aggregates.
        # In particular unavailable unused-side clocks cannot erase an aware
        # chosen-side ordering violation (FAIL outranks BLOCKED).
        report["tables"]["concentration"] = []
        report["tables"]["coverage"] = []
        for key in ("concentration", "coverage", "timing"):
            if key in report["requested_checks"] and report["checks"][key]["status"] != "FAIL":
                report["checks"][key] = {"status": "BLOCKED", "reason": "integrity_not_pass"}
        if report["tables"]["timing_violations"]:
            report["checks"]["timing"] = {"status": "FAIL", "reason": "ordering_violation"}
    report["exit_code"] = _exit_code(report)
    return report


def _path_flavor(value: str) -> Path:
    """Resolve a model token only for explicit export containment, never read it."""
    _need(type(value) is str and bool(value) and "\x00" not in value, "output_containment_unknown")
    if os.name == "nt":
        token = PureWindowsPath(value)
        if not token.is_absolute() or value.startswith("/") and not value.startswith("//"):
            raise EvidenceError("output_containment_unknown", "BLOCKED")
    else:
        if not PurePosixPath(value).is_absolute() or PureWindowsPath(value).drive:
            raise EvidenceError("output_containment_unknown", "BLOCKED")
    return Path(value).resolve(strict=False)


def _write_json(path: Path, value: dict, *, replace: bool = False) -> None:
    with path.open("w" if replace else "x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False, ensure_ascii=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    _same(_json(path.read_text(encoding="utf-8")), value, "report_readback")


def _directory_identity(path: Path) -> tuple[int, int]:
    literal = path.lstat()
    _need(not stat.S_ISLNK(literal.st_mode) and not getattr(literal, "st_reparse_tag", 0), "output_path_changed")
    _need(stat.S_ISDIR(literal.st_mode), "output_path_changed")
    resolved = path.resolve(strict=True)
    _need(resolved == path, "output_path_changed")
    info = resolved.stat()
    return info.st_dev, info.st_ino


def _outside_protected(paths: tuple[Path, ...], protected: tuple[Path, ...]) -> None:
    for path in paths:
        _need(not any(path == item or path.is_relative_to(item) for item in protected), "output_containment")


def _source_paths_stable(report: dict, source: Path) -> None:
    try:
        for relative, observed in report["source_files"].items():
            path = source / relative
            current = _identity(path)
            _need(current == {key: value for key, value in observed.items() if key != "sha256"}, "source_changed_during_audit")
        reader = _Reader(source)
        for relative, observed in report["source_directories"].items():
            _need(reader.listing(relative) == observed, "source_changed_during_audit")
    except (OSError, EvidenceError):
        raise EvidenceError("source_changed_during_audit", "BLOCKED") from None


def _export_paths_stable(state: dict) -> None:
    try:
        _outside_protected((state["final"], state["root"]), state["protected"])
        _need(state["final"].resolve(strict=False) == state["final"], "output_path_changed")
        _need(state["root"].resolve(strict=True) == state["root"], "output_path_changed")
        _need(not os.path.lexists(state["final"]), "output_collision")
        _need(_directory_identity(state["root"]) == state["root_identity"], "output_path_changed")
        for path, identity in state["ancestors"].items():
            _need(_directory_identity(path) == identity, "output_path_changed")
    except EvidenceError:
        raise
    except OSError:
        raise EvidenceError("output_path_changed") from None


def _export_checkpoint(state: dict, report: dict, source: Path) -> None:
    """Best-effort observation only; this is not a lock or hostile-race boundary."""
    _export_paths_stable(state)
    _source_paths_stable(report, source)


def _export(report: dict, source: Path, destination: str) -> None:
    """Exclusive incomplete creation and best-effort observed-change detection."""
    raw = Path(destination)
    _need(bool(raw.name) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", raw.name) is not None and not raw.name.endswith("_incomplete"), "output_directory_token")
    # Use a fresh strictly parsed manifest solely to obtain the containment path.
    # The saved plan is not validated by resolving this token, nor is a model read.
    _verify_report_sources(report, source)
    model_reader = _Reader(source)
    manifest = model_reader.read("sweep_manifest.json")
    _need(model_reader.files["sweep_manifest.json"] == report["source_files"].get("sweep_manifest.json"), "source_changed_during_audit")
    try:
        model = _path_flavor(manifest["plan"]["fixed_config"]["model_dir"])
        artifact_model = _path_flavor(manifest["plan"]["artifact"]["path"])
    except (KeyError, TypeError, EvidenceError, OSError, ValueError):
        raise EvidenceError("output_containment_unknown", "BLOCKED") from None
    final = raw.resolve(strict=False)
    root = final.with_name(final.name + "_incomplete")
    _need(raw.absolute() == final and raw.with_name(raw.name + "_incomplete").absolute() == root, "output_path_changed")
    protected = (source, model, artifact_model)
    _outside_protected((final, root), protected)
    if os.path.lexists(raw) or os.path.lexists(final) or os.path.lexists(root):
        raise EvidenceError("output_collision")
    audit_manifest = {"audit_schema_version": 1, "report_state": "incomplete", "decision_grade": False,
                      "historical_readiness": "unverified", "requested_checks": report["requested_checks"],
                      "source_files": report["source_files"], "checks": report["checks"], "outputs": {},
                      "audit_source": {"module": "src/backtesting/nba/evidence.py", "sha256": sha256_file(Path(__file__))}}
    try:
        created_root, created_final = create_exclusive_incomplete_dir(final.parent, final.name)
    except FileExistsError:
        raise EvidenceError("output_collision") from None
    except (OSError, ValueError):
        raise EvidenceError("output_creation_failed") from None
    _need(created_root == root and created_final == final, "output_path_changed")
    ancestors = {path: _directory_identity(path) for path in (final.parent, *final.parent.parents)}
    state = {"root": root, "final": final, "protected": protected,
             "root_identity": _directory_identity(root), "ancestors": ancestors}
    _serialize_report(report, source, model_reader, audit_manifest, state)


def _serialize_report(report: dict, source: Path, model_reader: _Reader,
                      audit_manifest: dict, state: dict) -> None:
    root = state["root"]
    try:
        _export_checkpoint(state, report, source)
        _write_json(root / "audit_manifest.json", audit_manifest)
        _export_checkpoint(state, report, source)
        _write_json(root / "audit_summary.json", report)
        _export_checkpoint(state, report, source)
        text = "NBA saved-evidence audit (not historical acceptance)\n\n" + "\n".join(f"{key}: {value['status']} ({value['reason']})" for key, value in report["checks"].items())
        text += "\n\nDecision grade: false. Report completion is serialization only.\n" + "\n".join(report["limitations"]) + "\n"
        with (root / "audit_summary.md").open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        _need((root / "audit_summary.md").read_text(encoding="utf-8") == text, "report_readback")
        _export_checkpoint(state, report, source)
        for table, columns in TABLE_COLUMNS.items():
            _export_checkpoint(state, report, source)
            path = root / (table + ".csv")
            with path.open("x", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns)
                writer.writeheader()
                writer.writerows(report["tables"][table])
                handle.flush()
                os.fsync(handle.fileno())
            with path.open(encoding="utf-8", newline="") as handle:
                rows = csv.reader(handle, strict=True)
                _need(next(rows) == columns and sum(1 for _ in rows) == len(report["tables"][table]), "report_readback")
            _export_checkpoint(state, report, source)
        model_reader.stable()
        # Final observed source verification includes every input file/directory.
        _verify_report_sources(report, source)
        for name in ("audit_summary.json", "audit_summary.md", *(key + ".csv" for key in TABLE_COLUMNS)):
            path = root / name
            audit_manifest["outputs"][name] = {"sha256": sha256_file(path), "bytes": path.stat().st_size}
        audit_manifest["report_state"] = "complete"
        _export_checkpoint(state, report, source)
        _write_json(root / "audit_manifest.json", audit_manifest, replace=True)
        _export_checkpoint(state, report, source)
        _verify_report_sources(report, source)
        _rename_no_replace(root, state["final"])
        _need(not os.path.lexists(root) and _directory_identity(state["final"]) == state["root_identity"], "output_path_changed")
    except Exception:
        audit_manifest["report_state"] = "failed"
        try:
            # Never write cleanup through a path whose observed identity or
            # containment has changed. The original incomplete receipt remains.
            _export_paths_stable(state)
            _write_json(root / "audit_manifest.json", audit_manifest, replace=True)
            _export_paths_stable(state)
        except Exception:
            pass
        raise


def _verify_report_sources(report: dict, source: Path) -> None:
    reader = _Reader(source)
    reader.files = report["source_files"]
    reader.directories = report["source_directories"]
    reader.stable()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        # argparse's default embeds untrusted tokens and exits 2. Contract uses 1.
        self.print_usage(sys.stderr)
        self.exit(1, "Invalid NBA evidence CLI request.\n")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = _Parser(description="Artifact-only NBA v1 saved-output consistency and descriptive audits; never promotion.", allow_abbrev=False)
    parser.add_argument("--sweep-dir", required=True, help="Explicit completed saved sweep directory")
    parser.add_argument("--checks", nargs="+", choices=(*DEFAULT_CHECKS, *CAPABILITIES), default=list(DEFAULT_CHECKS))
    parser.add_argument("--output-dir", help="New exclusive report directory outside input and recorded model (default stdout only)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    parser = build_arg_parser()
    if any(sum(token.split("=", 1)[0] == option for token in args_list) > 1 for option in ("--sweep-dir", "--checks", "--output-dir")):
        parser.error("duplicate option")
    args = parser.parse_args(args_list)
    report = audit_sweep(args.sweep_dir, args.checks)
    if args.output_dir is not None:
        try:
            _export(report, Path(args.sweep_dir).resolve(strict=True), args.output_dir)
        except EvidenceError as exc:
            report["report_state"] = "failed" if exc.status == "FAIL" else "blocked"
            report["issues"].append({"status": exc.status, "reason": exc.reason})
        except Exception:
            report["report_state"] = "failed"
            report["issues"].append({"status": "FAIL", "reason": "report_io_error"})
        report["exit_code"] = _exit_code(report)
    print(json.dumps(report, sort_keys=True, indent=2, allow_nan=False, ensure_ascii=False))
    return report["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
