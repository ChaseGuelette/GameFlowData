"""DB-first NBA injury recovery CLI for workstation or Railway.

Normal execution delegates to the existing production scraper. The legacy
file-recovery helpers below remain only for old evidence/controlled probes;
they are not the command-line ingestion path.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.scrapers import nba_injury_pilot as pilot  # noqa: E402

POLICY = "historical_game_dates_v1"
ROLE_BY_SEASON = {"22023": "fit", "22024": "fit", "22025": "calibration"}
MAX_DATES = 1097
BATCH_SIZE = 6
MIB = 1024 * 1024


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(MIB), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_csv(path: Path, rows: list[dict]) -> None:
    fields = ["requested_date", "status", "referenced_receipt", "body_sha256", "record_count"]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def load_cohort(path: Path) -> tuple[dict, list[str], list[str], str]:
    raw = path.read_bytes()
    data = json.loads(raw)
    if data.get("complete") is not True:
        raise ValueError("Cohort manifest must declare complete: true")
    if data.get("source_authority") != "canonical_remote":
        raise ValueError("Cohort source_authority must be canonical_remote")
    if data.get("selection_policy") != POLICY:
        raise ValueError(f"Cohort selection_policy must be {POLICY}")
    games = data.get("games")
    if not isinstance(games, list) or not games:
        raise ValueError("Cohort manifest requires a nonempty games array")

    unique: dict[str, tuple[str, str, str]] = {}
    counts = {season: 0 for season in ROLE_BY_SEASON}
    for game in games:
        if not isinstance(game, dict):
            raise ValueError("Every cohort game must be an object")
        game_id = game.get("game_id")
        if not isinstance(game_id, str) or len(game_id) != 10 or not game_id.isdigit():
            raise ValueError("Every game_id must be a literal ten-digit string")
        day = pilot.iso_date(game.get("game_date"))
        season = str(game.get("season_id"))
        role = game.get("role")
        if season not in ROLE_BY_SEASON or role != ROLE_BY_SEASON[season]:
            raise ValueError(f"Invalid season/role for game {game_id}")
        year = int(season[1:])
        parsed = date.fromisoformat(day)
        if not date(year, 7, 1) <= parsed < date(year + 1, 7, 1):
            raise ValueError(f"Game {game_id} is outside its season envelope")
        identity = (day, season, ROLE_BY_SEASON[season])
        if game_id in unique and unique[game_id] != identity:
            raise ValueError(f"Conflicting duplicate game_id: {game_id}")
        if game_id not in unique:
            unique[game_id] = identity
            counts[season] += 1
    empty_seasons = [season for season, count in counts.items() if count == 0]
    if empty_seasons:
        raise ValueError("No historical games for requested seasons: " + ", ".join(empty_seasons))

    declared = data.get("season_counts")
    if declared is not None:
        if not isinstance(declared, dict):
            raise ValueError("season_counts must be an object when provided")
        normalized = {
            str(key): value.get("retained_count") if isinstance(value, dict) else value
            for key, value in declared.items()
        }
        if normalized != counts:
            raise ValueError(f"season_counts does not match enumerated games: {counts}")

    targets = sorted({item[0] for item in unique.values()})
    requests = sorted(set(targets) | {(date.fromisoformat(day) - timedelta(days=1)).isoformat() for day in targets})
    if len(requests) > MAX_DATES:
        raise ValueError(f"Request-date cap exceeded ({len(requests)} > {MAX_DATES})")
    return data, targets, requests, hashlib.sha256(raw).hexdigest()


def manifest_paths(explicit: list[str], output: Path, resume: bool) -> list[Path]:
    paths = [Path(item).resolve() for item in explicit]
    if resume and output.exists():
        paths.extend(sorted(output.glob("batch[0-9][0-9][0-9][0-9]/capture/manifest.json")))
        paths.extend(sorted(output.glob("partitions/part*/manifest.json")))
    result: list[Path] = []
    seen: set[Path] = set()
    for path in paths:
        if path not in seen:
            seen.add(path)
            result.append(path)
    return result


def inspect_reuse(paths: list[Path], required: set[str]) -> tuple[dict[str, dict], dict, dict | None]:
    chosen: dict[str, dict] = {}
    stats = {"verified": 0, "failed": 0, "non_json": 0, "empty": 0, "missing_reports": 0, "irrelevant": 0}
    latest_quota = None
    for path in paths:
        data = pilot.read_json(path)
        entries = data.get("captures", data.get("receipts"))
        if not isinstance(entries, list):
            raise ValueError(f"No capture receipts in {path}")
        complete = data.get("complete") is not False
        for entry in entries:
            day = pilot.iso_date(entry.get("requested_date"))
            if day not in required:
                stats["irrelevant"] += 1
                continue
            if not complete and entry.get("body_complete") is not True:
                stats["failed"] += 1
                continue
            receipt, body = pilot.verified_capture(entry, path.parent)
            headers = receipt.get("safe_response_headers", {})
            remaining = headers.get("x-ratelimit-requests-remaining")
            if remaining is not None:
                candidate = {"remaining": remaining, "retrieved_at_utc": receipt.get("retrieved_at_utc")}
                if latest_quota is None or str(candidate["retrieved_at_utc"] or "") > str(latest_quota["retrieved_at_utc"] or ""):
                    latest_quota = candidate
            missing_report = pilot.is_missing_report(receipt, body)
            try:
                records = [] if missing_report else pilot.decode_body(receipt, body)
            except json.JSONDecodeError:
                stats["non_json"] += 1
                continue
            except ValueError as exc:
                if "HTTP " in str(exc):
                    stats["failed"] += 1
                else:
                    stats["non_json"] += 1
                continue
            stats["verified"] += 1
            stats["empty"] += not records and not missing_report
            stats["missing_reports"] += missing_report
            source = str(path)
            key = (str(receipt.get("retrieved_at_utc") or ""), receipt["raw_body_sha256"], source)
            old = chosen.get(day)
            old_key = None if old is None else (
                str(old["receipt"].get("retrieved_at_utc") or ""),
                old["receipt"]["raw_body_sha256"],
                old["manifest"],
            )
            if old_key is None or key > old_key:
                chosen[day] = {
                    "receipt": receipt,
                    "entry": dict(entry),
                    "manifest": source,
                    "record_count": len(records),
                    "missing_report": missing_report,
                }
    return chosen, stats, latest_quota


def coverage_rows(requests: list[str], chosen: dict, failures: set[str] | None = None) -> list[dict]:
    failures = failures or set()
    rows = []
    for day in requests:
        item = chosen.get(day)
        if item:
            receipt = item["receipt"]
            rows.append({"requested_date": day, "status": "missing_report" if item.get("missing_report") else "reusable_empty" if item["record_count"] == 0 else "reusable",
                         "referenced_receipt": item["manifest"], "body_sha256": receipt["raw_body_sha256"],
                         "record_count": item["record_count"]})
        else:
            rows.append({"requested_date": day, "status": "failed" if day in failures else "pending",
                         "referenced_receipt": "", "body_sha256": "", "record_count": ""})
    return rows


def nearest_existing(path: Path) -> Path:
    candidate = path.resolve()
    while not candidate.exists():
        if candidate.parent == candidate:
            break
        candidate = candidate.parent
    return candidate


def code_hashes() -> dict[str, str]:
    paths = {
        "wrapper": Path(__file__).resolve(),
        "pilot_helper": Path(pilot.__file__).resolve(),
        "client": ROOT / "src/scrapers/rapidapi_injury_backfill.py",
    }
    return {name: sha256_file(path) for name, path in paths.items()}


def write_summary(output: Path, status: str, requests: list[str], targets: list[str], chosen: dict, reuse_stats: dict,
                  reserved: int, confirmed: int, error_type: str | None = None, *, captured_dates=()) -> dict:
    failed = reuse_stats["failed"] + reuse_stats["non_json"]
    new_dates = set(captured_dates) & set(chosen)
    summary = {
        "status": status, "requested": len(requests), "target_dates": len(targets),
        "reused": len(chosen) - len(new_dates),
        "newly_captured": len(new_dates),
        "empty": sum(item["record_count"] == 0 and not item.get("missing_report") for item in chosen.values()),
        "missing_report_dates": sorted(day for day, item in chosen.items() if item.get("missing_report")),
        "failed": failed, "unresolved": len(set(requests) - set(chosen)),
        "date_counts": {"captured": len(chosen), "required": len(requests)},
        "reserved_requests": reserved, "confirmed_http_attempts": confirmed,
        "reuse_diagnostics": reuse_stats, "mechanics_complete": status == "COMPLETE" and len(chosen) == len(requests),
        "training_ready": False, "source_acceptance": "BLOCKED",
    }
    if error_type:
        summary["error_type"] = error_type
    pilot.write_json(output / "summary.json", summary)
    return summary


def publish_partitions(output: Path, requests: list[str], targets: list[str], chosen: dict) -> tuple[int, int]:
    if not chosen:
        raise ValueError("Refusing no-op execution with zero valid captures")
    partitions = output / "partitions"
    partitions.mkdir(exist_ok=True)
    row_count = quarantine = 0
    completed = []
    ordered = [chosen[day] for day in requests]
    for index in range(0, len(ordered), BATCH_SIZE):
        captures = []
        for item in ordered[index:index + BATCH_SIZE]:
            manifest = Path(item["manifest"])
            captures.append(pilot.verified_capture(item["entry"], manifest.parent))
        prefix = f"part{index // BATCH_SIZE + 1:04d}"
        expected = [(item[0]["requested_date"], item[0]["raw_body_sha256"]) for item in captures]
        part = None
        for candidate in sorted(partitions.glob(prefix + "*")):
            manifest = candidate / "manifest.json"
            outputs = ("normalized.jsonl", "quarantine.jsonl", "date_cycle_matrix.csv", "summary.json")
            if not manifest.is_file() or not all((candidate / name).is_file() for name in outputs):
                continue
            if pilot.read_json(manifest).get("complete") is not True:
                continue
            saved = pilot.verified_captures([manifest])
            actual = [(item[0]["requested_date"], item[0]["raw_body_sha256"]) for item in saved]
            if actual == expected and pilot.read_json(manifest).get("target_dates") == targets:
                part = candidate
                break
        if part is None:
            part = partitions / prefix
            attempt = 0
            while part.exists():
                attempt += 1
                part = partitions / f"{prefix}_retry{attempt:03d}"
            summary, rows = pilot.publish_bundle(pilot.new_output(part), captures, targets, allow_missing_reports=True)
        else:
            rows, summary, _ = pilot.normalize(captures, targets, allow_missing_reports=True)
        completed.append(str(part.relative_to(output) / "manifest.json"))
        row_count += len(rows) - summary["quarantined_records"]
        quarantine += summary["quarantined_records"]
    pilot.write_json(output / "partitions.json", {"complete": True, "manifests": completed})
    return row_count, quarantine


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--cohort-manifest", required=True)
    result.add_argument("--reuse-manifest", action="append", default=[])
    result.add_argument("--output-dir", required=True)
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    result.add_argument("--resume", action="store_true")
    result.add_argument("--max-total-requests", type=int)
    result.add_argument("--max-batches", type=int)
    return result


def legacy_file_main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    output = Path(args.output_dir).resolve()
    cohort_path = Path(args.cohort_manifest).resolve()
    try:
        _, targets, requests, cohort_sha = load_cohort(cohort_path)
    except ValueError as exc:
        output.mkdir(parents=True, exist_ok=True)
        pilot.write_json(output / "preflight.json", {
            "status": "BLOCKED_COHORT", "reason": str(exc), "cohort_manifest": str(cohort_path),
            "cohort_sha256": sha256_file(cohort_path), "network_requests": 0, "training_ready": False,
        })
        raise
    if args.resume and not args.execute:
        raise ValueError("--resume is execution-only")
    if args.execute and (not args.max_total_requests or args.max_total_requests < 1 or not args.max_batches or args.max_batches < 1):
        raise ValueError("Execute requires positive --max-total-requests and --max-batches")

    reuse_paths = manifest_paths(args.reuse_manifest, output, args.resume)
    chosen, reuse_stats, quota = inspect_reuse(reuse_paths, set(requests))
    pending = [day for day in requests if day not in chosen]
    free = shutil.disk_usage(nearest_existing(output)).free
    plan = {"cohort_sha256": cohort_sha, "target_dates": targets, "request_dates": requests,
            "reusable_dates": sorted(chosen), "pending_dates": pending, "reuse_manifests": [str(path) for path in reuse_paths],
            "estimated_required_bytes": 64 * MIB + 6 * MIB * len(requests), "available_disk_bytes": free,
            "observed_quota": quota,
            "database_writes": False, "network_on_dry_run": False, "training_ready": False}

    if not args.execute:
        output.mkdir(parents=True, exist_ok=True)
        pilot.write_json(output / "plan.json", plan)
        save_csv(output / "coverage.csv", coverage_rows(requests, chosen))
        pilot.write_json(output / "preflight.json", {
            "status": "DRY_RUN_PASSED", "cohort_sha256": cohort_sha,
            "requested_dates": len(requests), "reused_dates": len(chosen), "pending_dates": len(pending),
            "network_requests": 0, "training_ready": False,
        })
        print(json.dumps({"mode": "dry-run", "requested": len(requests), "reused": len(chosen), "pending": len(pending),
                          "observed_quota": quota}, sort_keys=True))
        return 0

    output.mkdir(parents=True, exist_ok=True)
    run_path = output / "run.json"
    if run_path.exists() and not args.resume:
        raise ValueError("run.json exists; use --resume")
    if args.resume and not run_path.exists():
        raise ValueError("--resume requires an existing run.json")
    needed_bytes = plan["estimated_required_bytes"]
    free = shutil.disk_usage(nearest_existing(output)).free
    if free < needed_bytes:
        raise ValueError(f"Insufficient disk headroom: need {needed_bytes} bytes, have {free}")

    if run_path.exists():
        run = pilot.read_json(run_path)
        if run.get("cohort_sha256") != cohort_sha or run.get("request_dates") != requests or run.get("target_dates") != targets:
            raise ValueError("Resume cohort/request dates do not match the original run")
        if args.max_total_requests < run["max_total_requests"] or args.max_batches < run["max_batches"]:
            raise ValueError("Resume budget ceilings may increase but cannot decrease")
        run["max_total_requests"], run["max_batches"] = args.max_total_requests, args.max_batches
    else:
        run = {"created_at_utc": pilot.utc_now(), "cohort_sha256": cohort_sha, "target_dates": targets,
               "request_dates": requests, "reuse_sources": [str(path) for path in reuse_paths], "code_hashes": code_hashes(),
               "max_total_requests": args.max_total_requests, "max_batches": args.max_batches,
               "reserved_requests": 0, "confirmed_http_attempts": 0, "batch_records": [], "status": "RUNNING"}
    pilot.write_json(run_path, run)
    journal = output / "execution.journal.jsonl"

    available_requests = run["max_total_requests"] - run["reserved_requests"]
    available_batches = run["max_batches"] - len(run["batch_records"])
    required_batches = (len(pending) + BATCH_SIZE - 1) // BATCH_SIZE
    if len(pending) > available_requests or required_batches > available_batches:
        run["status"] = "BLOCKED_BUDGET"
        pilot.write_json(run_path, run)
        save_csv(output / "coverage.csv", coverage_rows(requests, chosen))
        write_summary(output, "BLOCKED", requests, targets, chosen, reuse_stats,
                      run["reserved_requests"], run["confirmed_http_attempts"],
                      captured_dates=[day for batch in run["batch_records"] for day in batch["dates"]])
        raise RuntimeError("Pending dates cannot fit the remaining lifetime request/batch budget")
    if quota is not None:
        try:
            if int(quota["remaining"]) < len(pending):
                raise RuntimeError("Observed response quota is below the required new requests; no probe was made")
        except (TypeError, ValueError):
            pass

    try:
        for offset in range(0, len(pending), BATCH_SIZE):
            days = pending[offset:offset + BATCH_SIZE]
            sequence = len(run["batch_records"]) + 1
            batch_root = output / f"batch{sequence:04d}"
            batch_root.mkdir(exist_ok=False)
            request_manifest = batch_root / "request.json"
            pilot.write_json(request_manifest, {"dates": days, "target_dates": targets})
            record = {"sequence": sequence, "dates": days, "reserved_requests": len(days),
                      "status": "RESERVED", "reserved_at_utc": pilot.utc_now()}
            run["reserved_requests"] += len(days)
            run["batch_records"].append(record)
            pilot.write_json(run_path, run)

            def progress(receipt: dict) -> None:
                event = {"at_utc": pilot.utc_now(), "batch": sequence, "requested_date": receipt.get("requested_date"),
                         "http_status": receipt.get("http_status"), "raw_body_bytes": receipt.get("raw_body_bytes"),
                         "body_complete": receipt.get("body_complete")}
                with journal.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(event, sort_keys=True) + "\n")
                    handle.flush()
                run["confirmed_http_attempts"] += 1
                pilot.write_json(run_path, run)

            print(f"batch {sequence}: capturing {len(days)} date(s)")
            pilot.capture(request_manifest, batch_root / "capture", max_requests=len(days), progress=progress,
                          allow_missing_reports=True)
            record["status"] = "COMPLETE"
            record["completed_at_utc"] = pilot.utc_now()
            pilot.write_json(run_path, run)
            if offset + BATCH_SIZE < len(pending):
                time.sleep(1)

        all_paths = manifest_paths(args.reuse_manifest, output, True)
        chosen, reuse_stats, _ = inspect_reuse(all_paths, set(requests))
        unresolved = set(requests) - set(chosen)
        if unresolved:
            raise RuntimeError(f"Execution ended with {len(unresolved)} unresolved date(s)")
        save_csv(output / "coverage.csv", coverage_rows(requests, chosen))
        rows, quarantine = publish_partitions(output, requests, targets, chosen)
        summary = write_summary(output, "COMPLETE", requests, targets, chosen, reuse_stats,
                                run["reserved_requests"], run["confirmed_http_attempts"],
                                captured_dates=[day for batch in run["batch_records"] for day in batch["dates"]])
        summary.update({"normalized_rows": rows, "quarantined_rows": quarantine})
        pilot.write_json(output / "summary.json", summary)
        run["status"] = "COMPLETE"
        run["completed_at_utc"] = pilot.utc_now()
        pilot.write_json(run_path, run)
        print(json.dumps(summary, sort_keys=True))
        return 0
    except Exception as exc:
        run["status"] = "FAILED"
        run["failed_at_utc"] = pilot.utc_now()
        run["error_type"] = type(exc).__name__
        if isinstance(exc, pilot.CaptureError):
            run["error_context"] = str(exc)
        pilot.write_json(run_path, run)
        all_paths = manifest_paths(args.reuse_manifest, output, True)
        chosen, reuse_stats, _ = inspect_reuse(all_paths, set(requests))
        save_csv(output / "coverage.csv", coverage_rows(requests, chosen))
        write_summary(output, "FAILED", requests, targets, chosen, reuse_stats,
                      run["reserved_requests"], run["confirmed_http_attempts"], type(exc).__name__,
                      captured_dates=[day for batch in run["batch_records"] for day in batch["dates"]])
        detail = str(exc) if isinstance(exc, pilot.CaptureError) else f"{type(exc).__name__}; inspect redacted run artifacts"
        raise RuntimeError(f"Recovery stopped: {detail}") from None


def main(argv: list[str] | None = None) -> int:
    """Default to preview; --execute enables the existing DB writer."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    retired = {"--cohort-manifest", "--reuse-manifest", "--output-dir", "--max-total-requests", "--max-batches", "--resume"}
    if any(argument.split("=")[0] in retired for argument in arguments):
        raise ValueError("The file-only recovery command is retired. Use --import-captures or --seasons; --help lists DB options.")
    execute = "--execute" in arguments
    arguments = [argument for argument in arguments if argument != "--execute"]
    if execute and "--dry-run" in arguments:
        raise ValueError("Choose --execute or --dry-run, not both")
    if not arguments:
        arguments = ["--help"]
    elif not execute and "--dry-run" not in arguments:
        arguments.append("--dry-run")
    from src.scrapers.rapidapi_injury_backfill import main as database_main

    previous = sys.argv
    try:
        sys.argv = [str(Path(__file__)), *arguments]
        database_main()
    finally:
        sys.argv = previous
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"DB recovery failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
