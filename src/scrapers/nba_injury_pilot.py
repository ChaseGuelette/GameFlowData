"""File-only evidence capture and diagnostics for the NBA injury API.

No database or model access. Capture success is deliberately separate from
historical source acceptance; report-cycle labels are not publication clocks.
"""

import csv
import hashlib
import json
import time
from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

STATUSES = frozenset({"Out", "Doubtful", "Questionable", "Probable", "Available"})
MAX_REQUESTS = 6
MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_TOTAL_BYTES = 12 * 1024 * 1024
RUN_SECONDS = 180
VERSION = "nba-injury-pilot/v1"
SAFE_HEADERS = {
    "date", "content-type", "content-length", "etag", "last-modified",
    "x-rapidapi-request-id", "x-ratelimit-requests-limit",
    "x-ratelimit-requests-remaining", "x-ratelimit-requests-reset", "retry-after",
}


class CaptureError(RuntimeError):
    """Capture failure whose message contains only safe local diagnostics."""


def iso_date(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError(f"Expected literal YYYY-MM-DD date, got {value!r}")
    return value


def utc_now():
    return datetime.now(UTC).isoformat()


def read_json(path):
    path = Path(path)
    if path.stat().st_size > MAX_TOTAL_BYTES:
        raise ValueError("Input manifest exceeds twelve MiB")
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, data):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def new_output(path):
    path = Path(path).resolve()
    root = Path(__file__).resolve().parents[2]
    if any(path.is_relative_to(root / part) for part in ("src", "tests", "scripts")):
        raise ValueError("Output must be outside source, tests and production artifacts")
    path.mkdir(parents=True, exist_ok=False)
    (path / "raw").mkdir()
    return path


def request_plan(manifest, max_requests=3):
    data = read_json(manifest)
    dates = data.get("dates", [])
    if not isinstance(dates, list) or not dates:
        raise ValueError("Request manifest requires a nonempty dates list")
    dates = [iso_date(value) for value in dates]
    if len(set(dates)) != len(dates):
        raise ValueError("Duplicate request dates are not allowed")
    if not 1 <= max_requests <= MAX_REQUESTS or len(dates) > max_requests:
        raise ValueError("Request count exceeds the explicit budget (hard maximum six)")
    targets = [iso_date(value) for value in data.get("target_dates", dates)]
    if not targets:
        raise ValueError("target_dates cannot be empty")
    return {
        "dates": dates, "target_dates": targets, "max_requests": max_requests,
        "max_body_bytes": MAX_BODY_BYTES, "max_total_bytes": MAX_TOTAL_BYTES,
        "deadline_seconds": RUN_SECONDS, "automatic_retries": 0,
        "database_writes": False, "training_ready": False,
    }


def verified_capture(entry, directory):
    """Verify one receipt, also usable for successful rows in partial batches."""
    iso_date(entry["requested_date"])
    if entry.get("body_complete") is False:
        raise ValueError("Incomplete response body cannot be reused")
    raw_path = (Path(directory) / entry["raw_body_file"]).resolve()
    if raw_path.stat().st_size > MAX_BODY_BYTES:
        raise ValueError("Captured body exceeds two MiB")
    body = raw_path.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    if digest != entry["raw_body_sha256"] or len(body) != entry["raw_body_bytes"]:
        raise ValueError(f"Capture digest/length mismatch: {entry['requested_date']}")
    receipt = {key: entry.get(key) for key in (
        "requested_date", "endpoint", "retrieved_at_utc", "http_status",
        "raw_body_bytes", "raw_body_sha256", "fetch_started_at_utc", "body_complete",
    )}
    receipt["safe_response_headers"] = {
        key.lower(): value for key, value in entry.get("safe_response_headers", {}).items()
        if key.lower() in SAFE_HEADERS
    }
    return receipt, body


def verified_captures(manifests):
    """Read original pilot receipts or this tool's manifests, validating bytes."""
    result, seen, total = [], set(), 0
    for manifest in manifests:
        manifest = Path(manifest).resolve()
        data = read_json(manifest)
        if data.get("complete") is False:
            raise ValueError(f"Incomplete capture manifest: {manifest}")
        entries = data.get("captures", data.get("receipts"))
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"No capture receipts in {manifest}")
        for entry in entries:
            requested = iso_date(entry["requested_date"])
            receipt, body = verified_capture(entry, manifest.parent)
            digest = receipt["raw_body_sha256"]
            identity = (requested, digest, entry.get("retrieved_at_utc"))
            if identity in seen:
                raise ValueError("Duplicate input capture; do not count it twice")
            seen.add(identity)
            total += len(body)
            if total > MAX_TOTAL_BYTES or len(result) >= MAX_REQUESTS:
                raise ValueError("Pilot input exceeds six captures/twelve MiB")
            result.append((receipt, body))
    return result


def decode_body(receipt, body):
    if receipt["http_status"] != 200:
        raise ValueError(f"HTTP {receipt['http_status']} for {receipt['requested_date']}")
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError("API response is not a record list; not an empty injury report")
    return data


def is_missing_report(receipt, body):
    """Recognize the provider's dated no-report response, not arbitrary 404s."""
    if receipt.get("http_status") != 404 or receipt.get("body_complete") is not True:
        return False
    try:
        data = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return False
    return isinstance(data, dict) and data.get("detail") == f"Report not found for date {receipt['requested_date']}"


def normalize(captures, target_dates, *, allow_missing_reports=False):
    targets = set(target_dates)
    rows, counts, matrix, duplicate_loss = [], Counter(), Counter(), 0
    player_states = defaultdict(set)
    for receipt, body in captures:
        if allow_missing_reports and is_missing_report(receipt, body):
            counts["missing_reports"] += 1
            continue
        records = decode_body(receipt, body)
        counts["empty_responses"] += not records
        keys = set()
        for ordinal, raw in enumerate(records, 1):
            errors = []
            item = raw if isinstance(raw, dict) else {}
            if not isinstance(raw, dict):
                errors.append("record_not_object")
            game_date = item.get("date")
            try:
                iso_date(game_date)
            except (TypeError, ValueError):
                errors.append("invalid_game_date")
            for field in ("team", "player", "status", "reportTime"):
                if not isinstance(item.get(field), str) or not item[field].strip():
                    errors.append(f"missing_or_invalid_{field}")
            status = item.get("status")
            if not isinstance(status, str) or status not in STATUSES:
                errors.append("invalid_status")
                counts["invalid_status_records"] += 1
            disposition = "quarantined" if errors else ("target" if game_date in targets else "spillover")
            counts[disposition] += 1
            counts["input_records"] += 1
            counts["date_differs_from_request"] += game_date != receipt["requested_date"]
            row = {
                "capture_sha256": receipt["raw_body_sha256"], "record_ordinal": ordinal,
                "requested_date": receipt["requested_date"], "game_date": game_date,
                "raw_report_time": item.get("reportTime"), "team": item.get("team"),
                "player": item.get("player"), "status": status, "reason": item.get("reason"),
                "retrieved_at_utc": receipt.get("retrieved_at_utc"),
                "publication_time_utc": None, "edition_id": None,
                "disposition": disposition, "errors": errors, "raw_record": raw,
            }
            rows.append(row)
            # JSON encoding keeps malformed/unhashable fields observable, not fatal.
            key = json.dumps([receipt["requested_date"], item.get("team") or "", item.get("player"),
                              status, item.get("reason") or ""], sort_keys=True)
            duplicate_loss += key in keys
            keys.add(key)
            matrix[(receipt["requested_date"], str(game_date), str(item.get("reportTime")))] += 1
            player_key = json.dumps([game_date, item.get("team"), item.get("player")], sort_keys=True)
            player_states[player_key].add(json.dumps([status, item.get("reason")], sort_keys=True))
    summary = {
        "schema_version": VERSION, "capture_count": len(captures),
        "target_dates": sorted(targets), "input_records": counts["input_records"],
        "retained_target_candidates": counts["target"], "retained_spillover": counts["spillover"],
        "quarantined_records": counts["quarantined"], "invalid_status_records": counts["invalid_status_records"],
        "date_differs_from_request": counts["date_differs_from_request"],
        "empty_responses": counts["empty_responses"], "within_response_dedupe_loss": duplicate_loss,
        "missing_reports": counts["missing_reports"],
        "same_game_player_value_variants": sum(len(values) > 1 for values in player_states.values()),
        "mechanics_pass": True, "injury_source_pass": False, "native_consumer_pass": False,
        "training_ready": False,
        "source_blockers": ["publication_clock_unverified", "edition_selection_unverified", "team_completeness_unverified"],
    }
    assert summary["input_records"] == counts["target"] + counts["spillover"] + counts["quarantined"]
    return rows, summary, matrix


def write_csv(path, fields, records):
    with Path(path).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


def publish_bundle(output, captures, target_dates, *, already_saved=False, allow_missing_reports=False):
    rows, summary, matrix = normalize(captures, target_dates, allow_missing_reports=allow_missing_reports)
    receipts = []
    for index, (receipt, body) in enumerate(captures, 1):
        receipt = dict(receipt)
        filename = f"raw/{index:02d}-{receipt['requested_date']}-{receipt['raw_body_sha256'][:12]}.raw"
        if not already_saved:
            (output / filename).write_bytes(body)
        receipt["raw_body_file"] = filename
        write_json(output / (filename + ".receipt.json"), receipt)
        receipts.append(receipt)
    for filename, predicate in (
        ("normalized.jsonl", lambda row: row["disposition"] != "quarantined"),
        ("quarantine.jsonl", lambda row: row["disposition"] == "quarantined"),
    ):
        with (output / filename).open("w", encoding="utf-8") as handle:
            for row in rows:
                if predicate(row):
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    write_csv(output / "date_cycle_matrix.csv", ["requested_date", "game_date", "report_time", "rows"], [
        {"requested_date": requested, "game_date": day, "report_time": cycle, "rows": count}
        for (requested, day, cycle), count in sorted(matrix.items())
    ])
    write_summary(output, summary)
    write_json(output / "manifest.json", {
        "schema_version": VERSION, "complete": True, "created_at_utc": utc_now(),
        "target_dates": sorted(set(target_dates)), "captures": receipts,
        "mechanics_pass": True, "injury_source_pass": False, "training_ready": False,
    })
    return summary, rows


def write_summary(output, summary):
    write_json(output / "summary.json", summary)
    (output / "summary.md").write_text(
        "# NBA injury API pilot\n\n" + "\n".join(f"- {key}: {value}" for key, value in summary.items()) + "\n",
        encoding="utf-8",
    )


def replay(manifests, output_dir, target_dates=None):
    captures = verified_captures(manifests)
    targets = target_dates or sorted({receipt["requested_date"] for receipt, _ in captures})
    targets = [iso_date(day) for day in targets]
    # Validate before creating outputs, so corrupt inputs never create a success bundle.
    normalize(captures, targets)
    return publish_bundle(new_output(output_dir), captures, targets)


def capture(manifest, output_dir, max_requests=3, *, progress=None, allow_missing_reports=False):
    plan = request_plan(manifest, max_requests)
    # Lazy import: replay/plan never load credentials or import the DB-aware legacy module.
    import os

    from src.scrapers.rapidapi_injury_backfill import RAPIDAPI_BASE_URL, RapidAPIInjuryClient

    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        raise ValueError("RAPIDAPI_KEY is not configured")
    output = new_output(output_dir)
    captures, total, attempts = [], 0, 0
    client = RapidAPIInjuryClient(api_key=api_key, delay=1.0, max_retries=1)
    started = time.monotonic()
    write_json(output / "manifest.json", {"schema_version": VERSION, "complete": False, "captures": []})
    try:
        for index, day in enumerate(plan["dates"], 1):
            if time.monotonic() - started >= RUN_SECONDS - 20:
                raise TimeoutError("Pilot wall-clock budget exhausted before next request")
            attempts += 1
            receipt = {
                "requested_date": day, "endpoint": f"{RAPIDAPI_BASE_URL}/{day}",
                "fetch_started_at_utc": utc_now(), "http_status": None, "safe_response_headers": {},
                "body_complete": False,
            }
            body = bytearray()
            try:
                with client.fetch_date_response(date.fromisoformat(day)) as response:
                    receipt["http_status"] = response.status_code
                    receipt["safe_response_headers"] = {
                        key.lower(): value for key, value in response.headers.items() if key.lower() in SAFE_HEADERS
                    }
                    for chunk in response.iter_content(chunk_size=16384):
                        if time.monotonic() - started >= RUN_SECONDS:
                            raise TimeoutError("Pilot wall-clock budget exhausted while reading")
                        if len(body) + len(chunk) > MAX_BODY_BYTES or total + len(body) + len(chunk) > MAX_TOTAL_BYTES:
                            raise ValueError("Pilot response byte budget exceeded")
                        body.extend(chunk)
                    receipt["body_complete"] = True
            finally:
                receipt.update({
                    "retrieved_at_utc": utc_now(), "raw_body_bytes": len(body),
                    "raw_body_sha256": hashlib.sha256(body).hexdigest(),
                })
                filename = f"raw/{index:02d}-{day}-{receipt['raw_body_sha256'][:12]}.raw"
                receipt["raw_body_file"] = filename
                (output / filename).write_bytes(body)
                write_json(output / (filename + ".receipt.json"), receipt)
                captures.append((receipt, bytes(body)))
                write_json(output / "manifest.json", {
                    "schema_version": VERSION, "complete": False, "http_attempts": attempts,
                    "target_dates": plan["target_dates"], "captures": [entry for entry, _ in captures],
                })
                if progress is not None:
                    progress(dict(receipt))
            total += len(body)
            if not (allow_missing_reports and is_missing_report(receipt, body)):
                decode_body(receipt, body)  # Other HTTP/API/non-JSON failures remain fatal.
        summary, _ = publish_bundle(output, captures, plan["target_dates"], already_saved=True,
                                    allow_missing_reports=allow_missing_reports)
        summary["http_attempts"] = attempts
        summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
        write_summary(output, summary)
        return summary
    except Exception as exc:
        # Avoid exception text: HTTP libraries may include sensitive request details.
        write_summary(output, {
            "schema_version": VERSION, "mechanics_pass": False, "injury_source_pass": False,
            "native_consumer_pass": False, "training_ready": False, "http_attempts": attempts,
            "error_type": type(exc).__name__, "last_http_status": captures[-1][0]["http_status"] if captures else None,
        })
        last = captures[-1][0] if captures else {}
        raise CaptureError(
            f"Capture stopped ({type(exc).__name__}; date={last.get('requested_date')}; "
            f"HTTP {last.get('http_status')}); see redacted receipts in {output}"
        ) from None
    finally:
        client.session.close()


def audit(manifests, target_manifest, output_dir):
    """Diagnose the current API's genuine missing evidence; never invent attestations.

    Native acceptance requires the separately scoped publication/edition adapter.
    This command does not turn user-supplied PASS booleans into source authority.
    """
    target_data = read_json(target_manifest)
    games = target_data.get("games", [])
    if not isinstance(games, list) or len(games) > 6:
        raise ValueError("Audit accepts at most six real target games")
    if not games and target_data.get("status") != "blocked":
        raise ValueError("Empty target manifest needs an explicit blocked selection receipt")
    targets = sorted({iso_date(game["game_date"]) for game in games}) if games else [
        iso_date(day) for day in target_data.get("target_dates", [])
    ]
    if not targets:
        raise ValueError("Target manifest needs explicit game dates")
    summary, rows = replay(manifests, output_dir, targets)
    if not games:
        output = Path(output_dir)
        write_csv(output / "game_team_readiness.csv", [
            "game_id", "season_id", "game_date", "team_id", "team_name",
            "decision_time_utc", "candidate_records", "status", "reasons",
        ], [])
        summary.update({
            "audit_status": "SOURCE_BLOCKED", "target_games": 0, "target_team_reports": 0,
            "accepted_games": 0, "source_authority": target_data.get("source_authority", "unspecified"),
            "target_selection_reason": target_data.get("reason", "No eligible pilot games"),
            "target_selection_gaps": target_data.get("source_gaps", []),
        })
        write_summary(output, summary)
        return summary
    readiness = []
    for game in games:
        game_id = game.get("game_id")
        if not isinstance(game_id, str) or len(game_id) != 10 or not game_id.isdigit():
            raise ValueError("Target game IDs must be literal ten-digit strings")
        teams = game.get("teams") or [{"team_id": value} for value in game.get("team_ids", [])]
        if len(teams) != 2:
            raise ValueError(f"Target game {game_id} needs two team identities")
        timing_errors = []
        decision = None
        try:
            commence = datetime.fromisoformat(game["commence_time"].replace("Z", "+00:00"))
            if commence.utcoffset() is None:
                raise ValueError("Naive commence")
            decision = (commence.astimezone(UTC) - timedelta(minutes=60)).isoformat()
            if not game.get("commence_source"):
                timing_errors.append("commence_provenance_missing")
        except (KeyError, TypeError, ValueError, AttributeError):
            timing_errors.append("commence_time_unverified")
        for team in teams:
            name = team.get("team_name", team.get("name"))
            candidates = [row for row in rows if row["game_date"] == game["game_date"] and row["team"] == name] if name else []
            blockers = list(summary["source_blockers"]) + timing_errors
            if not name:
                blockers.append("team_name_mapping_missing")
            elif not candidates:
                blockers.append("no_matching_api_rows")
            if any(row["disposition"] == "quarantined" for row in candidates):
                blockers.append("malformed_team_records")
            blockers.extend(["pregame_membership_unverified", "native_consumer_not_integrated"])
            readiness.append({
                "game_id": game_id, "season_id": game.get("season_id"), "game_date": game["game_date"],
                "team_id": team.get("team_id"), "team_name": name, "decision_time_utc": decision,
                "candidate_records": len(candidates) if name else "", "status": "BLOCKED",
                "reasons": ";".join(blockers),
            })
    output = Path(output_dir)
    write_csv(output / "game_team_readiness.csv", list(readiness[0]), readiness)
    summary.update({"audit_status": "SOURCE_BLOCKED", "target_games": len(games),
                    "target_team_reports": len(readiness), "accepted_games": 0,
                    "source_authority": target_data.get("source_authority", "unspecified")})
    write_summary(output, summary)
    return summary
