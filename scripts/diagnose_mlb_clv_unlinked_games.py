#!/usr/bin/env python3
"""Diagnose unlinked game_id rows in dense MLB CLV snapshots.

SELECT-only, bounded by monotonically increasing id windows. Classifies rows whose
``game_id`` is still NULL into schedule-match buckets so we can decide whether a
second-pass game linker is safe.

Default mode writes reports under backtest_results/audits and does not mutate DB.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.append(str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBCLVUnlinkedGameDiagnostic")

TEAM_NORM_RE = re.compile(r"[^a-z0-9]+")


@dataclass
class EventFinding:
    api_game_id: str | None
    odds_api_event_id: str | None
    home_team: str | None
    away_team: str | None
    commence_time: str | None
    row_count: int
    first_id: int
    last_id: int
    category: str
    nearest_game_id: int | None = None
    nearest_game_time_utc: str | None = None
    nearest_delta_minutes: float | None = None
    same_day_candidate_count: int = 0
    adjacent_day_candidate_count: int = 0
    notes: str = ""


def norm_team(value: str | None) -> str:
    return TEAM_NORM_RE.sub("", (value or "").lower())


def parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Classify dense MLB CLV rows with NULL game_id")
    parser.add_argument("--table", default="mlb_player_props_clv_snapshots")
    parser.add_argument("--id-column", default="id")
    parser.add_argument("--database-url", default=None, help="Defaults to DATABASE_URL")
    parser.add_argument("--local", action="store_true", help="Use LOCAL_DATABASE_URL instead of DATABASE_URL")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--start-id", type=int, default=0)
    parser.add_argument("--max-batches", type=int, default=0, help="0 means scan until no more rows")
    parser.add_argument("--statement-timeout-ms", type=int, default=120_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    parser.add_argument("--output-dir", default="backtest_results/audits/mlb_clv_unlinked_game_diagnostic")
    parser.add_argument("--examples-per-category", type=int, default=20)
    parser.add_argument("--same-day-hours", type=float, default=1.0)
    parser.add_argument("--wide-hours", type=float, default=6.0)
    return parser


def load_deps() -> tuple[Any, Any]:
    from dotenv import load_dotenv
    from sqlalchemy import create_engine, text

    load_dotenv()
    return create_engine, text


def db_url(args: argparse.Namespace) -> str:
    env_name = "LOCAL_DATABASE_URL" if args.local else "DATABASE_URL"
    url = args.database_url or os.getenv(env_name)
    if not url:
        raise SystemExit(f"Missing {env_name}; pass --database-url or set the environment variable")
    return url


def quote_ident(identifier: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        raise SystemExit(f"Unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def set_session_safety(conn: Any, args: argparse.Namespace, text: Any) -> None:
    conn.execute(text(f"SET statement_timeout = {int(args.statement_timeout_ms)}"))
    conn.execute(text(f"SET lock_timeout = {int(args.lock_timeout_ms)}"))


def fetch_team_names(conn: Any, text: Any) -> set[str]:
    rows = conn.execute(text("SELECT team_name FROM public.mlb_teams WHERE team_name IS NOT NULL")).fetchall()
    return {norm_team(row[0]) for row in rows}


def fetch_batch(conn: Any, args: argparse.Namespace, text: Any, last_id: int) -> list[dict[str, Any]]:
    table = quote_ident(args.table)
    id_col = quote_ident(args.id_column)
    sql = text(
        f"""
        SELECT {id_col} AS id,
               api_game_id,
               odds_api_event_id,
               home_team,
               away_team,
               commence_time
        FROM public.{table}
        WHERE {id_col} > :last_id
          AND game_id IS NULL
        ORDER BY {id_col}
        LIMIT :batch_size
        """
    )
    return [dict(row) for row in conn.execute(sql, {"last_id": last_id, "batch_size": args.batch_size}).mappings()]


def compact_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("api_game_id"),
            row.get("odds_api_event_id"),
            row.get("home_team"),
            row.get("away_team"),
            parse_ts(row.get("commence_time")),
        )
        item = grouped.setdefault(
            key,
            {
                "api_game_id": row.get("api_game_id"),
                "odds_api_event_id": row.get("odds_api_event_id"),
                "home_team": row.get("home_team"),
                "away_team": row.get("away_team"),
                "commence_time": parse_ts(row.get("commence_time")),
                "row_count": 0,
                "first_id": int(row["id"]),
                "last_id": int(row["id"]),
            },
        )
        item["row_count"] += 1
        item["first_id"] = min(item["first_id"], int(row["id"]))
        item["last_id"] = max(item["last_id"], int(row["id"]))
    return list(grouped.values())


def fetch_schedule(conn: Any, text: Any, min_ts: datetime, max_ts: datetime) -> list[dict[str, Any]]:
    start = (min_ts - timedelta(days=1)).date()
    end = (max_ts + timedelta(days=1)).date()
    sql = text(
        """
        SELECT s.game_id,
               s.game_date,
               s.game_time_utc,
               ht.team_name AS home_team,
               at.team_name AS away_team
        FROM public.mlb_game_schedule s
        JOIN public.mlb_teams ht ON ht.team_id = s.home_team_id
        JOIN public.mlb_teams at ON at.team_id = s.away_team_id
        WHERE s.game_date BETWEEN :start_date AND :end_date
          AND s.status != 'Cancelled'
          AND s.game_time_utc IS NOT NULL
        """
    )
    out = []
    for row in conn.execute(sql, {"start_date": start, "end_date": end}).mappings():
        d = dict(row)
        d["game_time_utc"] = parse_ts(d.get("game_time_utc"))
        d["home_norm"] = norm_team(d.get("home_team"))
        d["away_norm"] = norm_team(d.get("away_team"))
        out.append(d)
    return out


def classify_event(event: dict[str, Any], schedule_rows: list[dict[str, Any]], known_teams: set[str], args: argparse.Namespace) -> EventFinding:
    commence = event["commence_time"]
    home_norm = norm_team(event.get("home_team"))
    away_norm = norm_team(event.get("away_team"))
    base = {
        "api_game_id": event.get("api_game_id"),
        "odds_api_event_id": event.get("odds_api_event_id"),
        "home_team": event.get("home_team"),
        "away_team": event.get("away_team"),
        "commence_time": commence.isoformat() if commence else None,
        "row_count": int(event["row_count"]),
        "first_id": int(event["first_id"]),
        "last_id": int(event["last_id"]),
    }

    if commence is None or not home_norm or not away_norm:
        return EventFinding(**base, category="missing_or_odd_team_names", notes="missing commence_time/home_team/away_team")
    if home_norm not in known_teams or away_norm not in known_teams:
        return EventFinding(**base, category="missing_or_odd_team_names", notes="home/away team not found in mlb_teams by normalized name")

    same_teams = [r for r in schedule_rows if r["home_norm"] == home_norm and r["away_norm"] == away_norm]
    same_day = [r for r in same_teams if r["game_time_utc"] and r["game_time_utc"].date() == commence.date()]
    adjacent = [r for r in same_teams if r["game_time_utc"] and abs((r["game_time_utc"].date() - commence.date()).days) <= 1]

    if not same_day:
        if adjacent:
            nearest = min(adjacent, key=lambda r: abs((r["game_time_utc"] - commence).total_seconds()))
            delta_min = abs((nearest["game_time_utc"] - commence).total_seconds()) / 60.0
            return EventFinding(
                **base,
                category="doubleheader_or_date_edge_case" if delta_min <= args.wide_hours * 60 else "no_same_team_schedule_same_day",
                nearest_game_id=int(nearest["game_id"]),
                nearest_game_time_utc=nearest["game_time_utc"].isoformat(),
                nearest_delta_minutes=round(delta_min, 2),
                same_day_candidate_count=0,
                adjacent_day_candidate_count=len(adjacent),
                notes="same teams only on adjacent date",
            )
        return EventFinding(**base, category="no_same_team_schedule_same_day", same_day_candidate_count=0, adjacent_day_candidate_count=0)

    nearest = min(same_day, key=lambda r: abs((r["game_time_utc"] - commence).total_seconds()))
    delta_min = abs((nearest["game_time_utc"] - commence).total_seconds()) / 60.0
    near_same_delta = [r for r in same_day if abs(abs((r["game_time_utc"] - commence).total_seconds()) / 60.0 - delta_min) <= 1.0]
    if len(near_same_delta) > 1:
        category = "ambiguous_candidates"
        notes = "multiple same-team same-day candidates with nearly identical deltas"
    elif len(same_day) > 1 and delta_min <= args.wide_hours * 60:
        category = "doubleheader_or_date_edge_case"
        notes = "multiple same-team same-day candidates; nearest is unique"
    elif delta_min <= args.same_day_hours * 60:
        category = "same_teams_nearest_within_1h"
        notes = ""
    elif delta_min <= args.wide_hours * 60:
        category = "same_teams_nearest_within_6h"
        notes = "strict 900s tolerance too narrow"
    else:
        category = "same_teams_nearest_over_6h"
        notes = "same teams same day but outside wide tolerance"

    return EventFinding(
        **base,
        category=category,
        nearest_game_id=int(nearest["game_id"]),
        nearest_game_time_utc=nearest["game_time_utc"].isoformat(),
        nearest_delta_minutes=round(delta_min, 2),
        same_day_candidate_count=len(same_day),
        adjacent_day_candidate_count=len(adjacent),
        notes=notes,
    )


def write_outputs(findings: list[EventFinding], output_dir: Path, examples_per_category: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = defaultdict(lambda: {"event_count": 0, "row_count": 0})
    for finding in findings:
        summary[finding.category]["event_count"] += 1
        summary[finding.category]["row_count"] += finding.row_count

    summary_rows = [
        {"category": category, **counts}
        for category, counts in sorted(summary.items(), key=lambda kv: (-kv[1]["row_count"], kv[0]))
    ]
    (output_dir / "summary.json").write_text(json.dumps(summary_rows, indent=2), encoding="utf-8")
    with (output_dir / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["category", "event_count", "row_count"])
        writer.writeheader()
        writer.writerows(summary_rows)

    fieldnames = list(asdict(findings[0]).keys()) if findings else list(EventFinding.__dataclass_fields__.keys())
    with (output_dir / "event_findings.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for finding in findings:
            writer.writerow(asdict(finding))

    examples: list[dict[str, Any]] = []
    per_cat = Counter()
    for finding in sorted(findings, key=lambda x: (-x.row_count, x.category)):
        if per_cat[finding.category] >= examples_per_category:
            continue
        examples.append(asdict(finding))
        per_cat[finding.category] += 1
    with (output_dir / "examples.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(examples)

    lines = ["# MLB CLV Unlinked Game Diagnostic", "", "## Summary", "", "| category | events | rows |", "|---|---:|---:|"]
    for row in summary_rows:
        lines.append(f"| {row['category']} | {row['event_count']} | {row['row_count']} |")
    lines.extend(["", "## Files", "", "- `summary.csv`", "- `event_findings.csv`", "- `examples.csv`", "- `summary.json`", ""])
    (output_dir / "diagnostic_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")
    create_engine, text = load_deps()
    engine = create_engine(db_url(args), pool_pre_ping=True)
    output_dir = Path(args.output_dir)

    all_findings: list[EventFinding] = []
    last_id = args.start_id
    batches = 0
    total_rows = 0
    started = time.monotonic()

    with engine.connect() as conn:
        set_session_safety(conn, args, text)
        known_teams = fetch_team_names(conn, text)
        while True:
            if args.max_batches and batches >= args.max_batches:
                break
            rows = fetch_batch(conn, args, text, last_id)
            if not rows:
                break
            batches += 1
            last_id = max(int(row["id"]) for row in rows)
            total_rows += len(rows)
            events = compact_events(rows)
            event_times = [e["commence_time"] for e in events if e["commence_time"] is not None]
            if not event_times:
                schedule = []
            else:
                schedule = fetch_schedule(conn, text, min(event_times), max(event_times))
            findings = [classify_event(e, schedule, known_teams, args) for e in events]
            all_findings.extend(findings)
            batch_counts = Counter()
            for f in findings:
                batch_counts[f.category] += f.row_count
            logger.info(
                "Batch %d: selected_rows=%d compact_events=%d max_id=%d categories=%s",
                batches,
                len(rows),
                len(events),
                last_id,
                dict(batch_counts),
            )

    write_outputs(all_findings, output_dir, args.examples_per_category)
    logger.info(
        "Finished diagnostic: batches=%d selected_rows=%d event_findings=%d last_id=%d seconds=%.1f output_dir=%s",
        batches,
        total_rows,
        len(all_findings),
        last_id,
        time.monotonic() - started,
        output_dir,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
