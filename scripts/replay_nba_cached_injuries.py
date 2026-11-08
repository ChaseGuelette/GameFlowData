#!/usr/bin/env python
"""Replay an explicit injury manifest from retained PDFs; plan-only by default."""
from __future__ import annotations

import argparse
import io
import json
import sys
from collections import defaultdict
from contextlib import redirect_stdout
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import recover_nba_injury_history as recovery  # noqa: E402

DEFAULT_MANIFEST = ROOT / "logs/receipts/nba_training_input_acceptance_20261107/calibration_cached_replay_manifest.json"
DEFAULT_STATE = ROOT / "logs/receipts/injury_calibration_cached_replay"


def build_batches(manifest: Path, destination: Path, execute: bool, mirror: bool) -> list[dict]:
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if payload.get("version") != 1 or not payload.get("games"):
        raise ValueError("Expected a nonempty version-1 game manifest")
    groups = defaultdict(list)
    seen = set()
    for item in payload["games"]:
        game_id = item["game_id"]
        if not recovery.is_competitive_nba_game_id(game_id) or game_id in seen:
            raise ValueError(f"Invalid or duplicate game ID: {game_id}")
        seen.add(game_id)
        source = Path(item["state_file"])
        source = source.resolve() if source.is_absolute() else (ROOT / source).resolve()
        groups[source].append(item)
    batches = []
    for source, items in sorted(groups.items()):
        retained = json.loads(source.read_text(encoding="utf-8"))
        scope = retained["scope"]
        items.sort(key=lambda item: (item["game_date"], item["game_id"]))
        for offset in range(0, len(items), 10):
            selected = items[offset:offset + 10]
            argv = ["--start-date", scope["start_date"], "--end-date", scope["end_date"],
                    "--state-file", str(destination / "pending.json"), "--max-games", "10", "--cache-only"]
            for game in selected:
                argv.extend(["--game-id", game["game_id"]])
            flag, key = ("--report-url", "report_urls") if scope["report_urls"] else ("--report-slot", "report_slots")
            for value in scope[key]:
                argv.extend([flag, value])
            if execute:
                argv.append("--execute")
            if mirror:
                argv.append("--mirror-local")
            args = recovery._parse_args(argv)
            seed = recovery._new_state(args)
            for game in selected:
                original = retained["games"].get(game["game_id"])
                if original is None or original["game_date"] != game["game_date"]:
                    raise ValueError("Manifest identity does not match retained checkpoint")
                game_date = date.fromisoformat(game["game_date"])
                if not args.start_date <= game_date <= args.end_date:
                    raise ValueError("Game date lies outside retained window")
                for url in recovery._candidate_urls(args, game_date):
                    cached = retained["report_cache"].get(url)
                    if not cached or cached.get("status") != "parsed":
                        raise ValueError(f"Parsed report missing for {game['game_id']}; refusing download")
                    recovery._report_from_dict(cached["report"])
                    seed["report_cache"][url] = cached
            state = destination / f"{scope['start_date']}_{seed['fingerprint'][:16]}.json"
            if state.resolve() == source:
                raise ValueError("Replay must not overwrite original checkpoints")
            argv[argv.index("--state-file") + 1] = str(state)
            batches.append({"state": state, "argv": argv, "seed": seed,
                            "game_ids": [item["game_id"] for item in selected]})
    return batches


def run_batches(batches: list[dict], destination: Path) -> dict:
    results = []
    # One launcher owns the separate replay directory; original states are read-only.
    with recovery._state_lock(destination / "launcher.json"):
        for index, batch in enumerate(batches, 1):
            state = batch["state"]
            receipt_path = state.with_suffix(".receipt.json")
            if receipt_path.exists():
                result = json.loads(receipt_path.read_text(encoding="utf-8"))
                if result["fingerprint"] != batch["seed"]["fingerprint"] or not state.exists():
                    raise RuntimeError("Replay receipt/state mismatch")
                print(f"batch {index}/{len(batches)} already accounted; skipped", flush=True)
            else:
                if not state.exists():
                    recovery._atomic_json(state, batch["seed"])
                captured = io.StringIO()
                with redirect_stdout(captured):
                    code = recovery.main([*batch["argv"], "--resume"])
                receipt = json.loads(captured.getvalue())
                if code not in (0, 4) or not receipt.get("scope_accounted") or receipt.get("remaining_due_to_budget"):
                    raise RuntimeError(f"Replay stopped; inspect {state}: {receipt}")
                saved = json.loads(state.read_text(encoding="utf-8"))
                outcomes = list(saved["games"].values())
                result = {"fingerprint": saved["fingerprint"], "games": len(outcomes),
                          "blocked_or_errors": receipt["blocked_or_errors"],
                          "remote_changed": sum(g.get("changed", 0) for g in outcomes),
                          "remote_verified": sum(g.get("verified", 0) for g in outcomes),
                          "local_changed": sum(g.get("mirror", {}).get("changed", 0) for g in outcomes),
                          "local_verified": sum(g.get("mirror", {}).get("verified", 0) for g in outcomes)}
                recovery._atomic_json(receipt_path, result)
                print(f"batch {index}/{len(batches)} games={result['games']} "
                      f"remote_verified={result['remote_verified']} local_verified={result['local_verified']} "
                      f"gaps={result['blocked_or_errors']}", flush=True)
            results.append(result)
    return {"mode": "execute", "cache_only": True, "completeness_claimed": False,
            "batches": len(results), "has_gaps": any(r["blocked_or_errors"] for r in results),
            **{key: sum(r[key] for r in results) for key in
               ("games", "remote_changed", "remote_verified", "local_changed", "local_verified")}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--state-directory", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--mirror-local", action="store_true")
    args = parser.parse_args(argv)
    if args.mirror_local and not args.execute:
        parser.error("--mirror-local requires --execute")
    batches = build_batches(args.manifest, args.state_directory, args.execute, args.mirror_local)
    if not args.execute:
        print(json.dumps({"mode": "plan_only", "games": sum(len(b['game_ids']) for b in batches),
                          "batches": len(batches), "cache_only": True, "database_calls": 0}, indent=2))
        return 0
    result = run_batches(batches, args.state_directory)
    recovery._atomic_json(args.state_directory / "summary.json", result)
    print(json.dumps(result, indent=2))
    return 4 if result["has_gaps"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
