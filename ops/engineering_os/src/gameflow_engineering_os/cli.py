from __future__ import annotations

import argparse
import json
import sys

from gameflow_engineering_os.config import load_config
from gameflow_engineering_os.render import generate_brief
from gameflow_engineering_os.runner import collect_all
from gameflow_engineering_os.state import store_for_config, worst_status


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gfos")
    parser.add_argument("--config", help="Path to engineering_os.yaml")
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check")
    check.add_argument("--json", action="store_true")
    sub.add_parser("collect")
    brief = sub.add_parser("brief")
    group = brief.add_mutually_exclusive_group(required=True)
    group.add_argument("--stdout", action="store_true")
    group.add_argument("--generate", action="store_true")
    sub.add_parser("events")
    sub.add_parser("status")
    sub.add_parser("serve")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    config.ensure_runtime_dirs()
    store = store_for_config(config)
    if args.command in {"check", "collect"}:
        results = collect_all(config)
        store.persist_results(results, config)
        if args.command == "check" and args.json:
            print(json.dumps([r.model_dump(mode="json") for r in results], indent=2, sort_keys=True))
        elif args.command == "collect":
            print(f"collected {len(results)} checks")
        return 0
    if args.command == "brief":
        if args.generate:
            results = collect_all(config)
            store.persist_results(results, config)
            brief = generate_brief(results, config)
            store.save_brief(brief, config.daily_brief.retain_days)
            print(brief.text)
            return 0
        brief = store.latest_brief()
        if not brief:
            print("no persisted brief", file=sys.stderr)
            return 2
        print(brief.text)
        return 0
    if args.command == "events":
        for event in store.events():
            print(f"{event.created_at.isoformat()} {event.transition_type} {event.check_id} {event.status.value}: {event.summary}")
        return 0
    if args.command == "status":
        results = store.latest_results()
        if not results:
            print("no persisted checks")
            return 2
        print(f"{worst_status([r.status for r in results]).value}: {len(results)} checks")
        for item in results:
            print(f"{item.status.value:14} {item.check_id} - {item.summary}")
        return 0
    if args.command == "serve":
        import uvicorn

        from gameflow_engineering_os.web.app import create_app

        uvicorn.run(create_app(config), host=config.web.bind_host, port=config.web.bind_port)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
