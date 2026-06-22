#!/usr/bin/env python3
"""Resolve remaining local dense CLV batter_hits player_id gaps by normalized names.

Local-only helper for 2026-05-18..2026-06-21 readiness cleanup.
"""
from __future__ import annotations

import os
import re
import unicodedata

import psycopg2
from dotenv import load_dotenv

START_TS = "2026-05-18 00:00:00+00"
END_TS = "2026-06-22 00:00:00+00"
MANUAL = {
    "Max Muncy": (571970, "Max Muncy"),
    "Max Muncy (2002)": (691777, "Max Muncy"),
    "Will Smith": (669257, "Will Smith"),
    "Jose Fermin": (665877, "José Fermín"),
    "Rafael Flores": (804668, "Rafael Flores Jr."),
}

def norm(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", s.lower())

def main() -> int:
    load_dotenv()
    conn = psycopg2.connect(os.environ["LOCAL_DATABASE_URL"], connect_timeout=15, options="-c statement_timeout=180000")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mlb_players (player_id, player_name, primary_position, bats, throws, active)
                VALUES (804668, 'Rafael Flores Jr.', 'C', 'R', 'R', true)
                ON CONFLICT (player_id) DO NOTHING
            """)
            cur.execute("SELECT player_id, player_name FROM mlb_players WHERE player_id IS NOT NULL AND player_name IS NOT NULL")
            by_norm: dict[str, list[tuple[int, str]]] = {}
            for pid, name in cur.fetchall():
                by_norm.setdefault(norm(name), []).append((pid, name))
            cur.execute("""
                SELECT api_player_name, count(*)
                FROM mlb_player_props_clv_snapshots
                WHERE market_key='batter_hits'
                  AND requested_snapshot_time >= %s::timestamptz
                  AND requested_snapshot_time < %s::timestamptz
                  AND player_id IS NULL
                  AND api_player_name IS NOT NULL
                GROUP BY api_player_name
                ORDER BY count(*) DESC
            """, (START_TS, END_TS))
            names = cur.fetchall()
            mappings: list[tuple[str, int, str]] = []
            unresolved: list[tuple[str, int, list[tuple[int, str]]]] = []
            for api_name, rows in names:
                if api_name in MANUAL:
                    pid, linked = MANUAL[api_name]
                    mappings.append((api_name, pid, linked))
                    continue
                candidates = by_norm.get(norm(api_name), [])
                if len(candidates) == 1:
                    pid, linked = candidates[0]
                    mappings.append((api_name, pid, linked))
                else:
                    unresolved.append((api_name, rows, candidates))
            print(f"candidate_mappings={len(mappings)} unresolved_names={len(unresolved)}")
            for item in unresolved:
                print("UNRESOLVED", item)
            total = 0
            for api_name, pid, linked in mappings:
                cur.execute("""
                    UPDATE mlb_player_props_clv_snapshots
                       SET player_id=%s,
                           linked_player_name=%s,
                           player_link_method='manual_or_unaccent_alias:dense_clv_residual:2026-06-21',
                           linked_at=now()
                     WHERE market_key='batter_hits'
                       AND requested_snapshot_time >= %s::timestamptz
                       AND requested_snapshot_time < %s::timestamptz
                       AND player_id IS NULL
                       AND api_player_name=%s
                """, (pid, linked, START_TS, END_TS, api_name))
                if cur.rowcount:
                    print(f"{api_name} -> {pid}:{linked} updated={cur.rowcount}")
                    total += cur.rowcount
            conn.commit()
            print(f"total_updated={total}")
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    raise SystemExit(main())
