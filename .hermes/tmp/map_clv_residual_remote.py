#!/usr/bin/env python3
"""Chunked remote/local cleanup for residual dense CLV batter_hits links.

Maps remaining player aliases by explicit MLBAM ids and the one known residual game event.
Default dry-run; --execute writes.
"""
from __future__ import annotations

import argparse
import os
import time

import psycopg2
from dotenv import load_dotenv

PLAYER_MAP = {
    "Ivan Herrera": (671056, "Iván Herrera"), "Julio Rodriguez": (677594, "Julio Rodríguez"),
    "Jeremy Pena": (665161, "Jeremy Peña"), "Mauricio Dubon": (643289, "Mauricio Dubón"),
    "Nasim Nunez": (683083, "Nasim Nuñez"), "Andres Gimenez": (665926, "Andrés Giménez"),
    "Eugenio Suarez": (553993, "Eugenio Suárez"), "Luis Garcia Jr.": (671277, "Luis García Jr."),
    "Christian Vazquez": (543877, "Christian Vázquez"), "Max Muncy": (571970, "Max Muncy"),
    "Yohendrick Pinango": (682818, "Yohendrick Piñango"), "Jesus Sanchez": (660821, "Jesús Sánchez"),
    "Yandy Diaz": (650490, "Yandy Díaz"), "Jose Ramirez": (608070, "José Ramírez"),
    "Ronald Acuna Jr.": (660670, "Ronald Acuña Jr."), "Adolis Garcia": (666969, "Adolis García"),
    "Angel Martinez": (682657, "Angel Martínez"), "Jose Caballero": (676609, "José Caballero"),
    "Endy Rodriguez": (682848, "Endy Rodríguez"), "Will Smith": (669257, "Will Smith"),
    "Wenceel Perez": (672761, "Wenceel Pérez"), "Jose Tena": (677588, "José Tena"),
    "Luisangel Acuna": (682668, "Luisangel Acuña"), "Jose Fermin": (665877, "José Fermín"),
    "Heriberto Hernandez": (681715, "Heriberto Hernández"), "Pedro Pages": (686780, "Pedro Pagés"),
    "Carlos Narvaez": (665966, "Carlos Narváez"), "Rodolfo Duran": (660710, "Rodolfo Durán"),
    "Elias Diaz": (553869, "Elias Díaz"), "Ramon Laureano": (657656, "Ramón Laureano"),
    "Teoscar Hernandez": (606192, "Teoscar Hernández"), "Sebastian Rivero": (665861, "Sebastián Rivero"),
    "Pedro Ramirez": (699393, "Pedro Ramírez"), "Leo Jimenez": (677870, "Leo Jiménez"),
    "Max Muncy (2002)": (691777, "Max Muncy"), "Gary Sanchez": (596142, "Gary Sánchez"),
    "Vidal Brujan": (660644, "Vidal Bruján"), "Nelson Velazquez": (676369, "Nelson Velázquez"),
    "Cesar Salazar": (663967, "César Salazar"), "Ali Sanchez": (645305, "Ali Sánchez"),
    "Kevin Alcantara": (682634, "Kevin Alcántara"), "Rafael Marchan": (665561, "Rafael Marchán"),
    "Elieser Hernández": (622694, "Elieser Hernandez"), "Esmerlyn Valdez": (699013, "Esmerlyn Valdez"),
    "Enrique Hernandez": (571771, "Enrique Hernández"), "Rafael Flores": (804668, "Rafael Flores Jr."),
    "Cooper Pratt": (806198, "Cooper Pratt"), "Nick Morabito": (703492, "Nick Morabito"),
    "Kyler Fedko": (693459, "Kyler Fedko"), "Yoan Moncada": (660162, "Yoán Moncada"),
}


def run() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--remote', action='store_true')
    ap.add_argument('--execute', action='store_true')
    ap.add_argument('--start-id', type=int, default=3017119)
    ap.add_argument('--end-id', type=int, default=4980677)
    ap.add_argument('--chunk-size', type=int, default=100000)
    ap.add_argument('--sleep-seconds', type=float, default=0.1)
    args = ap.parse_args()

    load_dotenv()
    env = 'DATABASE_URL' if args.remote else 'LOCAL_DATABASE_URL'
    conn = psycopg2.connect(
        os.environ[env],
        connect_timeout=15,
        application_name='gameflow:clv_residual_map',
        options='-c statement_timeout=180000 -c lock_timeout=10000',
    )
    values = ','.join(['(%s,%s,%s)'] * len(PLAYER_MAP))
    params = []
    for name, (pid, linked) in PLAYER_MAP.items():
        params.extend([name, pid, linked])

    total_player = 0
    total_game = 0
    try:
        with conn.cursor() as cur:
            if args.execute:
                cur.execute("""INSERT INTO mlb_players (player_id, player_name, primary_position, bats, throws, active)
                         VALUES (804668, 'Rafael Flores Jr.', 'C', 'R', 'R', true) ON CONFLICT (player_id) DO NOTHING""")
            lo = args.start_id
            while lo < args.end_id:
                hi = min(lo + args.chunk_size, args.end_id)
                if args.execute:
                    cur.execute(f"""WITH manual_map(api_player_name, player_id, linked_name) AS (VALUES {values})
              UPDATE mlb_player_props_clv_snapshots c
                 SET player_id=m.player_id, linked_player_name=m.linked_name,
                     player_link_method='manual_or_unaccent_alias:dense_clv_residual:2026-06-21', linked_at=now()
                FROM manual_map m
               WHERE c.api_player_name=m.api_player_name AND c.id>%s AND c.id<=%s AND c.market_key='batter_hits' AND c.player_id IS NULL""", [*params, lo, hi])
                    p = cur.rowcount
                    cur.execute("""UPDATE mlb_player_props_clv_snapshots
                              SET game_id=824516, game_link_method='manual_unique_team_date:dense_clv_residual:2026-06-21', linked_at=now()
                            WHERE id>%s AND id<=%s AND market_key='batter_hits' AND game_id IS NULL
                              AND api_game_id='8b80dbee1665ae5e68b02b12836273c5'
                              AND home_team='Cincinnati Reds' AND away_team='St. Louis Cardinals'
                              AND commence_time=%s::timestamptz""", (lo, hi, '2026-05-23 17:11:00+00'))
                    g = cur.rowcount
                    conn.commit()
                else:
                    cur.execute(f"""WITH manual_map(api_player_name, player_id, linked_name) AS (VALUES {values})
              SELECT count(*) FROM mlb_player_props_clv_snapshots c JOIN manual_map m ON c.api_player_name=m.api_player_name
               WHERE c.id>%s AND c.id<=%s AND c.market_key='batter_hits' AND c.player_id IS NULL""", [*params, lo, hi])
                    p = cur.fetchone()[0]
                    cur.execute("""SELECT count(*) FROM mlb_player_props_clv_snapshots
                            WHERE id>%s AND id<=%s AND market_key='batter_hits' AND game_id IS NULL
                              AND api_game_id='8b80dbee1665ae5e68b02b12836273c5'
                              AND home_team='Cincinnati Reds' AND away_team='St. Louis Cardinals'
                              AND commence_time=%s::timestamptz""", (lo, hi, '2026-05-23 17:11:00+00'))
                    g = cur.fetchone()[0]
                    conn.rollback()
                total_player += p
                total_game += g
                print(f"chunk ({lo},{hi}] player_{'updated' if args.execute else 'would_update'}={p:,} game_{'updated' if args.execute else 'would_update'}={g:,}")
                lo = hi
                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)
        print(f"target={env} execute={args.execute} total_player={total_player:,} total_game={total_game:,}")
        return 0
    finally:
        conn.close()


if __name__ == '__main__':
    raise SystemExit(run())
