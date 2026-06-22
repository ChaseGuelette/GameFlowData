#!/usr/bin/env python3
"""Chunked remote/local manual player mapping for dense CLV snapshots.

Default is dry-run. Use --execute only after reviewing dry-run chunk counts.
"""
from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass

import psycopg2
from dotenv import load_dotenv


@dataclass(frozen=True)
class Mapping:
    api_name: str
    player_id: int
    linked_name: str

MAPPINGS: tuple[Mapping, ...] = (
    Mapping("Jeremy Pena", 665161, "Jeremy Peña"), Mapping("Julio Rodriguez", 677594, "Julio Rodríguez"),
    Mapping("Yandy Diaz", 650490, "Yandy Díaz"), Mapping("Ivan Herrera", 671056, "Iván Herrera"),
    Mapping("Eugenio Suarez", 553993, "Eugenio Suárez"), Mapping("Mauricio Dubon", 643289, "Mauricio Dubón"),
    Mapping("Jose Caballero", 676609, "José Caballero"), Mapping("Nasim Nunez", 683083, "Nasim Nuñez"),
    Mapping("Luis Garcia Jr.", 671277, "Luis García Jr."), Mapping("Max Muncy", 571970, "Max Muncy"),
    Mapping("Christian Vazquez", 543877, "Christian Vázquez"), Mapping("Andres Gimenez", 665926, "Andrés Giménez"),
    Mapping("Jose Ramirez", 608070, "José Ramírez"), Mapping("Angel Martinez", 682657, "Angel Martínez"),
    Mapping("Yohendrick Pinango", 682818, "Yohendrick Piñango"), Mapping("Jesus Sanchez", 660821, "Jesús Sánchez"),
    Mapping("Adolis Garcia", 666969, "Adolis García"), Mapping("Ronald Acuna Jr.", 660670, "Ronald Acuña Jr."),
    Mapping("Endy Rodriguez", 682848, "Endy Rodríguez"), Mapping("Will Smith", 669257, "Will Smith"),
    Mapping("Wenceel Perez", 672761, "Wenceel Pérez"), Mapping("Heriberto Hernandez", 681715, "Heriberto Hernández"),
    Mapping("Rodolfo Duran", 660710, "Rodolfo Durán"), Mapping("Luisangel Acuna", 682668, "Luisangel Acuña"),
    Mapping("Carlos Narvaez", 665966, "Carlos Narváez"), Mapping("Jose Fermin", 665877, "José Fermín"),
    Mapping("Jose Tena", 677588, "José Tena"), Mapping("Max Muncy (2002)", 691777, "Max Muncy"),
    Mapping("Ramon Laureano", 657656, "Ramón Laureano"), Mapping("Leo Jimenez", 677870, "Leo Jiménez"),
    Mapping("Pedro Pages", 686780, "Pedro Pagés"), Mapping("Ali Sanchez", 645305, "Ali Sánchez"),
    Mapping("Elias Diaz", 553869, "Elias Díaz"), Mapping("Gary Sanchez", 596142, "Gary Sánchez"),
    Mapping("Nelson Velazquez", 676369, "Nelson Velázquez"), Mapping("Pedro Ramirez", 699393, "Pedro Ramírez"),
    Mapping("Teoscar Hernandez", 606192, "Teoscar Hernández"), Mapping("Sebastian Rivero", 665861, "Sebastián Rivero"),
    Mapping("Kevin Alcantara", 682634, "Kevin Alcántara"), Mapping("Cesar Salazar", 663967, "César Salazar"),
    Mapping("Rafael Marchan", 665561, "Rafael Marchán"), Mapping("Enrique Hernandez", 571771, "Enrique Hernández"),
    Mapping("Vidal Brujan", 660644, "Vidal Bruján"), Mapping("Rafael Flores", 804668, "Rafael Flores Jr."),
    Mapping("Elieser Hernández", 622694, "Elieser Hernandez"),
)

def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument('--start-id', type=int, default=4980677)
    ap.add_argument('--end-id', type=int, default=7440801)
    ap.add_argument('--chunk-size', type=int, default=100000)
    ap.add_argument('--remote', action='store_true')
    ap.add_argument('--execute', action='store_true')
    ap.add_argument('--sleep-seconds', type=float, default=0.1)
    args=ap.parse_args()
    load_dotenv()
    env_key='DATABASE_URL' if args.remote else 'LOCAL_DATABASE_URL'
    conn=psycopg2.connect(os.environ[env_key], connect_timeout=15, application_name='gameflow:manual_clv_mapping_chunked', options='-c statement_timeout=180000 -c lock_timeout=10000')
    values_sql=','.join(['(%s,%s,%s)']*len(MAPPINGS))
    params=[]
    for m in MAPPINGS:
        params.extend([m.api_name,m.player_id,m.linked_name])
    total=0
    try:
        with conn.cursor() as cur:
            # Needed for Rafael Flores. Harmless/no-op if already present.
            if args.execute:
                cur.execute("""
                  INSERT INTO mlb_players (player_id, player_name, primary_position, bats, throws, active)
                  VALUES (804668, 'Rafael Flores Jr.', 'C', 'R', 'R', true)
                  ON CONFLICT (player_id) DO NOTHING
                """)
            lo=args.start_id
            while lo < args.end_id:
                hi=min(lo+args.chunk_size, args.end_id)
                if args.execute:
                    sql=f"""
                    WITH manual_map(api_player_name, player_id, linked_name) AS (VALUES {values_sql})
                    UPDATE mlb_player_props_clv_snapshots c
                       SET player_id=m.player_id, linked_player_name=m.linked_name,
                           player_link_method='manual_alias:dense_clv:2026-06-21', linked_at=now()
                      FROM manual_map m
                     WHERE c.api_player_name=m.api_player_name
                       AND c.id > %s AND c.id <= %s
                       AND c.market_key='batter_hits'
                       AND c.player_id IS NULL
                    """
                    cur.execute(sql, [*params, lo, hi])
                    n=cur.rowcount
                    conn.commit()
                else:
                    sql=f"""
                    WITH manual_map(api_player_name, player_id, linked_name) AS (VALUES {values_sql})
                    SELECT count(*)
                      FROM mlb_player_props_clv_snapshots c
                      JOIN manual_map m ON c.api_player_name=m.api_player_name
                     WHERE c.id > %s AND c.id <= %s
                       AND c.market_key='batter_hits'
                       AND c.player_id IS NULL
                    """
                    cur.execute(sql, [*params, lo, hi])
                    n=cur.fetchone()[0]
                    conn.rollback()
                total += n
                print(f"chunk ({lo},{hi}] {'updated' if args.execute else 'would_update'}={n:,}")
                lo=hi
                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)
        print(f"target={env_key} execute={args.execute} total={total:,}")
        return 0
    finally:
        conn.close()
if __name__ == '__main__':
    raise SystemExit(main())
