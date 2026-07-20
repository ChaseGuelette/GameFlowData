#!/usr/bin/env python3
"""Temporary helper: sync dense CLV snapshots remote -> local by ID range.

Avoids sync_local_db.py's slow timestamp min/max aggregate on the dense table.
"""
from __future__ import annotations

import argparse
import os
import tempfile
import time

import psycopg2
from dotenv import load_dotenv


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-id", type=int, required=True)
    parser.add_argument("--end-id", type=int, required=True)
    parser.add_argument("--market-key", required=True)
    parser.add_argument("--table", default="mlb_player_props_clv_snapshots")
    args = parser.parse_args()

    remote_url = os.environ["DATABASE_URL"]
    local_url = os.getenv("LOCAL_DATABASE_URL_AGENT") or os.environ["LOCAL_DATABASE_URL"]
    table = args.table
    t0 = time.time()

    remote = psycopg2.connect(
        remote_url,
        connect_timeout=15,
        application_name="gameflow:manual_clv_id_sync:remote",
        options="-c statement_timeout=1800000 -c idle_in_transaction_session_timeout=60000",
    )
    local = psycopg2.connect(
        local_url,
        connect_timeout=15,
        application_name="gameflow:manual_clv_id_sync:local",
    )
    try:
        with remote.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE id >= %s AND id <= %s AND market_key = %s',
                (args.start_id, args.end_id, args.market_key),
            )
            count = cur.fetchone()[0]
        print(f"Remote rows in id window ({args.start_id}, {args.end_id}]: {count:,}")
        if count == 0:
            return 0

        with tempfile.TemporaryFile(mode="w+b") as tmp:
            with remote.cursor() as cur:
                copy_out = cur.mogrify(
                    f'COPY (SELECT * FROM "{table}" WHERE id >= %s AND id <= %s AND market_key = %s ORDER BY id) TO STDOUT WITH CSV HEADER',
                    (args.start_id, args.end_id, args.market_key),
                ).decode("utf-8")
                print("Exporting remote rows...")
                cur.copy_expert(copy_out, tmp)

            size_mb = tmp.tell() / (1024 * 1024)
            print(f"Export complete: {size_mb:.1f} MB")
            tmp.seek(0)
            header = tmp.readline().decode("utf-8").strip()
            cols = [c.strip('"') for c in header.split(",")]
            tmp.seek(0)

            with local.cursor() as cur:
                temp_table = f"_sync_{table}_{int(time.time() * 1000)}"
                print("Creating local staging table...")
                cur.execute(f'CREATE TEMP TABLE "{temp_table}" ON COMMIT DROP AS SELECT * FROM "{table}" WHERE false')
                col_list = ", ".join(f'"{c}"' for c in cols)
                print("Importing to local staging...")
                cur.copy_expert(f'COPY "{temp_table}" ({col_list}) FROM STDIN WITH CSV HEADER', tmp)
                update_cols = [c for c in cols if c != "id"]
                update_sql = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
                print("Upserting into local table...")
                cur.execute(
                    f'INSERT INTO "{table}" ({col_list}) OVERRIDING SYSTEM VALUE '
                    f'SELECT {col_list} FROM "{temp_table}" '
                    f'ON CONFLICT ("id") DO UPDATE SET {update_sql}'
                )
            local.commit()
        print(f"Done in {time.time() - t0:.1f}s")
        return 0
    finally:
        remote.close()
        local.close()


if __name__ == "__main__":
    raise SystemExit(main())
