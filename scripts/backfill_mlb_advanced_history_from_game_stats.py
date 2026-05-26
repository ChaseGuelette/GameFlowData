#!/usr/bin/env python3
"""Backfill mlb_player_season_advanced_history from GameFlow MLB game stats.

FanGraphs/pybaseball can be blocked by Cloudflare (403). This script provides a
production-safe fallback that keeps the point-in-time history table populated
from GameFlow's own batting/pitching game logs and Statcast rolling averages.

Rows are season-to-date snapshots as of a date. Feature stores use
`as_of_date < game_date`, so for same-day inference run this with yesterday's
date (or daily historical snapshot dates for backtests).
"""

from __future__ import annotations

import argparse
from datetime import date, datetime

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BATTER_SQL = text(
    """
    WITH batting AS (
        SELECT
            player_id,
            season,
            SUM(pa)::float AS pa,
            SUM(ab)::float AS ab,
            SUM(h)::float AS h,
            SUM(doubles)::float AS doubles,
            SUM(triples)::float AS triples,
            SUM(hr)::float AS hr,
            SUM(bb)::float AS bb,
            SUM(so)::float AS so,
            SUM(hbp)::float AS hbp,
            SUM(sf)::float AS sf,
            SUM(tb)::float AS tb
        FROM mlb_player_game_stats_batting
        WHERE season = :season
          AND game_date <= :as_of_date
          AND COALESCE(did_not_play, FALSE) = FALSE
        GROUP BY player_id, season
        HAVING SUM(pa) >= :min_pa
    ), league AS (
        SELECT
            SUM(pa)::float AS pa,
            SUM(ab)::float AS ab,
            SUM(h)::float AS h,
            SUM(doubles)::float AS doubles,
            SUM(triples)::float AS triples,
            SUM(hr)::float AS hr,
            SUM(bb)::float AS bb,
            SUM(so)::float AS so,
            SUM(hbp)::float AS hbp,
            SUM(sf)::float AS sf,
            SUM(tb)::float AS tb
        FROM mlb_player_game_stats_batting
        WHERE season = :season
          AND game_date <= :as_of_date
          AND COALESCE(did_not_play, FALSE) = FALSE
    ), league_woba AS (
        SELECT
            CASE WHEN (ab + bb - sf + hbp) > 0 THEN
                (0.69 * bb + 0.72 * hbp + 0.89 * (h - doubles - triples - hr)
                 + 1.27 * doubles + 1.62 * triples + 2.10 * hr)
                / NULLIF((ab + bb - sf + hbp), 0)
            END AS woba
        FROM league
    ), latest_statcast AS (
        SELECT DISTINCT ON (player_id)
            player_id,
            avg_woba_szn,
            avg_hard_hit_pct_szn
        FROM mlb_player_average_statcast_batting
        WHERE season = :season
          AND game_date <= :as_of_date
        ORDER BY player_id, game_date DESC
    )
    INSERT INTO mlb_player_season_advanced_history (
        player_id, season, player_type, as_of_date,
        war, babip, wrc_plus, woba, iso, bb_pct, k_pct, hard_pct,
        avg, obp, slg, ops, pa, fangraphs_id
    )
    SELECT
        b.player_id,
        b.season,
        'batter',
        CAST(:as_of_date AS date),
        NULL::float AS war,
        CASE WHEN (b.ab - b.so - b.hr + b.sf) > 0
             THEN (b.h - b.hr) / NULLIF((b.ab - b.so - b.hr + b.sf), 0)
        END AS babip,
        CASE
            WHEN lw.woba > 0 THEN 100.0 * COALESCE(
                sc.avg_woba_szn::float,
                (0.69 * b.bb + 0.72 * b.hbp + 0.89 * (b.h - b.doubles - b.triples - b.hr)
                 + 1.27 * b.doubles + 1.62 * b.triples + 2.10 * b.hr)
                / NULLIF((b.ab + b.bb - b.sf + b.hbp), 0)
            ) / lw.woba
        END AS wrc_plus,
        COALESCE(
            sc.avg_woba_szn::float,
            (0.69 * b.bb + 0.72 * b.hbp + 0.89 * (b.h - b.doubles - b.triples - b.hr)
             + 1.27 * b.doubles + 1.62 * b.triples + 2.10 * b.hr)
            / NULLIF((b.ab + b.bb - b.sf + b.hbp), 0)
        ) AS woba,
        CASE WHEN b.ab > 0 THEN (b.tb - b.h) / NULLIF(b.ab, 0) END AS iso,
        CASE WHEN b.pa > 0 THEN b.bb / NULLIF(b.pa, 0) END AS bb_pct,
        CASE WHEN b.pa > 0 THEN b.so / NULLIF(b.pa, 0) END AS k_pct,
        sc.avg_hard_hit_pct_szn::float AS hard_pct,
        CASE WHEN b.ab > 0 THEN b.h / NULLIF(b.ab, 0) END AS avg,
        CASE WHEN (b.ab + b.bb + b.hbp + b.sf) > 0
             THEN (b.h + b.bb + b.hbp) / NULLIF((b.ab + b.bb + b.hbp + b.sf), 0)
        END AS obp,
        CASE WHEN b.ab > 0 THEN b.tb / NULLIF(b.ab, 0) END AS slg,
        CASE WHEN b.ab > 0 AND (b.ab + b.bb + b.hbp + b.sf) > 0
             THEN (b.h + b.bb + b.hbp) / NULLIF((b.ab + b.bb + b.hbp + b.sf), 0)
                  + b.tb / NULLIF(b.ab, 0)
        END AS ops,
        b.pa::int,
        NULL::int AS fangraphs_id
    FROM batting b
    CROSS JOIN league_woba lw
    LEFT JOIN latest_statcast sc ON sc.player_id = b.player_id
    ON CONFLICT (player_id, season, player_type, as_of_date) DO UPDATE SET
        babip = EXCLUDED.babip,
        wrc_plus = EXCLUDED.wrc_plus,
        woba = EXCLUDED.woba,
        iso = EXCLUDED.iso,
        bb_pct = EXCLUDED.bb_pct,
        k_pct = EXCLUDED.k_pct,
        hard_pct = EXCLUDED.hard_pct,
        avg = EXCLUDED.avg,
        obp = EXCLUDED.obp,
        slg = EXCLUDED.slg,
        ops = EXCLUDED.ops,
        pa = EXCLUDED.pa,
        scraped_at = now()
    """
)

PITCHER_SQL = text(
    """
    WITH pitching AS (
        SELECT
            player_id,
            season,
            SUM(ip)::float AS ip,
            SUM(outs_recorded)::float AS outs_recorded,
            SUM(h_allowed)::float AS h_allowed,
            SUM(r_allowed)::float AS r_allowed,
            SUM(er)::float AS er,
            SUM(bb)::float AS bb,
            SUM(so)::float AS so,
            SUM(hr_allowed)::float AS hr_allowed
        FROM mlb_player_game_stats_pitching
        WHERE season = :season
          AND game_date <= :as_of_date
          AND COALESCE(did_not_play, FALSE) = FALSE
        GROUP BY player_id, season
        HAVING SUM(ip) >= :min_ip
    )
    INSERT INTO mlb_player_season_advanced_history (
        player_id, season, player_type, as_of_date,
        war, babip, fip, xfip, xera, siera, era, lob_pct, gb_pct,
        k_per_9, k_pct, bb_per_9, bb_pct, hr_per_9, ip, fangraphs_id
    )
    SELECT
        player_id,
        season,
        'pitcher',
        CAST(:as_of_date AS date),
        NULL::float AS war,
        CASE WHEN (outs_recorded + h_allowed - so - hr_allowed) > 0
             THEN (h_allowed - hr_allowed) / NULLIF((outs_recorded + h_allowed - so - hr_allowed), 0)
        END AS babip,
        CASE WHEN ip > 0 THEN (13.0 * hr_allowed + 3.0 * bb - 2.0 * so) / NULLIF(ip, 0) + 3.10 END AS fip,
        NULL::float AS xfip,
        NULL::float AS xera,
        NULL::float AS siera,
        CASE WHEN ip > 0 THEN 9.0 * er / NULLIF(ip, 0) END AS era,
        NULL::float AS lob_pct,
        NULL::float AS gb_pct,
        CASE WHEN ip > 0 THEN 9.0 * so / NULLIF(ip, 0) END AS k_per_9,
        CASE WHEN (outs_recorded + h_allowed + bb) > 0 THEN so / NULLIF((outs_recorded + h_allowed + bb), 0) END AS k_pct,
        CASE WHEN ip > 0 THEN 9.0 * bb / NULLIF(ip, 0) END AS bb_per_9,
        CASE WHEN (outs_recorded + h_allowed + bb) > 0 THEN bb / NULLIF((outs_recorded + h_allowed + bb), 0) END AS bb_pct,
        CASE WHEN ip > 0 THEN 9.0 * hr_allowed / NULLIF(ip, 0) END AS hr_per_9,
        ip,
        NULL::int AS fangraphs_id
    FROM pitching
    ON CONFLICT (player_id, season, player_type, as_of_date) DO UPDATE SET
        babip = EXCLUDED.babip,
        fip = EXCLUDED.fip,
        era = EXCLUDED.era,
        k_per_9 = EXCLUDED.k_per_9,
        k_pct = EXCLUDED.k_pct,
        bb_per_9 = EXCLUDED.bb_per_9,
        bb_pct = EXCLUDED.bb_pct,
        hr_per_9 = EXCLUDED.hr_per_9,
        ip = EXCLUDED.ip,
        scraped_at = now()
    """
)

COUNT_SQL = text(
    """
    SELECT player_type, COUNT(*)
    FROM mlb_player_season_advanced_history
    WHERE season = :season AND as_of_date = :as_of_date
    GROUP BY player_type
    ORDER BY player_type
    """
)

DATE_SQL = text(
    """
    SELECT DISTINCT game_date
    FROM mlb_game_schedule
    WHERE season = :season
      AND game_date BETWEEN :start_date AND :end_date
      AND status != 'Cancelled'
    ORDER BY game_date
    """
)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def run_snapshot(conn, season: int, as_of_date: date, min_pa: int, min_ip: float, execute: bool) -> dict[str, int]:
    params = {"season": season, "as_of_date": as_of_date, "min_pa": min_pa, "min_ip": min_ip}
    if execute:
        conn.execute(BATTER_SQL, params)
        conn.execute(PITCHER_SQL, params)
    rows = conn.execute(COUNT_SQL, params).fetchall()
    return {row[0]: int(row[1]) for row in rows}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill MLB advanced history from GameFlow game stats")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--as-of-date", type=parse_date, help="Single snapshot date to populate")
    parser.add_argument("--start-date", type=parse_date, help="First game/snapshot date for historical daily backfill")
    parser.add_argument("--end-date", type=parse_date, help="Last game/snapshot date for historical daily backfill")
    parser.add_argument("--min-pa", type=int, default=1)
    parser.add_argument("--min-ip", type=float, default=0.1)
    parser.add_argument("--execute", action="store_true", help="Actually write rows; default is dry-run/count only")
    args = parser.parse_args()

    if not args.as_of_date and not (args.start_date and args.end_date):
        parser.error("Specify --as-of-date or both --start-date and --end-date")

    load_dotenv(dotenv_path=".env")
    load_dotenv(dotenv_path=".env.local", override=True)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    engine = create_engine(database_url, pool_pre_ping=True, connect_args={"connect_timeout": 15})

    with engine.begin() as conn:
        conn.execute(text("SET statement_timeout = '300s'"))
        if args.as_of_date:
            before = run_snapshot(conn, args.season, args.as_of_date, args.min_pa, args.min_ip, execute=False)
            if args.execute:
                after = run_snapshot(conn, args.season, args.as_of_date, args.min_pa, args.min_ip, execute=True)
            else:
                after = before
            print(f"snapshot {args.season} {args.as_of_date}: before={before} after={after} execute={args.execute}")
        else:
            dates = [row.game_date for row in conn.execute(DATE_SQL, {
                "season": args.season,
                "start_date": args.start_date,
                "end_date": args.end_date,
            }).fetchall()]
            print(f"historical snapshots: season={args.season} dates={len(dates)} execute={args.execute}")
            last = {}
            for idx, snapshot_date in enumerate(dates, start=1):
                last = run_snapshot(conn, args.season, snapshot_date, args.min_pa, args.min_ip, execute=args.execute)
                if idx == 1 or idx == len(dates) or idx % 25 == 0:
                    print(f"  {idx}/{len(dates)} {snapshot_date}: {last}")
            print(f"done season={args.season} snapshots={len(dates)} last_counts={last}")


if __name__ == "__main__":
    main()
