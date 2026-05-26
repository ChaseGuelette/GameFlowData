#!/usr/bin/env python3
"""Run day-by-day validation of GameFlow advanced-history vs Baseball-Reference
ranges (batting_stats_range / pitching_stats_range), read-only.

Outputs:
- tmp/mlb_adv_day_validation_2026_results.csv
- tmp/mlb_adv_day_validation_2026_results.json
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import text

from dotenv import load_dotenv
from pybaseball import batting_stats_range, pitching_stats_range

from src.db.client import get_engine


def safe_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, str):
        v = v.strip().replace("%", "")
        if v in {"", "--", "-"}:
            return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(n):
        return None
    return n


def safe_int(v: Any) -> int | None:
    n = safe_float(v)
    if n is None:
        return None
    return int(n)


def safe_div(n: float | None, d: float | None) -> float | None:
    if n is None or d in (None, 0):
        return None
    return n / d


def innings_to_decimal(v: Any) -> float | None:
    n = safe_float(v)
    if n is None:
        return None
    sign = -1 if n < 0 else 1
    n = abs(n)
    whole = np.floor(n)
    frac = n - whole
    if abs(frac) < 1e-9:
        frac_d = 0.0
    elif abs(frac - 0.1) < 1e-6 or abs(frac - 0.10000000000000009) < 1e-6:
        frac_d = 1.0 / 3.0
    elif abs(frac - 0.2) < 1e-6 or abs(frac - 0.20000000000000018) < 1e-6:
        frac_d = 2.0 / 3.0
    else:
        frac_d = frac
    return sign * float(whole + frac_d)


def dedupe_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["player_id"] = pd.to_numeric(out["mlbID"], errors="coerce")
    out = out.dropna(subset=["player_id"])
    if out.empty:
        return out
    out["player_id"] = out["player_id"].astype(int)
    out["_tot"] = out.get("Tm", "").astype(str).eq("TOT")
    out = out.sort_values(["player_id", "_tot"], ascending=[True, False], kind="mergesort")
    out = out.drop_duplicates(subset=["player_id"], keep="first")
    return out.drop(columns=["_tot"]) 


def derive_batter_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = pd.DataFrame({"player_id": pd.to_numeric(df["mlbID"], errors="coerce").astype("Int64").astype(int)})
    out = out.dropna(subset=["player_id"]).astype({"player_id": int})

    pa = df.get("PA").apply(safe_float)
    ab = df.get("AB").apply(safe_float)
    h = df.get("H").apply(safe_float)
    hr = df.get("HR").apply(safe_float)
    so = df.get("SO").apply(safe_float)
    bb = df.get("BB").apply(safe_float)
    sf = df.get("SF").apply(safe_float)

    out["pa"] = df.get("PA").apply(safe_int)
    out["avg"] = df.get("BA").apply(safe_float)
    out["obp"] = df.get("OBP").apply(safe_float)
    out["slg"] = df.get("SLG").apply(safe_float)
    out["ops"] = df.get("OPS").apply(safe_float)
    out["k_pct"] = [safe_div(s, p) for s, p in zip(so, pa)]
    out["bb_pct"] = [safe_div(b, p) for b, p in zip(bb, pa)]
    denom = ab - so - hr + sf
    out["babip"] = [safe_div((hv - hrv) if (hv is not None and hrv is not None) else None, dv) for hv, hrv, dv in zip(h, hr, denom)]
    out["iso"] = [
        None if (s is None or a is None) else (s - a)
        for s, a in zip(out["slg"].tolist(), out["avg"].tolist())
    ]
    return out.set_index("player_id")


def derive_pitcher_stats(df: pd.DataFrame, season: int) -> pd.DataFrame:
    if df.empty:
        return df

    out = pd.DataFrame({"player_id": pd.to_numeric(df["mlbID"], errors="coerce").astype("Int64").astype(int)})
    out = out.dropna(subset=["player_id"]).astype({"player_id": int})

    bf = df.get("BF").apply(safe_float)
    so = df.get("SO").apply(safe_float)
    bb = df.get("BB").apply(safe_float)
    hr = df.get("HR").apply(safe_float)
    ip = df.get("IP").apply(innings_to_decimal)

    out["ip"] = ip
    out["babip"] = df.get("BAbip").apply(safe_float)
    out["era"] = df.get("ERA").apply(safe_float)
    out["k_per_9"] = df.get("SO9").apply(safe_float)
    out["k_pct"] = [safe_div(s, b) for s, b in zip(so, bf)]
    out["bb_pct"] = [safe_div(b, b2) for b, b2 in zip(bb, bf)]
    out["bb_per_9"] = [safe_div(9.0 * b, i) if i not in (None, 0) else None for b, i in zip(bb, ip)]
    out["hr_per_9"] = [safe_div(9.0 * h, i) if i not in (None, 0) else None for h, i in zip(hr, ip)]
    const = {2022: 3.09, 2023: 3.13, 2024: 3.13, 2025: 3.13, 2026: 3.13}.get(season, 3.13)
    out["fip"] = [
        ((13.0 * hrv + 3.0 * br + -2.0 * srv) / i + const)
        if i not in (None, 0) and hrv is not None and br is not None and srv is not None
        else None
        for hrv, br, srv, i in zip(hr, bb, so, ip)
    ]
    return out.set_index("player_id")


def compare_series(gf: pd.Series, br: pd.Series) -> dict[str, Any]:
    mask = gf.notna() & br.notna()
    a = gf[mask].astype(float)
    b = br[mask].astype(float)
    if len(a) == 0:
        return {
            "matched_rows": 0,
            "mae": None,
            "rmse": None,
            "corr": None,
            "p50_abs_err": None,
            "p90_abs_err": None,
            "p95_abs_err": None,
            "worst_examples": [],
        }

    err = (a - b).abs()
    merged = pd.DataFrame({"player_id": mask[mask].index, "gf": a, "br": b, "abs_err": err}).sort_values("abs_err", ascending=False)
    n = len(merged)
    mae = float(err.mean())
    rmse = float(np.sqrt(np.mean((a - b) ** 2)))
    corr = float(np.corrcoef(a, b)[0, 1]) if n > 1 and a.nunique() > 1 and b.nunique() > 1 else None

    return {
        "matched_rows": n,
        "mae": mae,
        "rmse": rmse,
        "corr": corr,
        "p50_abs_err": float(np.percentile(err, 50)),
        "p90_abs_err": float(np.percentile(err, 90)),
        "p95_abs_err": float(np.percentile(err, 95)),
        "worst_examples": [
            {
                "player_id": int(r["player_id"]),
                "gf": float(r["gf"]),
                "br": float(r["br"]),
                "abs_err": float(r["abs_err"]),
            }
            for _, r in merged.head(3).iterrows()
        ],
    }


def pick_source_engine(season: int, start: date, end: date):
    local = get_engine(local=True)
    with local.connect() as conn:
        n = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM mlb_player_season_advanced_history
                WHERE season=:season AND as_of_date BETWEEN :start_date AND :end_date
                """
            ),
            {"season": season, "start_date": start, "end_date": end},
        ).scalar()

    if n and int(n) > 0:
        return local, "local"
    return get_engine(), "remote"


def evaluate_day(br_bat: pd.DataFrame, br_pit: pd.DataFrame, gf_day: pd.DataFrame, player_type: str, metrics: list[str], stats_date: date) -> list[dict[str, Any]]:
    day_results = []
    if player_type == "batter":
        br = {
            "babip": br_bat.get("babip"),
            "avg": br_bat.get("avg"),
            "obp": br_bat.get("obp"),
            "slg": br_bat.get("slg"),
            "ops": br_bat.get("ops"),
            "pa": br_bat.get("pa"),
            "k_pct": br_bat.get("k_pct"),
            "bb_pct": br_bat.get("bb_pct"),
            "iso": br_bat.get("iso"),
        }
    else:
        br = {
            "babip": br_pit.get("babip"),
            "era": br_pit.get("era"),
            "fip": br_pit.get("fip"),
            "k_per_9": br_pit.get("k_per_9"),
            "k_pct": br_pit.get("k_pct"),
            "bb_per_9": br_pit.get("bb_per_9"),
            "bb_pct": br_pit.get("bb_pct"),
            "hr_per_9": br_pit.get("hr_per_9"),
            "ip": br_pit.get("ip"),
        }

    gf_day = gf_day.set_index("player_id")
    for stat in metrics:
        br_series = br.get(stat)
        if br_series is None:
            continue
        merged = gf_day[[stat]].join(br_series.rename("br"), how="inner").rename(columns={stat: "gf"})
        comp = compare_series(merged["gf"], merged["br"])
        comp.update({"date": stats_date.isoformat(), "player_type": player_type, "stat": stat})
        day_results.append(comp)

    return day_results


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate GameFlow MLB advanced history against BR range stats")
    parser.add_argument("--max-dates", type=int, default=None, help="Limit dates processed for smoke tests")
    args = parser.parse_args()

    load_dotenv(".env")
    season = 2026
    br_start = date(season, 3, 20)
    start_date = date(season, 3, 25)
    end_date = date(season, 5, 25)

    engine, engine_name = pick_source_engine(season, start_date, end_date)

    query = text(
        """
        SELECT as_of_date, player_type, player_id,
               babip, avg, obp, slg, ops, pa, k_pct, bb_pct, iso,
               war, wrc_plus, woba, hard_pct,
               era, fip, k_per_9, bb_per_9, hr_per_9, ip
        FROM mlb_player_season_advanced_history
        WHERE season=:season AND as_of_date BETWEEN :start_date AND :end_date
        """
    )

    with engine.connect() as conn:
        gf = pd.read_sql_query(
            query,
            conn,
            params={"season": season, "start_date": start_date, "end_date": end_date},
        )

    if gf.empty:
        raise RuntimeError("No GameFlow rows in requested range")

    gf["as_of_date"] = pd.to_datetime(gf["as_of_date"]).dt.date
    all_dates = sorted(gf["as_of_date"].unique())
    if args.max_dates is not None:
        all_dates = all_dates[: args.max_dates]

    batter_metrics = ["babip", "avg", "obp", "slg", "ops", "pa", "k_pct", "bb_pct", "iso"]
    pitcher_metrics = ["babip", "era", "fip", "k_per_9", "k_pct", "bb_per_9", "bb_pct", "hr_per_9", "ip"]

    all_results: list[dict[str, Any]] = []
    source_failures: list[dict[str, str]] = []
    source_empty_counts = {"batter": 0, "pitcher": 0}
    worst_examples: dict[str, dict[str, list[dict[str, Any]]]] = {
        "batter": defaultdict(list),
        "pitcher": defaultdict(list),
    }

    print(f"Starting day-by-day validation: dates={len(all_dates)} engine={engine_name}")

    for idx, d in enumerate(all_dates, start=1):
        if d < start_date or d > end_date:
            continue
        end_s = d.isoformat()

        try:
            br_bat_raw = batting_stats_range(br_start.isoformat(), end_s)
        except Exception as exc:
            source_failures.append({"date": end_s, "player_type": "batter", "error": f"{type(exc).__name__}: {exc}"})
            br_bat_raw = pd.DataFrame()

        try:
            br_pit_raw = pitching_stats_range(br_start.isoformat(), end_s)
        except Exception as exc:
            source_failures.append({"date": end_s, "player_type": "pitcher", "error": f"{type(exc).__name__}: {exc}"})
            br_pit_raw = pd.DataFrame()

        if br_bat_raw.empty:
            source_empty_counts["batter"] += 1
        if br_pit_raw.empty:
            source_empty_counts["pitcher"] += 1

        if "Lev" in br_bat_raw.columns:
            br_bat_raw = br_bat_raw[pd.Series(br_bat_raw["Lev"]).astype(str).str.lower().str.startswith("maj")]
        if "Lev" in br_pit_raw.columns:
            br_pit_raw = br_pit_raw[pd.Series(br_pit_raw["Lev"]).astype(str).str.lower().str.startswith("maj")]

        br_bat = dedupe_totals(br_bat_raw)
        br_pit = dedupe_totals(br_pit_raw)

        br_bat_derived = derive_batter_stats(br_bat)
        br_pit_derived = derive_pitcher_stats(br_pit, season)

        bat_slice = gf[(gf["as_of_date"] == d) & (gf["player_type"] == "batter")][
            ["player_id", "babip", "avg", "obp", "slg", "ops", "pa", "k_pct", "bb_pct", "iso"]
        ]
        pit_slice = gf[(gf["as_of_date"] == d) & (gf["player_type"] == "pitcher")][
            ["player_id", "babip", "era", "fip", "k_per_9", "k_pct", "bb_per_9", "bb_pct", "hr_per_9", "ip"]
        ]

        for r in evaluate_day(br_bat_derived, br_pit_derived, bat_slice, "batter", batter_metrics, d):
            all_results.append(r)
            for ex in r["worst_examples"]:
                key = (r["player_type"], r["stat"])
                worst_examples[key[0]][key[1]].append({"date": d.isoformat(), **ex})

        for r in evaluate_day(br_bat_derived, br_pit_derived, pit_slice, "pitcher", pitcher_metrics, d):
            all_results.append(r)
            for ex in r["worst_examples"]:
                key = (r["player_type"], r["stat"])
                worst_examples[key[0]][key[1]].append({"date": d.isoformat(), **ex})

        if idx == 1 or idx == len(all_dates) or idx % 15 == 0:
            print(f"  processed {idx}/{len(all_dates)} dates through {d}")

    df_out = pd.DataFrame(all_results)

    out_dir = Path("tmp")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "mlb_adv_day_validation_2026_results.csv"
    json_path = out_dir / "mlb_adv_day_validation_2026_results.json"

    summary = {
        "season": season,
        "date_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "engine": engine_name,
        "rows_in_range": int(len(gf)),
        "dates_processed": int(len(all_dates)),
        "groups": int(len(df_out)),
        "source_empty_counts": source_empty_counts,
        "source_failure_count": int(len(source_failures)),
        "source_failure_examples": source_failures[:5],
        "result_csv": str(csv_path.resolve()),
        "result_json": str(json_path.resolve()),
    }

    if df_out.empty:
        pd.DataFrame(columns=[
            "date", "player_type", "stat", "matched_rows", "mae", "rmse", "corr",
            "p50_abs_err", "p90_abs_err", "p95_abs_err", "worst_examples",
        ]).to_csv(csv_path, index=False)
        with json_path.open("w") as f:
            json.dump({"summary": summary, "results": [], "source_failures": source_failures}, f, indent=2)
        print("SUMMARY")
        print(json.dumps(summary, indent=2))
        print("No comparable rows were produced because the independent BR range source returned empty/failed for every requested date.")
        return

    # Keep only json-serializable data
    out = df_out.copy()
    out["worst_examples"] = out["worst_examples"].apply(lambda x: x if isinstance(x, list) else [])
    out.to_csv(csv_path, index=False)

    with json_path.open("w") as f:
        json.dump({"summary": summary, "results": all_results, "source_failures": source_failures}, f, indent=2)

    print("SUMMARY")
    print(json.dumps(summary, indent=2))

    for player_type, metrics in [("batter", batter_metrics), ("pitcher", pitcher_metrics)]:
        print(f"{player_type.upper()}:")
        dft = df_out[df_out["player_type"] == player_type]
        for stat in metrics:
            st = dft[dft["stat"] == stat]
            if st.empty:
                print(f"  {stat}: no comparisons")
                continue
            print(
                f"  {stat:9s} rows={len(st)} "
                f"mean_mae={st['mae'].astype(float).mean():.6f} "
                f"mean_rmse={st['rmse'].astype(float).mean():.6f} "
                f"mean_p95={st['p95_abs_err'].astype(float).mean():.6f}"
            )


if __name__ == "__main__":
    main()
