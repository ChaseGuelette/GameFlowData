"""Replay selected compact T-60 lines with real cached/inference prices.

The feature table deliberately has no prices. Historical prices come from the
original paid response cache; live prices come from the small inference table.
No provider requests, synthetic prices, or legacy raw-table fallback.
"""
from __future__ import annotations

import gzip
import json
import os
from dataclasses import asdict
from datetime import UTC, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text

from src.processing.nba_prop_snapshots import parse_odds_event, parse_utc
from src.processing.nba_quote_capture import EXCLUDED_BOOKS

TABLE = "nba_player_prop_feature_lines"
CLOCKS = ("decision_time", "commence_time", "source_snapshot_time", "requested_snapshot_time",
          "market_last_update", "bookmaker_last_update", "observed_at")


class CompactQuoteError(ValueError):
    """A selected feature cannot be paired with its actual source prices."""


def _query(engine, sql, params, expanding=()):
    statement = text(sql).bindparams(*(bindparam(name, expanding=True) for name in expanding))
    with engine.connect() as conn, conn.begin():
        conn.execute(text("SET TRANSACTION READ ONLY"))
        conn.execute(text("SET LOCAL statement_timeout = '15s'"))
        conn.execute(text("SET LOCAL lock_timeout = '2s'"))
        return pd.read_sql(statement, conn, params=params)


def _clock(value, name, required=False):
    return parse_utc(value, field=name, required=required)


def _cached_candidates(feature, cache_dir):
    target = feature["requested_snapshot_time"]
    if target is None:
        raise CompactQuoteError("Historical compact row has no requested snapshot")
    event_id = feature["api_game_id"]
    if not isinstance(event_id, str) or not event_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in event_id):
        raise CompactQuoteError("Unsafe cached event identifier")
    path = cache_dir / "events" / f"{event_id}_{target.astimezone(UTC):%Y%m%dT%H%M%SZ}.json.gz"
    if not path.is_file():
        raise CompactQuoteError(f"Missing historical price cache: {path}; no raw/API fallback")
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    snapshot = _clock(payload.get("timestamp"), "cached timestamp", True)
    if snapshot is None or snapshot > target:
        raise CompactQuoteError("Cached snapshot is later than requested cutoff")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise CompactQuoteError("Historical price cache has no event object")
    quotes = parse_odds_event(data, requested_snapshot_time=target,
        source_snapshot_time=snapshot, source_kind="historical", observed_at=snapshot,
        expected_event_id=event_id)
    return [asdict(quote) for quote in quotes]


def _live_candidates(engine, feature):
    frame = _query(engine, """SELECT to_jsonb(q) AS raw FROM nba_player_props_inference q
        WHERE api_game_id=:api_game_id AND api_player_name=:api_player_name
          AND bookmaker=:bookmaker AND market_key=:market_key AND line=:line
          AND source_snapshot_time=:source_snapshot_time
        ORDER BY staging_id LIMIT 5""", {key: feature[key] for key in (
            "api_game_id", "api_player_name", "bookmaker", "market_key", "line", "source_snapshot_time")})
    if len(frame) > 2:
        raise CompactQuoteError("Ambiguous live inference price pair")
    rows = frame["raw"].tolist() if "raw" in frame else []
    for row in rows:
        for name in CLOCKS:
            if name in row:
                row[name] = _clock(row[name], name)
        row["line"] = Decimal(str(row["line"]))
    return rows


def load_compact_pairs(engine, *, markets, bookmakers, game_ids=None,
                       start_date=None, end_date=None, max_rows=100000, cache_dir=None):
    """Read bounded selected lines and return one verified atomic pair per row."""
    if not markets or not bookmakers or game_ids == []:
        return pd.DataFrame()
    excluded = sorted(set(bookmakers) & set(EXCLUDED_BOOKS))
    if excluded:
        raise CompactQuoteError(f"DFS/sweepstakes bookmakers are not supported: {', '.join(excluded)}")
    if game_ids is None and (start_date is None or end_date is None):
        raise ValueError("Compact replay requires explicit games or date bounds")
    root = Path(cache_dir or os.getenv("NBA_PROPS_T60_STATE_DIR") or Path.home() / ".gameflow/nba-props-t60")
    clauses = ["selection_policy_version='t60_v1'", "market_key IN :markets", "bookmaker IN :books"]
    params = {"markets": list(markets), "books": list(bookmakers), "limit": max_rows + 1}
    expanding = ["markets", "books"]
    if game_ids is not None:
        clauses.append("game_id IN :games")
        params["games"] = list(game_ids)
        expanding.append("games")
    if start_date is not None:
        clauses.append("game_date >= :start")
        params["start"] = start_date
    if end_date is not None:
        clauses.append("game_date <= :end")
        params["end"] = end_date
    frame = _query(engine, "SELECT to_jsonb(f) AS raw FROM nba_player_prop_feature_lines f WHERE "
        + " AND ".join(clauses) + " ORDER BY game_date,game_id,player_id,market_key LIMIT :limit", params, expanding)
    if len(frame) > max_rows:
        raise CompactQuoteError("Compact replay row cap exceeded; narrow the date window")
    cache, output = {}, []
    for feature in frame["raw"].tolist() if "raw" in frame else []:
        for name in CLOCKS:
            feature[name] = _clock(feature.get(name), name, name in (
                "decision_time", "commence_time", "source_snapshot_time", "observed_at"))
        feature["line"] = Decimal(str(feature["line"]))
        decision = feature["decision_time"]
        if decision != feature["commence_time"] - timedelta(hours=1) or any(
            feature[name] is not None and feature[name] > decision for name in CLOCKS
            if name not in ("decision_time", "commence_time")
        ):
            raise CompactQuoteError("Compact row violates its T-60 clock contract")
        if feature["source_kind"] == "historical":
            key = (feature["api_game_id"], feature["requested_snapshot_time"])
            if key not in cache:
                cache[key] = _cached_candidates(feature, root)
            candidates = cache[key]
        elif feature["source_kind"] == "live":
            candidates = _live_candidates(engine, feature)
        else:
            raise CompactQuoteError("Unknown compact price source kind")
        sides = {}
        for quote in candidates:
            if not all(quote.get(name) == feature[name] for name in (
                "api_game_id", "api_player_name", "market_key", "bookmaker", "line",
                "source_snapshot_time", "commence_time", "market_last_update", "bookmaker_last_update"
            )):
                continue
            side = quote["outcome_label"].lower()
            if side not in ("over", "under"):
                continue
            price = int(quote["odds_american"])
            if side in sides and sides[side] != price:
                raise CompactQuoteError("Contradictory identical source prices")
            sides[side] = price
        if set(sides) != {"over", "under"}:
            raise CompactQuoteError(f"No exact source price pair for {feature['game_id']}/{feature['player_id']}/{feature['market_key']}")
        row: dict[str, Any] = dict(feature, line=float(feature["line"]), over_odds=sides["over"], under_odds=sides["under"],
                   bet_decision_time=decision, quote_decision_policy="t60_v1")
        row["game_date"] = pd.Timestamp(row["game_date"]).date()
        for side in ("over", "under"):
            row.update({f"{side}_snapshot_time": feature["source_snapshot_time"],
                        f"{side}_market_last_update": feature["market_last_update"],
                        f"{side}_bookmaker_last_update": feature["bookmaker_last_update"],
                        f"{side}_decision_time": decision, f"{side}_bookmaker": feature["bookmaker"]})
        output.append(row)
    return pd.DataFrame(output)


def historical_quote_rows(pairs, *, allowed_routes):
    """Adapt actual compact/cache clocks; route permission still comes from policy.

    The four t60_v1 markets are full-game main player props. An explicit matching
    route is required; a quarter/alternate route cannot relabel these quotes.
    """
    from src.models.features.nba.historical_observation_adapters import HistoricalSourceClass, SourceProvenance

    output = []
    for row in pairs.to_dict("records"):
        route = (row["bookmaker"], "full_game", "main")
        if route not in allowed_routes:
            raise CompactQuoteError(f"Compact replay needs an authorized full_game/main route for {row['bookmaker']}")
        snapshot_key = json.dumps([row["api_game_id"], row["bookmaker"], row["source_snapshot_time"].isoformat()], separators=(",", ":"))
        for side in ("over", "under"):
            identity = json.dumps([row["game_id"], row["player_id"], row["market_key"],
                                   row["selection_policy_version"], snapshot_key, side], separators=(",", ":"))
            provenance = SourceProvenance(TABLE, identity, (("api_game_id", row["api_game_id"]),
                ("task_id", str(row.get("task_id") or "")), ("price_source", "cached_response" if row["source_kind"] == "historical" else "nba_player_props_inference")))
            historical = row["source_kind"] == "historical"
            output.append({"game_id": row["game_id"], "player_id": row["player_id"],
                "market_key": row["market_key"], "bookmaker": row["bookmaker"], "period": "full_game", "variant": "main",
                "line": row["line"], "side": side, "price": row[f"{side}_odds"], "odds_format": "american",
                "source_namespace": TABLE, "source_record_id": identity, "provenance": provenance,
                "source_class": HistoricalSourceClass.AUTHENTICATED_HISTORICAL_REQUEST if historical else HistoricalSourceClass.LOCAL_OBSERVED_AT,
                "snapshot_compatibility_key": snapshot_key, "snapshot_time": row["source_snapshot_time"],
                "historical_requested_at": row["requested_snapshot_time"] if historical else None,
                "market_updated_at": row["market_last_update"], "bookmaker_updated_at": row["bookmaker_last_update"],
                "local_observed_at": None if historical else row["observed_at"], "commence_time": row["commence_time"]})
    return tuple(output)
