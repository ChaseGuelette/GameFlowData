"""Single owner for MLB prop-line feature SQL and single-row fetches."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.sql.elements import TextClause

from src.models.mlb.features.temporal_contracts import FeatureAsOfPolicy, resolve_as_of_policy

FEATURE_PROP_BOOKMAKERS = ("pinnacle", "draftkings")


def build_lateral_prop_line_join(
    *,
    row_alias: str,
    market_key_sql: str,
    join_alias: str = "props",
) -> str:
    """Build the shared lateral prop-line join for training/date-batch SQL.

    ``market_key_sql`` is SQL text by design: batch batter paths pass the
    ``:market_key`` bind placeholder, while pitcher paths preserve the legacy
    literal ``'pitcher_strikeouts'`` filter.
    """
    return f"""
            LEFT JOIN LATERAL (
                SELECT sub.line AS prop_line
                FROM (
                    SELECT DISTINCT ON (market_key) market_key, line
                    FROM mlb_raw_player_props
                    WHERE player_id = {row_alias}.player_id
                      AND game_id = {row_alias}.game_id
                      AND market_key = {market_key_sql}
                      AND bookmaker IN ('pinnacle', 'draftkings')
                      AND (
                          :as_of_time IS NULL
                          OR market_last_update <= :as_of_time
                      )
                      AND (
                          commence_time IS NULL
                          OR market_last_update IS NULL
                          OR market_last_update < commence_time
                      )
                      AND (
                          commence_time IS NULL
                          OR COALESCE(snapshot_time, inserted_at) < commence_time
                      )
                    ORDER BY market_key, market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST
                ) sub
                LIMIT 1
            ) {join_alias} ON TRUE"""


@dataclass(frozen=True)
class PropLineQuery:
    sql: TextClause
    params: dict[str, object]
    policy: FeatureAsOfPolicy


def build_single_prop_line_query(
    *,
    player_id: int,
    game_id: int,
    market_key: str,
    as_of_time: datetime | None = None,
) -> PropLineQuery:
    """Build the legacy single-player prop-line query with explicit as-of policy."""
    contract = resolve_as_of_policy(as_of_time)
    query = text("""
        SELECT line
        FROM mlb_raw_player_props
        WHERE player_id = :player_id
          AND game_id = :game_id
          AND market_key = :market_key
          AND bookmaker IN ('pinnacle', 'draftkings')
          AND (
              :as_of_time IS NULL
              OR market_last_update <= :as_of_time
          )
          AND (
              commence_time IS NULL
              OR market_last_update IS NULL
              OR market_last_update < commence_time
          )
          AND (
              commence_time IS NULL
              OR COALESCE(snapshot_time, inserted_at) < commence_time
          )
        ORDER BY market_last_update DESC NULLS LAST, COALESCE(snapshot_time, inserted_at) DESC NULLS LAST
        LIMIT 1
    """)
    return PropLineQuery(
        sql=query,
        params={
            "player_id": player_id,
            "game_id": game_id,
            "market_key": market_key,
            "as_of_time": as_of_time,
        },
        policy=contract.policy,
    )


def fetch_single_prop_line(
    engine: Engine,
    *,
    player_id: int,
    game_id: int,
    market_key: str,
    as_of_time: datetime | None = None,
) -> float:
    """Fetch one prop line, preserving legacy 0 fallback behavior."""
    query = build_single_prop_line_query(
        player_id=player_id,
        game_id=game_id,
        market_key=market_key,
        as_of_time=as_of_time,
    )
    with engine.connect() as conn:
        df = pd.read_sql(query.sql, conn, params=query.params)
    if df.empty:
        return 0
    line = df.iloc[0].line
    return float(line) if line else 0
