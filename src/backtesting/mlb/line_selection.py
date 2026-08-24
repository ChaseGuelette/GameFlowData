"""Shared MLB sportsbook line selection for backtests.

`run_mlb_sweep.py` is the canonical production-validation entry point, but
`mlb_backtest_harness.py` is retained as a single-config/legacy harness. Both
must use this helper so quote-clean/as-of leakage fixes live in one place.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime

import pandas as pd
from sqlalchemy import text

# Sportsbook market keys that differ from internal stat names.
STAT_TO_MARKET_KEY: dict[str, str] = {
}

DEFAULT_EXCLUDED_BOOKMAKERS: tuple[str, ...] = (
    "novig",
    "betonlineag",
    "dabble_us_dfs",
    "betr_us_dfs",
    "pick6",
    "prizepicks",
    "underdog",
)


def fetch_lines_at_decision_time(
    engine,
    *,
    game_ids: Iterable[int],
    market_keys: Iterable[str],
    as_of_time: datetime | None,
    allow_latest_without_as_of: bool = False,
    bookmakers: Iterable[str] | None = None,
    source_table: str = "mlb_raw_player_props",
) -> pd.DataFrame:
    """Fetch same-book, same-snapshot over/under MLB prop lines.

    Args:
        engine: SQLAlchemy engine.
        game_ids: MLB game IDs to include.
        market_keys: Internal stat keys such as ``batter_hits`` or
            ``pitcher_strikeouts``. Keys are translated to sportsbook DB market
            keys and mapped back before returning.
        as_of_time: Decision/bet timestamp. Historical quote-clean backtests
            should pass this. Lines with updates after this timestamp are
            excluded.
        allow_latest_without_as_of: Explicit legacy/backfill escape hatch for
            callers that are not doing quote-clean validation. Even in this mode,
            post-commence rows are excluded.
        bookmakers: Optional allowed-book list. If omitted, excluded bookmaker
            defaults are applied.

    Returns:
        DataFrame with one row per player/game/book/market/line and columns
        ``over_odds``, ``under_odds``, ``over_snapshot_time``,
        ``under_snapshot_time``, ``selected_snapshot_time``.
    """
    game_ids = list(game_ids)
    market_keys = list(market_keys)
    if not game_ids or not market_keys:
        return pd.DataFrame()
    if as_of_time is None and not allow_latest_without_as_of:
        raise ValueError(
            "fetch_lines_at_decision_time requires as_of_time unless "
            "allow_latest_without_as_of=True is set explicitly"
        )

    db_keys = [STAT_TO_MARKET_KEY.get(k, k) for k in market_keys]
    reverse_map = {v: k for k, v in STAT_TO_MARKET_KEY.items()}

    game_id_placeholders = ", ".join(f":gid_{i}" for i in range(len(game_ids)))
    market_placeholders = ", ".join(f":mk_{i}" for i in range(len(db_keys)))

    params: dict[str, object] = {}
    for i, gid in enumerate(game_ids):
        params[f"gid_{i}"] = gid
    for i, mk in enumerate(db_keys):
        params[f"mk_{i}"] = mk

    book_filter_sql = ""
    if bookmakers is not None:
        book_list = list(bookmakers)
        if book_list:
            book_placeholders = ", ".join(f":book_{i}" for i in range(len(book_list)))
            book_filter_sql = f"AND bookmaker IN ({book_placeholders})"
            for i, book in enumerate(book_list):
                params[f"book_{i}"] = book
    else:
        excl_placeholders = ", ".join(f":excl_{i}" for i in range(len(DEFAULT_EXCLUDED_BOOKMAKERS)))
        book_filter_sql = f"AND bookmaker NOT IN ({excl_placeholders})"
        for i, book in enumerate(DEFAULT_EXCLUDED_BOOKMAKERS):
            params[f"excl_{i}"] = book

    if source_table == "mlb_player_props_clv_snapshots":
        table_sql = "mlb_player_props_clv_snapshots"
        effective_snapshot_sql = "snapshot_time"
        linked_where_sql = "AND game_id IS NOT NULL AND player_id IS NOT NULL"
    elif source_table == "mlb_raw_player_props":
        table_sql = "mlb_raw_player_props"
        effective_snapshot_sql = "COALESCE(snapshot_time, inserted_at)"
        linked_where_sql = ""
    else:
        raise ValueError(f"Unsupported MLB line source table: {source_table}")

    as_of_sql = "/* latest-without-as-of is legacy/backfill only */"
    if as_of_time is not None:
        params["as_of_time"] = as_of_time
        as_of_sql = f"""
                  AND (
                      market_last_update IS NULL
                      OR market_last_update <= :as_of_time
                  )
                  AND {effective_snapshot_sql} <= :as_of_time
        """

    query = text(f"""
        WITH candidate_lines AS (
            SELECT
                player_id,
                game_id,
                bookmaker,
                market_key,
                line,
                outcome_label,
                odds_american,
                market_last_update,
                {effective_snapshot_sql} AS effective_snapshot_time,
                ROW_NUMBER() OVER (
                    PARTITION BY player_id, game_id, market_key, bookmaker, line, outcome_label
                    ORDER BY market_last_update DESC NULLS LAST,
                             {effective_snapshot_sql} DESC NULLS LAST
                ) AS rn
            FROM {table_sql}
            WHERE game_id IN ({game_id_placeholders})
              AND market_key IN ({market_placeholders})
              {book_filter_sql}
              AND player_id IS NOT NULL
              {linked_where_sql}
              AND {effective_snapshot_sql} IS NOT NULL
              {as_of_sql}
              AND (
                  commence_time IS NULL
                  OR market_last_update IS NULL
                  OR market_last_update < commence_time
              )
              AND (
                  commence_time IS NULL
                  OR {effective_snapshot_sql} < commence_time
              )
        )
        SELECT
            player_id,
            game_id,
            bookmaker,
            market_key,
            line,
            MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) AS over_odds,
            MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) AS under_odds,
            MAX(CASE WHEN outcome_label = 'Over' THEN effective_snapshot_time END) AS over_snapshot_time,
            MAX(CASE WHEN outcome_label = 'Under' THEN effective_snapshot_time END) AS under_snapshot_time,
            MAX(effective_snapshot_time) AS selected_snapshot_time
        FROM candidate_lines
        WHERE rn = 1
        GROUP BY player_id, game_id, bookmaker, market_key, line
        HAVING MAX(CASE WHEN outcome_label = 'Over' THEN odds_american END) IS NOT NULL
           AND MAX(CASE WHEN outcome_label = 'Under' THEN odds_american END) IS NOT NULL
           AND MAX(CASE WHEN outcome_label = 'Over' THEN effective_snapshot_time END)
               = MAX(CASE WHEN outcome_label = 'Under' THEN effective_snapshot_time END)
    """)

    with engine.connect() as conn:
        try:
            conn.execute(text("SET statement_timeout = '300000'"))
        except Exception:
            # Some tests/fake connections and non-Postgres engines do not support this.
            pass
        df = pd.read_sql(query, conn, params=params)

    if reverse_map and not df.empty and "market_key" in df.columns:
        df["market_key"] = df["market_key"].replace(reverse_map)
    return df
