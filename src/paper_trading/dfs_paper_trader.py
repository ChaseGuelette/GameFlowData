"""
DFS Paper Trading Engine
========================
Builds multi-leg DFS entries (PrizePicks flex, Underdog standard) daily
using devigged sportsbook consensus (market edge) to find mispriced props.
Tracks P&L including partial flex payouts.

Entry structure: one entry per slip type per day (4 entries/day).
Bankroll: separate from prop trading ($100 start, $10/entry).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.db.client import get_engine

logger = logging.getLogger(__name__)
UTC = UTC

# ── Market key ↔ stat mapping ──────────────────────────────────────────────
MARKET_TO_STAT = {
    "player_points": "pts",
    "player_rebounds": "reb",
    "player_assists": "ast",
}

# ── DFS platform bookmaker keys ───────────────────────────────────────────
DFS_BOOKMAKERS = ("prizepicks", "underdog", "pick6", "betr_us_dfs")

# ── Slip type definitions ─────────────────────────────────────────────────
SLIP_TYPES: dict[str, dict[str, Any]] = {
    "ud_3_standard": {
        "legs": 3,
        "break_even": 0.550,
        "platform": "underdog",
        "payouts": {3: 6.0},
    },
    "ud_5_standard": {
        "legs": 5,
        "break_even": 0.549,
        "platform": "underdog",
        "payouts": {5: 20.0},
    },
    "pp_5_flex": {
        "legs": 5,
        "break_even": 0.5425,
        "platform": "prizepicks",
        "payouts": {5: 10.0, 4: 2.0, 3: 0.4},
    },
    "pp_6_flex": {
        "legs": 6,
        "break_even": 0.5421,
        "platform": "prizepicks",
        "payouts": {6: 25.0, 5: 2.0, 4: 0.4},
    },
}


def _odds_to_prob(odds: float) -> float | None:
    """Convert American odds to implied probability."""
    if pd.isna(odds):
        return None
    if odds > 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def _devig(over_odds: float, under_odds: float) -> tuple[float, float]:
    """Multiplicative devig: remove vig and return (prob_over, prob_under)."""
    raw_over = _odds_to_prob(over_odds)
    raw_under = _odds_to_prob(under_odds)
    if raw_over is None or raw_under is None:
        return (0.5, 0.5)
    booksum = raw_over + raw_under
    if booksum <= 0:
        return (0.5, 0.5)
    return (raw_over / booksum, raw_under / booksum)


@dataclass
class DfsPaperTrader:
    starting_bankroll: float = 500.0
    stake_per_entry: float = 10.0
    allow_live: bool = True

    def __post_init__(self):
        self.engine = get_engine()

    # ── Data fetching ─────────────────────────────────────────────────────

    def _fetch_dfs_lines(self, game_date: date) -> pd.DataFrame:
        """Fetch latest DFS platform lines for a given game date.

        Returns DataFrame with columns:
            player_id, game_id, bookmaker, market_key, stat_type,
            line, over_odds, under_odds, player_name
        """
        query = text("""
            WITH game_ids AS (
                SELECT DISTINCT game_id
                FROM raw_player_props_combined
                WHERE commence_time >= CAST(:game_date AS timestamptz)
                  AND commence_time < (CAST(:game_date AS date) + 1)::timestamptz
                  AND game_id IS NOT NULL
                  AND bookmaker IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
            ),
            latest AS (
                SELECT
                    rp.player_id,
                    rp.game_id,
                    rp.bookmaker,
                    rp.market_key,
                    rp.outcome_label,
                    rp.line,
                    rp.odds_american,
                    rp.snapshot_time,
                    rp.api_player_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY rp.player_id, rp.game_id, rp.bookmaker,
                                     rp.market_key, rp.outcome_label
                        ORDER BY rp.snapshot_time DESC
                    ) AS rn
                FROM raw_player_props_combined rp
                JOIN game_ids g ON (
                    rp.game_id = g.game_id
                    OR LPAD(rp.game_id, 10, '0') = g.game_id
                )
                WHERE rp.market_key IN ('player_points', 'player_rebounds', 'player_assists')
                  AND rp.bookmaker IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
                  AND rp.player_id IS NOT NULL
                  AND rp.snapshot_time > now() - interval '24 hours'
            )
            SELECT
                l.player_id,
                l.game_id,
                l.bookmaker,
                l.market_key,
                l.line,
                MAX(CASE WHEN l.outcome_label = 'Over'  THEN l.odds_american END) AS over_odds,
                MAX(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END) AS under_odds,
                MAX(l.snapshot_time) AS snapshot_time,
                MAX(l.api_player_name) AS player_name
            FROM latest l
            WHERE l.rn = 1
            GROUP BY l.player_id, l.game_id, l.bookmaker, l.market_key, l.line
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if not df.empty:
            df["stat_type"] = df["market_key"].map(MARKET_TO_STAT)
        return df

    def _fetch_sportsbook_lines(self, game_date: date) -> pd.DataFrame:
        """Fetch latest non-DFS sportsbook lines for a given game date.

        Returns DataFrame with columns:
            player_id, game_id, bookmaker, market_key, stat_type,
            line, over_odds, under_odds
        """
        query = text("""
            WITH game_ids AS (
                SELECT DISTINCT game_id
                FROM raw_player_props_combined
                WHERE commence_time >= CAST(:game_date AS timestamptz)
                  AND commence_time < (CAST(:game_date AS date) + 1)::timestamptz
                  AND game_id IS NOT NULL
                  AND bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
            ),
            latest AS (
                SELECT
                    rp.player_id,
                    rp.game_id,
                    rp.bookmaker,
                    rp.market_key,
                    rp.outcome_label,
                    rp.line,
                    rp.odds_american,
                    rp.snapshot_time,
                    ROW_NUMBER() OVER (
                        PARTITION BY rp.player_id, rp.game_id, rp.bookmaker,
                                     rp.market_key, rp.outcome_label
                        ORDER BY rp.snapshot_time DESC
                    ) AS rn
                FROM raw_player_props_combined rp
                JOIN game_ids g ON (
                    rp.game_id = g.game_id
                    OR LPAD(rp.game_id, 10, '0') = g.game_id
                )
                WHERE rp.market_key IN ('player_points', 'player_rebounds', 'player_assists')
                  AND rp.bookmaker NOT IN ('prizepicks', 'underdog', 'pick6', 'betr_us_dfs')
                  AND rp.player_id IS NOT NULL
                  AND rp.snapshot_time > now() - interval '24 hours'
            )
            SELECT
                l.player_id,
                l.game_id,
                l.bookmaker,
                l.market_key,
                l.line,
                MAX(CASE WHEN l.outcome_label = 'Over'  THEN l.odds_american END) AS over_odds,
                MAX(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END) AS under_odds,
                MAX(l.snapshot_time) AS snapshot_time
            FROM latest l
            WHERE l.rn = 1
            GROUP BY l.player_id, l.game_id, l.bookmaker, l.market_key, l.line
            HAVING MAX(CASE WHEN l.outcome_label = 'Over'  THEN l.odds_american END) IS NOT NULL
               AND MAX(CASE WHEN l.outcome_label = 'Under' THEN l.odds_american END) IS NOT NULL
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"game_date": game_date})

        if not df.empty:
            df["stat_type"] = df["market_key"].map(MARKET_TO_STAT)
        return df

    # ── Market edge computation (port of TS logic) ────────────────────────

    def _compute_market_edges(
        self,
        dfs_lines: pd.DataFrame,
        sb_lines: pd.DataFrame,
        break_even: float,
    ) -> pd.DataFrame:
        """Compute market edge for each DFS line using devigged sportsbook consensus.

        For each DFS line, finds sportsbooks offering the exact same line value,
        devigs each match via multiplicative method, averages to consensus prob.
        Edge = consensus_prob - break_even.

        Returns DataFrame with columns:
            player_id, player_name, game_id, stat_type, dfs_bookmaker,
            line, direction, market_prob, market_books, edge
        """
        if dfs_lines.empty:
            return pd.DataFrame()

        # Index sportsbook lines by (player_id, game_id, stat_type)
        sb_index: dict[tuple, list[dict]] = {}
        for _, row in sb_lines.iterrows():
            key = (int(row["player_id"]), str(row["game_id"]), row["stat_type"])
            sb_index.setdefault(key, []).append(row.to_dict())

        results = []

        for _, dfs_row in dfs_lines.iterrows():
            player_id = int(dfs_row["player_id"])
            game_id = str(dfs_row["game_id"])
            stat = dfs_row["stat_type"]
            dfs_line = float(dfs_row["line"])
            dfs_bookmaker = dfs_row["bookmaker"]
            player_name = dfs_row.get("player_name", f"Player {player_id}")

            key = (player_id, game_id, stat)
            sb_for_player = sb_index.get(key, [])

            # Also check padded/unpadded game_id variants
            game_id_padded = game_id.zfill(10)
            game_id_stripped = game_id.lstrip("0")
            for alt_gid in (game_id_padded, game_id_stripped):
                alt_key = (player_id, alt_gid, stat)
                if alt_key != key and alt_key in sb_index:
                    sb_for_player = sb_for_player + sb_index[alt_key]

            # Find sportsbooks offering the exact same line value
            matching_books = [
                sb for sb in sb_for_player
                if float(sb["line"]) == dfs_line
                and sb["over_odds"] is not None
                and sb["under_odds"] is not None
                and not pd.isna(sb["over_odds"])
                and not pd.isna(sb["under_odds"])
            ]

            if not matching_books:
                continue

            # Devig each match → consensus probability
            sum_over = 0.0
            sum_under = 0.0
            for mb in matching_books:
                dev_over, dev_under = _devig(float(mb["over_odds"]), float(mb["under_odds"]))
                sum_over += dev_over
                sum_under += dev_under

            market_prob_over = sum_over / len(matching_books)
            market_prob_under = sum_under / len(matching_books)

            # Best direction = higher consensus probability
            if market_prob_over >= market_prob_under:
                direction = "over"
                market_prob = market_prob_over
            else:
                direction = "under"
                market_prob = market_prob_under

            edge = market_prob - break_even

            results.append({
                "player_id": player_id,
                "player_name": player_name,
                "game_id": game_id,
                "stat_type": stat,
                "dfs_bookmaker": dfs_bookmaker,
                "line": dfs_line,
                "direction": direction,
                "market_prob": round(market_prob, 5),
                "market_books": len(matching_books),
                "edge": round(edge, 5),
            })

        return pd.DataFrame(results) if results else pd.DataFrame()

    # ── Entry selection ───────────────────────────────────────────────────

    def _get_started_game_ids(self) -> set[str]:
        """Get game_ids for games that have already started."""
        query = text("""
            SELECT DISTINCT game_id, commence_time
            FROM raw_player_props_combined
            WHERE game_id IS NOT NULL
              AND commence_time IS NOT NULL
              AND commence_time < :now
              AND snapshot_time > :cutoff
        """)
        now = datetime.now(UTC)
        cutoff = now - pd.Timedelta(hours=24)
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"now": now, "cutoff": cutoff}).fetchall()
            started = set()
            for row in rows:
                gid = str(row[0]).zfill(10)
                started.add(gid)
                started.add(gid.lstrip("0"))
            return started
        except Exception as e:
            logger.warning(f"Could not check game start times: {e}")
            return set()

    def _select_entry_legs(
        self, edges_df: pd.DataFrame, slip_type: str,
    ) -> list[dict]:
        """Select legs for a single entry (slip).

        - Filter to positive edge only
        - Platform preference: UD slips prefer Underdog lines, PP prefer PrizePicks
        - Deduplicate: one leg per player (highest edge wins)
        - Exclude started games
        - Take top N by edge (N = required legs for slip type)
        """
        if edges_df.empty:
            return []

        cfg = SLIP_TYPES[slip_type]
        required_legs = cfg["legs"]
        preferred_platform = cfg["platform"]

        # Filter to positive edge
        df = edges_df[edges_df["edge"] > 0].copy()
        if df.empty:
            return []

        # Exclude started games (unless allow_live is True)
        if not self.allow_live:
            started = self._get_started_game_ids()
            if started:
                df = df[~df["game_id"].apply(
                    lambda gid: str(gid) in started or str(gid).zfill(10) in started or str(gid).lstrip("0") in started
                )]
            if df.empty:
                return []

        # Platform preference scoring (tiebreaker, not hard filter)
        df["platform_bonus"] = df["dfs_bookmaker"].apply(
            lambda b: 0.001 if b == preferred_platform else 0.0
        )
        df["sort_score"] = df["edge"] + df["platform_bonus"]

        # Deduplicate: one leg per player (highest sort_score wins)
        df = df.sort_values("sort_score", ascending=False)
        df = df.drop_duplicates(subset=["player_id"], keep="first")

        # Take top N by sort_score
        df = df.head(required_legs)

        if len(df) < required_legs:
            return []  # Insufficient legs

        return df.drop(columns=["platform_bonus", "sort_score"]).to_dict("records")

    # ── Build & place entries ─────────────────────────────────────────────

    def build_entries(self, game_date: date) -> list[dict]:
        """Orchestrator: fetch lines, compute edges, select legs for each slip type.

        Returns list of entry dicts, each with metadata and legs.
        """
        logger.info(f"Building DFS entries for {game_date}...")

        dfs_lines = self._fetch_dfs_lines(game_date)
        if dfs_lines.empty:
            logger.info("No DFS lines found for date")
            return []

        sb_lines = self._fetch_sportsbook_lines(game_date)
        if sb_lines.empty:
            logger.info("No sportsbook lines found for date")
            return []

        logger.info(f"Fetched {len(dfs_lines)} DFS lines, {len(sb_lines)} sportsbook lines")

        entries = []
        for slip_type, cfg in SLIP_TYPES.items():
            edges_df = self._compute_market_edges(dfs_lines, sb_lines, cfg["break_even"])

            if edges_df.empty:
                logger.info(f"  {slip_type}: no market edges found")
                continue

            positive = len(edges_df[edges_df["edge"] > 0])
            logger.info(f"  {slip_type}: {len(edges_df)} edges computed, {positive} positive")

            legs = self._select_entry_legs(edges_df, slip_type)
            if not legs:
                logger.info(f"  {slip_type}: insufficient legs ({cfg['legs']} needed)")
                continue

            edges_in_entry = [leg["edge"] for leg in legs]
            entry = {
                "entry_date": game_date,
                "slip_type": slip_type,
                "platform": cfg["platform"],
                "num_legs": len(legs),
                "stake": self.stake_per_entry,
                "avg_edge": round(sum(edges_in_entry) / len(edges_in_entry), 5),
                "min_edge": round(min(edges_in_entry), 5),
                "legs": legs,
            }
            entries.append(entry)
            logger.info(
                f"  {slip_type}: built entry with {len(legs)} legs "
                f"(avg edge {entry['avg_edge']:.3f}, min edge {entry['min_edge']:.3f})"
            )

        logger.info(f"Built {len(entries)} DFS entries for {game_date}")
        return entries

    def place_entries(self, entries: list[dict]) -> int:
        """UPSERT entries + legs into database.

        Uses ON CONFLICT (entry_date, slip_type) DO UPDATE WHERE status = 'pending'
        for idempotency.
        """
        if not entries:
            return 0

        placed = 0
        with self.engine.begin() as conn:
            for entry in entries:
                # UPSERT entry
                result = conn.execute(
                    text("""
                        INSERT INTO dfs_paper_entries (
                            entry_date, slip_type, platform, num_legs, stake,
                            avg_edge, min_edge, status
                        ) VALUES (
                            :entry_date, :slip_type, :platform, :num_legs, :stake,
                            :avg_edge, :min_edge, 'pending'
                        )
                        ON CONFLICT (entry_date, slip_type) DO UPDATE SET
                            platform = EXCLUDED.platform,
                            num_legs = EXCLUDED.num_legs,
                            stake = EXCLUDED.stake,
                            avg_edge = EXCLUDED.avg_edge,
                            min_edge = EXCLUDED.min_edge,
                            created_at = NOW()
                        WHERE dfs_paper_entries.status = 'pending'
                        RETURNING id
                    """),
                    {
                        "entry_date": entry["entry_date"],
                        "slip_type": entry["slip_type"],
                        "platform": entry["platform"],
                        "num_legs": entry["num_legs"],
                        "stake": entry["stake"],
                        "avg_edge": entry["avg_edge"],
                        "min_edge": entry["min_edge"],
                    },
                )
                row = result.fetchone()
                if row is None:
                    # Entry exists and is already resolved, skip
                    logger.info(f"  {entry['slip_type']}: already resolved, skipping")
                    continue

                entry_id = row[0]

                # Delete old legs for this entry (in case of re-place)
                conn.execute(
                    text("DELETE FROM dfs_paper_legs WHERE entry_id = :entry_id"),
                    {"entry_id": entry_id},
                )

                # Insert legs
                for leg in entry["legs"]:
                    conn.execute(
                        text("""
                            INSERT INTO dfs_paper_legs (
                                entry_id, player_id, player_name, game_id,
                                stat_type, line, direction, dfs_bookmaker,
                                market_prob, market_books, edge, status
                            ) VALUES (
                                :entry_id, :player_id, :player_name, :game_id,
                                :stat_type, :line, :direction, :dfs_bookmaker,
                                :market_prob, :market_books, :edge, 'pending'
                            )
                            ON CONFLICT (entry_id, player_id) DO UPDATE SET
                                player_name = EXCLUDED.player_name,
                                game_id = EXCLUDED.game_id,
                                stat_type = EXCLUDED.stat_type,
                                line = EXCLUDED.line,
                                direction = EXCLUDED.direction,
                                dfs_bookmaker = EXCLUDED.dfs_bookmaker,
                                market_prob = EXCLUDED.market_prob,
                                market_books = EXCLUDED.market_books,
                                edge = EXCLUDED.edge,
                                status = 'pending'
                        """),
                        {
                            "entry_id": entry_id,
                            "player_id": leg["player_id"],
                            "player_name": leg.get("player_name", f"Player {leg['player_id']}"),
                            "game_id": leg.get("game_id"),
                            "stat_type": leg["stat_type"],
                            "line": leg["line"],
                            "direction": leg["direction"],
                            "dfs_bookmaker": leg["dfs_bookmaker"],
                            "market_prob": leg.get("market_prob"),
                            "market_books": leg.get("market_books", 0),
                            "edge": leg.get("edge"),
                        },
                    )

                placed += 1
                logger.info(
                    f"  Placed {entry['slip_type']}: {entry['num_legs']} legs "
                    f"(entry_id={entry_id})"
                )

        return placed

    # ── Resolution ────────────────────────────────────────────────────────

    def resolve_entries(self, game_date: date) -> dict:
        """Resolve all pending entries for a given date using actual game stats.

        Each leg: actual vs line → won/lost/push/cancelled.
        Pushes/cancellations reduce effective entry (don't count).
        Looks up payouts[legs_won] for multiplier.
        P&L = (stake * multiplier) - stake.
        """
        # Get pending entries for date
        with self.engine.connect() as conn:
            entries = conn.execute(
                text("""
                    SELECT id, slip_type, stake
                    FROM dfs_paper_entries
                    WHERE entry_date = :game_date AND status = 'pending'
                """),
                {"game_date": game_date},
            ).fetchall()

        if not entries:
            return {"resolved": 0, "won": 0, "lost": 0, "partial": 0, "cancelled": 0}

        # Fetch actuals from player_game_stats
        with self.engine.connect() as conn:
            actuals_rows = conn.execute(
                text("""
                    SELECT
                        pgs.player_id,
                        pgs.pts, pgs.reb, pgs.ast,
                        pgs.did_not_play,
                        pgs.min AS minutes
                    FROM player_game_stats pgs
                    WHERE pgs.game_date = :game_date
                """),
                {"game_date": game_date},
            ).fetchall()

        if not actuals_rows:
            return {"resolved": 0, "won": 0, "lost": 0, "partial": 0, "cancelled": 0}

        # Build actuals lookup: player_id -> {pts, reb, ast} or None if DNP
        actuals: dict[int, dict[str, float | None]] = {}
        for row in actuals_rows:
            pid = int(row[0])
            dnp = row[4]
            minutes = row[5]
            if dnp or (minutes is not None and float(minutes) == 0):
                actuals[pid] = {"pts": None, "reb": None, "ast": None}
            else:
                actuals[pid] = {
                    "pts": float(row[1]) if row[1] is not None else None,
                    "reb": float(row[2]) if row[2] is not None else None,
                    "ast": float(row[3]) if row[3] is not None else None,
                }

        stats = {"resolved": 0, "won": 0, "lost": 0, "partial": 0, "cancelled": 0}

        with self.engine.begin() as conn:
            for entry_row in entries:
                entry_id = entry_row[0]
                slip_type = entry_row[1]
                stake = float(entry_row[2])
                cfg = SLIP_TYPES.get(slip_type)
                if cfg is None:
                    continue

                # Get legs for this entry
                legs = conn.execute(
                    text("""
                        SELECT id, player_id, stat_type, line, direction
                        FROM dfs_paper_legs
                        WHERE entry_id = :entry_id
                    """),
                    {"entry_id": entry_id},
                ).fetchall()

                legs_won = 0
                legs_lost = 0
                legs_push = 0
                legs_cancelled = 0

                for leg in legs:
                    leg_id = leg[0]
                    player_id = int(leg[1])
                    stat_type = leg[2]
                    line_val = float(leg[3])
                    direction = leg[4]

                    player_actuals = actuals.get(player_id)

                    if player_actuals is None:
                        # No stats available for player — likely game not played yet
                        # Don't resolve this leg
                        continue

                    actual_val = player_actuals.get(stat_type)

                    if actual_val is None:
                        # DNP or 0 minutes
                        leg_status = "cancelled"
                        legs_cancelled += 1
                    elif actual_val == line_val:
                        leg_status = "push"
                        legs_push += 1
                    elif direction == "over" and actual_val > line_val:
                        leg_status = "won"
                        legs_won += 1
                    elif direction == "under" and actual_val < line_val:
                        leg_status = "won"
                        legs_won += 1
                    else:
                        leg_status = "lost"
                        legs_lost += 1

                    conn.execute(
                        text("""
                            UPDATE dfs_paper_legs
                            SET status = :status, actual_value = :actual_value
                            WHERE id = :id
                        """),
                        {
                            "status": leg_status,
                            "actual_value": actual_val,
                            "id": leg_id,
                        },
                    )

                # Check if all legs resolved (no remaining pending legs)
                total_resolved_legs = legs_won + legs_lost + legs_push + legs_cancelled
                if total_resolved_legs < len(legs):
                    # Some legs don't have stats yet, skip this entry
                    continue

                # Determine payout multiplier
                # Pushes and cancellations don't count — reduce effective entry
                payouts = cfg["payouts"]
                multiplier = payouts.get(legs_won, 0.0)
                pnl = (stake * multiplier) - stake

                # Determine entry status
                if legs_won + legs_lost == 0:
                    entry_status = "cancelled"
                    pnl = 0.0
                    stats["cancelled"] += 1
                elif multiplier > 1:
                    entry_status = "won"
                    stats["won"] += 1
                elif 0 < multiplier <= 1:
                    entry_status = "partial"
                    stats["partial"] += 1
                else:
                    entry_status = "lost"
                    stats["lost"] += 1

                conn.execute(
                    text("""
                        UPDATE dfs_paper_entries
                        SET status = :status,
                            legs_won = :legs_won,
                            legs_lost = :legs_lost,
                            legs_push = :legs_push,
                            legs_cancelled = :legs_cancelled,
                            payout_multiplier = :multiplier,
                            pnl = :pnl,
                            resolved_at = NOW()
                        WHERE id = :id
                    """),
                    {
                        "status": entry_status,
                        "legs_won": legs_won,
                        "legs_lost": legs_lost,
                        "legs_push": legs_push,
                        "legs_cancelled": legs_cancelled,
                        "multiplier": multiplier,
                        "pnl": pnl,
                        "id": entry_id,
                    },
                )
                stats["resolved"] += 1

        # Update daily log
        self._update_daily_log(game_date)
        return stats

    def resolve_all_pending(self, exclude_today: bool = True) -> dict:
        """Resolve all pending entries across all dates (multi-day catchup)."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT DISTINCT entry_date
                    FROM dfs_paper_entries
                    WHERE status = 'pending'
                    ORDER BY entry_date
                """)
            ).fetchall()

        dates = [row[0] for row in rows]
        if exclude_today:
            today = date.today()
            dates = [d for d in dates if d < today]

        if not dates:
            return {
                "dates_processed": 0, "dates_skipped": 0,
                "total_resolved": 0, "total_won": 0, "total_lost": 0,
                "total_partial": 0, "total_cancelled": 0,
                "by_date": {},
            }

        agg = {
            "dates_processed": 0, "dates_skipped": 0,
            "total_resolved": 0, "total_won": 0, "total_lost": 0,
            "total_partial": 0, "total_cancelled": 0,
            "by_date": {},
        }

        for game_date in dates:
            # Check if stats available
            with self.engine.connect() as conn:
                has_stats = conn.execute(
                    text("SELECT COUNT(*) FROM team_game_stats WHERE game_date = :gd"),
                    {"gd": game_date},
                ).scalar()

            if not has_stats:
                agg["dates_skipped"] += 1
                agg["by_date"][str(game_date)] = {"skipped": True}
                continue

            result = self.resolve_entries(game_date)
            agg["dates_processed"] += 1
            agg["total_resolved"] += result["resolved"]
            agg["total_won"] += result["won"]
            agg["total_lost"] += result["lost"]
            agg["total_partial"] += result["partial"]
            agg["total_cancelled"] += result["cancelled"]
            agg["by_date"][str(game_date)] = result

            if result["resolved"] > 0:
                logger.info(
                    f"  {game_date}: resolved {result['resolved']} entries "
                    f"({result['won']}W {result['lost']}L {result['partial']}P)"
                )

        return agg

    # ── Daily log ─────────────────────────────────────────────────────────

    def _update_daily_log(self, game_date: date) -> None:
        """Aggregate and UPSERT daily metrics + cumulative P&L."""
        with self.engine.begin() as conn:
            # Aggregate today's entries
            row = conn.execute(
                text("""
                    SELECT
                        COUNT(*) AS entries_placed,
                        COUNT(*) FILTER (WHERE status = 'won') AS entries_won,
                        COUNT(*) FILTER (WHERE status = 'lost') AS entries_lost,
                        COUNT(*) FILTER (WHERE status = 'partial') AS entries_partial,
                        COALESCE(SUM(stake) FILTER (WHERE status IN ('won', 'lost', 'partial')), 0) AS total_staked,
                        COALESCE(SUM(pnl), 0) AS total_pnl
                    FROM dfs_paper_entries
                    WHERE entry_date = :game_date
                """),
                {"game_date": game_date},
            ).fetchone()

            entries_placed = int(row[0])
            entries_won = int(row[1])
            entries_lost = int(row[2])
            entries_partial = int(row[3])
            total_staked = float(row[4])
            total_pnl = float(row[5])
            roi_pct = (total_pnl / total_staked * 100) if total_staked > 0 else 0.0

            # Get previous cumulative P&L
            prev = conn.execute(
                text("""
                    SELECT cumulative_pnl, bankroll_after
                    FROM dfs_paper_daily_log
                    WHERE entry_date < :game_date
                    ORDER BY entry_date DESC
                    LIMIT 1
                """),
                {"game_date": game_date},
            ).fetchone()

            if prev:
                prev_cumulative = float(prev[0])
                prev_bankroll = float(prev[1])
            else:
                prev_cumulative = 0.0
                prev_bankroll = self.starting_bankroll

            cumulative_pnl = prev_cumulative + total_pnl
            bankroll_after = prev_bankroll + total_pnl

            # UPSERT daily log
            conn.execute(
                text("""
                    INSERT INTO dfs_paper_daily_log (
                        entry_date, entries_placed, entries_won, entries_lost,
                        entries_partial, total_staked, total_pnl, roi_pct,
                        cumulative_pnl, bankroll_after
                    ) VALUES (
                        :entry_date, :entries_placed, :entries_won, :entries_lost,
                        :entries_partial, :total_staked, :total_pnl, :roi_pct,
                        :cumulative_pnl, :bankroll_after
                    )
                    ON CONFLICT (entry_date) DO UPDATE SET
                        entries_placed = EXCLUDED.entries_placed,
                        entries_won = EXCLUDED.entries_won,
                        entries_lost = EXCLUDED.entries_lost,
                        entries_partial = EXCLUDED.entries_partial,
                        total_staked = EXCLUDED.total_staked,
                        total_pnl = EXCLUDED.total_pnl,
                        roi_pct = EXCLUDED.roi_pct,
                        cumulative_pnl = EXCLUDED.cumulative_pnl,
                        bankroll_after = EXCLUDED.bankroll_after
                """),
                {
                    "entry_date": game_date,
                    "entries_placed": entries_placed,
                    "entries_won": entries_won,
                    "entries_lost": entries_lost,
                    "entries_partial": entries_partial,
                    "total_staked": total_staked,
                    "total_pnl": total_pnl,
                    "roi_pct": roi_pct,
                    "cumulative_pnl": cumulative_pnl,
                    "bankroll_after": bankroll_after,
                },
            )

    def _get_current_bankroll(self) -> float:
        """Get current bankroll from latest daily log entry."""
        with self.engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT bankroll_after
                    FROM dfs_paper_daily_log
                    ORDER BY entry_date DESC
                    LIMIT 1
                """)
            ).fetchone()
        return float(row[0]) if row else self.starting_bankroll
