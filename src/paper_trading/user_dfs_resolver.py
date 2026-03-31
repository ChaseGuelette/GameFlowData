#!/usr/bin/env python3
"""
User DFS Entry Resolver
========================
Resolves user-placed DFS parlay entries against actual game results.
Follows the same pattern as UserBetResolver but handles parlay (all-or-nothing)
resolution across multiple legs.

Called from daily_stats_job.py after user bet resolution.
"""

import logging
from datetime import date
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.paper_trading.paper_trader import PaperTrader

logger = logging.getLogger(__name__)


class UserDfsResolver:
    """Resolve user DFS entries from user_dfs_entries/user_dfs_legs tables."""

    def __init__(self):
        trader = PaperTrader()
        self.engine = trader.engine

    def resolve_entries_for_date(self, game_date: date) -> dict[str, Any]:
        """
        Resolve pending DFS entries for a single game_date using actual stats.

        Standard parlay resolution:
        - Any leg lost → entry lost, pnl = -stake
        - All active legs won → entry won, pnl = (stake * payout) - stake
        - All legs cancelled/pushed → entry cancelled, pnl = 0
        """
        # Get pending entries for this date
        entries_query = text("""
            SELECT id, stake, payout_multiplier
            FROM user_dfs_entries
            WHERE entry_date = :game_date AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            entries_df = pd.read_sql(entries_query, conn, params={"game_date": game_date})

        if entries_df.empty:
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        entry_ids = entries_df["id"].tolist()

        # Get all legs for these entries
        legs_query = text("""
            SELECT id, entry_id, player_id, stat_type, line, direction
            FROM user_dfs_legs
            WHERE entry_id = ANY(:entry_ids) AND status = 'pending'
        """)

        with self.engine.connect() as conn:
            legs_df = pd.read_sql(legs_query, conn, params={"entry_ids": entry_ids})

        if legs_df.empty:
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Get actual stats
        actuals_query = text("""
            SELECT
                pgs.player_id,
                pgs.pts,
                pgs.reb,
                pgs.ast,
                pgs.stl,
                pgs.blk,
                pgs.fg3m AS "3pm",
                pgs.min,
                pgs.did_not_play
            FROM player_game_stats pgs
            JOIN team_game_stats tgs ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
            WHERE tgs.game_date = :game_date
        """)

        with self.engine.connect() as conn:
            actuals_df = pd.read_sql(actuals_query, conn, params={"game_date": game_date})

        if actuals_df.empty:
            logger.info(f"No game stats available yet for {game_date} — skipping DFS entry resolution")
            return {"resolved": 0, "won": 0, "lost": 0, "push": 0, "cancelled": 0}

        # Build actuals lookup
        actuals_lookup: dict[int, dict[str, float | None]] = {}
        for _, row in actuals_df.iterrows():
            player_id = int(row["player_id"])
            did_not_play = row.get("did_not_play", False) or False
            minutes = row.get("min", None)
            zero_minutes = minutes is not None and (
                minutes == 0 or (pd.notna(minutes) and float(minutes) == 0)
            )

            if did_not_play or zero_minutes:
                actuals_lookup[player_id] = {
                    "pts": None, "reb": None, "ast": None,
                    "stl": None, "blk": None, "3pm": None,
                    "pra": None, "pr": None, "pa": None, "ra": None,
                }
            else:
                vals: dict[str, float | None] = {}
                for stat in ["pts", "reb", "ast", "stl", "blk", "3pm"]:
                    vals[stat] = float(row[stat]) if pd.notna(row.get(stat)) else None

                pts, reb, ast = vals["pts"], vals["reb"], vals["ast"]
                vals["pra"] = (pts + reb + ast) if all(v is not None for v in [pts, reb, ast]) else None
                vals["pr"] = (pts + reb) if all(v is not None for v in [pts, reb]) else None
                vals["pa"] = (pts + ast) if all(v is not None for v in [pts, ast]) else None
                vals["ra"] = (reb + ast) if all(v is not None for v in [reb, ast]) else None
                actuals_lookup[player_id] = vals

        # Resolve each leg
        leg_updates = []
        for _, leg in legs_df.iterrows():
            player_id = int(leg["player_id"])
            stat_type = leg["stat_type"]
            line = float(leg["line"])
            direction = leg["direction"]
            leg_id = int(leg["id"])

            player_actuals = actuals_lookup.get(player_id, {})
            actual = player_actuals.get(stat_type)

            if actual is None:
                status = "cancelled"
            elif actual > line:
                status = "won" if direction == "over" else "lost"
            elif actual < line:
                status = "won" if direction == "under" else "lost"
            else:
                status = "push"

            leg_updates.append({
                "leg_id": leg_id,
                "entry_id": int(leg["entry_id"]),
                "status": status,
                "actual_value": actual,
            })

        # Update legs
        leg_update_query = text("""
            UPDATE user_dfs_legs
            SET status = :status,
                actual_value = :actual_value
            WHERE id = :leg_id
        """)

        with self.engine.connect() as conn:
            for update in leg_updates:
                conn.execute(leg_update_query, {
                    "leg_id": update["leg_id"],
                    "status": update["status"],
                    "actual_value": update["actual_value"],
                })
            conn.commit()

        # Now resolve entries based on leg outcomes
        results = {"won": 0, "lost": 0, "push": 0, "cancelled": 0}
        entry_updates = []

        for _, entry_row in entries_df.iterrows():
            entry_id = int(entry_row["id"])
            stake = float(entry_row["stake"])
            payout_mult = float(entry_row["payout_multiplier"])

            entry_legs = [u for u in leg_updates if u["entry_id"] == entry_id]
            if not entry_legs:
                continue

            statuses = [u["status"] for u in entry_legs]
            won_count = statuses.count("won")
            lost_count = statuses.count("lost")
            push_count = statuses.count("push")
            cancelled_count = statuses.count("cancelled")

            # Standard parlay resolution
            if lost_count > 0:
                # Any leg lost → entry lost
                entry_status = "lost"
                pnl = -stake
                results["lost"] += 1
            elif won_count > 0 and lost_count == 0 and (push_count + cancelled_count + won_count == len(statuses)):
                if won_count == len(statuses):
                    # All legs won → entry won
                    entry_status = "won"
                    pnl = round(stake * payout_mult - stake, 2)
                    results["won"] += 1
                elif push_count + cancelled_count == len(statuses):
                    # All non-active → cancelled
                    entry_status = "cancelled"
                    pnl = 0.0
                    results["cancelled"] += 1
                else:
                    # Mix of won + push/cancelled (no losses) → treat as won
                    # (Standard DFS: cancelled/push legs are dropped, remaining must all win)
                    entry_status = "won"
                    pnl = round(stake * payout_mult - stake, 2)
                    results["won"] += 1
            elif push_count + cancelled_count == len(statuses):
                entry_status = "cancelled"
                pnl = 0.0
                results["cancelled"] += 1
            else:
                # Should not happen, but treat as push
                entry_status = "push"
                pnl = 0.0
                results["push"] += 1

            entry_updates.append({
                "entry_id": entry_id,
                "status": entry_status,
                "pnl": round(pnl, 2),
                "legs_won": won_count,
                "legs_lost": lost_count,
                "legs_push": push_count,
                "legs_cancelled": cancelled_count,
            })

        # Batch update entries
        entry_update_query = text("""
            UPDATE user_dfs_entries
            SET status = :status,
                pnl = :pnl,
                legs_won = :legs_won,
                legs_lost = :legs_lost,
                legs_push = :legs_push,
                legs_cancelled = :legs_cancelled,
                resolved_at = NOW()
            WHERE id = :entry_id
        """)

        with self.engine.connect() as conn:
            for update in entry_updates:
                conn.execute(entry_update_query, update)
            conn.commit()

        results["resolved"] = len(entry_updates)
        logger.info(
            f"DFS entries resolved for {game_date}: "
            f"{results['won']}W {results['lost']}L {results['push']}P {results['cancelled']}C"
        )
        return results

    def resolve_all_pending(self, exclude_today: bool = True) -> dict[str, Any]:
        """
        Resolve ALL pending DFS entries where game results are available.
        Handles multi-day catchup.
        """
        today = date.today()

        pending_query = text("""
            SELECT DISTINCT entry_date
            FROM user_dfs_entries
            WHERE status = 'pending'
            ORDER BY entry_date
        """)

        with self.engine.connect() as conn:
            result = conn.execute(pending_query)
            pending_dates = [row[0] for row in result]

        if not pending_dates:
            logger.info("No pending DFS entries to resolve")
            return {
                "dates_processed": 0,
                "dates_skipped": 0,
                "total_resolved": 0,
                "total_won": 0,
                "total_lost": 0,
                "total_push": 0,
                "total_cancelled": 0,
            }

        totals = {
            "dates_processed": 0,
            "dates_skipped": 0,
            "total_resolved": 0,
            "total_won": 0,
            "total_lost": 0,
            "total_push": 0,
            "total_cancelled": 0,
        }

        for game_date in pending_dates:
            if exclude_today and game_date >= today:
                totals["dates_skipped"] += 1
                continue

            result = self.resolve_entries_for_date(game_date)
            if result["resolved"] > 0:
                totals["dates_processed"] += 1
                totals["total_resolved"] += result["resolved"]
                totals["total_won"] += result["won"]
                totals["total_lost"] += result["lost"]
                totals["total_push"] += result["push"]
                totals["total_cancelled"] += result["cancelled"]

        logger.info(
            f"DFS entry resolution complete: {totals['total_resolved']} entries across "
            f"{totals['dates_processed']} dates "
            f"[{totals['total_won']}W {totals['total_lost']}L {totals['total_push']}P]"
        )
        return totals
