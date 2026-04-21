#!/usr/bin/env python3
"""
Resolve User Paper Bets
=======================
Resolves pending user paper bets against actual game stats.

Runs daily at 9:30 AM ET after daily_stats_job completes.

Logic:
1. Fetch all user_bets where is_paper_trade=true, status='pending',
   game_date <= yesterday, player_id IS NOT NULL
2. Group by (stat_type, game_date)
3. Query actual stats from the appropriate source table
4. For each bet: compare actual_val vs line + bet_direction → won/lost/push
5. Compute PnL using American odds
6. Batch UPDATE user_bets with status/actual_value/pnl

Usage:
    python src/orchestration/resolve_user_paper_bets.py
    python src/orchestration/resolve_user_paper_bets.py --dry-run
"""

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

load_dotenv()

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "resolve_user_paper_bets.log"),
    ],
)
logger = logging.getLogger("ResolveUserPaperBets")

# Mapping from stat_type → (source_table, sql_expression_for_actual_value)
STAT_TO_SOURCE: dict[str, tuple[str, str]] = {
    # NBA
    "pts": ("player_game_stats", "pts"),
    "reb": ("player_game_stats", "reb"),
    "ast": ("player_game_stats", "ast"),
    "pra": ("player_game_stats", "pts + reb + ast"),
    "pr": ("player_game_stats", "pts + reb"),
    "pa": ("player_game_stats", "pts + ast"),
    "ra": ("player_game_stats", "reb + ast"),
    # MLB
    "pitcher_strikeouts": ("mlb_player_game_stats_pitching", "so"),
    "batter_hits": ("mlb_player_game_stats_batting", "h"),
    "batter_rbis": ("mlb_player_game_stats_batting", "rbi"),
    "batter_hrr": ("mlb_player_game_stats_batting", "h + r + rbi"),
}


def compute_pnl(stake: float, odds: float, outcome: str) -> float:
    """Compute P&L from American odds."""
    if outcome == "won":
        if odds > 0:
            return stake * (odds / 100.0)
        else:
            return stake * (100.0 / abs(odds))
    elif outcome == "lost":
        return -stake
    else:  # push
        return 0.0


def resolve_paper_bets(dry_run: bool = False) -> dict:
    """Main resolution logic."""
    from sqlalchemy import text
    from src.db.client import get_engine

    engine = get_engine()
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    with engine.connect() as conn:
        # Fetch pending paper bets from yesterday or earlier
        rows = conn.execute(
            text("""
                SELECT id, player_id, stat_type, game_date, line, bet_direction,
                       odds_at_bet, stake
                FROM user_bets
                WHERE is_paper_trade = true
                  AND status = 'pending'
                  AND game_date <= :yesterday
                  AND player_id IS NOT NULL
                ORDER BY game_date, stat_type
            """),
            {"yesterday": yesterday},
        ).fetchall()

    if not rows:
        logger.info("No pending paper bets to resolve.")
        return {"resolved": 0, "won": 0, "lost": 0, "push": 0}

    logger.info(f"Found {len(rows)} pending paper bet(s) to resolve.")

    # Group bets by (stat_type, game_date) for batch stats lookup
    groups: dict[tuple[str, str], list] = defaultdict(list)
    for row in rows:
        key = (row.stat_type, str(row.game_date))
        groups[key].append(row)

    # Results: list of (bet_id, status, actual_value, pnl)
    updates: list[tuple[int, str, float | None, float | None]] = []
    skipped = 0

    with engine.connect() as conn:
        for (stat_type, game_date), bets in groups.items():
            if stat_type not in STAT_TO_SOURCE:
                logger.warning(f"No source mapping for stat_type={stat_type!r}, skipping {len(bets)} bet(s)")
                skipped += len(bets)
                continue

            table, expr = STAT_TO_SOURCE[stat_type]
            player_ids = [b.player_id for b in bets]

            try:
                actual_rows = conn.execute(
                    text(f"""
                        SELECT player_id, ({expr}) AS actual_val
                        FROM {table}
                        WHERE game_date = :game_date
                          AND player_id = ANY(:player_ids)
                    """),
                    {"game_date": game_date, "player_ids": player_ids},
                ).fetchall()
            except Exception as e:
                logger.error(f"Failed to query {table} for {game_date} / {stat_type}: {e}")
                skipped += len(bets)
                continue

            actual_map: dict[int, float] = {
                r.player_id: float(r.actual_val)
                for r in actual_rows
                if r.actual_val is not None
            }

            for bet in bets:
                actual = actual_map.get(bet.player_id)
                if actual is None:
                    logger.debug(f"  No stats found for player_id={bet.player_id} on {game_date}, skipping bet {bet.id}")
                    skipped += 1
                    continue

                line = float(bet.line)
                direction = bet.bet_direction
                odds = float(bet.odds_at_bet) if bet.odds_at_bet is not None else -110.0
                stake = float(bet.stake) if bet.stake is not None else 0.0

                if actual > line:
                    raw_result = "over"
                elif actual < line:
                    raw_result = "under"
                else:
                    raw_result = "push"

                if raw_result == "push":
                    outcome = "push"
                elif direction == raw_result:
                    outcome = "won"
                else:
                    outcome = "lost"

                pnl = compute_pnl(stake, odds, outcome)
                updates.append((bet.id, outcome, actual, pnl))
                logger.debug(f"  Bet {bet.id}: {bet.player_id} {stat_type} {direction} {line} | actual={actual} → {outcome} (PnL={pnl:+.2f})")

    if not updates:
        logger.info(f"No bets could be resolved (skipped={skipped}).")
        return {"resolved": 0, "won": 0, "lost": 0, "push": 0}

    won = sum(1 for _, s, _, _ in updates if s == "won")
    lost = sum(1 for _, s, _, _ in updates if s == "lost")
    push = sum(1 for _, s, _, _ in updates if s == "push")

    logger.info(f"Resolved {len(updates)} bet(s): {won} won, {lost} lost, {push} push (skipped={skipped})")

    if dry_run:
        logger.info("[DRY RUN] Would update %d bets — no DB writes.", len(updates))
        return {"resolved": len(updates), "won": won, "lost": lost, "push": push}

    # Batch UPDATE
    with engine.begin() as conn:
        for bet_id, outcome, actual_val, pnl in updates:
            conn.execute(
                text("""
                    UPDATE user_bets
                    SET status = :status,
                        actual_value = :actual_value,
                        pnl = :pnl
                    WHERE id = :id
                """),
                {
                    "status": outcome,
                    "actual_value": actual_val,
                    "pnl": pnl,
                    "id": bet_id,
                },
            )

    logger.info(f"Done. Updated {len(updates)} paper bet(s) in user_bets.")
    return {"resolved": len(updates), "won": won, "lost": lost, "push": push}


def main():
    parser = argparse.ArgumentParser(description="Resolve pending user paper bets against actual game stats.")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be updated without writing to DB.")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Resolve User Paper Bets — Starting")
    logger.info("=" * 60)

    result = resolve_paper_bets(dry_run=args.dry_run)

    print(
        f"Resolved {result['resolved']} paper bets "
        f"({result['won']} won, {result['lost']} lost, {result['push']} push)"
    )


if __name__ == "__main__":
    main()
