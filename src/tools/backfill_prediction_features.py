#!/usr/bin/env python3
"""
Backfill feat_* columns for historical predictions.

This script populates the insight feature columns (feat_rest_days, feat_team_out_count, etc.)
for predictions that were generated before G2/G3 implementation.

It does NOT modify any prediction values (quantiles, edges, probabilities) - only the feat_* columns.

Usage:
    # Backfill a single date (recommended for testing)
    python src/tools/backfill_prediction_features.py --date 2026-02-13

    # Backfill a date range
    python src/tools/backfill_prediction_features.py --start 2026-02-01 --end 2026-02-13

    # Dry run (show what would be updated without making changes)
    python src/tools/backfill_prediction_features.py --date 2026-02-13 --dry-run
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

import pandas as pd
from sqlalchemy import bindparam, text

from src.db.client import get_engine
from src.models.feature_store import FeatureStore

# NBA team ID to abbreviation map (same as dashboard)
TEAM_ABBREV = {
    1610612737: 'ATL', 1610612738: 'BOS', 1610612751: 'BKN',
    1610612766: 'CHA', 1610612741: 'CHI', 1610612739: 'CLE',
    1610612742: 'DAL', 1610612743: 'DEN', 1610612765: 'DET',
    1610612744: 'GSW', 1610612745: 'HOU', 1610612754: 'IND',
    1610612746: 'LAC', 1610612747: 'LAL', 1610612763: 'MEM',
    1610612748: 'MIA', 1610612749: 'MIL', 1610612750: 'MIN',
    1610612740: 'NOP', 1610612752: 'NYK', 1610612760: 'OKC',
    1610612753: 'ORL', 1610612755: 'PHI', 1610612756: 'PHX',
    1610612757: 'POR', 1610612758: 'SAC', 1610612759: 'SAS',
    1610612761: 'TOR', 1610612762: 'UTA', 1610612764: 'WAS',
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def get_predictions_for_date(engine, target_date: date) -> pd.DataFrame:
    """Get all predictions for a date that need feature backfill."""
    query = text("""
        SELECT DISTINCT
            dp.player_id,
            dp.game_id,
            dp.stat,
            dp.opponent_id,
            p.player_name,
            -- Check if features are already populated
            dp.feat_rest_days IS NULL as needs_backfill
        FROM daily_predictions dp
        JOIN players p ON dp.player_id = p.player_id
        WHERE dp.prediction_date = :target_date
          AND dp.feat_rest_days IS NULL  -- Only get rows needing backfill
        ORDER BY dp.player_id, dp.stat
    """)

    with engine.connect() as conn:
        result = pd.read_sql(query, conn, params={"target_date": target_date})

    return result


def get_team_info_for_predictions(engine, predictions_df: pd.DataFrame) -> dict:
    """Get team_id and is_home for each player/game combination."""
    if predictions_df.empty:
        return {}

    # Get unique player/game combinations
    player_game_pairs = predictions_df[["player_id", "game_id"]].drop_duplicates()

    query = text("""
        SELECT
            pgs.player_id,
            pgs.game_id,
            pgs.team_id,
            CASE WHEN pgs.matchup LIKE '%vs.%' THEN true ELSE false END as is_home
        FROM player_game_stats pgs
        WHERE (pgs.player_id, pgs.game_id) IN :pairs
    """).bindparams(bindparam("pairs", expanding=True))

    pairs = [tuple(row) for row in player_game_pairs.values]

    with engine.connect() as conn:
        result = conn.execute(query, {"pairs": pairs})
        return {
            (row[0], row[1]): {"team_id": row[2], "is_home": row[3]}
            for row in result
        }


def get_opponent_abbrevs(opponent_ids: list) -> dict:
    """Get team abbreviations for opponent_ids using hardcoded map."""
    return {int(tid): TEAM_ABBREV.get(int(tid), 'UNK') for tid in opponent_ids if tid}


def build_features_for_predictions(
    feature_store: FeatureStore,
    predictions_df: pd.DataFrame,
    team_info: dict,
    target_date: date,
) -> dict:
    """Build features for each unique player/game combination."""
    features_map = {}

    # Get unique player/game combinations
    unique_players = predictions_df[["player_id", "game_id", "player_name", "opponent_id"]].drop_duplicates()

    for _, row in unique_players.iterrows():
        player_id = row["player_id"]
        game_id = row["game_id"]
        opponent_id = row["opponent_id"]

        info = team_info.get((player_id, game_id), {})
        team_id = info.get("team_id")
        is_home = info.get("is_home")

        try:
            features = feature_store.get_player_game_features(
                player_id=player_id,
                game_id=game_id,
                as_of_date=target_date,
                team_id=team_id,
                opponent_id=opponent_id,
                is_home=is_home,
            )
            if features:
                features_map[(player_id, game_id)] = features
        except Exception as e:
            logger.warning(f"Failed to build features for player {player_id}, game {game_id}: {e}")

    return features_map


def update_prediction_features(
    engine,
    target_date: date,
    predictions_df: pd.DataFrame,
    features_map: dict,
    opp_abbrev_map: dict,
    dry_run: bool = False,
) -> int:
    """Update feat_* columns for predictions."""
    updates = []

    for _, row in predictions_df.iterrows():
        player_id = row["player_id"]
        game_id = row["game_id"]
        stat = row["stat"]
        opponent_id = row["opponent_id"]

        features = features_map.get((player_id, game_id))
        if not features:
            continue

        # Build update values
        update = {
            "player_id": player_id,
            "game_id": game_id,
            "stat": stat,
            "feat_rest_days": int(features.get("rest_days", 0) or 0),
            "feat_is_back_to_back": bool(features.get("is_back_to_back", 0)),
            "feat_games_last_7d": int(features.get("games_in_last_7_days", 0) or 0),
            "feat_team_out_count": int(features.get("team_out_count", 0) or 0),
            "feat_team_out_min_sum": float(features.get("team_out_min_sum", 0) or 0),
            "feat_opp_out_count": int(features.get("opp_out_count", 0) or 0),
            "feat_player_is_questionable": bool(features.get("player_is_questionable", 0)),
            "feat_player_is_probable": bool(features.get("player_is_probable", 0)),
            "feat_opp_abbrev": opp_abbrev_map.get(opponent_id) if opponent_id else None,
        }

        # Stat-specific averages
        if stat in ("pts", "reb", "ast"):
            s = stat
            update["feat_player_avg_stat_l3"] = features.get(f"player_avg_{s}_l3")
            update["feat_player_avg_stat_l5"] = features.get(f"player_avg_{s}_l5")
            update["feat_player_avg_stat_l15"] = features.get(f"player_avg_{s}_l15")
            update["feat_stat_l3_l15_ratio"] = features.get(f"player_{s}_l3_l15_ratio")
            update["feat_stat_std_l5"] = features.get(f"player_std_{s}_l5")

        updates.append(update)

    if dry_run:
        logger.info(f"[DRY RUN] Would update {len(updates)} prediction rows")
        if updates:
            sample = updates[0]
            logger.info(f"Sample update: player_id={sample['player_id']}, stat={sample['stat']}")
            logger.info(f"  feat_rest_days={sample['feat_rest_days']}, feat_is_back_to_back={sample['feat_is_back_to_back']}")
            logger.info(f"  feat_team_out_count={sample['feat_team_out_count']}, feat_opp_out_count={sample['feat_opp_out_count']}")
        return 0

    if not updates:
        logger.info("No updates to apply")
        return 0

    # Execute updates in batches
    update_query = text("""
        UPDATE daily_predictions SET
            feat_rest_days = :feat_rest_days,
            feat_is_back_to_back = :feat_is_back_to_back,
            feat_games_last_7d = :feat_games_last_7d,
            feat_team_out_count = :feat_team_out_count,
            feat_team_out_min_sum = :feat_team_out_min_sum,
            feat_opp_out_count = :feat_opp_out_count,
            feat_player_is_questionable = :feat_player_is_questionable,
            feat_player_is_probable = :feat_player_is_probable,
            feat_player_avg_stat_l3 = :feat_player_avg_stat_l3,
            feat_player_avg_stat_l5 = :feat_player_avg_stat_l5,
            feat_player_avg_stat_l15 = :feat_player_avg_stat_l15,
            feat_stat_l3_l15_ratio = :feat_stat_l3_l15_ratio,
            feat_stat_std_l5 = :feat_stat_std_l5,
            feat_opp_abbrev = :feat_opp_abbrev
        WHERE prediction_date = :prediction_date
          AND player_id = :player_id
          AND game_id = :game_id
          AND stat = :stat
    """)

    updated_count = 0
    with engine.begin() as conn:
        for update in updates:
            update["prediction_date"] = target_date
            conn.execute(update_query, update)
            updated_count += 1

    return updated_count


def backfill_date(engine, feature_store: FeatureStore, target_date: date, dry_run: bool = False) -> int:
    """Backfill features for a single date."""
    logger.info(f"Backfilling features for {target_date}")

    # 1. Get predictions needing backfill
    predictions_df = get_predictions_for_date(engine, target_date)
    if predictions_df.empty:
        logger.info(f"No predictions need backfill for {target_date}")
        return 0

    logger.info(f"Found {len(predictions_df)} predictions needing backfill")

    # 2. Get team info for feature building
    team_info = get_team_info_for_predictions(engine, predictions_df)

    # 3. Build features
    features_map = build_features_for_predictions(feature_store, predictions_df, team_info, target_date)
    logger.info(f"Built features for {len(features_map)} player/game combinations")

    # 4. Get opponent abbreviations
    opp_ids = predictions_df["opponent_id"].dropna().unique().tolist()
    opp_abbrev_map = get_opponent_abbrevs(opp_ids)

    # 5. Update predictions
    updated = update_prediction_features(
        engine, target_date, predictions_df, features_map, opp_abbrev_map, dry_run
    )

    if not dry_run:
        logger.info(f"Updated {updated} predictions for {target_date}")

    return updated


def main():
    parser = argparse.ArgumentParser(description="Backfill feat_* columns for historical predictions")
    parser.add_argument("--date", type=str, help="Single date to backfill (YYYY-MM-DD)")
    parser.add_argument("--start", type=str, help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date for range (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")

    args = parser.parse_args()

    if not args.date and not (args.start and args.end):
        parser.error("Must specify either --date or both --start and --end")

    engine = get_engine()
    feature_store = FeatureStore(engine)

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        backfill_date(engine, feature_store, target_date, args.dry_run)
    else:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

        total_updated = 0
        current = start_date
        while current <= end_date:
            updated = backfill_date(engine, feature_store, current, args.dry_run)
            total_updated += updated
            current += timedelta(days=1)

        logger.info(f"Total updated across all dates: {total_updated}")


if __name__ == "__main__":
    main()
