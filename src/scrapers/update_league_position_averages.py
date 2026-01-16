"""
Update League Position Averages Table

This script calculates and updates league-wide averages per position group.
Unlike league_priors_history (which has monthly snapshots), this table stores
the CURRENT full-season averages for each season.

Usage:
  python update_league_position_averages.py                      # All seasons
  python update_league_position_averages.py --season 2024-25     # Specific season
  python update_league_position_averages.py --season 22025       # Using season_id format
"""

import argparse
import logging
from sqlalchemy import text
from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default seasons to process (add new seasons here)
DEFAULT_SEASONS = [
    '22018', '22019', '22020', '22021', '22022', '22023', '22024', '22025', '22026'
]


def normalize_season_id(season: str) -> str:
    """
    Convert season formats to the database format (e.g., '22024').

    Accepts:
      - '2024-25' -> '22024'
      - '22024'   -> '22024'
      - '2024'    -> '22024'
    """
    season = season.strip()

    # Already in correct format
    if season.startswith('2') and len(season) == 5 and season[1:].isdigit():
        return season

    # Format: '2024-25'
    if '-' in season:
        start_year = season.split('-')[0]
        return f"2{start_year}"

    # Format: '2024' (just the year)
    if len(season) == 4 and season.isdigit():
        return f"2{season}"

    return season


def update_league_averages(engine, seasons: list[str]):
    """
    Calculate and upsert league-wide position averages for specified seasons.

    Metrics calculated (all per 100 possessions):
      - Offensive rating (points)
      - Rebounds, Assists, Steals, Blocks, 3-pointers
      - Turnovers, Free throw attempts, Offensive rebounds, Personal fouls
    """
    # Normalize all season IDs
    normalized_seasons = [normalize_season_id(s) for s in seasons]
    logger.info(f"Updating league_position_averages for seasons: {normalized_seasons}")

    query = text("""
        INSERT INTO league_position_averages (
            season_id, position_group,
            league_off_rtg, league_reb_per100, league_ast_per100,
            league_stl_per100, league_blk_per100, league_threes_per100, league_tov_per100,
            league_fta_per100, league_oreb_per100, league_pf_per100,
            total_possessions, total_games
        )
        SELECT
            tgs.season_id,
            ph.position_group,

            -- Stats per 100 possessions
            ROUND((SUM(pgs.pts) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.reb) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.ast) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.stl) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.blk) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.fg3m) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.tov) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.fta) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.oreb) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),
            ROUND((SUM(pgs.pf) / NULLIF(SUM(adv.possessions), 0) * 100)::numeric, 2),

            SUM(adv.possessions),
            COUNT(DISTINCT pgs.game_id)

        FROM player_game_stats pgs
        JOIN team_game_stats tgs
            ON pgs.game_id = tgs.game_id AND pgs.team_id = tgs.team_id
        JOIN player_game_advanced_stats adv
            ON pgs.game_id = adv.game_id AND pgs.player_id = adv.player_id

        -- Get position from history (most recent snapshot before game)
        JOIN LATERAL (
            SELECT position_group
            FROM player_position_history ph
            WHERE ph.player_id = pgs.player_id
              AND ph.snapshot_date < tgs.game_date::DATE
            ORDER BY ph.snapshot_date DESC
            LIMIT 1
        ) ph ON TRUE

        WHERE tgs.season_id IN :seasons
          AND pgs.min > 0
          AND adv.possessions > 0

        GROUP BY tgs.season_id, ph.position_group

        ON CONFLICT (season_id, position_group) DO UPDATE SET
            league_off_rtg = EXCLUDED.league_off_rtg,
            league_reb_per100 = EXCLUDED.league_reb_per100,
            league_ast_per100 = EXCLUDED.league_ast_per100,
            league_stl_per100 = EXCLUDED.league_stl_per100,
            league_blk_per100 = EXCLUDED.league_blk_per100,
            league_threes_per100 = EXCLUDED.league_threes_per100,
            league_tov_per100 = EXCLUDED.league_tov_per100,
            league_fta_per100 = EXCLUDED.league_fta_per100,
            league_oreb_per100 = EXCLUDED.league_oreb_per100,
            league_pf_per100 = EXCLUDED.league_pf_per100,
            total_possessions = EXCLUDED.total_possessions,
            total_games = EXCLUDED.total_games,
            updated_at = NOW();
    """)

    with engine.begin() as conn:
        conn.execute(query, {'seasons': tuple(normalized_seasons)})
        logger.info(f"League averages updated for {len(normalized_seasons)} seasons")


def main():
    parser = argparse.ArgumentParser(description='Update league position averages')
    parser.add_argument('--season', type=str, help='Specific season (e.g., 2024-25 or 22025). If omitted, updates all seasons.')
    args = parser.parse_args()

    engine = get_engine()

    if args.season:
        seasons = [args.season]
    else:
        seasons = DEFAULT_SEASONS

    update_league_averages(engine, seasons)

    # Show current state
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT season_id, COUNT(*) as position_groups, SUM(total_games) as total_games
            FROM league_position_averages
            GROUP BY season_id
            ORDER BY season_id
        """))
        logger.info("Current league_position_averages state:")
        for row in result:
            logger.info(f"  {row[0]}: {row[1]} position groups, {row[2]:,} total games")


if __name__ == "__main__":
    main()
