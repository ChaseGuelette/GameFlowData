"""Debug feature store - check what data is available."""
from src.db.client import get_engine
from sqlalchemy import text
from datetime import date

engine = get_engine()
target_date = date(2026, 2, 9)

with engine.connect() as conn:
    # Check team_game_stats for today's games
    result = conn.execute(text("""
        SELECT game_id, team_id, game_date
        FROM team_game_stats
        WHERE game_date = :target_date
        LIMIT 10
    """), {"target_date": target_date})
    rows = result.fetchall()
    print(f"team_game_stats for {target_date}: {len(rows)} rows")
    for r in rows[:5]:
        print(f"  {r}")

    # Check player_game_stats for today
    result = conn.execute(text("""
        SELECT COUNT(*) FROM player_game_stats WHERE game_date = :target_date
    """), {"target_date": target_date})
    count = result.scalar()
    print(f"\nplayer_game_stats for {target_date}: {count} rows")

    # Check player_average_game_stats - do we have any data?
    result = conn.execute(text("""
        SELECT COUNT(*) FROM player_average_game_stats
    """))
    count = result.scalar()
    print(f"\nplayer_average_game_stats total: {count} rows")

    # Check most recent game_date in player_average_game_stats
    result = conn.execute(text("""
        SELECT MAX(game_date) FROM player_average_game_stats
    """))
    max_date = result.scalar()
    print(f"player_average_game_stats max game_date: {max_date}")

    # Check opponent_allowed_stats
    result = conn.execute(text("""
        SELECT COUNT(*) FROM opponent_allowed_stats
    """))
    count = result.scalar()
    print(f"\nopponent_allowed_stats total: {count} rows")

    # Check most recent date in opponent_allowed_stats
    result = conn.execute(text("""
        SELECT MAX(as_of_date) FROM opponent_allowed_stats
    """))
    max_date = result.scalar()
    print(f"opponent_allowed_stats max as_of_date: {max_date}")

    # Check if there are future games (games not yet played)
    result = conn.execute(text("""
        SELECT game_id, game_date
        FROM team_game_stats
        WHERE game_date >= :target_date
        ORDER BY game_date
        LIMIT 5
    """), {"target_date": target_date})
    rows = result.fetchall()
    print(f"\nUpcoming games in team_game_stats:")
    for r in rows:
        print(f"  {r}")
