import time

import pandas as pd
from nba_api.stats.endpoints import commonplayerinfo
from nba_unified_scraper import get_engine, rate_limit_delay
from sqlalchemy import text


def get_position_group(pos):
    """Maps specific NBA positions to broader groups for better model generalization."""
    if not pos:
        return None
    pos = pos.upper()
    if "GUARD" in pos:
        return "Guard"
    if "FORWARD" in pos and "CENTER" in pos:
        return "Big"
    if "FORWARD" in pos:
        return "Forward"
    if "CENTER" in pos:
        return "Big"
    return "Other"


def update_player_positions():
    engine = get_engine()

    # 1. Get all players that need position info
    with engine.connect() as conn:
        players = pd.read_sql("SELECT player_id FROM public.players WHERE primary_position IS NULL", conn)

    print(f"Found {len(players)} players needing position updates.")

    for i, row in players.iterrows():
        player_id = int(row["player_id"])
        try:
            print(
                f"[{i + 1}/{len(players)}] Fetching info for player {player_id}...",
                end=" ",
                flush=True,
            )

            # Call NBA API
            player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            df = player_info.get_data_frames()[0]

            if not df.empty:
                raw_pos = df["POSITION"].values[0]
                pos_group = get_position_group(raw_pos)

                # Update Database
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE public.players 
                            SET primary_position = :pos, position_group = :grp 
                            WHERE player_id = :id
                        """),
                        {"pos": raw_pos, "grp": pos_group, "id": player_id},
                    )
                print(f"✅ {raw_pos} ({pos_group})")
            else:
                print("⚠️ No data found")

            # Respect your existing rate limiting logic
            rate_limit_delay(i + 1)

        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(10)  # Safety pause on error


if __name__ == "__main__":
    update_player_positions()
