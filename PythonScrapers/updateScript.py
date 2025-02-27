from sqlalchemy import create_engine  # Only need this since you're creating the engine here
from miscTeamStats import update_misc_team_stats
from miscPlayerStats import update_misc_player_stats
from dailyPlayerGameStats import update_player_game_stats
from dailyTeamGameData import update_daily_team_game_data
from adv_est_playerStats import update_advanced_player_stats, update_estimated_player_stats
from clutchMetrics import all_player_clutch_stats

def main():
    # Create engine once and pass it to all functions
    engine = create_engine('postgresql://chase:yourpassword@localhost:5433/TeamData')
    print("entering main")
    try:
        print("Starting data update process...") 

        print("\n1. Updating player advanced stats...")
        update_advanced_player_stats(engine)

        print("\n2. Updating player estimated stats...")
        update_estimated_player_stats(engine) 

        print("\n3. Updating team misc stats...")
        update_misc_team_stats(engine)

        print("\n4. Updating team game data...")
        update_daily_team_game_data(engine)

        print("\n5. Updating player clutch data...")
        all_player_clutch_stats(engine)

        print("\n6. Updating player misc stats...")
        update_misc_player_stats(engine)
        
        print("\n7. Updating player game stats...")
        update_player_game_stats(engine)     
        
        print("\nAll updates completed successfully!")
        
    except Exception as e:
        print(f"\nError in update process: {str(e)}")
        raise  # Re-raise the exception for proper error handling

if __name__ == "__main__":
    main()