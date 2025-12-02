from sqlalchemy import create_engine  # Only need this since you're creating the engine here
from dotenv import load_dotenv
import os

from dailyPlayerStatScraper import update_player_stats
from dailyTeamStatScraper import update_game_data

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def main():

    engine = create_engine(DATABASE_URL)

    print("entering main")
    try:
        print("Starting data update process...") 
        print("\n2. Updating team game data...")
        update_game_data(engine, days_back=3)e)

        
        print("\n3. Updating player game stats...")
        update_player_stats(engine)     
        
        print("\nAll updates completed successfully!")
        
    except Exception as e:
        print(f"\nError in update process: {str(e)}")
        raise  # Re-raise the exception for proper error handling

if __name__ == "__main__":
    main()