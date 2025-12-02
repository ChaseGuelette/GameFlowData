from sqlalchemy import create_engine, text
from requests.exceptions import ReadTimeout
from datetime import datetime, timedelta
import pandas as pd
import random
import time 
import json
from nba_api.stats.endpoints import leaguegamefinder

# from dotenv import load_dotenv
# import os

# load_dotenv()

# DATABASE_URL = os.getenv("DATABASE_URL")
# engine = create_engine(DATABASE_URL)

def normalize_for_postgres(df):
    """Normalize DataFrame column names for PostgreSQL"""
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

def update_game_data(engine, days_back=3):
    """Update team game stats with recent games
    
    Args:
        engine: SQLAlchemy engine for database connection
        days_back: Number of days back to fetch games (default: 3)
    """
    
    # Calculate dates for recent games
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    season = '2024-25'
    
    print(f"Fetching team game data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
    
    try:
        # Get recent games
        game_finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=start_date.strftime('%m/%d/%Y'),
            date_to_nullable=end_date.strftime('%m/%d/%Y'),
            league_id_nullable='00',
            season_type_nullable='Regular Season'
        )
        
        # Convert to DataFrame
        games_df = game_finder.get_data_frames()[0]
        
        if not games_df.empty:
            print(f"Found {len(games_df)} team game records")
            
            # Add season column to track which season this data belongs to
            games_df['SEASON'] = season
            
            # Sort by team and game date (but DON'T set as index - bad database design)
            games_df = games_df.sort_values(['TEAM_ID', 'GAME_DATE'], ascending=[True, False])
            
            # Normalize column names for PostgreSQL
            games_df = normalize_for_postgres(games_df)
            
            # Update the database
            with engine.connect() as conn:
                with conn.begin():
                    # Delete recent records if we already added them 
                    delete_query = text("""
                        DELETE FROM team_game_stats
                        WHERE game_date >= :start_date
                    """)
                    
                    result = conn.execute(delete_query, {
                        'start_date': start_date.strftime('%Y-%m-%d')
                    })
                    
                    deleted_count = result.rowcount
                    if deleted_count > 0:
                        print(f"Deleted {deleted_count} old records")
                    
                    # Append new records (index=False since team_id should be a regular column)
                    games_df.to_sql('team_game_stats', 
                                    conn, 
                                    if_exists='append', 
                                    index=False)
                    
                    print(f"✓ Successfully added {len(games_df)} new team game records")
                
            print(f"✓ Finished updating team game stats for {season} season")
            
        else:
            print("No new games found in date range")
    
    except (ReadTimeout, json.decoder.JSONDecodeError) as e:
        print(f"✗ Error: {e}")
        print("  Retrying after 60 seconds...")
        time.sleep(60)
        
        # Retry once
        try:
            game_finder = leaguegamefinder.LeagueGameFinder(
                date_from_nullable=start_date.strftime('%m/%d/%Y'),
                date_to_nullable=end_date.strftime('%m/%d/%Y'),
                league_id_nullable='00',
                season_type_nullable='Regular Season'
            )
            
            games_df = game_finder.get_data_frames()[0]
            
            if not games_df.empty:
                games_df['SEASON'] = season
                games_df = games_df.sort_values(['TEAM_ID', 'GAME_DATE'], ascending=[True, False])
                games_df = normalize_for_postgres(games_df)
                
                with engine.connect() as conn:
                    with conn.begin():
                        delete_query = text("""
                            DELETE FROM team_game_stats
                            WHERE game_date >= :start_date
                        """)
                        conn.execute(delete_query, {
                            'start_date': start_date.strftime('%Y-%m-%d')
                        })
                        
                        games_df.to_sql('team_game_stats', 
                                       conn, 
                                       if_exists='append', 
                                       index=False)
                
                print(f"✓ Retry successful - added {len(games_df)} records")
            
        except Exception as retry_error:
            print(f"✗ Retry failed: {retry_error}")
    
    except Exception as e:
        print(f"✗ Unexpected error: {e}")

# # Run the update
# print("=" * 60)
# print("DAILY TEAM GAME STATS UPDATE")
# print("=" * 60)
# update_game_data(days_back=3, engine)
# print("=" * 60)