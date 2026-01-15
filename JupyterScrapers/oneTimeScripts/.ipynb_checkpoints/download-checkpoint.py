import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from tqdm import tqdm

# Load credentials from .env file
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found! Check your .env file.")

# Create Database Connection Pool (SQLAlchemy Engine)
engine = create_engine(DATABASE_URL)

def download_csvs():
    # Create an exports directory if it doesn't exist
    export_dir = "exports"
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    # Mapping of "Friendly Name" -> "Database Table Name"
    # Based on the schema provided in the prompt
    tables_to_export = {
        "player_game_stats": "player_game_stats",             # 
        "raw_game_lines": "raw_game_lines_staging",           # 
        "player_props_combined": "raw_player_props_combined", # 
        "team_game_stats": "team_game_stats"                  # 
    }

    print(f"Starting download of {len(tables_to_export)} tables...")

    # Using tqdm for a progress bar as requested in your imports
    for file_name, table_name in tqdm(tables_to_export.items(), desc="Exporting Tables"):
        try:
            # Query the full table
            query = f"SELECT * FROM public.{table_name}"
            
            # Read into DataFrame
            df = pd.read_sql(query, engine)
            
            # Save to CSV
            file_path = os.path.join(export_dir, f"{file_name}.csv")
            df.to_csv(file_path, index=False)
            
            # Print explicit confirmation for log visibility
            tqdm.write(f"✓ Saved {table_name} ({len(df)} rows) to {file_path}")
            
        except Exception as e:
            tqdm.write(f"❌ Error exporting {table_name}: {e}")

if __name__ == "__main__":
    download_csvs()