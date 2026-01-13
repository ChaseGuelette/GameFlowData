import pandas as pd
from pathlib import Path

DATA_DIR = Path("./linker_data")

def check_ids():
    print("Loading data for inspection...")
    # 1. Load Stats (The Source of Truth)
    stats = pd.read_csv(DATA_DIR / "player_game_stats.csv", nrows=5000)
    
    # 2. Load Props Updates (The rows failing to match)
    updates = pd.read_csv(DATA_DIR / "props_full_updates.csv", nrows=5000)
    
    # 3. Load Props Original (To get the game_id)
    props = pd.read_csv(DATA_DIR / "player_props.csv", nrows=5000)
    
    # Merge game_id into updates to see what we are working with
    props_map = dict(zip(props['staging_id'], props['game_id']))
    updates['game_id'] = updates['staging_id'].map(props_map)
    
    print("\n" + "="*50)
    print("DEBUGGING ID MISMATCH")
    print("="*50)
    
    print("\n1. SAMPLE IDs FROM STATS (What we are looking for):")
    print(stats[['player_id', 'game_id', 'team_id']].head(5))
    print(f"   Format seems to be: {type(stats.iloc[0]['game_id'])}")

    print("\n2. SAMPLE IDs FROM PROPS (What we have):")
    print(updates[['player_id', 'game_id']].head(5))
    print(f"   Format seems to be: {type(updates.iloc[0]['game_id'])}")
    
    # Check for direct overlap
    stats_ids = set(stats['game_id'].astype(str).unique())
    props_ids = set(updates['game_id'].astype(str).unique())
    
    overlap = stats_ids.intersection(props_ids)
    print(f"\n3. OVERLAP CHECK:")
    print(f"   Stats Game IDs found: {len(stats_ids)}")
    print(f"   Props Game IDs found: {len(props_ids)}")
    print(f"   IDs present in BOTH:  {len(overlap)}")
    
    if len(overlap) == 0:
        print("\n   CRITICAL: No Game IDs match! This is why team_id lookup fails.")
        print(f"   Stats Example: '{list(stats_ids)[0]}'")
        print(f"   Props Example: '{list(props_ids)[0]}'")
    else:
        print(f"   Some IDs match ({len(overlap)}). The issue might be missing stats for specific games.")

if __name__ == "__main__":
    check_ids()