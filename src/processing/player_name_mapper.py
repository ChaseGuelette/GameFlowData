"""
Player Name Mapping Utility
===========================
Helps identify and manage player name mismatches between betting APIs and NBA database.

Usage:
    python player_name_mapper.py analyze    # Find unmatched players with suggestions
    python player_name_mapper.py add        # Interactive mode to add mappings
    python player_name_mapper.py export     # Export current mappings
    python player_name_mapper.py apply      # Apply mappings to raw_player_props_combined
"""

import sys
import csv
import argparse
from difflib import SequenceMatcher
from collections import defaultdict

from sqlalchemy import text
from src.db.client import get_engine

# ============================================================================
# FUZZY MATCHING
# ============================================================================

def similarity(a: str, b: str) -> float:
    """Calculate string similarity ratio."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_best_matches(api_name: str, db_players: dict, top_n: int = 5) -> list:
    """
    Find best matching players from database.
    Returns list of (player_id, player_name, similarity_score)
    """
    scores = []
    api_lower = api_name.lower().strip()
    
    for player_id, db_name in db_players.items():
        score = similarity(api_lower, db_name.lower())
        
        # Bonus for matching last name
        api_parts = api_lower.split()
        db_parts = db_name.lower().split()
        if api_parts and db_parts and api_parts[-1] == db_parts[-1]:
            score += 0.15
        
        scores.append((player_id, db_name, score))
    
    scores.sort(key=lambda x: x[2], reverse=True)
    return scores[:top_n]

# ============================================================================
# COMMANDS
# ============================================================================

def cmd_analyze(engine):
    """Analyze unmatched players and suggest mappings."""
    print("Loading database players...")
    
    # Get all players from database
    db_players = {}
    with engine.connect() as conn:
        result = conn.execute(text("SELECT player_id, player_name FROM players"))
        for row in result:
            db_players[row[0]] = row[1]
    
    print(f"Loaded {len(db_players)} players from database")
    
    # Get unmatched API player names
    unmatched_query = """
    SELECT DISTINCT api_player_name, COUNT(*) as occurrences
    FROM raw_player_props_combined
    WHERE game_id IS NOT NULL 
      AND player_id IS NULL
      AND api_player_name IS NOT NULL
    GROUP BY api_player_name
    ORDER BY occurrences DESC
    """
    
    unmatched = []
    with engine.connect() as conn:
        result = conn.execute(text(unmatched_query))
        for row in result:
            unmatched.append((row[0], row[1]))
    
    print(f"\nFound {len(unmatched)} unmatched player names")
    print("=" * 80)
    
    # Write analysis to CSV
    output_file = "player_mapping_suggestions.csv"
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'api_player_name', 
            'occurrences',
            'best_match_id',
            'best_match_name', 
            'confidence',
            'other_suggestions'
        ])
        
        for api_name, count in unmatched:
            matches = find_best_matches(api_name, db_players)
            
            if matches:
                best_id, best_name, best_score = matches[0]
                other = "; ".join([f"{n} ({s:.2f})" for _, n, s in matches[1:3]])
                
                writer.writerow([
                    api_name,
                    count,
                    best_id,
                    best_name,
                    f"{best_score:.2f}",
                    other
                ])
                
                # Print high-confidence matches
                if best_score >= 0.85:
                    print(f"[OK] HIGH CONFIDENCE: '{api_name}' -> '{best_name}' ({best_score:.2f})")
                elif best_score >= 0.70:
                    print(f"? LIKELY: '{api_name}' -> '{best_name}' ({best_score:.2f})")
            else:
                writer.writerow([api_name, count, '', '', '', ''])
    
    print(f"\nFull analysis written to {output_file}")
    print("\nReview the CSV, then use 'add' command to create mappings")

def cmd_add(engine):
    """Interactive mode to add player mappings."""
    # Ensure mapping table exists
    create_table = """
    CREATE TABLE IF NOT EXISTS player_name_mappings (
        api_name TEXT PRIMARY KEY,
        player_id BIGINT REFERENCES players(player_id),
        created_at TIMESTAMP DEFAULT NOW(),
        notes TEXT
    )
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_table))
    
    print("Player Name Mapping - Interactive Mode")
    print("Enter mappings as: api_name|player_id")
    print("Or: api_name|player_id|notes")
    print("Type 'quit' to exit\n")
    
    while True:
        try:
            line = input("> ").strip()
            
            if line.lower() in ('quit', 'exit', 'q'):
                break
            
            if not line or '|' not in line:
                continue
            
            parts = line.split('|')
            api_name = parts[0].strip()
            player_id = int(parts[1].strip())
            notes = parts[2].strip() if len(parts) > 2 else None
            
            # Verify player exists
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT player_name FROM players WHERE player_id = :pid"),
                    {"pid": player_id}
                ).fetchone()
                
                if not result:
                    print(f"  ERROR: player_id {player_id} not found in database")
                    continue
                
                player_name = result[0]
            
            # Insert mapping
            insert_query = """
            INSERT INTO player_name_mappings (api_name, player_id, notes)
            VALUES (:api_name, :player_id, :notes)
            ON CONFLICT (api_name) DO UPDATE 
            SET player_id = EXCLUDED.player_id, notes = EXCLUDED.notes
            """
            
            with engine.begin() as conn:
                conn.execute(text(insert_query), {
                    "api_name": api_name,
                    "player_id": player_id,
                    "notes": notes
                })
            
            print(f"  [OK] Mapped '{api_name}' -> '{player_name}' (ID: {player_id})")
            
        except ValueError as e:
            print(f"  ERROR: Invalid input - {e}")
        except Exception as e:
            print(f"  ERROR: {e}")
    
    print("\nDone!")

def cmd_import(engine, filepath: str):
    """Import mappings from CSV file."""
    # Ensure table exists
    create_table = """
    CREATE TABLE IF NOT EXISTS player_name_mappings (
        api_name TEXT PRIMARY KEY,
        player_id BIGINT REFERENCES players(player_id),
        created_at TIMESTAMP DEFAULT NOW(),
        notes TEXT
    )
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_table))
    
    # Read CSV (expects columns: api_name, player_id, [notes])
    imported = 0
    skipped = 0
    
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            api_name = row.get('api_name', row.get('api_player_name', '')).strip()
            player_id_str = row.get('player_id', row.get('best_match_id', '')).strip()
            notes = row.get('notes', '').strip()
            
            if not api_name or not player_id_str:
                skipped += 1
                continue
            
            try:
                player_id = int(player_id_str)
            except ValueError:
                skipped += 1
                continue
            
            insert_query = """
            INSERT INTO player_name_mappings (api_name, player_id, notes)
            VALUES (:api_name, :player_id, :notes)
            ON CONFLICT (api_name) DO UPDATE 
            SET player_id = EXCLUDED.player_id
            """
            
            with engine.begin() as conn:
                conn.execute(text(insert_query), {
                    "api_name": api_name,
                    "player_id": player_id,
                    "notes": notes or None
                })
            imported += 1
    
    print(f"Imported {imported} mappings, skipped {skipped} rows")

def cmd_export(engine):
    """Export current mappings to CSV."""
    query = """
    SELECT m.api_name, m.player_id, p.player_name, m.notes, m.created_at
    FROM player_name_mappings m
    JOIN players p ON m.player_id = p.player_id
    ORDER BY m.api_name
    """
    
    output_file = "player_name_mappings_export.csv"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['api_name', 'player_id', 'player_name', 'notes', 'created_at'])
            
            count = 0
            for row in result:
                writer.writerow(row)
                count += 1
    
    print(f"Exported {count} mappings to {output_file}")

def cmd_apply(engine):
    """Apply mappings to raw_player_props_combined."""
    print("Applying player name mappings...")
    
    # Update player_id from mappings
    update_query = """
    UPDATE raw_player_props_combined rp
    SET player_id = m.player_id
    FROM player_name_mappings m
    WHERE rp.api_player_name = m.api_name
      AND rp.player_id IS NULL
      AND rp.game_id IS NOT NULL
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(update_query))
        print(f"Updated player_id for {result.rowcount:,} rows")
    
    # Update team_id from player_game_stats
    team_update = """
    UPDATE raw_player_props_combined rp
    SET team_id = pgs.team_id
    FROM player_game_stats pgs
    WHERE rp.player_id = pgs.player_id
      AND rp.game_id = pgs.game_id
      AND rp.team_id IS NULL
    """
    
    with engine.begin() as conn:
        result = conn.execute(text(team_update))
        print(f"Updated team_id for {result.rowcount:,} rows")
    
    print("Done!")

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Manage player name mappings')
    parser.add_argument('command', choices=['analyze', 'add', 'import', 'export', 'apply'],
                        help='Command to run')
    parser.add_argument('--file', type=str, help='CSV file for import command')
    
    args = parser.parse_args()
    
    engine = get_engine()
    print("Database connected [OK]\n")
    
    if args.command == 'analyze':
        cmd_analyze(engine)
    elif args.command == 'add':
        cmd_add(engine)
    elif args.command == 'import':
        if not args.file:
            print("ERROR: --file required for import command")
            sys.exit(1)
        cmd_import(engine, args.file)
    elif args.command == 'export':
        cmd_export(engine)
    elif args.command == 'apply':
        cmd_apply(engine)

if __name__ == "__main__":
    main()