"""
NBA Data Linker - Local Processing Version (FIXED with Fuzzy Date Matching)
============================================================================
Downloads data locally, processes in pandas, uploads results.

KEY FIX: Player props now use fuzzy date matching (±90 days) to handle
incorrect commence_times from Odds API.

Steps:
1. python nba_linker_local_fixed.py download   - Pull tables to CSV
2. python nba_linker_local_fixed.py process    - Match IDs locally
3. python nba_linker_local_fixed.py upload     - Push results back

For player name mismatches:
- After process, check linker_data/unmatched_players.csv
- Edit linker_data/player_mappings.csv to add manual mappings
- Re-run process to apply them

Requirements:
    pip install pandas sqlalchemy psycopg2-binary python-dotenv tqdm
"""

import argparse
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import pytz
from sqlalchemy import text
from tqdm import tqdm

from src.db.client import get_engine

# ============================================================================
# CONFIG
# ============================================================================

DATA_DIR = Path("./linker_data")
EASTERN = pytz.timezone("America/New_York")
FUZZY_DATE_WINDOW_DAYS = 90  # Match games within ±90 days

# Team name aliases
TEAM_NAME_ALIASES = {
    # Current variations
    "LA Clippers": "Los Angeles Clippers",
    "L.A. Clippers": "Los Angeles Clippers",
    "LAC": "Los Angeles Clippers",
    "LA Lakers": "Los Angeles Lakers",
    "L.A. Lakers": "Los Angeles Lakers",
    "LAL": "Los Angeles Lakers",
    # Historical franchises
    "New Jersey Nets": "Brooklyn Nets",
    "Charlotte Bobcats": "Charlotte Hornets",
    "New Orleans Hornets": "New Orleans Pelicans",
    "New Orleans/Oklahoma City Hornets": "New Orleans Pelicans",
    "Seattle SuperSonics": "Oklahoma City Thunder",
    "Vancouver Grizzlies": "Memphis Grizzlies",
}

# ============================================================================
# STEP 1: DOWNLOAD
# ============================================================================


def download_tables():
    """Download all needed tables to local CSVs (Optimized for large tables)."""
    DATA_DIR.mkdir(exist_ok=True)
    engine = get_engine()

    # 1. SMALL TABLES - Safe to download in one go
    small_tables = {
        "teams": "SELECT team_id, team_name FROM teams",
        "players": "SELECT player_id, player_name FROM players",
        "team_game_stats": """
            SELECT game_id, team_id, team_name, team_game_date, team_matchup, opponent_id 
            FROM team_game_stats
        """,
        "player_game_stats": "SELECT player_id, game_id, team_id FROM player_game_stats",
    }

    for name, query in small_tables.items():
        filepath = DATA_DIR / f"{name}.csv"
        if filepath.exists():
            print(f"Skipping {name} (already exists)...")
            continue

        print(f"Downloading {name}...")
        try:
            with engine.connect() as conn:
                df = pd.read_sql(query, conn)
            df.to_csv(filepath, index=False)
            print(f"  Saved {len(df):,} rows to {filepath}")
        except Exception as e:
            print(f"Error downloading {name}: {e}")

    # 2. GAME LINES - Medium table, batch by ID
    game_lines_file = DATA_DIR / "game_lines.csv"
    if game_lines_file.exists():
        print("Skipping game_lines (already exists)...")
    else:
        print("\nDownloading game_lines (batched)...")

        range_query = "SELECT MIN(staging_id), MAX(staging_id) FROM raw_game_lines_staging"
        with engine.connect() as conn:
            result = conn.execute(text(range_query)).fetchone()
            min_id, max_id = result[0], result[1]

        if min_id:
            print(f"  ID Range: {min_id} to {max_id}")
            batch_size = 100000
            current = min_id
            total_rows = 0
            first_batch = True

            pbar = tqdm(total=(max_id - min_id), unit="rows", desc="game_lines")

            while current <= max_id:
                query = text("""
                    SELECT staging_id, home_team, commence_time, nba_game_id
                    FROM raw_game_lines_staging
                    WHERE staging_id >= :start AND staging_id < :end
                """)

                try:
                    with engine.connect() as conn:
                        chunk = pd.read_sql(query, conn, params={"start": current, "end": current + batch_size})

                    if not chunk.empty:
                        mode = "w" if first_batch else "a"
                        chunk.to_csv(game_lines_file, mode=mode, header=first_batch, index=False)
                        total_rows += len(chunk)
                        first_batch = False

                except Exception as e:
                    print(f"\n  Error on batch {current}: {e}")

                current += batch_size
                pbar.update(batch_size)

            pbar.close()
            print(f"  Saved {total_rows:,} rows")

    # 3. PLAYER PROPS - Large table, batch by ID
    props_file = DATA_DIR / "player_props.csv"
    if props_file.exists():
        print("Skipping player_props (already exists)...")
    else:
        print("\nDownloading player_props (batched)...")

        range_query = "SELECT MIN(staging_id), MAX(staging_id) FROM raw_player_props_combined"
        with engine.connect() as conn:
            result = conn.execute(text(range_query)).fetchone()
            min_id, max_id = result[0], result[1]

        if min_id:
            print(f"  ID Range: {min_id} to {max_id}")
            batch_size = 100000
            current = min_id
            total_rows = 0
            first_batch = True

            pbar = tqdm(total=(max_id - min_id), unit="rows", desc="player_props")

            while current <= max_id:
                query = text("""
                    SELECT staging_id, api_player_name, home_team, away_team, commence_time, 
                           game_id, player_id, team_id
                    FROM raw_player_props_combined
                    WHERE staging_id >= :start AND staging_id < :end
                """)

                try:
                    with engine.connect() as conn:
                        chunk = pd.read_sql(query, conn, params={"start": current, "end": current + batch_size})

                    if not chunk.empty:
                        mode = "w" if first_batch else "a"
                        chunk.to_csv(props_file, mode=mode, header=first_batch, index=False)
                        total_rows += len(chunk)
                        first_batch = False

                except Exception as e:
                    print(f"\n  Error on batch {current}: {e}")

                current += batch_size
                pbar.update(batch_size)

            pbar.close()
            print(f"  Saved {total_rows:,} rows")

    print("\n[OK] Download complete!")


# ============================================================================
# STEP 2: PROCESS (WITH FUZZY DATE MATCHING)
# ============================================================================


def normalize_team(name):
    """Normalize team name."""
    if pd.isna(name):
        return name
    name = str(name).strip()
    return TEAM_NAME_ALIASES.get(name, name)


def find_closest_game_date(candidates, target_date_str, max_days=FUZZY_DATE_WINDOW_DAYS):
    """
    Find the game with the closest date to target within max_days window.

    Args:
        candidates: List of (game_id, date_str) tuples
        target_date_str: Target date as 'YYYY-MM-DD' string
        max_days: Maximum days difference to consider

    Returns:
        (game_id, date_difference_days) or (None, None) if no match within window
    """
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    except:
        return None, None

    best_game_id = None
    best_diff = float("inf")

    for game_id, game_date_str in candidates:
        try:
            game_date = datetime.strptime(game_date_str, "%Y-%m-%d")
            diff_days = abs((target_date - game_date).days)

            if diff_days <= max_days and diff_days < best_diff:
                best_diff = diff_days
                best_game_id = game_id
        except:
            continue

    if best_game_id:
        return best_game_id, best_diff
    else:
        return None, None


def process_local():
    """Process all matching locally in pandas with FUZZY DATE MATCHING."""
    print("Loading CSVs...")

    teams = pd.read_csv(DATA_DIR / "teams.csv")
    players = pd.read_csv(DATA_DIR / "players.csv")
    team_game_stats = pd.read_csv(DATA_DIR / "team_game_stats.csv")
    player_game_stats = pd.read_csv(DATA_DIR / "player_game_stats.csv")
    game_lines = pd.read_csv(DATA_DIR / "game_lines.csv")
    player_props = pd.read_csv(DATA_DIR / "player_props.csv")

    print(f"  teams: {len(teams):,}")
    print(f"  players: {len(players):,}")
    print(f"  team_game_stats: {len(team_game_stats):,}")
    print(f"  player_game_stats: {len(player_game_stats):,}")
    print(f"  game_lines: {len(game_lines):,}")
    print(f"  player_props: {len(player_props):,}")

    # Build team_id -> team_name lookup
    team_names = dict(zip(teams["team_id"], teams["team_name"]))

    # ========================================
    # Build game lookup: (home_team, date) -> (game_id, home_team_id, away_team_id)
    # ========================================
    print("\nBuilding game lookup...")

    # Filter to home games (matchup contains 'vs.')
    home_games = team_game_stats[team_game_stats["team_matchup"].str.contains("vs.", na=False)].copy()
    home_games["game_date"] = home_games["team_game_date"].str[:10]
    home_games["home_team_norm"] = home_games["team_name"].apply(normalize_team)

    # Build lookup dict
    game_lookup = {}
    for _, row in home_games.iterrows():
        key = (row["home_team_norm"], row["game_date"])
        game_lookup[key] = (row["game_id"], row["team_id"], row["opponent_id"])

    print(f"  {len(game_lookup):,} home team/date combinations")

    # ========================================
    # Build props game lookup WITH FUZZY MATCHING: (home, away) -> [(game_id, date), ...]
    # ========================================
    print("\nBuilding props game lookup (FUZZY MATCHING ENABLED)...")

    # Store as {(home, away): [(game_id, date), ...]}
    props_game_lookup = defaultdict(list)

    for _, row in home_games.iterrows():
        away_team = team_names.get(row["opponent_id"])
        if away_team:
            away_norm = normalize_team(away_team)
            key = (row["home_team_norm"], away_norm)
            props_game_lookup[key].append((row["game_id"], row["game_date"]))

    # Convert to regular dict
    props_game_lookup = dict(props_game_lookup)

    total_matchups = len(props_game_lookup)
    total_games = sum(len(games) for games in props_game_lookup.values())
    print(f"  {total_matchups:,} team matchups, {total_games:,} total games")
    print(f"  Will search ±{FUZZY_DATE_WINDOW_DAYS} days from props date")

    # ========================================
    # Build player lookup: normalized_name -> player_id
    # ========================================
    print("\nBuilding player lookup...")

    def normalize_player(name):
        if pd.isna(name):
            return name
        name = str(name).lower().strip()
        for old, new in [(".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", "")]:
            name = name.replace(old, new)
        return " ".join(name.split())

    players["name_norm"] = players["player_name"].apply(normalize_player)
    # Filter out NaN values before building lookup
    valid_players = players[players["name_norm"].notna()]
    player_lookup = dict(zip(valid_players["name_norm"], valid_players["player_id"]))
    player_name_by_id = dict(zip(players["player_id"], players["player_name"]))
    print(f"  {len(player_lookup):,} players")

    # Load manual mappings if they exist
    manual_mappings = {}
    mappings_file = DATA_DIR / "player_mappings.csv"
    if mappings_file.exists():
        mappings_df = pd.read_csv(mappings_file)
        for _, row in mappings_df.iterrows():
            if pd.notna(row.get("api_name")) and pd.notna(row.get("player_id")):
                manual_mappings[row["api_name"]] = int(row["player_id"])
        print(f"  Loaded {len(manual_mappings)} manual mappings from {mappings_file}")

    # ========================================
    # MATCH GAME LINES (unchanged - still exact date)
    # ========================================
    print("\n" + "=" * 60)
    print("MATCHING GAME LINES")
    print("=" * 60)

    # Only process unlinked rows
    unlinked_lines = game_lines[game_lines["nba_game_id"].isna()].copy()
    print(f"Unlinked rows: {len(unlinked_lines):,}")

    if len(unlinked_lines) > 0:
        # Convert commence_time to Eastern date
        unlinked_lines["commence_time"] = pd.to_datetime(unlinked_lines["commence_time"], utc=True)
        unlinked_lines["game_date"] = unlinked_lines["commence_time"].dt.tz_convert(EASTERN).dt.strftime("%Y-%m-%d")
        unlinked_lines["home_team_norm"] = unlinked_lines["home_team"].apply(normalize_team)

        # Match
        def match_game_line(row):
            key = (row["home_team_norm"], row["game_date"])
            match = game_lookup.get(key)
            if match:
                return pd.Series(
                    {
                        "nba_game_id": match[0],
                        "nba_home_team_id": match[1],
                        "nba_away_team_id": match[2],
                    }
                )
            return pd.Series({"nba_game_id": None, "nba_home_team_id": None, "nba_away_team_id": None})

        tqdm.pandas(desc="Matching game lines")
        matches = unlinked_lines.progress_apply(match_game_line, axis=1)
        unlinked_lines = pd.concat([unlinked_lines[["staging_id", "home_team", "game_date"]], matches], axis=1)

        # Filter to matched only
        matched_lines = unlinked_lines[unlinked_lines["nba_game_id"].notna()][
            ["staging_id", "nba_game_id", "nba_home_team_id", "nba_away_team_id"]
        ]
        print(f"Matched: {len(matched_lines):,}")

        # Track unmatched
        unmatched = unlinked_lines[unlinked_lines["nba_game_id"].isna()][["home_team", "game_date"]].drop_duplicates()
        if len(unmatched) > 0:
            unmatched.to_csv(DATA_DIR / "unmatched_game_lines.csv", index=False)
            print(f"  {len(unmatched)} unmatched game/date combos saved to {DATA_DIR}/unmatched_game_lines.csv")

        # Save
        matched_lines.to_csv(DATA_DIR / "game_lines_updates.csv", index=False)
        print(f"Saved to {DATA_DIR}/game_lines_updates.csv")

    # ========================================
    # MATCH PLAYER PROPS - GAMES (WITH FUZZY DATE MATCHING)
    # ========================================
    print("\n" + "=" * 60)
    print("MATCHING PLAYER PROPS -> GAMES (FUZZY DATE MATCHING)")
    print("=" * 60)

    unlinked_props = player_props[player_props["game_id"].isna()].copy()
    print(f"Unlinked rows: {len(unlinked_props):,}")

    if len(unlinked_props) > 0:
        unlinked_props["commence_time"] = pd.to_datetime(unlinked_props["commence_time"], utc=True)
        unlinked_props["game_date"] = unlinked_props["commence_time"].dt.tz_convert(EASTERN).dt.strftime("%Y-%m-%d")
        unlinked_props["home_team_norm"] = unlinked_props["home_team"].apply(normalize_team)
        unlinked_props["away_team_norm"] = unlinked_props["away_team"].apply(normalize_team)

        # Track fuzzy matching stats
        exact_matches = 0
        fuzzy_matches = 0
        date_diffs = []

        def match_prop_game_fuzzy(row):
            nonlocal exact_matches, fuzzy_matches, date_diffs

            key = (row["home_team_norm"], row["away_team_norm"])
            candidates = props_game_lookup.get(key, [])

            if not candidates:
                return None

            # Find closest game by date
            matched_game_id, date_diff = find_closest_game_date(candidates, row["game_date"])

            if matched_game_id:
                if date_diff == 0:
                    exact_matches += 1
                else:
                    fuzzy_matches += 1
                    date_diffs.append(date_diff)

            return matched_game_id

        tqdm.pandas(desc="Matching props to games (fuzzy)")
        unlinked_props["matched_game_id"] = unlinked_props.progress_apply(match_prop_game_fuzzy, axis=1)

        matched_props = unlinked_props[unlinked_props["matched_game_id"].notna()][["staging_id", "matched_game_id"]]
        matched_props.columns = ["staging_id", "game_id"]

        print("\nMatching Results:")
        print(f"  Total matched: {len(matched_props):,}")
        print(f"  Exact date matches: {exact_matches:,}")
        print(f"  Fuzzy date matches: {fuzzy_matches:,}")

        if date_diffs:
            avg_diff = sum(date_diffs) / len(date_diffs)
            max_diff = max(date_diffs)
            print(f"  Average date difference (fuzzy): {avg_diff:.1f} days")
            print(f"  Maximum date difference: {max_diff} days")

        # Track unmatched games
        unmatched_games = unlinked_props[unlinked_props["matched_game_id"].isna()][
            ["home_team", "away_team", "game_date"]
        ].drop_duplicates()
        if len(unmatched_games) > 0:
            unmatched_games.to_csv(DATA_DIR / "unmatched_games.csv", index=False)
            print(f"  {len(unmatched_games)} unmatched games saved to {DATA_DIR}/unmatched_games.csv")

        matched_props.to_csv(DATA_DIR / "props_game_updates.csv", index=False)
        print(f"Saved to {DATA_DIR}/props_game_updates.csv")

    # ========================================
    # MATCH PLAYER PROPS - PLAYERS
    # ========================================
    print("\n" + "=" * 60)
    print("MATCHING PLAYER PROPS -> PLAYERS")
    print("=" * 60)

    # Reload with game_ids we just matched
    props_with_games = player_props.copy()
    if (DATA_DIR / "props_game_updates.csv").exists():
        game_updates = pd.read_csv(DATA_DIR / "props_game_updates.csv")
        update_dict = dict(zip(game_updates["staging_id"], game_updates["game_id"]))
        props_with_games["game_id"] = props_with_games.apply(
            lambda r: update_dict.get(r["staging_id"], r["game_id"]), axis=1
        )

    # Only process rows with game_id but no player_id
    needs_player = props_with_games[props_with_games["game_id"].notna() & props_with_games["player_id"].isna()].copy()
    print(f"Rows needing player match: {len(needs_player):,}")

    if len(needs_player) > 0:
        needs_player["player_name_norm"] = needs_player["api_player_name"].apply(normalize_player)

        def match_player(row):
            api_name = row["api_player_name"]
            norm_name = row["player_name_norm"]

            # Check manual mappings first (exact api_name match)
            if api_name in manual_mappings:
                return manual_mappings[api_name]

            # Check normalized lookup
            if norm_name in player_lookup:
                return player_lookup[norm_name]

            return None

        tqdm.pandas(desc="Matching players")
        needs_player["matched_player_id"] = needs_player.progress_apply(match_player, axis=1)

        # Track unmatched
        unmatched = needs_player[needs_player["matched_player_id"].isna()]["api_player_name"].unique()

        if len(unmatched) > 0:
            print(f"\n  {len(unmatched)} unmatched player names - generating suggestions...")

            # Fuzzy match suggestions
            def find_best_matches(api_name, top_n=3):
                api_lower = api_name.lower().strip()
                scores = []
                for norm_name, pid in player_lookup.items():
                    # Skip NaN/None values
                    if pd.isna(norm_name) or not isinstance(norm_name, str):
                        continue
                    score = SequenceMatcher(None, api_lower, norm_name).ratio()
                    # Bonus for matching last name
                    api_parts = api_lower.split()
                    norm_parts = norm_name.split()
                    if api_parts and norm_parts and api_parts[-1] == norm_parts[-1]:
                        score += 0.15
                    scores.append((pid, player_name_by_id.get(pid, ""), score))
                scores.sort(key=lambda x: x[2], reverse=True)
                return scores[:top_n]

            suggestions = []
            for api_name in tqdm(unmatched, desc="Finding suggestions"):
                matches = find_best_matches(api_name)
                if matches:
                    best_id, best_name, best_score = matches[0]
                    suggestions.append(
                        {
                            "api_name": api_name,
                            "player_id": best_id if best_score >= 0.7 else "",  # Only pre-fill if confident
                            "suggested_name": best_name,
                            "confidence": f"{best_score:.2f}",
                            "other_suggestions": "; ".join([f"{n} ({s:.2f})" for _, n, s in matches[1:]]),
                        }
                    )

            suggestions_df = pd.DataFrame(suggestions)
            suggestions_df.to_csv(DATA_DIR / "unmatched_players.csv", index=False)
            print(f"  Saved suggestions to {DATA_DIR}/unmatched_players.csv")
            print(f"  Review and copy confirmed mappings to {DATA_DIR}/player_mappings.csv")
            print("  Format: api_name,player_id")

        matched_players = needs_player[needs_player["matched_player_id"].notna()][["staging_id", "matched_player_id"]]
        matched_players.columns = ["staging_id", "player_id"]
        matched_players["player_id"] = matched_players["player_id"].astype(int)
        print(f"Matched: {len(matched_players):,}")

        matched_players.to_csv(DATA_DIR / "props_player_updates.csv", index=False)
        print(f"Saved to {DATA_DIR}/props_player_updates.csv")

    # ========================================
    # MATCH TEAM_ID from player_game_stats
    # ========================================
    print("\n" + "=" * 60)
    print("MATCHING TEAM_ID FROM PLAYER_GAME_STATS")
    print("=" * 60)

    # 1. Check if the file exists immediately
    updates_file = DATA_DIR / "props_player_updates.csv"
    if not updates_file.exists():
        print("Skipping Team ID match: props_player_updates.csv not found.")
        print("Make sure the previous step (MATCHING PLAYERS) ran successfully.")
    else:
        # 2. Load the data
        player_updates = pd.read_csv(updates_file)

        # 3. HELPER: Clean Game IDs (PRESERVE LEADING ZEROS!)
        def clean_id(val):
            if pd.isna(val) or str(val).strip() == "":
                return None
            # Keep as string to preserve leading zeros - NBA game IDs are 10-char strings
            s = str(val).strip()
            # If it looks like a float string (has decimal), convert carefully
            if "." in s:
                try:
                    s = str(int(float(s)))
                except:
                    pass
            # Pad to 10 characters with leading zeros if needed
            if s.isdigit() and len(s) < 10:
                s = s.zfill(10)
            return s

        # 4. Build Lookup (player_id, clean_game_id) -> team_id
        # Clean stats IDs first
        player_game_stats["clean_game_id"] = player_game_stats["game_id"].apply(clean_id)

        pgs_lookup = {}
        for _, row in player_game_stats.iterrows():
            if pd.notna(row["player_id"]) and row["clean_game_id"]:
                pgs_lookup[(int(row["player_id"]), row["clean_game_id"])] = row["team_id"]

        # 5. Merge Game IDs if available
        if (DATA_DIR / "props_game_updates.csv").exists():
            game_updates = pd.read_csv(DATA_DIR / "props_game_updates.csv")
            player_updates = player_updates.merge(game_updates, on="staging_id", how="left")

        # 6. Fill missing Game IDs from original data
        orig_games = dict(zip(player_props["staging_id"], player_props["game_id"]))
        player_updates["game_id"] = player_updates.apply(
            lambda r: r["game_id"] if pd.notna(r.get("game_id")) else orig_games.get(r["staging_id"]),
            axis=1,
        )

        # 7. Clean IDs in the updates
        player_updates["clean_game_id"] = player_updates["game_id"].apply(clean_id)

        # 8. Perform Match
        def get_team_id(row):
            if pd.notna(row["player_id"]) and row["clean_game_id"]:
                return pgs_lookup.get((int(row["player_id"]), row["clean_game_id"]))
            return None

        tqdm.pandas(desc="Matching team_id")
        player_updates["team_id"] = player_updates.progress_apply(get_team_id, axis=1)

        # 9. Filter Results (Keep row if EITHER Player ID or Team ID is found)
        team_updates = player_updates[(player_updates["player_id"].notna()) | (player_updates["team_id"].notna())][
            ["staging_id", "player_id", "team_id"]
        ]

        print(f"Matched player_id: {len(team_updates[team_updates['player_id'].notna()]):,}")
        print(f"Matched team_id:   {len(team_updates[team_updates['team_id'].notna()]):,}")

        team_updates.to_csv(DATA_DIR / "props_full_updates.csv", index=False)
        print(f"Saved to {DATA_DIR}/props_full_updates.csv")

    print("\n" + "=" * 60)
    print("PROCESSING COMPLETE - SUMMARY")
    print("=" * 60)

    files = [
        ("game_lines_updates.csv", "Game lines to update"),
        ("props_game_updates.csv", "Props game_id to update (with fuzzy matching!)"),
        ("props_full_updates.csv", "Props player_id/team_id to update"),
        ("unmatched_game_lines.csv", "Unmatched game lines (review)"),
        ("unmatched_games.csv", "Unmatched prop games (review)"),
        ("unmatched_players.csv", "Unmatched players (review & add to player_mappings.csv)"),
    ]

    for filename, desc in files:
        filepath = DATA_DIR / filename
        if filepath.exists():
            df = pd.read_csv(filepath)
            print(f"  {filename}: {len(df):,} rows - {desc}")

    print("\nNext steps:")
    print("  1. Review unmatched_players.csv for suggested mappings")
    print("  2. Add confirmed mappings to player_mappings.csv (api_name,player_id)")
    print("  3. Re-run 'python nba_linker_local_fixed.py process' to apply new mappings")
    print("  4. Run 'python nba_linker_local_fixed.py upload' to push to database")
    print("\n[OK] Processing complete!")


# ============================================================================
# STEP 3: UPLOAD
# ============================================================================


def init_mappings():
    """Create empty player_mappings.csv template."""
    DATA_DIR.mkdir(exist_ok=True)
    mappings_file = DATA_DIR / "player_mappings.csv"

    if mappings_file.exists():
        print(f"{mappings_file} already exists!")
        return

    pd.DataFrame(columns=["api_name", "player_id"]).to_csv(mappings_file, index=False)
    print(f"Created {mappings_file}")
    print("Add mappings in format: api_name,player_id")
    print("Example: Shai Gilgeous-Alexander,1628983")


def upload_results():
    """Upload matched results back to database using chunked updates to avoid timeouts."""
    engine = get_engine()
    CHUNK_SIZE = 50000  # Adjust based on DB performance

    def chunked_update(df, temp_table_name, update_sql, description):
        if df.empty:
            return

        print(f"Uploading {description} ({len(df):,} rows)...")

        # Split the dataframe into smaller chunks
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE]

            with engine.begin() as conn:
                # 1. Upload chunk to temp table
                chunk.to_sql(temp_table_name, conn, if_exists="replace", index=False)

                # 2. Add an index to the temp table to make the join faster
                conn.execute(text(f"CREATE INDEX idx_{temp_table_name}_id ON {temp_table_name}(staging_id)"))

                # 3. Execute the update
                result = conn.execute(text(update_sql))

                # 4. Cleanup
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))

                print(f"  Processed rows {i:,} to {min(i + CHUNK_SIZE, len(df)):,}... Updated {result.rowcount:,} rows")

    # ========================================
    # 1. Game Lines Updates
    # ========================================
    game_lines_file = DATA_DIR / "game_lines_updates.csv"
    if game_lines_file.exists():
        df_lines = pd.read_csv(game_lines_file)
        df_lines = df_lines[df_lines["nba_game_id"].notna()]

        sql = """
            UPDATE raw_game_lines_staging r
            SET nba_game_id = t.nba_game_id,
                nba_home_team_id = t.nba_home_team_id::bigint,
                nba_away_team_id = t.nba_away_team_id::bigint
            FROM temp_game_lines_updates t
            WHERE r.staging_id = t.staging_id
        """
        chunked_update(df_lines, "temp_game_lines_updates", sql, "game lines")

    # ========================================
    # 2. Player Props Game ID Updates
    # ========================================
    props_game_file = DATA_DIR / "props_game_updates.csv"
    if props_game_file.exists():
        df_props_game = pd.read_csv(props_game_file)

        sql = """
            UPDATE raw_player_props_combined r
            SET game_id = t.game_id
            FROM temp_props_game_updates t
            WHERE r.staging_id = t.staging_id
        """
        chunked_update(df_props_game, "temp_props_game_updates", sql, "player props game_ids")

    # ========================================
    # 3. Player Props Player/Team Updates
    # ========================================
    props_full_file = DATA_DIR / "props_full_updates.csv"
    if props_full_file.exists():
        df_props_full = pd.read_csv(props_full_file)

        sql = """
            UPDATE raw_player_props_combined r
            SET player_id = t.player_id::bigint,
                team_id = t.team_id::bigint
            FROM temp_props_player_updates t
            WHERE r.staging_id = t.staging_id
        """
        chunked_update(df_props_full, "temp_props_player_updates", sql, "player props player/team info")

    print("\n[OK] Upload complete!")


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="NBA Data Linker - Local Processing (FIXED)")
    parser.add_argument("command", choices=["download", "process", "upload", "all", "init"], help="Command to run")

    args = parser.parse_args()

    if args.command == "download":
        download_tables()
    elif args.command == "process":
        process_local()
    elif args.command == "upload":
        upload_results()
    elif args.command == "init":
        init_mappings()
    elif args.command == "all":
        download_tables()
        process_local()
        upload_results()


if __name__ == "__main__":
    main()
