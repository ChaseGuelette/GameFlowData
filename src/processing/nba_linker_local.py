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
import logging
import sys
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
import pytz
from sqlalchemy import text
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _progress_apply(df, func, desc="Processing", **kwargs):
    """Apply with tqdm progress bar, falling back to plain apply if incompatible (pandas 3.0+)."""
    try:
        tqdm.pandas(desc=desc)
        return df.progress_apply(func, **kwargs)
    except (AttributeError, ImportError):
        return df.apply(func, **kwargs)


# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine  # noqa: E402

# ============================================================================
# CONFIG
# ============================================================================

DATA_DIR = Path("./linker_data")
EASTERN = pytz.timezone("America/New_York")
FUZZY_DATE_WINDOW_DAYS = 90  # Match games within ±90 days

# Team name aliases - normalize everything to 3-letter abbreviations
TEAM_NAME_ALIASES = {
    # Full names to abbreviations (for Odds API data)
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
    # LA variations
    "LA Clippers": "LAC",
    "L.A. Clippers": "LAC",
    "LA Lakers": "LAL",
    "L.A. Lakers": "LAL",
    # Historical franchises
    "New Jersey Nets": "BKN",
    "Charlotte Bobcats": "CHA",
    "New Orleans Hornets": "NOP",
    "New Orleans/Oklahoma City Hornets": "NOP",
    "Seattle SuperSonics": "OKC",
    "Vancouver Grizzlies": "MEM",
    # Also handle NOH -> NOP for team_matchup
    "NOH": "NOP",
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
            SELECT game_id, team_id, team_name, game_date as team_game_date, team_matchup, opponent_id
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
            print(f"CRITICAL ERROR: Failed to download {name}: {e}")
            sys.exit(1)

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


def normalize_player(name):
    """Normalize player name for matching."""
    if pd.isna(name):
        return name
    # Unicode normalization (strip accents)
    name = str(name)
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    name = name.lower().strip()
    for old, new in [(".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", "")]:
        name = name.replace(old, new)
    return " ".join(name.split())


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
    except Exception:
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
        except (ValueError, TypeError):
            continue

    if best_game_id:
        return best_game_id, best_diff
    else:
        return None, None


def process_local():
    """Process all matching locally in pandas with FUZZY DATE MATCHING."""
    print("Loading CSVs...")

    required_files = [
        "teams.csv",
        "players.csv",
        "team_game_stats.csv",
        "player_game_stats.csv",
        "game_lines.csv",
        "player_props.csv",
    ]
    missing = [f for f in required_files if not (DATA_DIR / f).exists()]
    if missing:
        print(f"\nCRITICAL ERROR: Missing required files: {missing}")
        print("Please run the 'download' command first to fetch these tables.")
        sys.exit(1)

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
        # Ensure game_id is a 10-digit string with leading zeros
        game_id = str(row["game_id"]).zfill(10) if pd.notna(row["game_id"]) else None
        game_lookup[key] = (game_id, row["team_id"], row["opponent_id"])

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
            # Ensure game_id is a 10-digit string with leading zeros
            game_id = str(row["game_id"]).zfill(10) if pd.notna(row["game_id"]) else None
            if game_id:
                props_game_lookup[key].append((game_id, row["game_date"]))

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
        # Unicode normalization (strip accents)
        name = str(name)
        name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
        name = name.lower().strip()
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

        matches = _progress_apply(unlinked_lines, match_game_line, desc="Matching game lines", axis=1)
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

        unlinked_props["matched_game_id"] = _progress_apply(
            unlinked_props, match_prop_game_fuzzy, desc="Matching props to games (fuzzy)", axis=1
        )

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

        needs_player["matched_player_id"] = _progress_apply(needs_player, match_player, desc="Matching players", axis=1)

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
                except (ValueError, TypeError):
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

        player_updates["team_id"] = _progress_apply(player_updates, get_team_id, desc="Matching team_id", axis=1)

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
# INCREMENTAL LINKING (Lightweight - No CSV Download)
# ============================================================================


def _fetch_upcoming_games_from_nba_api(target_date: date | None = None) -> list[dict]:
    """
    Fetch upcoming/today's games from NBA API ScoreboardV2.

    Returns list of dicts with: game_id, home_team, away_team, game_date
    """
    try:
        from nba_api.stats.endpoints import ScoreboardV2
        from nba_api.stats.static import teams as nba_teams
    except ImportError:
        logging.warning("nba_api not installed - cannot fetch upcoming games")
        return []

    if target_date is None:
        target_date = datetime.now(EASTERN).date()

    games = []

    # Build team ID to abbreviation lookup
    team_id_to_abbrev = {}
    for team in nba_teams.get_teams():
        team_id_to_abbrev[team["id"]] = team["abbreviation"]

    # Fetch games for today and next 2 days (to catch upcoming games)
    for days_ahead in range(3):
        check_date = target_date + timedelta(days=days_ahead)
        date_str = check_date.strftime("%Y-%m-%d")

        try:
            scoreboard = ScoreboardV2(game_date=date_str, timeout=10)
            game_header = scoreboard.get_data_frames()[0]  # GameHeader DataFrame

            for _, row in game_header.iterrows():
                game_id = str(row.get("GAME_ID", "")).zfill(10)
                home_team_id = row.get("HOME_TEAM_ID")
                away_team_id = row.get("VISITOR_TEAM_ID")

                home_abbrev = team_id_to_abbrev.get(home_team_id)
                away_abbrev = team_id_to_abbrev.get(away_team_id)

                if game_id and home_abbrev and away_abbrev:
                    games.append({
                        "game_id": game_id,
                        "home_team": home_abbrev,
                        "away_team": away_abbrev,
                        "game_date": date_str,
                    })

        except Exception as e:
            logging.warning(f"Failed to fetch games for {date_str}: {e}")
            continue

    logging.info(f"Fetched {len(games)} upcoming games from NBA API")

    # CDN fallback if NBA API returned nothing
    if not games:
        games = _fetch_upcoming_games_from_cdn(target_date)

    return games


def _fetch_upcoming_games_from_cdn(target_date: date | None = None) -> list[dict]:
    """Fallback: fetch games from cdn.nba.com schedule when stats.nba.com is blocked."""
    try:
        import requests

        TEAM_ID_TO_ABBREV = {
            1610612737: "ATL", 1610612738: "BOS", 1610612751: "BKN", 1610612766: "CHA",
            1610612741: "CHI", 1610612739: "CLE", 1610612742: "DAL", 1610612743: "DEN",
            1610612765: "DET", 1610612744: "GSW", 1610612745: "HOU", 1610612754: "IND",
            1610612746: "LAC", 1610612747: "LAL", 1610612763: "MEM", 1610612748: "MIA",
            1610612749: "MIL", 1610612750: "MIN", 1610612740: "NOP", 1610612752: "NYK",
            1610612760: "OKC", 1610612753: "ORL", 1610612755: "PHI", 1610612756: "PHX",
            1610612757: "POR", 1610612758: "SAC", 1610612759: "SAS", 1610612761: "TOR",
            1610612762: "UTA", 1610612764: "WAS",
        }

        if target_date is None:
            target_date = datetime.now(EASTERN).date()

        url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        games = []
        for days_ahead in range(3):
            check_date = target_date + timedelta(days=days_ahead)
            date_str = check_date.strftime("%m/%d/%Y 00:00:00")

            for gd in data.get("leagueSchedule", {}).get("gameDates", []):
                if gd.get("gameDate") == date_str:
                    for g in gd.get("games", []):
                        if g.get("weekNumber", 0) > 0:
                            home_abbrev = g["homeTeam"]["teamTricode"]
                            away_abbrev = g["awayTeam"]["teamTricode"]
                            games.append({
                                "game_id": str(g["gameId"]).zfill(10),
                                "home_team": home_abbrev,
                                "away_team": away_abbrev,
                                "game_date": check_date.strftime("%Y-%m-%d"),
                            })
                    break

        logging.info(f"Fetched {len(games)} upcoming games from CDN fallback")
        return games

    except Exception as e:
        logging.warning(f"CDN fallback also failed: {e}")
        return []


def link_incremental(batch_size: int = 50000, limit: int | None = None):
    """
    Lightweight incremental linker for daily use.

    - No CSV download of entire tables
    - Queries only unlinked records
    - Updates directly via batched SQL

    Args:
        batch_size: Number of records to process per batch
        limit: Optional limit on total records to process (for testing)
    """
    engine = get_engine()

    print("=" * 60)
    print("INCREMENTAL LINKER (Lightweight Mode)")
    print("=" * 60)

    # 1. Load reference tables (small, one-time)
    print("\n[1/4] Loading reference tables...")

    with engine.connect() as conn:
        teams_df = pd.read_sql("SELECT team_id, team_name FROM teams", conn)
        players_df = pd.read_sql("SELECT player_id, player_name FROM players", conn)
        games_df = pd.read_sql("""
            SELECT game_id, team_id, team_name, game_date as team_game_date, team_matchup, opponent_id
            FROM team_game_stats
        """, conn)

    print(f"  Loaded {len(teams_df)} teams, {len(players_df)} players, {len(games_df)} game records")

    # Build lookups
    team_lookup = {}
    for _, row in teams_df.iterrows():
        norm = normalize_team(row["team_name"])
        if norm:
            team_lookup[norm] = int(row["team_id"])

    player_lookup = {}
    for _, row in players_df.iterrows():
        norm = normalize_player(row["player_name"])
        if norm and not pd.isna(norm) and isinstance(norm, str):
            player_lookup[norm] = int(row["player_id"])

    # Load manual mappings if available
    manual_mappings = {}
    mappings_file = DATA_DIR / "player_mappings.csv"
    if mappings_file.exists():
        try:
            mappings_df = pd.read_csv(mappings_file)
            for _, row in mappings_df.iterrows():
                if pd.notna(row.get("api_name")) and pd.notna(row.get("player_id")):
                    manual_mappings[row["api_name"]] = int(row["player_id"])
            print(f"  Loaded {len(manual_mappings)} manual player mappings")
        except Exception as e:
            print(f"  Warning: Could not load manual mappings: {e}")

    # Build game lookup: (home_team_norm, away_team_norm) -> [(game_id, game_date), ...]
    props_game_lookup = defaultdict(list)

    # Source 1: Historical games from team_game_stats (completed games)
    historical_count = 0
    for _, row in games_df.iterrows():
        matchup = row.get("team_matchup", "")
        if not matchup or pd.isna(matchup):
            continue

        if " vs. " in matchup:
            home_team = matchup.split(" vs. ")[0].strip()
            away_team = matchup.split(" vs. ")[1].strip()
        elif " @ " in matchup:
            away_team = matchup.split(" @ ")[0].strip()
            home_team = matchup.split(" @ ")[1].strip()
        else:
            continue

        home_norm = normalize_team(home_team)
        away_norm = normalize_team(away_team)
        game_date = str(row["team_game_date"])[:10]

        if home_norm and away_norm:
            # Ensure game_id is a 10-digit string with leading zeros
            game_id = str(row["game_id"]).zfill(10) if pd.notna(row["game_id"]) else None
            if game_id:
                props_game_lookup[(home_norm, away_norm)].append((game_id, game_date))
                historical_count += 1

    print(f"  Historical games from DB: {historical_count}")

    # Source 2: Upcoming games from NBA API (today + next 2 days)
    # This is critical for linking props for games that haven't been played yet
    upcoming_games = _fetch_upcoming_games_from_nba_api()
    upcoming_count = 0
    for game in upcoming_games:
        home_norm = normalize_team(game["home_team"])
        away_norm = normalize_team(game["away_team"])
        game_id = game["game_id"]
        game_date = game["game_date"]

        if home_norm and away_norm and game_id:
            # Add to lookup (may duplicate with historical, but that's fine)
            props_game_lookup[(home_norm, away_norm)].append((game_id, game_date))
            upcoming_count += 1

    print(f"  Upcoming games from NBA API: {upcoming_count}")
    print(f"  Built game lookup with {len(props_game_lookup)} unique matchups")

    # 2. Count unlinked records
    print("\n[2/4] Counting unlinked records...")

    with engine.connect() as conn:
        count_result = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined WHERE player_id IS NULL
        """))
        total_unlinked = count_result.scalar()

    print(f"  Total unlinked: {total_unlinked:,}")

    if total_unlinked == 0:
        print("\n[OK] No unlinked records - nothing to do!")
        return

    if limit:
        total_to_process = min(total_unlinked, limit)
        print(f"  Processing limit: {limit:,}")
    else:
        total_to_process = total_unlinked

    # 3. Process in batches
    print(f"\n[3/4] Processing {total_to_process:,} records in batches of {batch_size:,}...")

    total_game_matched = 0
    total_player_matched = 0
    offset = 0

    while offset < total_to_process:
        current_batch_size = min(batch_size, total_to_process - offset)

        # Fetch batch of unlinked records
        with engine.connect() as conn:
            batch_df = pd.read_sql(f"""
                SELECT staging_id, api_player_name, home_team, away_team, commence_time
                FROM raw_player_props_combined
                WHERE player_id IS NULL
                ORDER BY staging_id
                LIMIT {current_batch_size} OFFSET {offset}
            """, conn)

        if batch_df.empty:
            break

        # Process batch
        batch_df["commence_time"] = pd.to_datetime(batch_df["commence_time"], utc=True)
        batch_df["game_date"] = batch_df["commence_time"].dt.tz_convert(EASTERN).dt.strftime("%Y-%m-%d")
        batch_df["home_team_norm"] = batch_df["home_team"].apply(normalize_team)
        batch_df["away_team_norm"] = batch_df["away_team"].apply(normalize_team)
        batch_df["player_name_norm"] = batch_df["api_player_name"].apply(normalize_player)

        # Match games
        def match_game(row):
            key = (row["home_team_norm"], row["away_team_norm"])
            candidates = props_game_lookup.get(key, [])
            if not candidates:
                return None
            matched_game_id, _ = find_closest_game_date(candidates, row["game_date"])
            return matched_game_id

        batch_df["matched_game_id"] = batch_df.apply(match_game, axis=1)

        # Match players
        def match_player(row):
            api_name = row["api_player_name"]
            norm_name = row["player_name_norm"]

            if api_name in manual_mappings:
                return manual_mappings[api_name]
            if norm_name in player_lookup:
                return player_lookup[norm_name]

            # Fuzzy match fallback
            if norm_name and isinstance(norm_name, str):
                best_match = None
                best_score = 0.80  # Threshold
                for pname, pid in player_lookup.items():
                    if not pname or not isinstance(pname, str):
                        continue
                    score = SequenceMatcher(None, norm_name, pname).ratio()
                    # Bonus for matching last name
                    norm_parts = norm_name.split()
                    pname_parts = pname.split()
                    if norm_parts and pname_parts and norm_parts[-1] == pname_parts[-1]:
                        score += 0.15
                    if score > best_score:
                        best_score = score
                        best_match = pid
                return best_match
            return None

        batch_df["matched_player_id"] = batch_df.apply(match_player, axis=1)

        # Get team_id for matched players
        def get_team_id(row):
            if pd.isna(row["matched_player_id"]) or pd.isna(row["matched_game_id"]):
                return None
            # Look up from player_game_stats or infer from home/away
            home_norm = row["home_team_norm"]
            away_norm = row["away_team_norm"]
            # Simple heuristic: check if player is typically on home or away team
            # For now, return None and let it be filled later
            return team_lookup.get(home_norm) or team_lookup.get(away_norm)

        batch_df["matched_team_id"] = batch_df.apply(get_team_id, axis=1)

        # Prepare updates
        game_updates = batch_df[batch_df["matched_game_id"].notna()][["staging_id", "matched_game_id"]].copy()
        player_updates = batch_df[batch_df["matched_player_id"].notna()][
            ["staging_id", "matched_player_id", "matched_team_id"]
        ].copy()

        # Apply updates
        with engine.begin() as conn:
            # Update game_id
            if not game_updates.empty:
                game_updates.to_sql("temp_game_updates", conn, if_exists="replace", index=False)
                conn.execute(text("CREATE INDEX idx_temp_game_staging ON temp_game_updates(staging_id)"))
                result = conn.execute(text("""
                    UPDATE raw_player_props_combined r
                    SET game_id = t.matched_game_id
                    FROM temp_game_updates t
                    WHERE r.staging_id = t.staging_id
                """))
                total_game_matched += result.rowcount
                conn.execute(text("DROP TABLE temp_game_updates"))

            # Update player_id and team_id
            if not player_updates.empty:
                player_updates.to_sql("temp_player_updates", conn, if_exists="replace", index=False)
                conn.execute(text("CREATE INDEX idx_temp_player_staging ON temp_player_updates(staging_id)"))
                result = conn.execute(text("""
                    UPDATE raw_player_props_combined r
                    SET player_id = t.matched_player_id::bigint,
                        team_id = t.matched_team_id::bigint
                    FROM temp_player_updates t
                    WHERE r.staging_id = t.staging_id
                """))
                total_player_matched += result.rowcount
                conn.execute(text("DROP TABLE temp_player_updates"))

        offset += current_batch_size
        pct = (offset / total_to_process) * 100
        print(f"  Processed {offset:,}/{total_to_process:,} ({pct:.1f}%) - Games: {total_game_matched:,}, Players: {total_player_matched:,}")

    # 4. Summary
    print("\n[4/4] Summary")
    print("=" * 60)
    print(f"  Total records processed: {total_to_process:,}")
    print(f"  Games matched: {total_game_matched:,}")
    print(f"  Players matched: {total_player_matched:,}")

    # Check remaining unlinked (player_id)
    with engine.connect() as conn:
        remaining = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined WHERE player_id IS NULL
        """)).scalar()

    print(f"  Remaining unlinked (no player_id): {remaining:,}")

    # 5. Link game_id for records that have player_id but missing game_id
    #    (props scraper sets player_id from odds API, but game_id needs NBA game ID mapping)
    with engine.connect() as conn:
        missing_game_id = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined
            WHERE player_id IS NOT NULL AND game_id IS NULL
        """)).scalar()

    if missing_game_id > 0:
        print(f"\n[5/5] Linking game_id for {missing_game_id:,} records (have player_id, missing game_id)...")
        game_id_matched = 0

        with engine.connect() as conn:
            batch_df = pd.read_sql("""
                SELECT DISTINCT staging_id, home_team, away_team, commence_time
                FROM raw_player_props_combined
                WHERE player_id IS NOT NULL AND game_id IS NULL
                ORDER BY staging_id
            """, conn)

        if not batch_df.empty:
            batch_df["commence_time"] = pd.to_datetime(batch_df["commence_time"], utc=True)
            batch_df["game_date"] = batch_df["commence_time"].dt.tz_convert(EASTERN).dt.strftime("%Y-%m-%d")
            batch_df["home_team_norm"] = batch_df["home_team"].apply(normalize_team)
            batch_df["away_team_norm"] = batch_df["away_team"].apply(normalize_team)

            def match_game(row):
                key = (row["home_team_norm"], row["away_team_norm"])
                candidates = props_game_lookup.get(key, [])
                if not candidates:
                    return None
                # Use max_days=0 for exact date match — props should only link
                # to games on the same date (prevents linking to wrong historical games)
                matched_game_id, diff = find_closest_game_date(candidates, row["game_date"], max_days=0)
                if matched_game_id is None:
                    # Fallback: allow ±1 day for timezone edge cases
                    matched_game_id, _ = find_closest_game_date(candidates, row["game_date"], max_days=1)
                return matched_game_id

            batch_df["matched_game_id"] = batch_df.apply(match_game, axis=1)
            game_updates = batch_df[batch_df["matched_game_id"].notna()][["staging_id", "matched_game_id"]]

            if not game_updates.empty:
                with engine.begin() as conn:
                    game_updates.to_sql("temp_game_id_updates", conn, if_exists="replace", index=False)
                    conn.execute(text("CREATE INDEX idx_temp_gid_staging ON temp_game_id_updates(staging_id)"))
                    result = conn.execute(text("""
                        UPDATE raw_player_props_combined r
                        SET game_id = t.matched_game_id
                        FROM temp_game_id_updates t
                        WHERE r.staging_id = t.staging_id
                    """))
                    game_id_matched = result.rowcount
                    conn.execute(text("DROP TABLE temp_game_id_updates"))

            print(f"  Game IDs linked: {game_id_matched:,}")
    else:
        print("\n  No records missing game_id — skipping game_id linking pass.")

    print("\n[OK] Incremental linking complete!")


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="NBA Data Linker - Local Processing (FIXED)")
    parser.add_argument(
        "command",
        choices=["download", "process", "upload", "all", "init", "incremental"],
        help="Command to run. Use 'incremental' for lightweight daily linking."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Batch size for incremental linking (default: 50000)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit total records to process (for testing)"
    )

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
    elif args.command == "incremental":
        link_incremental(batch_size=args.batch_size, limit=args.limit)


if __name__ == "__main__":
    main()
