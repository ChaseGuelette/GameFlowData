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
import json
import logging
import sys
import unicodedata
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, cast

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
from src.models.features.nba.season import is_competitive_nba_game_id  # noqa: E402
from src.scrapers.nba_cdn_scraper import TEAM_ID_TO_ABBREV, fetch_schedule  # noqa: E402
from src.scrapers.nba_schedule import parse_schedule, validate_source_freshness  # noqa: E402

# ============================================================================
# CONFIG
# ============================================================================

DATA_DIR = Path("./linker_data")
EASTERN = pytz.timezone("America/New_York")
FUZZY_DATE_WINDOW_DAYS = 90  # Match games within ±90 days
FUZZY_PLAYER_THRESHOLD = 0.80  # Minimum SequenceMatcher score for fuzzy match

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

        # Load fuzzy cache
        fuzzy_cache, cached_player_count = _load_fuzzy_cache()
        current_player_count = len(player_lookup)
        if cached_player_count != current_player_count:
            if fuzzy_cache:
                print(f"  Fuzzy cache invalidated: player count changed ({cached_player_count} -> {current_player_count})")
            fuzzy_cache = {}
        else:
            print(f"  Fuzzy cache loaded: {len(fuzzy_cache)} cached name mappings")

        # Step A: Manual mappings (api_name -> player_id)
        needs_player["matched_player_id"] = needs_player["api_player_name"].map(manual_mappings)

        # Step B: Exact match on normalized name (vectorized)
        unmatched_mask = needs_player["matched_player_id"].isna()
        if unmatched_mask.any():
            exact = needs_player.loc[unmatched_mask, "player_name_norm"].map(player_lookup)
            has_exact = exact.notna()
            if has_exact.any():
                needs_player.loc[has_exact[has_exact].index, "matched_player_id"] = exact[has_exact]

        # Step C: Fuzzy match with cache
        unmatched_mask = needs_player["matched_player_id"].isna() & needs_player["player_name_norm"].notna()
        if unmatched_mask.any():
            unmatched_names = set(needs_player.loc[unmatched_mask, "player_name_norm"].unique())
            new_names = unmatched_names - set(fuzzy_cache.keys())

            if new_names:
                print(f"  Fuzzy matching {len(new_names)} new names (cache has {len(fuzzy_cache)})...")
                new_fuzzy = _resolve_fuzzy_names(new_names, player_lookup)
                fuzzy_cache.update(new_fuzzy)

            cache_hits = len(unmatched_names) - len(new_names)
            print(f"  Cache: {cache_hits} hits, {len(new_names)} new fuzzy lookups")

            # Apply fuzzy results from cache
            fuzzy_mapped = needs_player.loc[unmatched_mask, "player_name_norm"].map(fuzzy_cache)
            has_fuzzy = fuzzy_mapped.notna()
            if has_fuzzy.any():
                needs_player.loc[has_fuzzy[has_fuzzy].index, "matched_player_id"] = fuzzy_mapped[has_fuzzy]

        # Save fuzzy cache
        _save_fuzzy_cache(fuzzy_cache, current_player_count)
        print(f"  Fuzzy cache saved: {len(fuzzy_cache)} entries")

        # Track unmatched (still no match after fuzzy)
        unmatched = needs_player[needs_player["matched_player_id"].isna()]["api_player_name"].unique()

        if len(unmatched) > 0:
            print(f"\n  {len(unmatched)} unmatched player names - generating suggestions...")

            # Fuzzy match suggestions for human review
            def find_best_matches(api_name, top_n=3):
                api_lower = api_name.lower().strip()
                scores = []
                for norm_name, pid in player_lookup.items():
                    if pd.isna(norm_name) or not isinstance(norm_name, str):
                        continue
                    score = SequenceMatcher(None, api_lower, norm_name).ratio()
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
                            "player_id": best_id if best_score >= 0.7 else "",
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


def _linkable_schedule_games(
    rows: list[dict], target_date: date, end_date: date | None = None
) -> list[dict]:
    """Map competitive schedule rows to the existing props-linker contract."""
    end_date = end_date or target_date + timedelta(days=3)
    games = []
    for row in rows:
        game_date = row["game_date"]
        if isinstance(game_date, str):
            game_date = date.fromisoformat(game_date)
        if not target_date <= game_date < end_date:
            continue
        game_id = str(row["game_id"])
        status_text = (row.get("status_text") or "").lower()
        if (
            not is_competitive_nba_game_id(game_id)
            or row.get("game_status") == 4
            or "cancel" in status_text
            or "postpon" in status_text
            or row.get("postponed_status") not in (None, "", "N")
            or row.get("commence_time_utc") is None
        ):
            continue
        home_id = row.get("home_team_id")
        away_id = row.get("away_team_id")
        home = TEAM_ID_TO_ABBREV.get(home_id) if isinstance(home_id, int) else None
        away = TEAM_ID_TO_ABBREV.get(away_id) if isinstance(away_id, int) else None
        if home and away and home != away:
            games.append({
                "game_id": game_id,
                "home_team": home,
                "away_team": away,
                "game_date": game_date.isoformat(),
            })
    return games


def _fetch_upcoming_games_from_schedule(
    engine, target_date: date | None = None, *, observed_at: datetime | None = None,
    bounded: bool = False,
    status_out: dict[str, Any] | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """Use recent persisted CDN schedule rows, falling back to direct CDN."""
    legacy_fallback = status_out is None and end_date is None
    target_date = target_date or datetime.now(EASTERN).date()
    end_date = end_date or target_date + timedelta(days=3)

    def fallback():
        if legacy_fallback:
            return _fetch_upcoming_games_from_cdn(target_date)
        return _fetch_upcoming_games_from_cdn(target_date, status_out=status_out, end_date=end_date)

    now = observed_at or datetime.now(UTC)
    query = text("""
        SELECT game_id, game_date, home_team_id, away_team_id, commence_time_utc,
               game_status, status_text, postponed_status, observed_at, source_updated_at
        FROM public.nba_game_schedule
        WHERE game_date >= :target_date AND game_date < :end_date
          AND game_type IN ('002', '004', '005')
        ORDER BY game_date, game_id
    """)
    try:
        with engine.connect() as conn:
            if bounded:
                conn.execute(text("SET LOCAL statement_timeout = '15s'"))
                conn.execute(text("SET LOCAL lock_timeout = '2s'"))
                conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = '15s'"))
            rows = [dict(row._mapping) for row in conn.execute(
                query, {"target_date": target_date, "end_date": end_date}
            )]
    except Exception as exc:
        logging.warning("NBA schedule DB read failed; using CDN: %s", exc)
        return fallback()
    if not rows:
        return fallback()
    for row in rows:
        max_age = timedelta(minutes=90) if row["game_date"] <= now.astimezone(EASTERN).date() else timedelta(hours=48)
        timestamps = (row.get("observed_at"), row.get("source_updated_at"))
        if any(ts is None or ts.tzinfo is None or not -timedelta(minutes=15) <= now - ts <= max_age for ts in timestamps):
            logging.warning("NBA schedule snapshot is stale or incomplete; using CDN")
            return fallback()
        if row.get("game_status") != 4 and row.get("postponed_status") in (None, "", "N"):
            if row.get("home_team_id") is None or row.get("away_team_id") is None or row.get("commence_time_utc") is None:
                return fallback()
    if status_out is not None:
        status_out.update(reference_available=True, source="persisted_schedule")
    return _linkable_schedule_games(rows, target_date, end_date)


def _fetch_upcoming_games_from_cdn(
    target_date: date | None = None, *, status_out: dict[str, Any] | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """Railway-safe direct CDN fallback for upcoming competitive games."""
    target_date = target_date or datetime.now(EASTERN).date()
    end_date = end_date or target_date + timedelta(days=3)
    try:
        rows = parse_schedule(fetch_schedule())
        validate_source_freshness(rows, datetime.now(UTC))
        games = _linkable_schedule_games(rows, target_date, end_date)
        if status_out is not None:
            status_out.update(reference_available=True, source="direct_cdn")
        logging.info("Fetched %d upcoming games from CDN", len(games))
        return games
    except Exception as exc:
        if status_out is not None:
            status_out.update(reference_available=False, source="unavailable", error=str(exc))
        logging.warning("CDN schedule fallback failed: %s", exc)
        return []


# ============================================================================
# FUZZY CACHE — persist name→player_id mappings to disk
# ============================================================================


def _load_fuzzy_cache() -> tuple[dict[str, int | None], int]:
    """Load cached fuzzy matches and the player count used to build them.

    Returns:
        (cache_dict, player_count) where cache_dict maps normalized_name -> player_id or None,
        and player_count is the number of players when the cache was last saved (for invalidation).
    """
    cache_file = DATA_DIR / "_fuzzy_cache.json"
    if not cache_file.exists():
        return {}, 0
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        cache = data.get("cache", {})
        player_count = data.get("player_count", 0)
        return cache, player_count
    except (json.JSONDecodeError, KeyError, OSError) as e:
        logging.warning(f"Could not load fuzzy cache, starting fresh: {e}")
        return {}, 0


def _save_fuzzy_cache(cache: dict[str, int | None], player_count: int) -> None:
    """Persist fuzzy cache to disk."""
    DATA_DIR.mkdir(exist_ok=True)
    cache_file = DATA_DIR / "_fuzzy_cache.json"
    payload = {"player_count": player_count, "cache": cache}
    cache_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _resolve_fuzzy_names(
    unmatched_names: set[str], player_lookup: dict[str, int]
) -> dict[str, int | None]:
    """Resolve unique unmatched names via fuzzy matching (run once per unique name).

    Returns dict mapping normalized_name -> player_id (or None if no match).
    """
    result: dict[str, int | None] = {}
    lookup_items = list(player_lookup.items())

    for norm in unmatched_names:
        best_match = None
        best_score = FUZZY_PLAYER_THRESHOLD
        norm_parts = norm.split()
        norm_last = norm_parts[-1] if norm_parts else ""

        for pname, pid in lookup_items:
            if not pname or not isinstance(pname, str):
                continue
            score = SequenceMatcher(None, norm, pname).ratio()
            # Bonus for matching last name
            pname_parts = pname.split()
            if norm_last and pname_parts and norm_last == pname_parts[-1]:
                score += 0.15
            if score > best_score:
                best_score = score
                best_match = pid

        result[norm] = best_match

    return result


LINKER_STATE_NAME = "production"
LINKER_ADVISORY_LOCK_KEY = 629_817_204_113
LINKER_STATE_TABLE = "public.nba_props_linker_state"


def _set_linker_timeouts(conn, *, local: bool = True) -> None:
    prefix = "SET LOCAL" if local else "SET"
    for setting in (
        "statement_timeout = '20s'",
        "lock_timeout = '2s'",
        "idle_in_transaction_session_timeout = '20s'",
    ):
        conn.execute(text(f"{prefix} {setting}"))


def _read_linker_records(conn, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    return [dict(row._mapping) for row in conn.execute(text(query), params or {})]


def _window_fixture_dates(engine, cursor: int, high_water: int, *, row_limit: int | None = None) -> tuple[date, date]:
    """Return ET fixture dates for only the next processable PK rows."""
    with engine.begin() as conn:
        _set_linker_timeouts(conn)
        row = conn.execute(text("""
            SELECT MIN(commence_time) AS min_time, MAX(commence_time) AS max_time
            FROM (
                SELECT commence_time FROM raw_player_props_combined
                WHERE staging_id > :cursor AND staging_id <= :high_water
                ORDER BY staging_id LIMIT :row_limit
            ) bounded
        """), {"cursor": cursor, "high_water": high_water, "row_limit": row_limit}).one()
    if row.min_time is None:
        today = datetime.now(EASTERN).date()
        return today, today + timedelta(days=1)
    start = pd.to_datetime(row.min_time, utc=True).tz_convert(EASTERN).date()
    end = pd.to_datetime(row.max_time, utc=True).tz_convert(EASTERN).date() + timedelta(days=1)
    return start, end


def _load_linker_context(
    engine, fixture_start: date | None = None, fixture_end: date | None = None
) -> dict[str, Any]:
    with engine.begin() as conn:
        _set_linker_timeouts(conn)
        teams = _read_linker_records(conn, "SELECT team_id, team_name FROM teams")
        players = _read_linker_records(conn, "SELECT player_id, player_name FROM players")
        games = _read_linker_records(conn, """
            SELECT game_id, team_id, team_name, game_date AS team_game_date,
                   team_matchup, opponent_id
            FROM team_game_stats
        """)

    team_ids: defaultdict[str, set[int]] = defaultdict(set)
    player_names: defaultdict[str, set[int]] = defaultdict(set)
    manual: defaultdict[str, set[int]] = defaultdict(set)
    for row in teams:
        normalized = normalize_team(row["team_name"])
        if isinstance(normalized, str) and normalized:
            team_ids[normalized].add(int(row["team_id"]))
    known_players = {int(row["player_id"]) for row in players if row.get("player_id") is not None}
    for row in players:
        normalized = normalize_player(row["player_name"])
        if isinstance(normalized, str) and normalized and row.get("player_id") is not None:
            player_names[normalized].add(int(row["player_id"]))

    mappings_file = DATA_DIR / "player_mappings.csv"
    if mappings_file.exists():
        for row in pd.read_csv(mappings_file).to_dict("records"):
            api_name, player_id = row.get("api_name"), row.get("player_id")
            if isinstance(api_name, str) and pd.notna(player_id):
                manual[api_name].add(int(player_id))

    by_match: defaultdict[tuple[str, str, str], set[str]] = defaultdict(set)
    by_game: defaultdict[str, set[tuple[str, str, str]]] = defaultdict(set)

    def add_game(game_id: Any, home: Any, away: Any, game_date: Any) -> None:
        game_id = str(game_id) if game_id is not None else ""
        if not is_competitive_nba_game_id(game_id) or not isinstance(home, str) or not isinstance(away, str):
            return
        key = (normalize_team(home), normalize_team(away), str(game_date)[:10])
        if not all(isinstance(value, str) and value for value in key) or key[0] == key[1]:
            return
        by_match[key].add(game_id)
        by_game[game_id].add(key)

    for row in games:
        matchup = row.get("team_matchup")
        if not isinstance(matchup, str):
            continue
        if " vs. " in matchup:
            home, away = matchup.split(" vs. ", 1)
        elif " @ " in matchup:
            away, home = matchup.split(" @ ", 1)
        else:
            continue
        add_game(row.get("game_id"), home, away, row.get("team_game_date"))

    schedule_status: dict[str, Any] = {}
    for row in _fetch_upcoming_games_from_schedule(
        engine,
        target_date=fixture_start,
        end_date=fixture_end,
        bounded=True,
        status_out=schedule_status,
    ):
        add_game(row["game_id"], row["home_team"], row["away_team"], row["game_date"])

    return {
        "team_ids": team_ids,
        "player_names": player_names,
        "manual": manual,
        "known_players": known_players,
        "by_match": by_match,
        "by_game": by_game,
        "schedule_status": schedule_status,
    }


def _unique_reference(values: set[Any]) -> Any | None:
    return next(iter(values)) if len(values) == 1 else None


def _resolve_linker_page(
    conn, rows: list[dict[str, Any]], context: dict[str, Any], *, apply_updates: bool,
    repair_game_conflicts: bool = False,
) -> dict[str, Any]:
    proposed = {"player_id": 0, "game_id": 0, "team_id": 0}
    corrected_game_ids = 0
    conflicts: set[int] = set()
    reference_unverified: set[int] = set()
    resolved: list[
        tuple[dict[str, Any], dict[str, Any], Any, Any, tuple[Any, Any, Any], bool]
    ] = []
    team_eligible: set[int] = set()

    for row in rows:
        staging_id = int(row["staging_id"])
        old = {column: None if pd.isna(row.get(column)) else row.get(column) for column in proposed}
        normalized_name = normalize_player(row.get("api_player_name"))
        candidates = context["manual"].get(
            row.get("api_player_name"), context["player_names"].get(normalized_name, set())
        )
        candidate_player = _unique_reference(candidates)
        player_id = old["player_id"] if old["player_id"] is not None else candidate_player
        player_verified = player_id is not None and int(player_id) in context["known_players"]
        if old["player_id"] is not None and candidates and int(old["player_id"]) not in candidates:
            conflicts.add(staging_id)
            player_verified = False
        elif old["player_id"] is not None and not candidates:
            reference_unverified.add(staging_id)
        if old["player_id"] is None and not player_verified:
            player_id = None

        parsed = pd.to_datetime(row.get("commence_time"), utc=True, errors="coerce")
        game_date = parsed.tz_convert(EASTERN).strftime("%Y-%m-%d") if pd.notna(parsed) else ""
        key = (normalize_team(row.get("home_team")), normalize_team(row.get("away_team")), game_date)
        expected_game = _unique_reference(context["by_match"].get(key, set()))
        game_id = old["game_id"] if old["game_id"] is not None else expected_game
        expected_game_verified = (
            isinstance(expected_game, str)
            and is_competitive_nba_game_id(expected_game)
            and len(context["by_game"].get(expected_game, set())) == 1
        )
        game_verified = expected_game_verified and game_id == expected_game
        repair_candidate = bool(
            repair_game_conflicts
            and old["game_id"] is not None
            and old["game_id"] != expected_game
            and expected_game_verified
            and candidate_player is not None
            and old["player_id"] is not None
            and int(candidate_player) == int(old["player_id"])
            and player_verified
        )
        if expected_game is None:
            reference_unverified.add(staging_id)
        elif old["game_id"] is not None and not game_verified and not repair_candidate:
            conflicts.add(staging_id)
        if old["game_id"] is None and not game_verified:
            game_id = None
        elif repair_candidate:
            game_id = expected_game
            game_verified = True
        if player_verified and game_verified:
            team_eligible.add(staging_id)
        resolved.append((row, old, player_id, game_id, key, repair_candidate))

    pairs = sorted(
        {(int(player_id), game_id) for row, _, player_id, game_id, _, _ in resolved
         if int(row["staging_id"]) in team_eligible}
    )
    relations: defaultdict[tuple[int, str], set[int]] = defaultdict(set)
    if pairs:
        params = {
            f"{column}{index}": value
            for index, pair in enumerate(pairs)
            for column, value in zip(("p", "g"), pair)
        }
        values = ", ".join(f"(:p{index}, :g{index})" for index in range(len(pairs)))
        for relation in _read_linker_records(conn, f"""
            SELECT player_id, game_id, team_id
            FROM player_game_stats
            WHERE (player_id, game_id) IN ({values})
        """, params):
            relations[(int(relation["player_id"]), str(relation["game_id"]))].add(int(relation["team_id"]))

    updates: list[dict[str, Any]] = []
    unresolved_ids: list[int] = []
    for row, old, player_id, game_id, key, repair_candidate in resolved:
        staging_id = int(row["staging_id"])
        team_id = None
        if staging_id in team_eligible:
            team_id = _unique_reference(relations.get((int(player_id), str(game_id)), set()))
            home_id = _unique_reference(context["team_ids"].get(key[0], set()))
            away_id = _unique_reference(context["team_ids"].get(key[1], set()))
            if home_id is None or away_id is None or home_id == away_id or team_id not in (home_id, away_id):
                team_id = None
        game_correction = bool(
            repair_candidate
            and team_id is not None
            and (old["team_id"] is None or int(old["team_id"]) == int(team_id))
        )
        if repair_candidate and not game_correction:
            game_id = old["game_id"]
            conflicts.add(staging_id)
            if team_id is None:
                reference_unverified.add(staging_id)
        if old["team_id"] is not None:
            if team_id is None:
                reference_unverified.add(staging_id)
            elif int(old["team_id"]) != int(team_id):
                conflicts.add(staging_id)
            team_id = old["team_id"]

        new = {"player_id": player_id, "game_id": game_id, "team_id": team_id}
        fills = [column for column in proposed if old[column] is None and new[column] is not None]
        for column in fills:
            proposed[column] += 1
        if game_correction:
            corrected_game_ids += 1
        effective = {column: old[column] if old[column] is not None else new[column] for column in proposed}
        if game_correction:
            effective["game_id"] = game_id
        if any(value is None for value in effective.values()):
            unresolved_ids.append(staging_id)
        if fills or game_correction:
            updates.append({
                "id": staging_id,
                "repair_game": game_correction,
                **new,
                **{f"old_{column}": value for column, value in old.items()},
                **{column: row.get(column) for column in (
                    "api_player_name", "home_team", "away_team", "commence_time"
                )},
            })

    changed = 0
    if updates and apply_updates:
        result = conn.execute(text("""
            UPDATE raw_player_props_combined
            SET player_id = COALESCE(player_id, :player_id),
                game_id = CASE WHEN :repair_game THEN :game_id ELSE COALESCE(game_id, :game_id) END,
                team_id = COALESCE(team_id, :team_id)
            WHERE staging_id = :id
              AND player_id IS NOT DISTINCT FROM :old_player_id
              AND game_id IS NOT DISTINCT FROM :old_game_id
              AND team_id IS NOT DISTINCT FROM :old_team_id
              AND api_player_name IS NOT DISTINCT FROM :api_player_name
              AND home_team IS NOT DISTINCT FROM :home_team
              AND away_team IS NOT DISTINCT FROM :away_team
              AND commence_time IS NOT DISTINCT FROM :commence_time
        """), updates)
        changed = int(result.rowcount or 0)
        if changed != len(updates):
            raise RuntimeError(
                f"stale source or partial linker write: expected {len(updates)} rows, updated {changed}; checkpoint not advanced"
            )

    return {
        "proposed": proposed,
        "corrected_game_ids": corrected_game_ids,
        "changed": changed,
        "conflicts": sorted(conflicts),
        "reference_unverified": sorted(reference_unverified),
        "first_unresolved_id": min(
            [*unresolved_ids, *conflicts, *reference_unverified], default=None
        ),
    }


def _summary_json(summary: dict[str, Any]) -> None:
    print("LINKER_SUMMARY " + json.dumps(summary, sort_keys=True), flush=True)


def _run_fixed_linker(
    engine, context: dict[str, Any], *, cursor: int, high_water: int,
    batch_size: int, max_batches: int, dry_run: bool, mode: str = "fixed_window",
    repair_game_conflicts: bool = False,
) -> dict[str, Any]:
    current = cursor
    totals = {"player_id": 0, "game_id": 0, "team_id": 0}
    scanned = changed = batches = corrected_game_ids = 0
    first_unresolved = None
    conflict_ids: set[int] = set()
    unverified_ids: set[int] = set()

    while current < high_water and batches < max_batches:
        with engine.begin() as conn:
            _set_linker_timeouts(conn)
            rows = _read_linker_records(conn, """
                SELECT staging_id, api_player_name, home_team, away_team, commence_time,
                       player_id, game_id, team_id
                FROM raw_player_props_combined
                WHERE staging_id > :cursor AND staging_id <= :high_water
                ORDER BY staging_id
                LIMIT :batch_size
            """, {"cursor": current, "high_water": high_water, "batch_size": batch_size})
            if not rows:
                current = high_water
                break
            result = _resolve_linker_page(
                conn, rows, context, apply_updates=not dry_run,
                repair_game_conflicts=repair_game_conflicts,
            )
            current = int(rows[-1]["staging_id"])
        batches += 1
        scanned += len(rows)
        changed += result["changed"]
        corrected_game_ids += result["corrected_game_ids"]
        for column in totals:
            totals[column] += result["proposed"][column]
        if result["first_unresolved_id"] is not None:
            first_unresolved = min(first_unresolved, result["first_unresolved_id"]) if first_unresolved else result["first_unresolved_id"]
        conflict_ids.update(result["conflicts"])
        unverified_ids.update(result["reference_unverified"])
        print(f"LINKER_RESUME cursor={current} high_water={high_water}", flush=True)

    complete = current >= high_water
    summary = {
        "mode": mode,
        "status": "complete" if complete else "incomplete",
        "cursor": current,
        "high_water": high_water,
        "batches": batches,
        "rows_scanned": scanned,
        "rows_updated": changed,
        "fill_candidates": totals,
        "corrected_game_ids": corrected_game_ids,
        "first_unresolved_id": first_unresolved,
        "identity_ready": first_unresolved is None,
        "conflict_count": len(conflict_ids),
        "reference_unverified_count": len(unverified_ids),
        "reference_available": context["schedule_status"].get("reference_available"),
        "dry_run": dry_run,
    }
    _summary_json(summary)
    return summary


def _load_resume_state(conn, *, for_update: bool = False) -> dict[str, Any]:
    suffix = " FOR UPDATE" if for_update else ""
    try:
        rows = _read_linker_records(conn, f"""
            SELECT state_name, replay_floor, cursor, high_water, first_unresolved_id, updated_at
            FROM {LINKER_STATE_TABLE}
            WHERE state_name = :state_name{suffix}
        """, {"state_name": LINKER_STATE_NAME})
    except Exception as exc:
        raise RuntimeError(
            "production linker state table is unavailable; apply the prepared migration and provision the production row explicitly"
        ) from exc
    if not rows:
        raise RuntimeError(
            "production linker state row is missing; provision state_name='production' explicitly (no automatic initialization)"
        )
    return rows[0]


def _run_resume_linker(
    engine, *, batch_size: int, max_batches: int, dry_run: bool,
    repair_game_conflicts: bool = False,
) -> dict[str, Any]:
    if dry_run:
        # Snapshot the checkpoint without advisory/row locks or state mutation.
        with engine.begin() as conn:
            _set_linker_timeouts(conn)
            state = _load_resume_state(conn)
            current = int(state["cursor"])
            high_water = state["high_water"]
            if high_water is None:
                source_high = conn.execute(text(
                    "SELECT MAX(staging_id) FROM raw_player_props_combined"
                )).scalar_one()
                high_water = max(current, int(source_high) if source_high is not None else current)
        fixture_start, fixture_end = _window_fixture_dates(engine, current, int(high_water), row_limit=batch_size * max_batches)
        context = _load_linker_context(engine, fixture_start, fixture_end) if current < int(high_water) else {
            "schedule_status": {"reference_available": None}
        }
        return _run_fixed_linker(
            engine, context, cursor=current, high_water=int(high_water),
            batch_size=batch_size, max_batches=max_batches, dry_run=True, mode="resume_preview",
            repair_game_conflicts=repair_game_conflicts,
        )

    totals = {"player_id": 0, "game_id": 0, "team_id": 0}
    scanned = changed = batches = corrected_game_ids = 0
    conflict_ids: set[int] = set()
    unverified_ids: set[int] = set()
    completed_cycle = False

    with engine.connect() as conn:
        _set_linker_timeouts(conn, local=False)
        acquired = bool(conn.execute(
            text("SELECT pg_try_advisory_lock(:lock_key)"), {"lock_key": LINKER_ADVISORY_LOCK_KEY}
        ).scalar_one())
        conn.commit()
        if not acquired:
            raise RuntimeError("another production props linker invocation holds the durable checkpoint lock")
        try:
            with conn.begin():
                state = _load_resume_state(conn, for_update=True)
                replay_floor = int(state["replay_floor"])
                current = int(state["cursor"])
                high_water = state["high_water"]
                first_unresolved = state["first_unresolved_id"]
                if high_water is None:
                    source_high = conn.execute(text(
                        "SELECT MAX(staging_id) FROM raw_player_props_combined"
                    )).scalar_one()
                    high_water = max(current, int(source_high) if source_high is not None else current)
                    if not dry_run:
                        conn.execute(text(f"""
                            UPDATE {LINKER_STATE_TABLE}
                            SET high_water = :high_water, first_unresolved_id = NULL, updated_at = NOW()
                            WHERE state_name = :state_name
                        """), {"high_water": high_water, "state_name": LINKER_STATE_NAME})
                    first_unresolved = None
                if int(high_water) <= current and not dry_run:
                    next_cursor = (
                        max(replay_floor, int(first_unresolved) - 1)
                        if first_unresolved is not None
                        else int(high_water)
                    )
                    conn.execute(text(f"""
                        UPDATE {LINKER_STATE_TABLE}
                        SET cursor = :next_cursor, high_water = NULL,
                            first_unresolved_id = NULL, updated_at = NOW()
                        WHERE state_name = :state_name
                    """), {"next_cursor": next_cursor, "state_name": LINKER_STATE_NAME})
                    completed_cycle = True

            if current < int(high_water):
                fixture_start, fixture_end = _window_fixture_dates(engine, current, int(high_water), row_limit=batch_size * max_batches)
                context = _load_linker_context(engine, fixture_start, fixture_end)
            else:
                context = {"schedule_status": {"reference_available": None}}
            while current < int(high_water) and batches < max_batches:
                with conn.begin():
                    state = _load_resume_state(conn, for_update=True)
                    if not dry_run and (int(state["cursor"]) != current or int(state["high_water"]) != int(high_water)):
                        raise RuntimeError("production linker state changed while lock held; checkpoint not advanced")
                    rows = _read_linker_records(conn, """
                        SELECT staging_id, api_player_name, home_team, away_team, commence_time,
                               player_id, game_id, team_id
                        FROM raw_player_props_combined
                        WHERE staging_id > :cursor AND staging_id <= :high_water
                        ORDER BY staging_id
                        LIMIT :batch_size
                    """, {"cursor": current, "high_water": high_water, "batch_size": batch_size})
                    if not rows:
                        current = int(high_water)
                        page_result = None
                    else:
                        page_result = _resolve_linker_page(
                            conn, rows, context, apply_updates=not dry_run,
                            repair_game_conflicts=repair_game_conflicts,
                        )
                        current = int(rows[-1]["staging_id"])
                        page_unresolved = page_result["first_unresolved_id"]
                        if page_unresolved is not None:
                            first_unresolved = min(int(first_unresolved), page_unresolved) if first_unresolved is not None else page_unresolved
                    if not dry_run:
                        if current >= int(high_water):
                            next_cursor = max(replay_floor, int(first_unresolved) - 1) if first_unresolved is not None else int(high_water)
                            conn.execute(text(f"""
                                UPDATE {LINKER_STATE_TABLE}
                                SET cursor = :next_cursor, high_water = NULL,
                                    first_unresolved_id = NULL, updated_at = NOW()
                                WHERE state_name = :state_name
                            """), {"next_cursor": next_cursor, "state_name": LINKER_STATE_NAME})
                            completed_cycle = True
                        else:
                            conn.execute(text(f"""
                                UPDATE {LINKER_STATE_TABLE}
                                SET cursor = :cursor, first_unresolved_id = :first_unresolved_id,
                                    updated_at = NOW()
                                WHERE state_name = :state_name
                            """), {
                                "cursor": current,
                                "first_unresolved_id": first_unresolved,
                                "state_name": LINKER_STATE_NAME,
                            })
                if rows:
                    batches += 1
                    scanned += len(rows)
                    changed += page_result["changed"]
                    corrected_game_ids += page_result["corrected_game_ids"]
                    for column in totals:
                        totals[column] += page_result["proposed"][column]
                    conflict_ids.update(page_result["conflicts"])
                    unverified_ids.update(page_result["reference_unverified"])
                print(f"LINKER_RESUME cursor={current} high_water={high_water}", flush=True)
                if current >= int(high_water):
                    completed_cycle = True
                    break

            complete = current >= int(high_water)
            summary = {
                "mode": "resume",
                "status": "complete" if complete else "incomplete",
                "cursor": current,
                "high_water": int(high_water),
                "batches": batches,
                "rows_scanned": scanned,
                "rows_updated": changed,
                "fill_candidates": totals,
                "corrected_game_ids": corrected_game_ids,
                "first_unresolved_id": first_unresolved,
                "identity_ready": first_unresolved is None,
                "scanned_window_complete": complete,
                "cycle_checkpoint_rotated": completed_cycle and not dry_run,
                "conflict_count": len(conflict_ids),
                "reference_unverified_count": len(unverified_ids),
                "reference_available": context["schedule_status"].get("reference_available"),
                "dry_run": dry_run,
            }
            _summary_json(summary)
            return summary
        finally:
            try:
                conn.execute(text("SELECT pg_advisory_unlock(:lock_key)"), {"lock_key": LINKER_ADVISORY_LOCK_KEY})
                conn.commit()
            except Exception:
                logging.exception("Failed to release production props linker advisory lock")



def link_incremental(
    batch_size: int = 50000, limit: int | None = None, days: int = 30, *,
    dry_run: bool = False, cursor: int | None = None, high_water: int | None = None,
    max_batches: int | None = None, resume: bool = False, local: bool = False,
    repair_game_conflicts: bool = False,
) -> dict[str, Any] | None:
    """
    Incremental linker: bounded recent-window smoke with --limit, legacy/unbounded otherwise.

    Bounded mode shares one PK window across player, game, and independent team fills.
    Reference tables and the existing CDN schedule route are reused; no fuzzy writes.
    No-limit behavior below is retained for legacy callers, not recommended for smoke runs.

    Args:
        batch_size: Number of records to process per batch
        limit: Recent PK-window cap (1..5000); None retains legacy/unbounded behavior
        days: Only process records with commence_time within this many days (default 30)
    """
    if batch_size <= 0 or (limit is not None and not 1 <= limit <= 5000):
        raise ValueError("batch_size must be positive; bounded --limit must be between 1 and 5000")
    fixed_requested = cursor is not None or high_water is not None or max_batches is not None
    if repair_game_conflicts and not (resume or fixed_requested):
        raise ValueError("--repair-game-conflicts requires bounded --resume or fixed-window mode")
    if local and (resume or not fixed_requested):
        raise ValueError("--local requires explicit fixed-window bounds; local resume/legacy modes are not supported")
    if (resume or fixed_requested) and batch_size > 5000:
        raise ValueError("production bounded --batch-size must be between 1 and 5000")
    if dry_run and limit is None and not (resume or fixed_requested):
        raise ValueError("--dry-run requires a bounded --limit, --resume, or fixed-window scope")
    if resume or fixed_requested:
        if limit is not None:
            raise ValueError("--limit cannot be mixed with --resume or fixed-window bounds")
        if max_batches is None or max_batches <= 0:
            raise ValueError("--max-batches must be positive for production bounded modes")
        if resume:
            if cursor is not None or high_water is not None:
                raise ValueError("--resume cannot be mixed with --cursor or --high-water")
        elif cursor is None or high_water is None:
            raise ValueError("fixed-window mode requires both --cursor and --high-water")
        elif cursor < 0 or high_water < 0 or cursor > high_water:
            raise ValueError("fixed-window bounds require 0 <= cursor <= high-water")
        print("Target: LOCAL_DATABASE_URL" if local else "Target: canonical DATABASE_URL (local processing, NOT automatic LOCAL_DATABASE_URL selection)")
        engine = get_engine(local=True) if local else get_engine()
        if resume:
            with engine.begin() as conn:
                _set_linker_timeouts(conn)
                _load_resume_state(conn)
        if resume:
            return _run_resume_linker(
                engine, batch_size=batch_size, max_batches=max_batches, dry_run=dry_run,
                repair_game_conflicts=repair_game_conflicts,
            )
        assert cursor is not None and high_water is not None
        fixed_cursor, fixed_high_water = cursor, high_water
        fixture_start, fixture_end = _window_fixture_dates(engine, fixed_cursor, fixed_high_water, row_limit=batch_size * max_batches)
        context = _load_linker_context(engine, fixture_start, fixture_end)
        return _run_fixed_linker(
            engine, context, cursor=fixed_cursor, high_water=fixed_high_water, batch_size=batch_size,
            max_batches=max_batches, dry_run=dry_run,
            repair_game_conflicts=repair_game_conflicts,
        )

    print("Target: canonical DATABASE_URL (local processing, NOT automatic LOCAL_DATABASE_URL selection)")
    engine = get_engine()
    if limit is not None:
        # Retain the reviewed newest-window diagnostic implementation unchanged.
        # One materialized source window serves BOTH former passes and independent team fills.
        # No timestamp/null filter may precede the inner PK LIMIT.
        def set_timeouts(conn):
            for setting in ("statement_timeout = '15s'", "lock_timeout = '2s'",
                            "idle_in_transaction_session_timeout = '15s'"):
                conn.execute(text(f"SET LOCAL {setting}"))

        def read_bounded(query, params=None):
            with engine.begin() as conn:
                set_timeouts(conn)
                return pd.read_sql(text(query), conn, params=params)

        bounds = read_bounded("""
            SELECT MIN(staging_id) AS lower_id, MAX(staging_id) AS upper_id FROM (
                SELECT staging_id FROM raw_player_props_combined
                ORDER BY staging_id DESC LIMIT :limit
            ) bounded
        """, {"limit": limit}).iloc[0]
        if pd.isna(bounds["lower_id"]):
            print("Recent-window smoke: empty source table; no-op (NOT all-history completion).")
            return
        lower, upper = int(bounds["lower_id"]), int(bounds["upper_id"])
        print(f"Recent-window smoke: staging_id [{lower}, {upper}], cap={limit}; NOT all-history completion.")
        window, last_id = [], lower - 1
        while len(window) < limit:
            page = read_bounded("""
                SELECT staging_id, api_player_name, home_team, away_team, commence_time,
                       player_id, game_id, team_id
                FROM raw_player_props_combined
                WHERE staging_id >= :lower AND staging_id <= :upper AND staging_id > :last_id
                ORDER BY staging_id LIMIT :batch_size
            """, {"lower": lower, "upper": upper, "last_id": last_id,
                  "batch_size": min(batch_size, limit - len(window))})
            if page.empty:
                break
            page_rows = cast(list[dict[str, Any]], page.to_dict("records"))
            window.extend(page_rows)
            last_id = int(page_rows[-1]["staging_id"])
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
        selected = [r for r in window if pd.notna(r["commence_time"]) and
                    pd.to_datetime(r["commence_time"], utc=True, errors="coerce") >= cutoff]
        print(f"Unique window rows: {len(window)}; eligible last {days}d: {len(selected)}")
        print("Pass candidates: " + ", ".join(
            f"missing {col}={sum(pd.isna(r[col]) for r in selected)}"
            for col in ("player_id", "game_id", "team_id")))
        if not selected:
            return

        teams = read_bounded("SELECT team_id, team_name FROM teams")
        players = read_bounded("SELECT player_id, player_name FROM players")
        games = read_bounded("""
            SELECT game_id, team_id, team_name, game_date as team_game_date, team_matchup, opponent_id
            FROM team_game_stats
        """)
        team_ids, names, manual = defaultdict(set), defaultdict(set), defaultdict(set)
        for r in teams.to_dict("records"):
            team_ids[normalize_team(r["team_name"])].add(int(r["team_id"]))
        known_players = set(players["player_id"])
        for r in players.to_dict("records"):
            norm = normalize_player(r["player_name"])
            if isinstance(norm, str) and norm and pd.notna(r["player_id"]):
                names[norm].add(int(r["player_id"]))
        mappings_file = DATA_DIR / "player_mappings.csv"
        if mappings_file.exists():
            for r in pd.read_csv(mappings_file).to_dict("records"):
                api_name, mapped_player_id = r.get("api_name"), r.get("player_id")
                if (isinstance(api_name, str) and isinstance(mapped_player_id, str | int | float)
                        and pd.notna(mapped_player_id)):
                    manual[api_name].add(int(mapped_player_id))

        def unique(values):
            return next(iter(values)) if len(values) == 1 else None

        by_match, by_game = defaultdict(set), defaultdict(set)

        def add_game(gid, home, away, game_date):
            if is_competitive_nba_game_id(gid) and isinstance(home, str) and isinstance(away, str) and home != away:
                key = (normalize_team(home), normalize_team(away), str(game_date)[:10])
                if not key[0] or not key[1] or key[0] == key[1]:
                    return
                by_match[key].add(gid)
                by_game[gid].add(key)

        for r in games.to_dict("records"):
            matchup = r["team_matchup"]
            if not isinstance(matchup, str):
                continue
            if " vs. " in matchup:
                home, away = matchup.split(" vs. ", 1)
            elif " @ " in matchup:
                away, home = matchup.split(" @ ", 1)
            else:
                continue
            add_game(r["game_id"], home, away, r["team_game_date"])
        fixture_dates = [
            pd.to_datetime(r["commence_time"], utc=True).tz_convert(EASTERN).date()
            for r in selected
        ]
        fixture_start = min(fixture_dates)
        fixture_end = max(fixture_dates) + timedelta(days=1)
        for r in _fetch_upcoming_games_from_schedule(
            engine, target_date=fixture_start, end_date=fixture_end, bounded=True
        ):
            add_game(r["game_id"], r["home_team"], r["away_team"], r["game_date"])

        proposed: dict[str, int] = dict.fromkeys(("player_id", "game_id", "team_id"), 0)
        conflicts, changed = set(), 0
        for start in range(0, len(selected), batch_size):
            batch = selected[start:start + batch_size]
            resolved = []
            team_eligible = set()
            for r in batch:
                old = {col: None if pd.isna(r[col]) else r[col] for col in proposed}
                candidates = manual.get(r["api_player_name"], names.get(normalize_player(r["api_player_name"]), set()))
                pid = old["player_id"] if old["player_id"] is not None else unique(candidates)
                stamp = pd.to_datetime(r["commence_time"], utc=True).tz_convert(EASTERN).strftime("%Y-%m-%d")
                key = (normalize_team(r["home_team"]), normalize_team(r["away_team"]), stamp)
                expected_game = unique(by_match.get(key, set()))
                gid = old["game_id"] if old["game_id"] is not None else expected_game
                player_verified = pid is not None and pid in known_players
                if old["player_id"] is not None and candidates and pid not in candidates:
                    player_verified = False
                game_verified = (isinstance(gid, str) and is_competitive_nba_game_id(gid) and gid == expected_game
                                 and len(by_game[gid]) == 1)
                if ((old["player_id"] is not None and candidates and not player_verified)
                        or (old["game_id"] is not None and expected_game is not None and not game_verified)):
                    conflicts.add(int(r["staging_id"]))
                if old["player_id"] is None and not player_verified:
                    pid = None
                if old["game_id"] is None and not game_verified:
                    gid = None
                if player_verified and game_verified:
                    team_eligible.add(int(r["staging_id"]))
                resolved.append((r, old, pid, gid, key))

            # Indexed PK pair lookups only; never download player_game_stats.
            pairs = sorted({(int(pid), gid) for r, _, pid, gid, _ in resolved
                            if int(r["staging_id"]) in team_eligible})
            relations = defaultdict(set)
            if pairs:
                params = {f"{col}{i}": value for i, pair in enumerate(pairs)
                          for col, value in zip(("p", "g"), pair)}
                values = ", ".join(f"(:p{i}, :g{i})" for i in range(len(pairs)))
                stats = read_bounded(f"""
                    SELECT player_id, game_id, team_id FROM player_game_stats
                    WHERE (player_id, game_id) IN ({values})
                """, params)
                for r in stats.to_dict("records"):
                    relations[(r["player_id"], r["game_id"])].add(r["team_id"])
            updates = []
            for r, old, pid, gid, key in resolved:
                tid = unique(relations.get((pid, gid), set())) if int(r["staging_id"]) in team_eligible else None
                home_id, away_id = unique(team_ids[key[0]]), unique(team_ids[key[1]])
                if home_id is None or away_id is None or home_id == away_id or tid not in (home_id, away_id):
                    tid = None
                if old["team_id"] is not None and tid is not None and old["team_id"] != tid:
                    conflicts.add(int(r["staging_id"]))
                if old["team_id"] is not None:
                    tid = old["team_id"]
                new = {"player_id": pid, "game_id": gid, "team_id": tid}
                fills = [col for col in proposed if old[col] is None and new[col] is not None]
                if not fills:
                    continue
                for col in fills:
                    proposed[col] += 1
                updates.append({"id": int(r["staging_id"]), **new,
                                **{f"old_{col}": value for col, value in old.items()},
                                **{col: r[col] for col in ("api_player_name", "home_team", "away_team", "commence_time")}})
            if updates and not dry_run:
                with engine.begin() as conn:
                    set_timeouts(conn)
                    result = conn.execute(text("""
                        UPDATE raw_player_props_combined
                        SET player_id = COALESCE(player_id, :player_id),
                            game_id = COALESCE(game_id, :game_id), team_id = COALESCE(team_id, :team_id)
                        WHERE staging_id = :id
                          AND player_id IS NOT DISTINCT FROM :old_player_id
                          AND game_id IS NOT DISTINCT FROM :old_game_id
                          AND team_id IS NOT DISTINCT FROM :old_team_id
                          AND api_player_name IS NOT DISTINCT FROM :api_player_name
                          AND home_team IS NOT DISTINCT FROM :home_team
                          AND away_team IS NOT DISTINCT FROM :away_team
                          AND commence_time IS NOT DISTINCT FROM :commence_time
                    """), updates)
                    changed += result.rowcount
        print(f"{'DRY RUN: would fill' if dry_run else 'Resolved fill candidates'}: {proposed}")
        print(f"Rows updated: {changed}; invalid/conflicting/unverified nonnull identities: {len(conflicts)}")
        if conflicts:
            print(f"Review staging IDs (first 20): {sorted(conflicts)[:20]}; existing nonnull IDs preserved")
        print("Recent-window smoke complete; unresolved rows remain null; NOT all-history completion.")
        return

    print("WARNING: no --limit: LEGACY/UNBOUNDED path, fuzzy matches and shared staging; NOT the safe smoke path.")

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

    # Load fuzzy cache (invalidate if player roster changed)
    fuzzy_cache, cached_player_count = _load_fuzzy_cache()
    current_player_count = len(player_lookup)
    if cached_player_count != current_player_count:
        if fuzzy_cache:
            print(f"  Fuzzy cache invalidated: player count changed ({cached_player_count} -> {current_player_count})")
        fuzzy_cache = {}
    else:
        print(f"  Fuzzy cache loaded: {len(fuzzy_cache)} cached name mappings")

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

    # Source 2: Upcoming games from the durable CDN schedule (today + next 2 days)
    # This is critical for linking props for games that haven't been played yet
    upcoming_games = _fetch_upcoming_games_from_schedule(engine)
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

    print(f"  Upcoming games from NBA schedule: {upcoming_count}")
    print(f"  Built game lookup with {len(props_game_lookup)} unique matchups")

    # 2. Count unlinked records (scoped to recent window)
    print(f"\n[2/4] Counting unlinked records (last {days} days)...")

    with engine.connect() as conn:
        count_result = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined
            WHERE player_id IS NULL
              AND commence_time >= NOW() - CAST(:days || ' days' AS INTERVAL)
        """), {"days": days})
        total_unlinked = int(count_result.scalar_one())

    print(f"  Unlinked (last {days}d): {total_unlinked:,}")

    if total_unlinked == 0:
        print("\n[OK] No unlinked records in window - nothing to do!")
        return

    if limit:
        total_to_process = min(total_unlinked, limit)
        print(f"  Processing limit: {limit:,}")
    else:
        total_to_process = total_unlinked

    # 3. Process in batches using keyset pagination (avoids O(n²) OFFSET scans)
    print(f"\n[3/4] Processing {total_to_process:,} records in batches of {batch_size:,}...")

    total_game_matched = 0
    total_player_matched = 0
    total_processed = 0
    last_staging_id = 0  # keyset cursor

    while True:
        if limit and total_processed >= total_to_process:
            break

        current_batch_size = batch_size
        if limit:
            current_batch_size = min(batch_size, total_to_process - total_processed)

        # Fetch batch using keyset pagination — no OFFSET, O(1) per batch
        with engine.connect() as conn:
            batch_df = pd.read_sql(text("""
                SELECT staging_id, api_player_name, home_team, away_team, commence_time
                FROM raw_player_props_combined
                WHERE player_id IS NULL
                  AND commence_time >= NOW() - CAST(:days || ' days' AS INTERVAL)
                  AND staging_id > :last_id
                ORDER BY staging_id
                LIMIT :batch_size
            """), conn, params={"days": days, "last_id": last_staging_id, "batch_size": current_batch_size})

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

        # Match players — batch approach with fuzzy cache
        # Step A: Manual mappings (api_name -> player_id)
        batch_df["matched_player_id"] = batch_df["api_player_name"].map(manual_mappings)

        # Step B: Exact match on normalized name (vectorized)
        unmatched_mask = batch_df["matched_player_id"].isna()
        if unmatched_mask.any():
            exact = batch_df.loc[unmatched_mask, "player_name_norm"].map(player_lookup)
            has_exact = exact.notna()
            if has_exact.any():
                batch_df.loc[has_exact[has_exact].index, "matched_player_id"] = exact[has_exact]

        # Step C: Fuzzy match — check cache first, only resolve truly new names
        unmatched_mask = batch_df["matched_player_id"].isna() & batch_df["player_name_norm"].notna()
        if unmatched_mask.any():
            unmatched_names = set(batch_df.loc[unmatched_mask, "player_name_norm"].unique())
            new_names = unmatched_names - set(fuzzy_cache.keys())

            if new_names:
                print(f"    Fuzzy matching {len(new_names)} new names (cache has {len(fuzzy_cache)})...")
                new_fuzzy = _resolve_fuzzy_names(new_names, player_lookup)
                fuzzy_cache.update(new_fuzzy)

            cache_hits = len(unmatched_names) - len(new_names)
            print(f"    Cache: {cache_hits} hits, {len(new_names)} new fuzzy lookups")

            # Apply fuzzy results from cache
            fuzzy_mapped = batch_df.loc[unmatched_mask, "player_name_norm"].map(fuzzy_cache)
            has_fuzzy = fuzzy_mapped.notna()
            if has_fuzzy.any():
                batch_df.loc[has_fuzzy[has_fuzzy].index, "matched_player_id"] = fuzzy_mapped[has_fuzzy]

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

        # Advance keyset cursor
        last_staging_id = int(batch_df["staging_id"].max())
        total_processed += len(batch_df)
        pct = min((total_processed / total_unlinked) * 100, 100.0)
        print(f"  Processed {total_processed:,}/{total_unlinked:,} ({pct:.1f}%) - Games: {total_game_matched:,}, Players: {total_player_matched:,}")

    # Save fuzzy cache to disk
    _save_fuzzy_cache(fuzzy_cache, current_player_count)
    print(f"  Fuzzy cache saved: {len(fuzzy_cache)} entries")

    # 4. Summary
    print("\n[4/4] Summary")
    print("=" * 60)
    print(f"  Total records processed: {total_processed:,}")
    print(f"  Games matched: {total_game_matched:,}")
    print(f"  Players matched: {total_player_matched:,}")

    # Check remaining unlinked (player_id) — scoped to same window
    with engine.connect() as conn:
        remaining = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined
            WHERE player_id IS NULL
              AND commence_time >= NOW() - CAST(:days || ' days' AS INTERVAL)
        """), {"days": days}).scalar()

    print(f"  Remaining unlinked in window (no player_id): {remaining:,}")

    # 5. Link game_id for records that have player_id but missing game_id
    #    (props scraper sets player_id from odds API, but game_id needs NBA game ID mapping)
    with engine.connect() as conn:
        missing_game_id = conn.execute(text("""
            SELECT COUNT(*) FROM raw_player_props_combined
            WHERE player_id IS NOT NULL AND game_id IS NULL
              AND commence_time >= NOW() - CAST(:days || ' days' AS INTERVAL)
        """), {"days": days}).scalar()

    if missing_game_id > 0:
        print(f"\n[5/5] Linking game_id for {missing_game_id:,} records (have player_id, missing game_id)...")
        game_id_matched = 0

        with engine.connect() as conn:
            batch_df = pd.read_sql(text("""
                SELECT DISTINCT staging_id, home_team, away_team, commence_time
                FROM raw_player_props_combined
                WHERE player_id IS NOT NULL AND game_id IS NULL
                  AND commence_time >= NOW() - CAST(:days || ' days' AS INTERVAL)
                ORDER BY staging_id
            """), conn, params={"days": days})

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
    parser = argparse.ArgumentParser(description="NBA Data Linker - canonical DATABASE_URL by default; --local selects LOCAL_DATABASE_URL for bounded fixed-window repair")
    parser.add_argument(
        "command",
        choices=["download", "process", "upload", "all", "init", "incremental"],
        help="Command to run. Safe incremental smoke requires --limit; omission is LEGACY/UNBOUNDED."
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
        help="Recent PK-window smoke cap, 1..5000, shared across all passes; omission is LEGACY/UNBOUNDED"
    )
    parser.add_argument("--cursor", type=int, default=None, help="Exclusive staging_id for fixed-window mode")
    parser.add_argument("--high-water", type=int, default=None, help="Inclusive frozen staging_id for fixed-window mode")
    parser.add_argument("--max-batches", type=int, default=None, help="Positive bounded batch count for fixed-window/resume mode")
    parser.add_argument("--resume", action="store_true", help="Use the pre-provisioned durable production checkpoint")
    parser.add_argument("--local", action="store_true", help="Target LOCAL_DATABASE_URL; explicit fixed-window incremental mode only")
    parser.add_argument(
        "--repair-game-conflicts", action="store_true",
        help="Strictly repair qualified nonnull game_id conflicts in bounded fixed-window/resume mode",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Only process records with commence_time within this many days (default: 30)"
    )

    parser.add_argument("--dry-run", action="store_true", help="Resolve only, no writes; requires a bounded incremental scope")

    args = parser.parse_args()
    if args.local and (args.command != "incremental" or args.resume or args.cursor is None or args.high_water is None):
        parser.error("--local requires incremental --cursor and --high-water; production resume is not supported locally")
    production_flags = args.resume or args.cursor is not None or args.high_water is not None or args.max_batches is not None
    if args.command != "incremental" and (
        args.dry_run or args.limit is not None or production_flags or args.repair_game_conflicts
    ):
        parser.error("incremental scope flags are only valid with the incremental command")
    if args.command == "incremental":
        if args.batch_size <= 0:
            parser.error("--batch-size must be positive")
        if production_flags and args.batch_size > 5000:
            parser.error("production bounded --batch-size must be between 1 and 5000")
        if args.limit is not None and not 1 <= args.limit <= 5000:
            parser.error("--limit must be between 1 and 5000")
        if args.limit is not None and production_flags:
            parser.error("--limit cannot be mixed with --resume or fixed-window bounds")
        if args.resume and (args.cursor is not None or args.high_water is not None):
            parser.error("--resume cannot be mixed with --cursor or --high-water")
        if production_flags and (args.max_batches is None or not 1 <= args.max_batches <= 100):
            parser.error("production bounded modes require --max-batches between 1 and 100")
        if not args.resume and (args.cursor is None) != (args.high_water is None):
            parser.error("fixed-window mode requires both --cursor and --high-water")
        if args.cursor is not None and (args.cursor < 0 or args.high_water < args.cursor):
            parser.error("fixed-window bounds require 0 <= cursor <= high-water")
        if args.dry_run and args.limit is None and not production_flags:
            parser.error("--dry-run requires --limit, --resume, or fixed-window bounds")
        if args.repair_game_conflicts and not production_flags:
            parser.error("--repair-game-conflicts requires bounded --resume or fixed-window mode")

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
        try:
            summary = link_incremental(
                batch_size=args.batch_size,
                limit=args.limit,
                days=args.days,
                dry_run=args.dry_run,
                cursor=args.cursor,
                high_water=args.high_water,
                max_batches=args.max_batches,
                resume=args.resume,
                local=args.local,
                repair_game_conflicts=args.repair_game_conflicts,
            )
        except Exception as exc:
            logging.error("Incremental linker failed: %s", exc, exc_info=True)
            return 1
        if summary and summary.get("status") == "incomplete":
            if (args.resume and not args.dry_run and summary.get("rows_scanned", 0) > 0
                    and summary.get("identity_ready") and summary.get("conflict_count") == 0
                    and summary.get("reference_unverified_count") == 0):
                logging.warning("Bounded resume batch committed successfully; historical backlog remains at cursor=%s high_water=%s",
                                summary["cursor"], summary["high_water"])
                return 0
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
