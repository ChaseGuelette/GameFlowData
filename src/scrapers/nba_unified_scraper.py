#!/usr/bin/env python3
"""
NBA Unified Stats Scraper
=========================
Combines team game stats, player game stats, and advanced stats into a single script.
Designed to be run periodically to fetch new games.

Tables updated:
- team_game_stats: Basic + Advanced team stats per game
- player_game_advanced_stats: Player advanced metrics per game

Usage:
    python nba_unified_scraper.py --cdn-only
    python nba_unified_scraper.py --season YYYY-YY [--season-type TYPE] [--skip-team] [--skip-advanced]

Examples:
    python nba_unified_scraper.py --cdn-only                  # Railway-safe CDN-only ingestion
    python nba_unified_scraper.py --season current --advanced-limit 25  # Unattended local catch-up
    python nba_unified_scraper.py --season 2025-26            # Scrape explicit historical season locally
    python nba_unified_scraper.py --season 2023-24            # Scrape specific regular season
    python nba_unified_scraper.py --season 2025-26 --season-type Playoffs  # Historical playoffs
    python nba_unified_scraper.py --season 2025-26 --season-type PlayIn    # Historical play-in
    python nba_unified_scraper.py --season 2023-24 --season-type Playoffs  # Scrape 2023-24 playoffs
    python nba_unified_scraper.py --season 2025-26 --skip-advanced  # Only fetch basic team stats
"""

import argparse
import os
import random
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import cast

# Path setup and env loading must precede src.* imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import pandas as pd  # noqa: E402
from nba_api.stats.endpoints import (  # noqa: E402
    boxscoreadvancedv3,
    boxscoretraditionalv3,
    leaguegamefinder,
)
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout  # noqa: E402
from sqlalchemy import inspect, text  # noqa: E402

from src.db.client import get_engine  # noqa: E402
from src.scrapers.nba_cdn_scraper import scrape_games_cdn  # noqa: E402

# =============================================================================
# Configuration
# =============================================================================

# Rate limiting settings
SHORT_DELAY_MIN = 3.0
SHORT_DELAY_MAX = 6.0
LONG_PAUSE_EVERY = 20
LONG_PAUSE_MIN = 30
LONG_PAUSE_MAX = 60
BAN_COOLDOWN = 120  # 2 minutes if rate limited (was 10 — too aggressive)
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120  # seconds per request (nba_api default is 30)
_runtime_options: dict[str, int] = {}

# Optional proxy; CLI opts in explicitly so a stale SOCKS route cannot break direct local scraping.
PROXY_URL = os.getenv("PROXY_URL")


def configure_local_stats_http(client: str, max_requests: int | None = None):
    """Configure stats transport; an optional budget counts actual HTTP GET attempts."""
    from nba_api.stats.library.http import NBAStatsHTTP

    if client == "curl":
        try:
            from curl_cffi import CurlOpt
            from curl_cffi.requests import Session as CurlSession
            from curl_cffi.requests.exceptions import RequestException as CurlRequestException
            from curl_cffi.requests.exceptions import Timeout as CurlTimeout
        except ImportError as exc:
            raise RuntimeError("Local stats require curl_cffi==0.14.0; install it or use --http-client requests") from exc
        class StatsSession(CurlSession):
            def get(self, *args, **kwargs):
                try:
                    return super().get(*args, **kwargs)
                except CurlTimeout as exc:
                    raise ReadTimeout(safe_error(exc)) from None
                except CurlRequestException as exc:
                    raise ConnectionError(safe_error(exc)) from None

        # curl_cffi 0.14 stores trust_env but does not apply it to libcurl;
        # an empty CURLOPT_PROXY explicitly disables ambient proxy variables.
        session = StatsSession(
            impersonate="chrome", trust_env=False,
            curl_options={} if PROXY_URL else {CurlOpt.PROXY: ""},
        )
    else:
        from requests import Session

        session = Session()
        session.trust_env = False
    original_get = session.get
    session.stats_request_count = 0

    def budgeted_get(*args, **kwargs):
        if max_requests is not None and session.stats_request_count >= max_requests:
            raise RuntimeError(f"stats.nba.com request budget exhausted ({max_requests})")
        session.stats_request_count += 1
        # A redirect must not silently spend another request outside the budget.
        if max_requests is not None:
            kwargs["allow_redirects"] = False
        return original_get(*args, **kwargs)

    session.get = budgeted_get
    NBAStatsHTTP.set_session(session)
    return session


def current_nba_season_label(as_of: date | None = None) -> str:
    """Return the competitive season containing ``as_of``.

    Competitive NBA game IDs use a September season boundary; this mirrors
    ``src.models.features.nba.season.resolve_nba_season`` without requiring a
    game ID.  ``--season current`` is intended for unattended local advanced
    scraping only; explicit historical labels remain unchanged.
    """
    as_of = as_of or date.today()
    start_year = as_of.year if as_of.month >= 9 else as_of.year - 1
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def season_label_to_id(season: str) -> str:
    """Convert a validated YYYY-YY season label to the database season_id."""
    if not re.fullmatch(r"\d{4}-\d{2}", season):
        raise ValueError(f"Invalid NBA season label: {season!r}")
    start_year = int(season[:4])
    if int(season[-2:]) != (start_year + 1) % 100:
        raise ValueError(f"Invalid NBA season span: {season!r}")
    return f"2{start_year}"


def safe_error(exc: BaseException) -> str:
    """Return an error message without proxy/credential-bearing URL details."""
    message = str(exc)
    message = re.sub(r"(?i)\b(?:https?|socks5?)://[^\s]+", "[REDACTED_URL]", message)
    if PROXY_URL:
        message = message.replace(PROXY_URL, "[REDACTED_PROXY]")
    return message


# =============================================================================
# Helper Functions
# =============================================================================


def normalize_for_postgres(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names for PostgreSQL (lowercase + underscores)."""
    df = df.copy()
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    return df


def camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def parse_minutes(minutes_str) -> float:
    """Parse minutes from various formats to float."""
    if pd.isna(minutes_str) or minutes_str == "" or minutes_str is None:
        return 0.0

    if isinstance(minutes_str, int | float):
        return float(minutes_str)

    minutes_str = str(minutes_str)

    # MM:SS format
    if ":" in minutes_str and "PT" not in minutes_str:
        try:
            parts = minutes_str.split(":")
            mins = int(parts[0])
            secs = int(parts[1]) if len(parts) > 1 else 0
            return round(mins + secs / 60, 3)
        except (ValueError, IndexError):
            return 0.0

    # ISO 8601 format (PT28M17.00S)
    if "PT" in minutes_str:
        try:
            match = re.match(r"PT(\d+)M([\d.]+)S", minutes_str)
            if match:
                mins = int(match.group(1))
                secs = float(match.group(2))
                return round(mins + secs / 60, 3)
        except (ValueError, AttributeError):
            return 0.0

    return 0.0


def determine_dnp(row: pd.Series) -> bool:
    """Determine if a player did not play."""
    comment = row.get("comment", "")
    minutes = row.get("minutes", 0)
    if minutes == 0:
        minutes = row.get("min", 0)

    if pd.notna(comment) and str(comment).strip() != "":
        return True

    if pd.isna(minutes) or minutes == 0:
        return True

    return False


def rate_limit_delay(game_num: int):
    """Apply rate limiting between API calls."""
    if game_num > 0 and game_num % LONG_PAUSE_EVERY == 0:
        pause = round(random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX), 1)  # nosec
        print(f"\n  ⏸️  Long pause: {pause}s (every {LONG_PAUSE_EVERY} games)\n")
        time.sleep(pause)
    else:
        delay = round(random.uniform(SHORT_DELAY_MIN, SHORT_DELAY_MAX), 2)  # nosec
        time.sleep(delay)


def extract_opponent_id(matchup: str | None, team_id: int, team_abbrevs: dict) -> int | None:
    """Extract opponent team ID from matchup string like 'LAL vs. GSW' or 'LAL @ GSW'."""
    if not matchup or pd.isna(matchup):
        return None

    # Parse matchup: "TEAM vs. OPP" or "TEAM @ OPP"
    if " vs. " in matchup:
        parts = matchup.split(" vs. ")
    elif " @ " in matchup:
        parts = matchup.split(" @ ")
    else:
        return None

    if len(parts) != 2:
        return None

    # Get the opponent abbreviation (the one that's not our team)
    opp_abbrev = parts[1].strip()

    return team_abbrevs.get(opp_abbrev)


# =============================================================================
# Team Game Stats Scraper
# =============================================================================


def get_existing_game_ids(engine, table_name: str = "team_game_stats") -> set[str]:
    """Get set of game IDs already in the database."""
    inspector = inspect(engine)

    if table_name not in inspector.get_table_names():
        return set()

    # Validate table_name contains only safe characters
    if not re.match(r"^[a-zA-Z0-9_]+$", table_name):
        raise ValueError(f"Invalid table name: {table_name}")

    existing_games = pd.read_sql(f"SELECT DISTINCT game_id FROM {table_name}", engine)  # nosec
    return set(existing_games["game_id"].unique())


def get_team_abbreviation_map(engine) -> dict:
    """Get mapping of team abbreviation to team_id."""
    # NBA team abbreviation mapping
    abbrev_map = {
        "ATL": 1610612737,
        "BOS": 1610612738,
        "BKN": 1610612751,
        "CHA": 1610612766,
        "CHI": 1610612741,
        "CLE": 1610612739,
        "DAL": 1610612742,
        "DEN": 1610612743,
        "DET": 1610612765,
        "GSW": 1610612744,
        "HOU": 1610612745,
        "IND": 1610612754,
        "LAC": 1610612746,
        "LAL": 1610612747,
        "MEM": 1610612763,
        "MIA": 1610612748,
        "MIL": 1610612749,
        "MIN": 1610612750,
        "NOP": 1610612740,
        "NYK": 1610612752,
        "OKC": 1610612760,
        "ORL": 1610612753,
        "PHI": 1610612755,
        "PHX": 1610612756,
        "POR": 1610612757,
        "SAC": 1610612758,
        "SAS": 1610612759,
        "TOR": 1610612761,
        "UTA": 1610612762,
        "WAS": 1610612764,
    }

    return abbrev_map


def scrape_team_game_stats(engine, seasons: list[str], season_type: str = "Regular Season") -> int:
    """
    Scrape basic team game stats from NBA API.

    Args:
        engine: Database engine
        seasons: List of seasons to scrape (e.g., ['2024-25'])
        season_type: 'Regular Season', 'Playoffs', or 'PlayIn' (default: 'Regular Season')

    Returns:
        Number of new games added.
    """
    print("\n" + "=" * 60)
    print(f"STEP 1: Fetching Team Game Stats ({season_type})")
    print("=" * 60)

    existing_game_ids = get_existing_game_ids(engine)
    print(f"Found {len(existing_game_ids)} games already in database.")

    team_abbrev_map = get_team_abbreviation_map(engine)

    all_seasons_data = []

    for season in seasons:
        try:
            print(f"\nFetching {season_type} games for {season}...")
            game_finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season, league_id_nullable="00", season_type_nullable=season_type,
                proxy=PROXY_URL or "", timeout=_runtime_options.get("request_timeout", REQUEST_TIMEOUT),
            )

            games_df = game_finder.get_data_frames()[0]
            games_df["SEASON"] = season

            # Filter out games we already have
            new_games_df = games_df[~games_df["GAME_ID"].isin(existing_game_ids)]

            if not new_games_df.empty:
                all_seasons_data.append(new_games_df)
                print(f"✓ Found {len(new_games_df)} new game records for {season}")
            else:
                print(f"- No new games found for {season}")

            time.sleep(round(random.uniform(1, 3), 1))  # nosec

        except (ReadTimeout, ConnectionError, ConnectionResetError) as e:
            print(f"✗ Connection error for {season}: {safe_error(e)}")
            raise  # Let caller handle CDN fallback

        except Exception as e:
            print(f"✗ Error for {season}: {safe_error(e)}")
            raise

    if not all_seasons_data:
        print("\n🙌 No new team data to add!")
        return 0

    # Combine all seasons
    print("\nCombining collected seasons into single dataframe...")
    combined_df = pd.concat(all_seasons_data, ignore_index=True)

    # Normalize column names
    combined_df = normalize_for_postgres(combined_df)

    # Map to database schema
    mapping = {
        "game_date": "game_date",
        "matchup": "team_matchup",
        "wl": "team_wl",
        "min": "team_min",
        "pts": "team_pts",
        "fgm": "team_fgm",
        "fga": "team_fga",
        "fg_pct": "team_fg_pct",
        "fg3m": "team_fg3m",
        "fg3a": "team_fg3a",
        "fg3_pct": "team_fg3_pct",
        "ftm": "team_ftm",
        "fta": "team_fta",
        "ft_pct": "team_ft_pct",
        "oreb": "team_oreb",
        "dreb": "team_dreb",
        "reb": "team_reb",
        "ast": "team_ast",
        "stl": "team_stl",
        "blk": "team_blk",
        "tov": "team_tov",
        "pf": "team_pf",
        "plus_minus": "team_plus_minus",
    }
    combined_df = combined_df.rename(columns=mapping)

    # Add opponent_id by parsing matchup
    combined_df["opponent_id"] = combined_df.apply(
        lambda row: extract_opponent_id(row.get("team_matchup"), row.get("team_id"), team_abbrev_map),
        axis=1,
    )

    # Filter for columns the DB actually has (basic stats only - advanced will be added later)
    db_cols = [
        "season_id",
        "team_id",
        "team_abbreviation",
        "team_name",
        "game_id",
        "game_date",
        "team_matchup",
        "team_wl",
        "team_min",
        "team_pts",
        "team_fgm",
        "team_fga",
        "team_fg_pct",
        "team_fg3m",
        "team_fg3a",
        "team_fg3_pct",
        "team_ftm",
        "team_fta",
        "team_ft_pct",
        "team_oreb",
        "team_dreb",
        "team_reb",
        "team_ast",
        "team_stl",
        "team_blk",
        "team_tov",
        "team_pf",
        "team_plus_minus",
        "season",
        "opponent_id",
    ]

    final_df = combined_df[[c for c in db_cols if c in combined_df.columns]]

    # Push to SQL
    print(f"\nPushing {len(final_df)} records to 'team_game_stats'...")

    try:
        final_df.to_sql("team_game_stats", engine, if_exists="append", index=False)
        print("✅ Successfully updated team_game_stats!")
        return len(final_df)
    except Exception as e:
        print(f"❌ Error during SQL push: {safe_error(e)}")
        raise


# =============================================================================
# Advanced Stats Scraper
# =============================================================================


def transform_player_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform player DataFrame from API format to database format."""
    if df.empty:
        return df

    # Rename columns: camelCase -> snake_case
    df = df.rename(columns={col: camel_to_snake(col) for col in df.columns})

    # Rename person_id to player_id to match schema
    if "person_id" in df.columns:
        df = df.rename(columns={"person_id": "player_id"})

    # Parse minutes
    if "minutes" in df.columns:
        df["minutes"] = df["minutes"].apply(parse_minutes)

    # Add DNP flag
    df["did_not_play"] = df.apply(determine_dnp, axis=1)

    # Add timestamp
    df["created_at"] = datetime.now()

    # Remove duplicates
    df = df.drop_duplicates(subset=["game_id", "player_id"], keep="first")

    return df


def transform_team_adv_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform team advanced stats DataFrame for database update."""
    if df.empty:
        return df

    # Rename columns: camelCase -> snake_case
    df = df.rename(columns={col: camel_to_snake(col) for col in df.columns})

    # Parse minutes
    if "minutes" in df.columns:
        df["minutes"] = df["minutes"].apply(parse_minutes)

    return df


def get_games_missing_advanced_stats(
    engine, season: str | None = None, limit: int | None = None, *, game_id: str | None = None
) -> list[str]:
    """Get game IDs that have basic stats but are missing advanced stats."""
    if limit is not None and limit <= 0:
        raise ValueError("advanced limit must be positive")

    season_clause = ""
    params: dict[str, object] = {}
    if season is not None:
        season_clause = "AND tgs.season_id = :season_id"
        params["season_id"] = season_label_to_id(season)
    limit_clause = ""
    if limit is not None:
        limit_clause = "LIMIT :limit"
        params["limit"] = limit

    game_clause = ""
    if game_id is not None:
        game_clause = "AND tgs.game_id = :game_id"
        params["game_id"] = game_id
    sql = f"""
    SELECT DISTINCT tgs.game_id
    FROM team_game_stats tgs
    WHERE (
        tgs.offensive_rating IS NULL OR tgs.possessions IS NULL OR tgs.pace IS NULL
        OR NOT EXISTS (
            SELECT 1 FROM player_game_stats pgs
            WHERE pgs.game_id = tgs.game_id AND pgs.min > 0
        )
        OR EXISTS (
            SELECT 1 FROM player_game_stats pgs
            LEFT JOIN player_game_advanced_stats adv
              ON adv.game_id = pgs.game_id AND adv.player_id = pgs.player_id
            WHERE pgs.game_id = tgs.game_id AND pgs.min > 0
              AND (adv.player_id IS NULL OR adv.possessions IS NULL)
        )
    )
      AND (tgs.game_id LIKE '002%' OR tgs.game_id LIKE '004%' OR tgs.game_id LIKE '005%')
      {season_clause}
      {game_clause}
    ORDER BY tgs.game_id ASC
    {limit_clause}
    """  # nosec B608 -- fixed clauses; values remain bound parameters

    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        return [row[0] for row in result]


def scrape_single_game_advanced(game_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Scrape advanced stats for a single game.
    Returns: (player_df, team_df)
    """
    box_adv = boxscoreadvancedv3.BoxScoreAdvancedV3(game_id=game_id, proxy=PROXY_URL or "", timeout=_runtime_options.get("request_timeout", REQUEST_TIMEOUT))
    dfs = box_adv.get_data_frames()

    # dfs[0] = players, dfs[1] = teams
    player_df = transform_player_df(dfs[0].copy())
    team_df = transform_team_adv_df(dfs[1].copy())

    return player_df, team_df


def update_team_advanced_stats(engine, team_df: pd.DataFrame, game_id: str):
    """Update team_game_stats with advanced stats for a specific game."""
    if team_df.empty:
        return

    # Advanced stat columns to update in team_game_stats
    adv_columns = [
        "minutes",
        "offensive_rating",
        "defensive_rating",
        "net_rating",
        "estimated_offensive_rating",
        "estimated_defensive_rating",
        "estimated_net_rating",
        "effective_field_goal_percentage",
        "true_shooting_percentage",
        "pie",
        "possessions",
        "pace",
        "estimated_pace",
        "pace_per40",
        "assist_ratio",
        "turnover_ratio",
        "assist_to_turnover",
        "estimated_team_turnover_percentage",
        "assist_percentage",
        "rebound_percentage",
        "offensive_rebound_percentage",
        "defensive_rebound_percentage",
    ]

    for _, row in team_df.iterrows():
        team_id = row.get("team_id")

        # Build SET clause for UPDATE
        set_parts = []
        params = {"game_id": game_id, "team_id": team_id}

        for col in adv_columns:
            if col in row.index and pd.notna(row[col]):
                set_parts.append(f"{col} = :{col}")
                params[col] = row[col]

        if not set_parts:
            continue

        sql = f"""
        UPDATE team_game_stats
        SET {", ".join(set_parts)}
        WHERE game_id = :game_id AND team_id = :team_id
        """  # nosec

        with engine.begin() as conn:
            conn.execute(text(sql), params)


def get_player_game_advanced_stats_columns() -> list[str]:
    """Return the columns for player_game_advanced_stats table."""
    return [
        "game_id",
        "player_id",
        "team_id",
        "first_name",
        "family_name",
        "name_i",
        "player_slug",
        "position",
        "comment",
        "jersey_num",
        "team_city",
        "team_name",
        "team_tricode",
        "team_slug",
        "minutes",
        "estimated_offensive_rating",
        "offensive_rating",
        "estimated_defensive_rating",
        "defensive_rating",
        "estimated_net_rating",
        "net_rating",
        "assist_percentage",
        "assist_to_turnover",
        "assist_ratio",
        "offensive_rebound_percentage",
        "defensive_rebound_percentage",
        "rebound_percentage",
        "estimated_team_turnover_percentage",
        "turnover_ratio",
        "effective_field_goal_percentage",
        "true_shooting_percentage",
        "usage_percentage",
        "estimated_usage_percentage",
        "estimated_pace",
        "pace",
        "possessions",
        "pie",
        "did_not_play",
        "created_at",
        "pace_per40",
    ]


def ensure_players_exist(engine, player_df: pd.DataFrame):
    """
    Check if all players in the DataFrame exist in the players table.
    Insert any missing players.
    """
    if player_df.empty:
        return

    # Get unique players from this game
    players_in_game = player_df[["player_id", "first_name", "family_name"]].drop_duplicates()
    player_ids = players_in_game["player_id"].tolist()

    # Check which players already exist
    placeholders = ",".join([":id" + str(i) for i in range(len(player_ids))])
    params = {f"id{i}": pid for i, pid in enumerate(player_ids)}

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT player_id FROM players WHERE player_id IN ({placeholders})"), params)  # nosec
        existing_ids = {row[0] for row in result}

    # Find missing players
    missing_players = players_in_game[~players_in_game["player_id"].isin(existing_ids)]

    if missing_players.empty:
        return

    # Insert missing players
    print(f"    📝 Adding {len(missing_players)} new player(s) to players table...")

    with engine.begin() as conn:
        for _, player in missing_players.iterrows():
            player_name = f"{player['first_name']} {player['family_name']}"
            conn.execute(
                text(
                    "INSERT INTO players (player_id, player_name) VALUES (:id, :name) ON CONFLICT (player_id) DO NOTHING"
                ),
                {"id": player["player_id"], "name": player_name},
            )

    print("    ✓ Players added")


def scrape_advanced_stats(
    engine, limit: int | None = None, *, season: str | None = None
) -> tuple[int, int]:
    """
    Scrape advanced stats for all games missing them.
    Returns: (games_processed, games_failed)
    """
    print("\n" + "=" * 60)
    print("STEP 2: Fetching Advanced Stats")
    print("=" * 60)

    games_to_scrape = get_games_missing_advanced_stats(engine, season=season, limit=limit)

    if not games_to_scrape:
        print("\n✅ All games already have advanced stats!")
        return 0, 0


    total = len(games_to_scrape)
    print(f"\nFound {total} games missing advanced stats")

    player_columns = get_player_game_advanced_stats_columns()
    games_processed = 0
    games_failed = 0

    for i, game_id in enumerate(games_to_scrape, 1):
        success = False
        attempt = 0

        while attempt < _runtime_options.get("max_retries", MAX_RETRIES) and not success:
            attempt += 1
            try:
                print(f"[{i}/{total}] Scraping {game_id} (Try {attempt})... ", end="", flush=True)

                # Scrape
                player_df, team_df = scrape_single_game_advanced(game_id)

                # Check if data is empty
                if player_df.empty and team_df.empty:
                    print("⚠️  Empty data returned (Skipping)")
                    break

                # Update team advanced stats
                if not team_df.empty:
                    update_team_advanced_stats(engine, team_df, game_id)

                # Repair individual missing/partial player rows idempotently.
                if not player_df.empty:
                    # Ensure all players exist in the players table first
                    ensure_players_exist(engine, player_df)

                    # Filter to only columns that exist in schema
                    available_cols = [c for c in player_columns if c in player_df.columns]
                    player_df_filtered = cast(pd.DataFrame, player_df[available_cols])
                    if not {"player_id", "game_id", "possessions"}.issubset(available_cols):
                        raise ValueError("Advanced response lacks identity/possession columns")
                    columns_sql = ", ".join(f'"{col}"' for col in available_cols)
                    values_sql = ", ".join(f":{col}" for col in available_cols)
                    updates_sql = ", ".join(
                        f'"{col}" = EXCLUDED."{col}"' for col in available_cols
                        if col not in {"player_id", "game_id"}
                    )
                    clean_frame = cast(pd.DataFrame, player_df_filtered.astype(object).where(player_df_filtered.notna(), None))
                    records = clean_frame.to_dict(orient="records")
                    if any(row["game_id"] != game_id for row in records):
                        raise ValueError("Advanced response game identity contradicts the requested game")
                    with engine.begin() as conn:
                        conn.execute(text(
                            f'INSERT INTO player_game_advanced_stats ({columns_sql}) VALUES ({values_sql}) '
                            f'ON CONFLICT (player_id, game_id) DO UPDATE SET {updates_sql}'
                        ), records)

                if get_games_missing_advanced_stats(engine, season=season, limit=1, game_id=game_id):
                    raise ValueError("Advanced team/player coverage remains incomplete after this game")

                print("✓ Saved")
                success = True
                games_processed += 1

                # Rate limiting
                rate_limit_delay(i)

            except (ReadTimeout, ConnectionError, ConnectionResetError) as e:
                print(f"\n🛑 CONNECTION BLOCKED: {safe_error(e)}")
                if attempt < _runtime_options.get("max_retries", MAX_RETRIES):
                    print(f"💤 Sleeping for {BAN_COOLDOWN / 60} minutes to reset...")
                    time.sleep(BAN_COOLDOWN)

            except Exception as e:
                print(f"\n❌ Error: {safe_error(e)}")
                # Legitimate duplicates are handled by the upsert, not by
                # converting an arbitrary write/coverage error into success.
                if attempt < _runtime_options.get("max_retries", MAX_RETRIES):
                    time.sleep(5)

        if not success:
            print(f"💀 Giving up on {game_id} after {attempt} attempts.")
            games_failed += 1

    return games_processed, games_failed


# =============================================================================
# Traditional Stats Scraper (Player Game Stats)
# =============================================================================


def transform_player_traditional_df(df: pd.DataFrame) -> pd.DataFrame:
    """Transform player traditional stats DataFrame for database."""
    if df.empty:
        return df

    # 1. Convert camelCase to snake_case (e.g. firstName -> first_name)
    df = df.rename(columns={col: camel_to_snake(col) for col in df.columns})

    # 2. Map V3 API column names to database schema abbreviations
    v3_to_db_mapping = {
        "person_id": "player_id",
        "field_goals_made": "fgm",
        "field_goals_attempted": "fga",
        "field_goals_percentage": "fg_pct",
        "three_pointers_made": "fg3m",
        "three_pointers_attempted": "fg3a",
        "three_pointers_percentage": "fg3_pct",
        "free_throws_made": "ftm",
        "free_throws_attempted": "fta",
        "free_throws_percentage": "ft_pct",
        "rebounds_offensive": "oreb",
        "rebounds_defensive": "dreb",
        "rebounds_total": "reb",
        "assists": "ast",
        "steals": "stl",
        "blocks": "blk",
        "turnovers": "tov",
        "fouls_personal": "pf",
        "points": "pts",
        "plus_minus_points": "plus_minus",
    }
    df = df.rename(columns=v3_to_db_mapping)

    # 3. Parse minutes
    # Schema has 'min' as bigint, so we must round to nearest int.
    # API gives "MM:SS", parse_minutes gives float. We round that float.
    if "minutes" in df.columns:
        # Parse to float first (e.g. 34.5)
        df["minutes"] = df["minutes"].apply(parse_minutes)
        # Rename to 'min' immediately to match schema
        df = df.rename(columns={"minutes": "min"})
        # Round and convert to integer (Handle NaNs just in case)
        df["min"] = df["min"].fillna(0).round().astype(int)

    # 4. Add DNP flag (derived from comment or minutes)
    df["did_not_play"] = df.apply(determine_dnp, axis=1)

    return df


def scrape_traditional_stats(engine, limit: int | None = None) -> tuple[int, int]:
    """
    Scrape traditional box scores for games missing them in 'player_game_stats'.
    Uses BoxScoreTraditionalV3.
    """
    print("\n" + "=" * 60)
    print("STEP 1.5: Fetching Traditional Player Stats (V3)")
    print("=" * 60)

    # 1. Find games that exist in team_stats but NOT in player_game_stats.
    # We also fetch the context (date, matchup, wl, season) to merge later.
    sql_missing = """
    SELECT
        t.game_id,
        t.team_id,
        t.season_id,
        t.game_date,
        t.team_matchup as matchup,
        t.team_wl as wl
    FROM team_game_stats t
    LEFT JOIN player_game_stats p ON t.game_id = p.game_id AND t.team_id = p.team_id
    WHERE p.game_id IS NULL
    """

    # Fetch metadata for missing games
    print("Finding missing games and fetching context info...")
    metadata_df = pd.read_sql(sql_missing, engine)

    if metadata_df.empty:
        print("\n✅ All games have traditional player stats!")
        return 0, 0

    # Get unique game IDs to iterate over
    unique_game_ids = metadata_df["game_id"].unique()

    if limit:
        unique_game_ids = unique_game_ids[:limit]

    total = len(unique_game_ids)
    print(f"\nFound {total} games missing traditional player stats")

    games_processed = 0
    games_failed = 0

    # DB Columns based on your schema
    db_cols = [
        "season_id",
        "player_id",
        "game_id",
        "game_date",
        "matchup",
        "wl",
        "min",
        "fgm",
        "fga",
        "fg_pct",
        "fg3m",
        "fg3a",
        "fg3_pct",
        "ftm",
        "fta",
        "ft_pct",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts",
        "plus_minus",
        "video_available",
        "team_id",
        "comment",
        "did_not_play",
    ]

    for i, game_id in enumerate(unique_game_ids, 1):
        success = False
        attempt = 0

        while attempt < _runtime_options.get("max_retries", MAX_RETRIES) and not success:
            attempt += 1
            try:
                print(
                    f"[{i}/{total}] Scraping Traditional {game_id} (Try {attempt})... ",
                    end="",
                    flush=True,
                )

                # Call API (V3)
                box = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, proxy=PROXY_URL or "", timeout=_runtime_options.get("request_timeout", REQUEST_TIMEOUT))
                player_df = box.player_stats.get_data_frame()

                if player_df.empty:
                    print("⚠️ Empty data (Skipping)")
                    break

                # Transform (Renames cols, parses minutes, adds DNP)
                # Note: V3 API column mapping (points->pts, etc.) handled in transform function
                player_df = transform_player_traditional_df(player_df)

                # --- Merge Metadata (game_date, wl, matchup, etc) ---
                # We filter metadata_df for just this game to avoid large merges
                game_meta = metadata_df[metadata_df["game_id"] == game_id]

                # Merge on team_id so players get their specific team's WL/Matchup
                # (Inner join ensures we only keep players matching our known teams)
                merged_df = pd.merge(player_df, game_meta, on=["game_id", "team_id"], how="inner")

                if merged_df.empty:
                    print("⚠️ No matching team IDs found in metadata (Skipping)")
                    break

                # Ensure players exist in 'players' table
                ensure_players_exist(engine, merged_df)

                # Select only valid columns
                valid_cols = [c for c in db_cols if c in merged_df.columns]
                final_df = merged_df[valid_cols]

                # Save
                final_df.to_sql("player_game_stats", engine, if_exists="append", index=False)

                print("✓ Saved")
                success = True
                games_processed += 1

                # Rate limiting
                rate_limit_delay(i)

            except (ReadTimeout, ConnectionError, ConnectionResetError, ChunkedEncodingError) as e:
                print(f"\n🛑 CONNECTION BLOCKED: {safe_error(e)}")
                print(f"💤 Sleeping for {BAN_COOLDOWN / 60} minutes to reset...")
                time.sleep(BAN_COOLDOWN)

            except Exception as e:
                error_str = str(e)
                print(f"\n❌ Error: {safe_error(e)}")

                if "duplicate key" in error_str or "UniqueViolation" in error_str:
                    print("   (Game already exists, moving on)")
                    success = True
                    games_processed += 1
                else:
                    time.sleep(5)  # Brief pause before retry

        if not success:
            print(f"💀 Giving up on {game_id} after {attempt} attempts.")
            games_failed += 1

    return games_processed, games_failed


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    global PROXY_URL
    parser = argparse.ArgumentParser(description="NBA Unified Stats Scraper")
    parser.add_argument(
        "--season",
        type=str,
        help="Season to scrape locally (YYYY-YY), or 'current'; required outside --cdn-only",
    )
    parser.add_argument(
        "--season-type",
        type=str,
        default="Regular Season",
        choices=["Regular Season", "Playoffs", "PlayIn", "In-Season Tournament"],
        help="Season type to scrape (default: Regular Season)",
    )
    parser.add_argument("--skip-team", action="store_true", help="Skip team game stats scraping")
    parser.add_argument("--skip-traditional", action="store_true", help="Skip traditional player stats scraping")
    parser.add_argument("--skip-advanced", action="store_true", help="Skip advanced stats scraping")
    parser.add_argument(
        "--cdn-only",
        action="store_true",
        help="Skip stats.nba.com entirely, use CDN scraper only (no advanced stats)",
    )
    parser.add_argument(
        "--advanced-limit",
        type=int,
        default=None,
        help="Positive limit for traditional/advanced missing games",
    )
    proxy_group = parser.add_mutually_exclusive_group()
    proxy_group.add_argument("--no-proxy", action="store_true", help="Use the default direct local route")
    proxy_group.add_argument("--use-proxy", action="store_true", help="Explicitly use configured PROXY_URL")
    parser.add_argument("--http-client", choices=["curl", "requests"], default="curl", help="Local stats transport (default: Chrome-compatible curl); unused in CDN-only mode")
    parser.add_argument("--request-timeout", type=int, default=REQUEST_TIMEOUT, help="stats.nba.com request timeout, 1..120 seconds")
    parser.add_argument("--max-retries", type=int, default=MAX_RETRIES, help="Maximum stats.nba.com attempts per game, 1..5")
    parser.add_argument("--max-requests", type=int, help="Hard cap on stats HTTP GET attempts, including retries")

    args = parser.parse_args()
    if not 1 <= args.request_timeout <= 120 or not 1 <= args.max_retries <= 5:
        parser.error("--request-timeout must be 1..120; --max-retries must be 1..5")
    _runtime_options.update(request_timeout=args.request_timeout, max_retries=args.max_retries)
    if not args.cdn_only and not args.season:
        parser.error("--season is required outside --cdn-only mode")
    if args.advanced_limit is not None and args.advanced_limit <= 0:
        parser.error("--advanced-limit must be positive")
    if args.max_requests is not None and args.max_requests <= 0:
        parser.error("--max-requests must be positive")
    if args.season == "current":
        args.season = current_nba_season_label()
    if args.season:
        try:
            season_label_to_id(args.season)
        except ValueError as exc:
            parser.error(str(exc))

    if not args.use_proxy:
        PROXY_URL = None
    elif not PROXY_URL:
        parser.error("--use-proxy requires a configured PROXY_URL")

    print("=" * 60)
    print("NBA UNIFIED STATS SCRAPER")
    print("=" * 60)
    print(f"Season: {'from CDN game IDs' if args.cdn_only else args.season}")
    print(f"Season Type: {args.season_type}")
    print(f"Skip Team Stats: {args.skip_team}")
    print(f"Skip Traditional Stats: {args.skip_traditional}")
    print(f"Skip Advanced Stats: {args.skip_advanced}")
    print(f"Proxy: {'Enabled' if PROXY_URL else 'Disabled'}")
    print(f"Stats HTTP client: {'unused (CDN-only)' if args.cdn_only else args.http_client}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    new_games = 0
    cdn_failed = 0
    trad_processed = 0
    trad_failed = 0
    adv_processed = 0
    adv_failed = 0
    cdn_used = False
    primary_errors: list[str] = []
    http_session = None

    try:
        if not args.cdn_only:
            http_session = configure_local_stats_http(args.http_client, args.max_requests)
        print("\nConnecting to database...")
        engine = get_engine()
        print("✓ Database connection verified")

        if args.cdn_only:
            print("\n📡 CDN-ONLY MODE: Skipping stats.nba.com, using cdn.nba.com directly")
            new_games, cdn_failed = scrape_games_cdn(engine, scrape_all_missing=True)
            cdn_used = True
            print(f"  CDN scraped: {new_games} games ({cdn_failed} failed)")
        else:
            if not args.skip_team:
                try:
                    new_games = scrape_team_game_stats(engine, [args.season], args.season_type)
                except (ReadTimeout, ConnectionError, ConnectionResetError) as exc:
                    if args.use_proxy:
                        raise
                    primary_errors.append(f"team source failed: {type(exc).__name__}: {safe_error(exc)}")
                    print(f"\n⚠️  stats.nba.com team source failed: {type(exc).__name__}")
                    print("Attempting CDN recovery; the primary failure will remain non-success.")
                    new_games, cdn_failed = scrape_games_cdn(engine, scrape_all_missing=True)
                    cdn_used = True
            else:
                print("\n⏭️  Skipping team game stats (--skip-team)")

            if not args.skip_traditional and not cdn_used:
                try:
                    trad_processed, trad_failed = scrape_traditional_stats(engine, args.advanced_limit)
                except (ReadTimeout, ConnectionError, ConnectionResetError) as exc:
                    if args.use_proxy:
                        raise
                    primary_errors.append(f"traditional source failed: {type(exc).__name__}: {safe_error(exc)}")
                    print(f"\n⚠️  stats.nba.com traditional source failed: {type(exc).__name__}")
                    print("Attempting CDN recovery; the primary failure will remain non-success.")
                    recovered, fallback_failed = scrape_games_cdn(engine, scrape_all_missing=True)
                    new_games += recovered
                    cdn_failed += fallback_failed
                    cdn_used = True
            elif cdn_used:
                print("\n⏭️  Skipping traditional stats (already recovered via CDN)")
            else:
                print("\n⏭️  Skipping traditional player stats (--skip-traditional)")

            if not args.skip_advanced and not cdn_used:
                adv_processed, adv_failed = scrape_advanced_stats(
                    engine, args.advanced_limit, season=args.season
                )
            elif cdn_used:
                print("\n⏭️  Advanced stats unavailable via CDN; local advanced stage not completed")
            else:
                print("\n⏭️  Skipping advanced stats (--skip-advanced)")
    except Exception as exc:
        primary_errors.append(f"{type(exc).__name__}: {safe_error(exc)}")
    finally:
        if http_session is not None:
            from nba_api.stats.library.http import NBAStatsHTTP

            NBAStatsHTTP.set_session(None)
            http_session.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if cdn_used:
        print("CDN source used (advanced stats were not supplied by CDN)")
    print(f"New team game records added:      {new_games} (CDN failed: {cdn_failed})")
    print(f"Traditional stats updated:        {trad_processed} (Failed: {trad_failed})")
    print(f"Advanced stats updated:           {adv_processed} (Failed: {adv_failed})")
    if http_session is not None:
        print(f"Stats HTTP requests attempted:    {http_session.stats_request_count}")
    for error in primary_errors:
        print(f"Source error: {error}")
    failed = bool(primary_errors or cdn_failed or trad_failed or adv_failed)
    print(f"Result: {'FAILED' if failed else 'SUCCESS'}")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
