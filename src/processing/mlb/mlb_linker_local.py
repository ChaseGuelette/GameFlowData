"""
MLB Data Linker - Local Processing Version with Checkpoint/Resume
==================================================================
Downloads data locally, processes in pandas, uploads results.
Checkpoint system allows interrupted runs to pick up where they left off.

Steps:
1. python -m src.processing.mlb.mlb_linker_local download   - Pull tables to CSV
2. python -m src.processing.mlb.mlb_linker_local process    - Match IDs locally (includes team_id backfill)
3. python -m src.processing.mlb.mlb_linker_local upload     - Push results back (includes team_id backfill)
4. python -m src.processing.mlb.mlb_linker_local all        - All 3 stages
5. python -m src.processing.mlb.mlb_linker_local status     - Show progress
6. python -m src.processing.mlb.mlb_linker_local init       - Create player_mappings.csv template
7. python -m src.processing.mlb.mlb_linker_local reset      - Clear checkpoint

For player name mismatches:
- After process, check mlb_linker_data/unmatched_players.csv
- Edit mlb_linker_data/player_mappings.csv to add manual mappings
- Re-run process to apply them
"""

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.processing.mlb.mlb_config import (
    FUZZY_THRESHOLD,
    MLB_TEAM_ALIASES,
)
from src.processing.mlb.mlb_linker import (
    _resolve_fuzzy_names,
    find_closest_game_date,
    normalize_player,
    normalize_team,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path("./mlb_linker_data")
CHECKPOINT_FILE = DATA_DIR / "_checkpoint.json"
DOWNLOAD_BATCH_SIZE = 100_000
UPLOAD_CHUNK_SIZE = 50_000
MAX_RETRIES = 20


# ============================================================================
# CHECKPOINT SYSTEM
# ============================================================================


def load_checkpoint() -> dict:
    """Load checkpoint from disk, returning empty structure if none exists."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"version": 1, "started_at": None, "stages": {}}


def save_checkpoint(checkpoint: dict) -> None:
    """Persist checkpoint to disk."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2, default=str)


def get_stage_status(checkpoint: dict, stage: str) -> str:
    """Get status of a stage: 'not_started', 'in_progress', or 'completed'."""
    return checkpoint.get("stages", {}).get(stage, {}).get("status", "not_started")


def mark_stage(checkpoint: dict, stage: str, status: str, **kwargs) -> dict:
    """Update a stage's status and metadata, then save."""
    if "stages" not in checkpoint:
        checkpoint["stages"] = {}
    if stage not in checkpoint["stages"]:
        checkpoint["stages"][stage] = {}
    checkpoint["stages"][stage]["status"] = status
    checkpoint["stages"][stage].update(kwargs)
    save_checkpoint(checkpoint)
    return checkpoint


def reset_checkpoint() -> None:
    """Delete checkpoint file."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logger.info("Checkpoint cleared.")
    else:
        logger.info("No checkpoint to clear.")


# ============================================================================
# STAGE 1: DOWNLOAD
# ============================================================================


def _download_small_table(engine, name: str, query: str, checkpoint: dict) -> dict:
    """Download a small table to CSV. Skip if already completed in checkpoint."""
    filepath = DATA_DIR / f"{name}.csv"
    tables_completed = checkpoint.get("stages", {}).get("download", {}).get("tables_completed", [])

    if filepath.exists() and name in tables_completed:
        logger.info(f"  Skipping {name} (completed in checkpoint)")
        return checkpoint

    logger.info(f"  Downloading {name}...")
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    df.to_csv(filepath, index=False)
    logger.info(f"    Saved {len(df):,} rows")

    # Update checkpoint
    if name not in tables_completed:
        tables_completed.append(name)
    checkpoint = mark_stage(checkpoint, "download", "in_progress", tables_completed=tables_completed)
    return checkpoint


def _download_batched_table(
    engine, name: str, table: str, columns: str, checkpoint: dict
) -> dict:
    """Download a large table in batches by staging_id. Skip if completed."""
    filepath = DATA_DIR / f"{name}.csv"
    tables_completed = checkpoint.get("stages", {}).get("download", {}).get("tables_completed", [])

    if filepath.exists() and name in tables_completed:
        logger.info(f"  Skipping {name} (completed in checkpoint)")
        return checkpoint

    logger.info(f"  Downloading {name} (batched)...")

    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT MIN(staging_id), MAX(staging_id) FROM {table}"))
        row = result.fetchone()
        min_id, max_id = row[0], row[1]

    if not min_id:
        logger.warning(f"    {name}: table is empty")
        pd.DataFrame().to_csv(filepath, index=False)
        tables_completed.append(name)
        checkpoint = mark_stage(checkpoint, "download", "in_progress", tables_completed=tables_completed)
        return checkpoint

    logger.info(f"    ID range: {min_id:,} to {max_id:,}")
    current = min_id
    total_rows = 0
    first_batch = True
    pbar = tqdm(total=(max_id - min_id + 1), unit="rows", desc=name)

    while current <= max_id:
        query = text(f"""
            SELECT {columns}
            FROM {table}
            WHERE staging_id >= :start AND staging_id < :end
        """)
        with engine.connect() as conn:
            chunk = pd.read_sql(query, conn, params={"start": current, "end": current + DOWNLOAD_BATCH_SIZE})

        if not chunk.empty:
            mode = "w" if first_batch else "a"
            chunk.to_csv(filepath, mode=mode, header=first_batch, index=False)
            total_rows += len(chunk)
            first_batch = False

        current += DOWNLOAD_BATCH_SIZE
        pbar.update(DOWNLOAD_BATCH_SIZE)

    pbar.close()
    logger.info(f"    Saved {total_rows:,} rows")

    tables_completed.append(name)
    checkpoint = mark_stage(checkpoint, "download", "in_progress", tables_completed=tables_completed)
    return checkpoint


def download_tables(force: bool = False) -> None:
    """Download all reference and staging tables to local CSVs."""
    DATA_DIR.mkdir(exist_ok=True)
    engine = get_engine()
    checkpoint = load_checkpoint()

    if force:
        checkpoint["stages"].pop("download", None)
        save_checkpoint(checkpoint)

    if get_stage_status(checkpoint, "download") == "completed" and not force:
        logger.info("Download stage already completed (use --force to re-download)")
        return

    if not checkpoint.get("started_at"):
        checkpoint["started_at"] = datetime.now().isoformat()
        save_checkpoint(checkpoint)

    logger.info("=" * 60)
    logger.info("STAGE 1: DOWNLOAD")
    logger.info("=" * 60)

    # Small tables
    checkpoint = _download_small_table(
        engine, "mlb_teams", "SELECT team_id, team_abbreviation, team_name FROM mlb_teams", checkpoint
    )
    checkpoint = _download_small_table(
        engine, "mlb_players", "SELECT player_id, player_name FROM mlb_players", checkpoint
    )
    checkpoint = _download_small_table(
        engine, "mlb_game_schedule",
        """SELECT game_id, game_date::text AS game_date, home_team_id, away_team_id
           FROM mlb_game_schedule""",
        checkpoint,
    )
    checkpoint = _download_small_table(
        engine, "mlb_player_game_stats",
        """SELECT player_id, game_id, team_id FROM mlb_player_game_stats_batting
           WHERE team_id IS NOT NULL
           UNION
           SELECT player_id, game_id, team_id FROM mlb_player_game_stats_pitching
           WHERE team_id IS NOT NULL""",
        checkpoint,
    )

    # Large batched tables
    checkpoint = _download_batched_table(
        engine,
        "mlb_raw_player_props",
        "mlb_raw_player_props",
        "staging_id, api_player_name, home_team, away_team, commence_time, game_id, player_id, team_id",
        checkpoint,
    )
    checkpoint = _download_batched_table(
        engine,
        "mlb_raw_game_lines",
        "mlb_raw_game_lines",
        "staging_id, home_team, away_team, commence_time, mlb_game_id, mlb_home_team_id, mlb_away_team_id",
        checkpoint,
    )

    checkpoint = mark_stage(
        checkpoint, "download", "completed",
        tables_completed=checkpoint["stages"]["download"]["tables_completed"],
    )
    logger.info("\n[OK] Download complete!")


# ============================================================================
# STAGE 2: PROCESS
# ============================================================================


def _build_local_game_lookup(teams_df: pd.DataFrame, schedule_df: pd.DataFrame) -> dict:
    """Build game lookup from local CSVs: (home_abbrev, away_abbrev) -> [(game_id, date_str), ...]."""
    abbrev_map = dict(zip(teams_df["team_id"], teams_df["team_abbreviation"]))
    lookup = defaultdict(list)

    for _, row in schedule_df.iterrows():
        home_abbrev = abbrev_map.get(row["home_team_id"])
        away_abbrev = abbrev_map.get(row["away_team_id"])
        if home_abbrev and away_abbrev:
            lookup[(home_abbrev, away_abbrev)].append(
                (int(row["game_id"]), str(row["game_date"])[:10])
            )

    logger.info(f"  Built game lookup: {len(lookup)} matchups, {sum(len(v) for v in lookup.values())} games")
    return dict(lookup)


def _build_local_player_lookup(players_df: pd.DataFrame) -> tuple[dict, dict]:
    """Build player lookup from local CSV. Returns (norm_name->id, id->name)."""
    lookup = {}
    name_by_id = {}
    for _, row in players_df.iterrows():
        norm = normalize_player(row["player_name"])
        if norm:
            lookup[norm] = int(row["player_id"])
        name_by_id[int(row["player_id"])] = row["player_name"]
    logger.info(f"  Built player lookup: {len(lookup)} players")
    return lookup, name_by_id


def _build_local_team_id_lookup(stats_df: pd.DataFrame) -> dict:
    """Build (player_id, game_id) -> team_id from local boxscore CSV."""
    lookup = {}
    for _, row in stats_df.iterrows():
        if pd.notna(row["player_id"]) and pd.notna(row["game_id"]) and pd.notna(row["team_id"]):
            lookup[(int(row["player_id"]), int(row["game_id"]))] = int(row["team_id"])
    logger.info(f"  Built team_id lookup: {len(lookup)} (player, game) pairs")
    return lookup


def process_game_lines(
    game_lines_df: pd.DataFrame,
    game_lookup: dict,
    teams_df: pd.DataFrame,
    checkpoint: dict,
    force: bool = False,
) -> dict:
    """Sub-stage 1: Match mlb_raw_game_lines to mlb_game_schedule."""
    if get_stage_status(checkpoint, "process_game_lines") == "completed" and not force:
        logger.info("  process_game_lines: already completed (skipping)")
        return checkpoint

    logger.info("\n--- Process Game Lines ---")

    # Build team name -> abbreviation lookup
    abbrev_by_name = {}
    for alias, abbrev in MLB_TEAM_ALIASES.items():
        abbrev_by_name[alias] = abbrev

    unlinked = game_lines_df[game_lines_df["mlb_game_id"].isna()].copy()
    logger.info(f"  Unlinked game lines: {len(unlinked):,}")

    if unlinked.empty:
        checkpoint = mark_stage(checkpoint, "process_game_lines", "completed", rows_matched=0)
        return checkpoint

    unlinked["game_date"] = pd.to_datetime(unlinked["commence_time"], utc=True).dt.strftime("%Y-%m-%d")
    unlinked["home_norm"] = unlinked["home_team"].apply(normalize_team)
    unlinked["away_norm"] = unlinked["away_team"].apply(normalize_team)

    # Build reverse lookup: abbreviation -> team_id
    team_id_by_abbrev = dict(zip(teams_df["team_abbreviation"], teams_df["team_id"]))

    results = []
    for _, row in tqdm(unlinked.iterrows(), total=len(unlinked), desc="Matching game lines"):
        home = row["home_norm"]
        away = row["away_norm"]
        candidates = game_lookup.get((home, away), [])
        if not candidates:
            continue
        game_id, _ = find_closest_game_date(candidates, row["game_date"], max_days=1)
        if game_id:
            results.append({
                "staging_id": row["staging_id"],
                "mlb_game_id": game_id,
                "mlb_home_team_id": team_id_by_abbrev.get(home),
                "mlb_away_team_id": team_id_by_abbrev.get(away),
            })

    if results:
        updates_df = pd.DataFrame(results)
        updates_df.to_csv(DATA_DIR / "game_lines_updates.csv", index=False)
        logger.info(f"  Matched: {len(updates_df):,} -> game_lines_updates.csv")
    else:
        logger.info("  No matches found")

    checkpoint = mark_stage(checkpoint, "process_game_lines", "completed", rows_matched=len(results))
    return checkpoint


def process_player_props_games(
    props_df: pd.DataFrame,
    game_lookup: dict,
    checkpoint: dict,
    force: bool = False,
) -> dict:
    """Sub-stage 2: Match mlb_raw_player_props to games by (home, away, date) ±1 day."""
    if get_stage_status(checkpoint, "process_props_games") == "completed" and not force:
        logger.info("  process_props_games: already completed (skipping)")
        return checkpoint

    logger.info("\n--- Process Player Props -> Games ---")

    unlinked = props_df[props_df["game_id"].isna()].copy()
    logger.info(f"  Unlinked props: {len(unlinked):,}")

    if unlinked.empty:
        checkpoint = mark_stage(checkpoint, "process_props_games", "completed", rows_matched=0)
        return checkpoint

    unlinked["game_date"] = pd.to_datetime(unlinked["commence_time"], utc=True).dt.strftime("%Y-%m-%d")
    unlinked["home_norm"] = unlinked["home_team"].apply(normalize_team)
    unlinked["away_norm"] = unlinked["away_team"].apply(normalize_team)

    exact_matches = 0
    fuzzy_matches = 0
    game_cache = {}

    def match_game(row):
        nonlocal exact_matches, fuzzy_matches
        key = (row["home_norm"], row["away_norm"], row["game_date"])
        if key in game_cache:
            return game_cache[key]
        candidates = game_lookup.get((key[0], key[1]), [])
        if not candidates:
            game_cache[key] = None
            return None
        game_id, diff = find_closest_game_date(candidates, key[2], max_days=1)
        game_cache[key] = game_id
        if game_id is not None:
            if diff == 0:
                exact_matches += 1
            else:
                fuzzy_matches += 1
        return game_id

    tqdm.pandas(desc="Matching props to games")
    try:
        unlinked["matched_game_id"] = unlinked.progress_apply(match_game, axis=1)
    except (AttributeError, ImportError):
        unlinked["matched_game_id"] = unlinked.apply(match_game, axis=1)

    matched = unlinked[unlinked["matched_game_id"].notna()][["staging_id", "matched_game_id"]].copy()
    matched.columns = ["staging_id", "game_id"]
    matched["game_id"] = matched["game_id"].astype(int)

    if not matched.empty:
        matched.to_csv(DATA_DIR / "props_game_updates.csv", index=False)
        logger.info(f"  Matched: {len(matched):,} (exact={exact_matches}, fuzzy_date={fuzzy_matches})")
        logger.info("  -> props_game_updates.csv")
    else:
        logger.info("  No matches found")

    # Save unmatched games for review
    unmatched = unlinked[unlinked["matched_game_id"].isna()][
        ["home_team", "away_team", "game_date"]
    ].drop_duplicates()
    if not unmatched.empty:
        unmatched.to_csv(DATA_DIR / "unmatched_games.csv", index=False)
        logger.info(f"  {len(unmatched)} unmatched game combos -> unmatched_games.csv")

    checkpoint = mark_stage(checkpoint, "process_props_games", "completed", rows_matched=len(matched))
    return checkpoint


def process_player_props_players(
    props_df: pd.DataFrame,
    player_lookup: dict,
    player_name_by_id: dict,
    checkpoint: dict,
    force: bool = False,
) -> dict:
    """Sub-stage 3: Match api_player_name to mlb_players via exact + fuzzy matching."""
    if get_stage_status(checkpoint, "process_props_players") == "completed" and not force:
        logger.info("  process_props_players: already completed (skipping)")
        return checkpoint

    logger.info("\n--- Process Player Props -> Players ---")

    # Merge in game updates if available
    props_with_games = props_df.copy()
    game_updates_file = DATA_DIR / "props_game_updates.csv"
    if game_updates_file.exists():
        game_updates = pd.read_csv(game_updates_file)
        update_dict = dict(zip(game_updates["staging_id"], game_updates["game_id"]))
        props_with_games["game_id"] = props_with_games.apply(
            lambda r: update_dict.get(r["staging_id"], r["game_id"]), axis=1
        )

    # Only process rows needing player match
    needs_player = props_with_games[props_with_games["player_id"].isna()].copy()
    logger.info(f"  Rows needing player match: {len(needs_player):,}")

    if needs_player.empty:
        checkpoint = mark_stage(checkpoint, "process_props_players", "completed", rows_matched=0)
        return checkpoint

    needs_player["player_norm"] = needs_player["api_player_name"].apply(normalize_player)

    # Load manual mappings
    manual_mappings = {}
    mappings_file = DATA_DIR / "player_mappings.csv"
    if mappings_file.exists():
        try:
            mappings_df = pd.read_csv(mappings_file)
            for _, row in mappings_df.iterrows():
                if pd.notna(row.get("api_name")) and pd.notna(row.get("player_id")):
                    manual_mappings[row["api_name"]] = int(row["player_id"])
            logger.info(f"  Loaded {len(manual_mappings)} manual mappings")
        except Exception as e:
            logger.warning(f"  Could not load manual mappings: {e}")

    # Step 1: Manual mappings (exact api_name match)
    needs_player["matched_player_id"] = needs_player["api_player_name"].map(manual_mappings)

    # Step 2: Exact normalized match
    still_unmatched = needs_player["matched_player_id"].isna() & needs_player["player_norm"].notna()
    needs_player.loc[still_unmatched, "matched_player_id"] = (
        needs_player.loc[still_unmatched, "player_norm"].map(player_lookup)
    )

    # Step 3: Fuzzy matching on remaining unique names
    unmatched_mask = needs_player["matched_player_id"].isna() & needs_player["player_norm"].notna()
    unmatched_names = set(needs_player.loc[unmatched_mask, "player_norm"].unique())

    if unmatched_names:
        logger.info(f"  Running fuzzy matching on {len(unmatched_names)} unique unmatched names...")
        fuzzy_results = _resolve_fuzzy_names(unmatched_names, player_lookup)
        fuzzy_mapped = needs_player.loc[unmatched_mask, "player_norm"].map(fuzzy_results)
        has_fuzzy = fuzzy_mapped.notna()
        if has_fuzzy.any():
            needs_player.loc[has_fuzzy[has_fuzzy].index, "matched_player_id"] = fuzzy_mapped[has_fuzzy]

    # Output matched
    matched = needs_player[needs_player["matched_player_id"].notna()][["staging_id", "matched_player_id"]].copy()
    matched.columns = ["staging_id", "player_id"]
    matched["player_id"] = matched["player_id"].astype(int)

    if not matched.empty:
        matched.to_csv(DATA_DIR / "props_player_updates.csv", index=False)
        logger.info(f"  Matched: {len(matched):,} -> props_player_updates.csv")

    # Diagnostics: unmatched players with suggestions
    final_unmatched = needs_player[needs_player["matched_player_id"].isna()]["api_player_name"].unique()
    if len(final_unmatched) > 0:
        logger.info(f"  {len(final_unmatched)} unmatched player names — generating suggestions...")
        lookup_items = list(player_lookup.items())
        suggestions = []

        for api_name in tqdm(final_unmatched, desc="Finding suggestions"):
            norm = normalize_player(api_name)
            if not norm:
                continue
            best_matches = []
            norm_parts = norm.split()
            norm_last = norm_parts[-1] if norm_parts else ""

            for pname, pid in lookup_items:
                score = SequenceMatcher(None, norm, pname).ratio()
                pname_parts = pname.split()
                if norm_last and pname_parts and norm_last == pname_parts[-1]:
                    score += 0.15
                best_matches.append((pid, player_name_by_id.get(pid, ""), score))

            best_matches.sort(key=lambda x: x[2], reverse=True)
            top3 = best_matches[:3]
            if top3:
                best_id, best_name, best_score = top3[0]
                suggestions.append({
                    "api_name": api_name,
                    "player_id": best_id if best_score >= FUZZY_THRESHOLD else "",
                    "suggested_name": best_name,
                    "confidence": f"{best_score:.2f}",
                    "other_suggestions": "; ".join(
                        [f"{n} ({s:.2f})" for _, n, s in top3[1:]]
                    ),
                })

        if suggestions:
            pd.DataFrame(suggestions).to_csv(DATA_DIR / "unmatched_players.csv", index=False)
            logger.info(f"  -> unmatched_players.csv ({len(suggestions)} names)")
            logger.info("  Review and copy confirmed mappings to player_mappings.csv")

    checkpoint = mark_stage(checkpoint, "process_props_players", "completed", rows_matched=len(matched))
    return checkpoint


def process_player_props_teams(
    props_df: pd.DataFrame,
    team_id_lookup: dict,
    checkpoint: dict,
    force: bool = False,
) -> dict:
    """Sub-stage 4: Resolve team_id from (player_id, game_id) via boxscore data."""
    if get_stage_status(checkpoint, "process_props_teams") == "completed" and not force:
        logger.info("  process_props_teams: already completed (skipping)")
        return checkpoint

    logger.info("\n--- Process Player Props -> Teams ---")

    # Load player updates
    player_updates_file = DATA_DIR / "props_player_updates.csv"
    if not player_updates_file.exists():
        logger.info("  Skipping: props_player_updates.csv not found (run player matching first)")
        checkpoint = mark_stage(checkpoint, "process_props_teams", "completed", rows_matched=0)
        return checkpoint

    player_updates = pd.read_csv(player_updates_file)

    # Merge game_id from game updates or original data
    game_updates_file = DATA_DIR / "props_game_updates.csv"
    if game_updates_file.exists():
        game_updates = pd.read_csv(game_updates_file)
        player_updates = player_updates.merge(game_updates, on="staging_id", how="left")

    # Fill missing game_ids from original props
    orig_games = dict(zip(props_df["staging_id"], props_df["game_id"]))
    if "game_id" not in player_updates.columns:
        player_updates["game_id"] = player_updates["staging_id"].map(orig_games)
    else:
        player_updates["game_id"] = player_updates.apply(
            lambda r: r["game_id"] if pd.notna(r.get("game_id")) else orig_games.get(r["staging_id"]),
            axis=1,
        )

    # Match team_id
    def get_team_id(row):
        pid = row["player_id"]
        gid = row.get("game_id")
        if pd.isna(pid) or pd.isna(gid):
            return None
        return team_id_lookup.get((int(pid), int(gid)))

    tqdm.pandas(desc="Matching team_id")
    try:
        player_updates["team_id"] = player_updates.progress_apply(get_team_id, axis=1)
    except (AttributeError, ImportError):
        player_updates["team_id"] = player_updates.apply(get_team_id, axis=1)

    # Output: staging_id, player_id, team_id
    result = player_updates[
        player_updates["player_id"].notna() | player_updates["team_id"].notna()
    ][["staging_id", "player_id", "team_id"]].copy()

    if not result.empty:
        result.to_csv(DATA_DIR / "props_full_updates.csv", index=False)
        team_matched = int(result["team_id"].notna().sum())
        logger.info(f"  player_id matched: {len(result):,}")
        logger.info(f"  team_id matched:   {team_matched:,}")
        logger.info("  -> props_full_updates.csv")

    checkpoint = mark_stage(
        checkpoint, "process_props_teams", "completed",
        rows_matched=len(result),
        team_matched=int(result["team_id"].notna().sum()) if not result.empty else 0,
    )
    return checkpoint


def process_player_props_teams_backfill(
    props_df: pd.DataFrame,
    team_id_lookup: dict,
    checkpoint: dict,
    force: bool = False,
) -> dict:
    """Sub-stage 5: Backfill team_id for rows that already have game_id + player_id but missing team_id."""
    if get_stage_status(checkpoint, "process_props_teams_backfill") == "completed" and not force:
        logger.info("  process_props_teams_backfill: already completed (skipping)")
        return checkpoint

    logger.info("\n--- Backfill team_id (game_id + player_id set, team_id missing) ---")

    # Find rows with game_id and player_id already set, but team_id missing
    needs_team = props_df[
        props_df["game_id"].notna()
        & props_df["player_id"].notna()
        & props_df["team_id"].isna()
    ].copy()

    # Exclude rows already covered by props_full_updates.csv (newly matched this run)
    full_updates_file = DATA_DIR / "props_full_updates.csv"
    if full_updates_file.exists():
        already_handled = set(pd.read_csv(full_updates_file)["staging_id"])
        needs_team = needs_team[~needs_team["staging_id"].isin(already_handled)]

    logger.info(f"  Rows with game_id + player_id but no team_id: {len(needs_team):,}")

    if needs_team.empty:
        checkpoint = mark_stage(checkpoint, "process_props_teams_backfill", "completed", rows_matched=0)
        return checkpoint

    def get_team_id(row):
        pid = row["player_id"]
        gid = row["game_id"]
        if pd.isna(pid) or pd.isna(gid):
            return None
        return team_id_lookup.get((int(pid), int(gid)))

    tqdm.pandas(desc="Backfilling team_id")
    try:
        needs_team["team_id_resolved"] = needs_team.progress_apply(get_team_id, axis=1)
    except (AttributeError, ImportError):
        needs_team["team_id_resolved"] = needs_team.apply(get_team_id, axis=1)

    matched = needs_team[needs_team["team_id_resolved"].notna()][["staging_id", "team_id_resolved"]].copy()
    matched.columns = ["staging_id", "team_id"]
    matched["team_id"] = matched["team_id"].astype(int)

    if not matched.empty:
        matched.to_csv(DATA_DIR / "props_team_backfill_updates.csv", index=False)
        logger.info(f"  Resolved: {len(matched):,} -> props_team_backfill_updates.csv")
    else:
        logger.info("  No team_id matches found (boxscore data may not cover these games)")

    unresolved = len(needs_team) - len(matched)
    if unresolved > 0:
        logger.info(f"  Unresolved: {unresolved:,} (no boxscore entry for that player+game combo)")

    checkpoint = mark_stage(
        checkpoint, "process_props_teams_backfill", "completed",
        rows_matched=len(matched),
        rows_unresolved=unresolved,
    )
    return checkpoint


def process_local(force: bool = False) -> None:
    """Run all 5 processing sub-stages on local CSVs."""
    checkpoint = load_checkpoint()

    if force:
        for stage in ["process_game_lines", "process_props_games", "process_props_players",
                       "process_props_teams", "process_props_teams_backfill"]:
            checkpoint["stages"].pop(stage, None)
        save_checkpoint(checkpoint)

    logger.info("=" * 60)
    logger.info("STAGE 2: PROCESS")
    logger.info("=" * 60)

    # Verify required downloads exist
    required = ["mlb_teams.csv", "mlb_players.csv", "mlb_game_schedule.csv",
                 "mlb_player_game_stats.csv", "mlb_raw_player_props.csv", "mlb_raw_game_lines.csv"]
    missing = [f for f in required if not (DATA_DIR / f).exists()]
    if missing:
        logger.error(f"Missing required files: {missing}")
        logger.error("Run 'download' first.")
        sys.exit(1)

    # Load reference data
    logger.info("Loading local CSVs...")
    teams_df = pd.read_csv(DATA_DIR / "mlb_teams.csv")
    players_df = pd.read_csv(DATA_DIR / "mlb_players.csv")
    schedule_df = pd.read_csv(DATA_DIR / "mlb_game_schedule.csv")
    stats_df = pd.read_csv(DATA_DIR / "mlb_player_game_stats.csv")
    props_df = pd.read_csv(DATA_DIR / "mlb_raw_player_props.csv")
    game_lines_df = pd.read_csv(DATA_DIR / "mlb_raw_game_lines.csv")

    logger.info(f"  teams: {len(teams_df):,} | players: {len(players_df):,} | "
                f"schedule: {len(schedule_df):,} | stats: {len(stats_df):,}")
    logger.info(f"  props: {len(props_df):,} | game_lines: {len(game_lines_df):,}")

    # Build lookups
    logger.info("Building lookups...")
    game_lookup = _build_local_game_lookup(teams_df, schedule_df)
    player_lookup, player_name_by_id = _build_local_player_lookup(players_df)
    team_id_lookup = _build_local_team_id_lookup(stats_df)

    # Run 4 sub-stages
    checkpoint = process_game_lines(game_lines_df, game_lookup, teams_df, checkpoint, force)
    checkpoint = process_player_props_games(props_df, game_lookup, checkpoint, force)
    checkpoint = process_player_props_players(props_df, player_lookup, player_name_by_id, checkpoint, force)
    checkpoint = process_player_props_teams(props_df, team_id_lookup, checkpoint, force)
    checkpoint = process_player_props_teams_backfill(props_df, team_id_lookup, checkpoint, force)

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSING COMPLETE — SUMMARY")
    logger.info("=" * 60)
    for filename, desc in [
        ("game_lines_updates.csv", "Game lines to update"),
        ("props_game_updates.csv", "Props game_id to update"),
        ("props_player_updates.csv", "Props player_id to update"),
        ("props_full_updates.csv", "Props player_id + team_id to update"),
        ("props_team_backfill_updates.csv", "Backfill team_id (had game_id + player_id)"),
        ("unmatched_games.csv", "Unmatched games (review)"),
        ("unmatched_players.csv", "Unmatched players (review & add to player_mappings.csv)"),
    ]:
        filepath = DATA_DIR / filename
        if filepath.exists():
            df = pd.read_csv(filepath)
            logger.info(f"  {filename}: {len(df):,} rows — {desc}")


# ============================================================================
# STAGE 3: UPLOAD
# ============================================================================


def _upload_with_retry(engine, df: pd.DataFrame, temp_table: str, update_sql: str,
                       description: str, checkpoint: dict, stage_name: str,
                       batch_delay: float = 0) -> dict:
    """Upload a DataFrame in chunks with retry/backoff and checkpoint tracking."""
    if df.empty:
        logger.info(f"  {description}: nothing to upload")
        checkpoint = mark_stage(checkpoint, stage_name, "completed", chunks_uploaded=0, total_chunks=0)
        return checkpoint

    total_chunks = (len(df) + UPLOAD_CHUNK_SIZE - 1) // UPLOAD_CHUNK_SIZE
    chunks_uploaded = checkpoint.get("stages", {}).get(stage_name, {}).get("chunks_uploaded", 0)

    if get_stage_status(checkpoint, stage_name) == "completed":
        logger.info(f"  {description}: already completed (skipping)")
        return checkpoint

    logger.info(f"  Uploading {description} ({len(df):,} rows in {total_chunks} chunks)...")
    checkpoint = mark_stage(
        checkpoint, stage_name, "in_progress",
        chunks_uploaded=chunks_uploaded, total_chunks=total_chunks,
    )

    consecutive_errors = 0

    for i in range(chunks_uploaded, total_chunks):
        start = i * UPLOAD_CHUNK_SIZE
        end = min(start + UPLOAD_CHUNK_SIZE, len(df))
        chunk = df.iloc[start:end]

        while True:
            try:
                with engine.begin() as conn:
                    chunk.to_sql(temp_table, conn, if_exists="replace", index=False)
                    conn.execute(text(f"CREATE INDEX idx_{temp_table}_sid ON {temp_table}(staging_id)"))
                    result = conn.execute(text(update_sql))
                    conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

                logger.info(f"    Chunk {i + 1}/{total_chunks}: rows {start:,}-{end:,} -> {result.rowcount:,} updated")
                consecutive_errors = 0

                # Update checkpoint
                checkpoint = mark_stage(
                    checkpoint, stage_name, "in_progress",
                    chunks_uploaded=i + 1, total_chunks=total_chunks,
                )

                if batch_delay > 0:
                    time.sleep(batch_delay)

                break  # success, move to next chunk

            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors > MAX_RETRIES:
                    logger.error(f"    Failed {MAX_RETRIES} consecutive times, giving up. Last error: {e}")
                    return checkpoint
                wait = min(60, 10 * consecutive_errors)
                logger.warning(f"    Error (attempt {consecutive_errors}/{MAX_RETRIES}): {e}")
                logger.info(f"    Recycling pool, retrying in {wait}s...")
                engine.dispose()
                time.sleep(wait)

    checkpoint = mark_stage(
        checkpoint, stage_name, "completed",
        chunks_uploaded=total_chunks, total_chunks=total_chunks,
    )
    return checkpoint


def upload_results(force: bool = False, batch_delay: float = 0) -> None:
    """Upload processed results back to the database."""
    engine = get_engine()
    checkpoint = load_checkpoint()

    if force:
        for stage in ["upload_game_lines", "upload_props_games", "upload_props_players", "upload_props_teams_backfill"]:
            checkpoint["stages"].pop(stage, None)
        save_checkpoint(checkpoint)

    logger.info("=" * 60)
    logger.info("STAGE 3: UPLOAD")
    logger.info("=" * 60)

    # 1. Game lines
    game_lines_file = DATA_DIR / "game_lines_updates.csv"
    if game_lines_file.exists():
        df = pd.read_csv(game_lines_file)
        df = df[df["mlb_game_id"].notna()]
        checkpoint = _upload_with_retry(
            engine, df,
            "_tmp_mlb_gl_upload",
            """UPDATE mlb_raw_game_lines r
               SET mlb_game_id = t.mlb_game_id::integer,
                   mlb_home_team_id = t.mlb_home_team_id::integer,
                   mlb_away_team_id = t.mlb_away_team_id::integer
               FROM _tmp_mlb_gl_upload t
               WHERE r.staging_id = t.staging_id""",
            "game lines",
            checkpoint,
            "upload_game_lines",
            batch_delay,
        )
    else:
        logger.info("  No game_lines_updates.csv found — skipping")

    # 2. Props game_id
    props_game_file = DATA_DIR / "props_game_updates.csv"
    if props_game_file.exists():
        df = pd.read_csv(props_game_file)
        checkpoint = _upload_with_retry(
            engine, df,
            "_tmp_mlb_pg_upload",
            """UPDATE mlb_raw_player_props r
               SET game_id = t.game_id::integer
               FROM _tmp_mlb_pg_upload t
               WHERE r.staging_id = t.staging_id
                 AND r.game_id IS NULL""",
            "props game_id",
            checkpoint,
            "upload_props_games",
            batch_delay,
        )
    else:
        logger.info("  No props_game_updates.csv found — skipping")

    # 3. Props player_id + team_id
    props_full_file = DATA_DIR / "props_full_updates.csv"
    if props_full_file.exists():
        df = pd.read_csv(props_full_file)
        checkpoint = _upload_with_retry(
            engine, df,
            "_tmp_mlb_pp_upload",
            """UPDATE mlb_raw_player_props r
               SET player_id = t.player_id::integer,
                   team_id = t.team_id::integer
               FROM _tmp_mlb_pp_upload t
               WHERE r.staging_id = t.staging_id""",
            "props player_id + team_id",
            checkpoint,
            "upload_props_players",
            batch_delay,
        )
    else:
        logger.info("  No props_full_updates.csv found — skipping")

    # 4. Props team_id backfill (rows that already had game_id + player_id)
    props_backfill_file = DATA_DIR / "props_team_backfill_updates.csv"
    if props_backfill_file.exists():
        df = pd.read_csv(props_backfill_file)
        checkpoint = _upload_with_retry(
            engine, df,
            "_tmp_mlb_tb_upload",
            """UPDATE mlb_raw_player_props r
               SET team_id = t.team_id::integer
               FROM _tmp_mlb_tb_upload t
               WHERE r.staging_id = t.staging_id
                 AND r.team_id IS NULL""",
            "props team_id backfill",
            checkpoint,
            "upload_props_teams_backfill",
            batch_delay,
        )
    else:
        logger.info("  No props_team_backfill_updates.csv found — skipping")

    logger.info("\n[OK] Upload complete!")


# ============================================================================
# STATUS & INIT
# ============================================================================


def show_status() -> None:
    """Show checkpoint progress and DB linking stats."""
    checkpoint = load_checkpoint()

    logger.info("=" * 60)
    logger.info("MLB LOCAL LINKER — STATUS")
    logger.info("=" * 60)

    if not checkpoint.get("started_at"):
        logger.info("  No checkpoint found. Run 'download' to begin.")
        logger.info("=" * 60)
        return

    logger.info(f"  Started: {checkpoint['started_at']}")

    all_stages = [
        "download",
        "process_game_lines", "process_props_games",
        "process_props_players", "process_props_teams",
        "process_props_teams_backfill",
        "upload_game_lines", "upload_props_games", "upload_props_players",
        "upload_props_teams_backfill",
    ]

    for stage in all_stages:
        info = checkpoint.get("stages", {}).get(stage, {})
        status = info.get("status", "not_started")
        details = ""

        if stage == "download" and "tables_completed" in info:
            details = f" ({len(info['tables_completed'])}/6 tables)"
        elif "rows_matched" in info:
            details = f" ({info['rows_matched']:,} matched)"
        elif "chunks_uploaded" in info:
            details = f" ({info['chunks_uploaded']}/{info.get('total_chunks', '?')} chunks)"

        logger.info(f"  {stage:30s} {status:15s}{details}")

    # DB stats if available
    try:
        engine = get_engine()
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM mlb_raw_player_props")).scalar()
            linked = conn.execute(
                text("SELECT COUNT(*) FROM mlb_raw_player_props WHERE player_id IS NOT NULL")
            ).scalar()
        unlinked = total - linked
        pct = (linked / total * 100) if total > 0 else 0.0
        logger.info(f"\n  DB: {total:,} total | {linked:,} linked ({pct:.1f}%) | {unlinked:,} unlinked")
    except Exception:
        logger.info("\n  (Could not query DB stats)")

    # Local file stats
    logger.info("\n  Local files:")
    for f in sorted(DATA_DIR.glob("*.csv")):
        try:
            row_count = sum(1 for _ in open(f)) - 1  # subtract header
            logger.info(f"    {f.name}: {row_count:,} rows")
        except Exception:
            logger.info(f"    {f.name}: (could not read)")

    logger.info("=" * 60)


def init_mappings() -> None:
    """Create empty player_mappings.csv template."""
    DATA_DIR.mkdir(exist_ok=True)
    mappings_file = DATA_DIR / "player_mappings.csv"

    if mappings_file.exists():
        logger.info(f"{mappings_file} already exists!")
        return

    pd.DataFrame(columns=["api_name", "player_id"]).to_csv(mappings_file, index=False)
    logger.info(f"Created {mappings_file}")
    logger.info("Add mappings in format: api_name,player_id")
    logger.info("Example: Shohei Ohtani,660271")


# ============================================================================
# CLI
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="MLB Data Linker - Local Processing with Checkpoint/Resume")
    parser.add_argument(
        "command",
        choices=["download", "process", "upload", "all", "status", "init", "reset"],
        help="download=pull tables; process=match locally; upload=push results; "
             "all=full pipeline; status=show progress; init=create mappings template; reset=clear checkpoint",
    )
    parser.add_argument("--force", action="store_true", help="Ignore checkpoint, re-run stage")
    parser.add_argument(
        "--batch-delay", type=float, default=0,
        help="Seconds to sleep between upload chunks (default 0)",
    )

    args = parser.parse_args()
    start = datetime.now()

    if args.command == "download":
        download_tables(force=args.force)
    elif args.command == "process":
        process_local(force=args.force)
    elif args.command == "upload":
        upload_results(force=args.force, batch_delay=args.batch_delay)
    elif args.command == "all":
        download_tables(force=args.force)
        process_local(force=args.force)
        upload_results(force=args.force, batch_delay=args.batch_delay)
    elif args.command == "status":
        show_status()
    elif args.command == "init":
        init_mappings()
    elif args.command == "reset":
        reset_checkpoint()

    elapsed = datetime.now() - start
    logger.info(f"Total elapsed: {elapsed}")


if __name__ == "__main__":
    main()
