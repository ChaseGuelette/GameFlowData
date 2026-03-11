"""
MLB Data Linker
================
Links mlb_raw_player_props rows by populating game_id, player_id, team_id.

Pattern: src/processing/nba_linker_local.py (NBA equivalent) with MLB-specific
adaptations: INTEGER game_id (not zero-padded string), ±1 day date window,
team_id from boxscore cross-reference.

Three modes:
  - status:      Check linking progress without running anything
  - incremental: Daily use, processes WHERE player_id IS NULL in batches
  - backfill:    One-time, processes all unlinked rows with progress logging

Usage:
    python -m src.processing.mlb.mlb_linker status
    python -m src.processing.mlb.mlb_linker incremental
    python -m src.processing.mlb.mlb_linker backfill
    python -m src.processing.mlb.mlb_linker backfill --delay 2
    python -m src.processing.mlb.mlb_linker incremental --batch-size 100000
"""

import argparse
import logging
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine
from src.processing.mlb.mlb_config import (
    FUZZY_THRESHOLD,
    LINKER_BATCH_SIZE,
    MLB_TEAM_ALIASES,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================================
# NORMALIZATION
# ============================================================================


def normalize_player(name: str) -> str | None:
    """Normalize player name for matching.

    Strips accents, removes suffixes (Jr, III, II), punctuation.
    Identical to NBA linker logic.
    """
    if pd.isna(name):
        return None
    name = str(name)
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    name = name.lower().strip()
    for old, new in [(".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", ""), (" iv", "")]:
        name = name.replace(old, new)
    return " ".join(name.split()) or None


def normalize_team(name: str) -> str | None:
    """Normalize team name using MLB_TEAM_ALIASES dict."""
    if pd.isna(name):
        return None
    name = str(name).strip()
    return MLB_TEAM_ALIASES.get(name, name)


# ============================================================================
# FUZZY DATE MATCHING
# ============================================================================


def find_closest_game_date(
    candidates: list[tuple[int, str]], target_date_str: str, max_days: int = 1
) -> tuple[int | None, int | None]:
    """Find the game with closest date within ±max_days window.

    Args:
        candidates: List of (game_id, "YYYY-MM-DD") tuples for a given matchup.
        target_date_str: Target date from commence_time.
        max_days: Maximum days difference to accept (default ±1 for MLB).

    Returns:
        (game_id, date_diff) or (None, None) if no match.
    """
    try:
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None, None

    best_game_id = None
    best_diff = float("inf")

    for game_id, game_date_str in candidates:
        try:
            game_date = datetime.strptime(str(game_date_str)[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue
        diff_days = abs((target_date - game_date).days)
        if diff_days <= max_days and diff_days < best_diff:
            best_diff = diff_days
            best_game_id = game_id

    if best_game_id is not None:
        return best_game_id, int(best_diff)
    return None, None


# ============================================================================
# LOOKUP BUILDERS
# ============================================================================


def build_game_lookup(engine) -> dict[tuple[str, str], list[tuple[int, str]]]:
    """Build game lookup from mlb_game_schedule + mlb_teams.

    Key: (home_abbrev, away_abbrev) → [(game_id, game_date_str), ...]
    """
    query = """
        SELECT
            s.game_id,
            s.game_date::text AS game_date,
            ht.team_abbreviation AS home_abbrev,
            at.team_abbreviation AS away_abbrev
        FROM mlb_game_schedule s
        JOIN mlb_teams ht ON s.home_team_id = ht.team_id
        JOIN mlb_teams at ON s.away_team_id = at.team_id
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    lookup = defaultdict(list)
    for _, row in df.iterrows():
        key = (row["home_abbrev"], row["away_abbrev"])
        lookup[key].append((int(row["game_id"]), row["game_date"][:10]))

    logger.info(f"Built game lookup: {len(lookup)} unique matchups, {len(df)} total games")
    return dict(lookup)


def build_player_lookup(engine) -> dict[str, int]:
    """Build player lookup from mlb_players.

    Key: normalized_name → player_id
    """
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT player_id, player_name FROM mlb_players"), conn)

    lookup = {}
    for _, row in df.iterrows():
        norm = normalize_player(row["player_name"])
        if norm:
            lookup[norm] = int(row["player_id"])

    logger.info(f"Built player lookup: {len(lookup)} players")
    return lookup


def build_team_id_lookup(engine) -> dict[tuple[int, int], int]:
    """Build team_id lookup from actual boxscore data.

    Key: (player_id, game_id) → team_id
    Uses UNION of batting + pitching tables for full coverage.
    """
    query = """
        SELECT player_id, game_id, team_id
        FROM mlb_player_game_stats_batting
        WHERE team_id IS NOT NULL
        UNION
        SELECT player_id, game_id, team_id
        FROM mlb_player_game_stats_pitching
        WHERE team_id IS NOT NULL
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    lookup = {}
    for _, row in df.iterrows():
        lookup[(int(row["player_id"]), int(row["game_id"]))] = int(row["team_id"])

    logger.info(f"Built team_id lookup: {len(lookup)} (player, game) pairs")
    return lookup


# ============================================================================
# BATCH MATCHING LOGIC
# ============================================================================


def _resolve_fuzzy_names(
    unmatched_names: set[str], player_lookup: dict[str, int]
) -> dict[str, int | None]:
    """Resolve unique unmatched names via fuzzy matching (run once, cache result).

    Only iterates the player list once per unique unmatched name, not per row.
    """
    result = {}
    lookup_items = list(player_lookup.items())  # avoid repeated .items()

    for norm in unmatched_names:
        best_match = None
        best_score = FUZZY_THRESHOLD
        norm_parts = norm.split()
        norm_last = norm_parts[-1] if norm_parts else ""

        for pname, pid in lookup_items:
            score = SequenceMatcher(None, norm, pname).ratio()
            pname_parts = pname.split()
            if norm_last and pname_parts and norm_last == pname_parts[-1]:
                score += 0.15
            if score > best_score:
                best_score = score
                best_match = pid

        result[norm] = best_match

    return result


def match_batch(
    batch_df: pd.DataFrame,
    game_lookup: dict,
    player_lookup: dict,
    team_id_lookup: dict,
    fuzzy_cache: dict[str, int | None] | None = None,
) -> pd.DataFrame:
    """Match game_id, player_id, team_id for a batch of unlinked props rows.

    Uses vectorized .map() for exact matches. Fuzzy matching runs only on
    unique unmatched names (deduplicated), then results are cached across batches.
    """
    if fuzzy_cache is None:
        fuzzy_cache = {}

    # Normalize columns
    batch_df["game_date"] = pd.to_datetime(batch_df["commence_time"], utc=True).dt.strftime("%Y-%m-%d")
    batch_df["home_norm"] = batch_df["home_team"].apply(normalize_team)
    batch_df["away_norm"] = batch_df["away_team"].apply(normalize_team)
    batch_df["player_norm"] = batch_df["api_player_name"].apply(normalize_player)

    # --- Match game_id (vectorized where possible) ---
    # Group by unique (home, away, date) to avoid redundant lookups
    game_cache = {}

    def _match_game(row):
        key = (row["home_norm"], row["away_norm"], row["game_date"])
        if key in game_cache:
            return game_cache[key]
        candidates = game_lookup.get((key[0], key[1]), [])
        if not candidates:
            game_cache[key] = None
            return None
        game_id, _ = find_closest_game_date(candidates, key[2], max_days=1)
        game_cache[key] = game_id
        return game_id

    batch_df["matched_game_id"] = batch_df.apply(_match_game, axis=1)

    # --- Match player_id (exact via .map, fuzzy only on unique misses) ---
    batch_df["matched_player_id"] = batch_df["player_norm"].map(player_lookup)

    # Find unmatched unique names not already in fuzzy cache
    unmatched_mask = batch_df["matched_player_id"].isna() & batch_df["player_norm"].notna()
    unmatched_names = set(batch_df.loc[unmatched_mask, "player_norm"].unique())
    new_names = unmatched_names - set(fuzzy_cache.keys())

    if new_names:
        new_fuzzy = _resolve_fuzzy_names(new_names, player_lookup)
        fuzzy_cache.update(new_fuzzy)

    # Apply fuzzy results to unmatched rows
    if unmatched_names:
        fuzzy_mapped = batch_df.loc[unmatched_mask, "player_norm"].map(fuzzy_cache)
        # Only assign non-null fuzzy results to avoid dtype conflicts
        has_fuzzy = fuzzy_mapped.notna()
        if has_fuzzy.any():
            batch_df.loc[has_fuzzy[has_fuzzy].index, "matched_player_id"] = fuzzy_mapped[has_fuzzy]

    # --- Match team_id (vectorized via tuple key lookup) ---
    def _match_team(row):
        pid = row["matched_player_id"]
        gid = row["matched_game_id"]
        if pd.isna(pid) or pd.isna(gid):
            return None
        return team_id_lookup.get((int(pid), int(gid)))

    batch_df["matched_team_id"] = batch_df.apply(_match_team, axis=1)

    return batch_df


# ============================================================================
# TEMP TABLE UPDATE PATTERN
# ============================================================================


def apply_updates(engine, batch_df: pd.DataFrame) -> tuple[int, int, int]:
    """Apply matched IDs back to mlb_raw_player_props via temp tables.

    Returns:
        (game_matched, player_matched, team_matched) counts.
    """
    game_matched = 0
    player_matched = 0
    team_matched = 0

    with engine.begin() as conn:
        # Update game_id
        game_updates = batch_df[batch_df["matched_game_id"].notna()][
            ["staging_id", "matched_game_id"]
        ].copy()
        if not game_updates.empty:
            game_updates["matched_game_id"] = game_updates["matched_game_id"].astype(int)
            game_updates.to_sql("_tmp_mlb_game_updates", conn, if_exists="replace", index=False)
            conn.execute(text("CREATE INDEX idx_tmp_mlb_g ON _tmp_mlb_game_updates(staging_id)"))
            result = conn.execute(text("""
                UPDATE mlb_raw_player_props r
                SET game_id = t.matched_game_id::integer
                FROM _tmp_mlb_game_updates t
                WHERE r.staging_id = t.staging_id
                  AND r.game_id IS NULL
            """))
            game_matched = result.rowcount
            conn.execute(text("DROP TABLE _tmp_mlb_game_updates"))

        # Update player_id + team_id
        player_updates = batch_df[batch_df["matched_player_id"].notna()][
            ["staging_id", "matched_player_id", "matched_team_id"]
        ].copy()
        if not player_updates.empty:
            player_updates["matched_player_id"] = player_updates["matched_player_id"].astype(int)
            player_updates["matched_team_id"] = (
                player_updates["matched_team_id"]
                .where(player_updates["matched_team_id"].notna(), None)
            )
            player_updates.to_sql("_tmp_mlb_player_updates", conn, if_exists="replace", index=False)
            conn.execute(text("CREATE INDEX idx_tmp_mlb_p ON _tmp_mlb_player_updates(staging_id)"))
            result = conn.execute(text("""
                UPDATE mlb_raw_player_props r
                SET player_id = t.matched_player_id::integer,
                    team_id   = t.matched_team_id::integer
                FROM _tmp_mlb_player_updates t
                WHERE r.staging_id = t.staging_id
            """))
            player_matched = result.rowcount
            conn.execute(text("DROP TABLE _tmp_mlb_player_updates"))

        # Count team updates separately (subset of player updates that had team_id)
        if not player_updates.empty:
            team_matched = int(player_updates["matched_team_id"].notna().sum())

    return game_matched, player_matched, team_matched


# ============================================================================
# INCREMENTAL MODE
# ============================================================================


def link_incremental(batch_size: int = LINKER_BATCH_SIZE, limit: int | None = None):
    """Lightweight incremental linker for daily use.

    Queries only unlinked rows (WHERE player_id IS NULL), processes in batches.
    """
    engine = get_engine()

    logger.info("=" * 60)
    logger.info("MLB LINKER — INCREMENTAL MODE")
    logger.info("=" * 60)

    # 1. Build lookups
    logger.info("[1/3] Building lookups...")
    game_lookup = build_game_lookup(engine)
    player_lookup = build_player_lookup(engine)
    team_id_lookup = build_team_id_lookup(engine)

    # 2. Count unlinked
    logger.info("[2/3] Counting unlinked records...")
    with engine.connect() as conn:
        total_unlinked = conn.execute(
            text("SELECT COUNT(*) FROM mlb_raw_player_props WHERE player_id IS NULL")
        ).scalar()

    logger.info(f"Total unlinked: {total_unlinked:,}")
    if total_unlinked == 0:
        logger.info("Nothing to link — done.")
        return

    total_to_process = min(total_unlinked, limit) if limit else total_unlinked

    # 3. Process in batches
    logger.info(f"[3/3] Processing {total_to_process:,} records in batches of {batch_size:,}...")

    total_game = 0
    total_player = 0
    total_team = 0
    offset = 0
    fuzzy_cache: dict[str, int | None] = {}

    while offset < total_to_process:
        current_size = min(batch_size, total_to_process - offset)

        with engine.connect() as conn:
            batch_df = pd.read_sql(
                text("""
                    SELECT staging_id, api_player_name, home_team, away_team, commence_time
                    FROM mlb_raw_player_props
                    WHERE player_id IS NULL
                    ORDER BY staging_id
                    LIMIT :lim OFFSET :off
                """),
                conn,
                params={"lim": current_size, "off": offset},
            )

        if batch_df.empty:
            break

        batch_df = match_batch(batch_df, game_lookup, player_lookup, team_id_lookup, fuzzy_cache)
        g, p, t = apply_updates(engine, batch_df)
        total_game += g
        total_player += p
        total_team += t

        offset += current_size
        pct = (offset / total_to_process) * 100
        logger.info(
            f"Progress: {offset:,}/{total_to_process:,} ({pct:.1f}%) — "
            f"games={total_game:,} players={total_player:,} teams={total_team:,}"
        )

    # Summary
    logger.info("=" * 60)
    logger.info("INCREMENTAL LINK COMPLETE")
    logger.info(f"  Games matched:   {total_game:,}")
    logger.info(f"  Players matched: {total_player:,}")
    logger.info(f"  Teams matched:   {total_team:,}")

    with engine.connect() as conn:
        remaining = conn.execute(
            text("SELECT COUNT(*) FROM mlb_raw_player_props WHERE player_id IS NULL")
        ).scalar()
    logger.info(f"  Remaining unlinked: {remaining:,}")
    logger.info("=" * 60)


# ============================================================================
# STATUS MODE
# ============================================================================


def show_status():
    """Show linking progress without processing anything."""
    engine = get_engine()

    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM mlb_raw_player_props")).scalar()
        linked = conn.execute(
            text("SELECT COUNT(*) FROM mlb_raw_player_props WHERE player_id IS NOT NULL")
        ).scalar()

    unlinked = total - linked
    pct = (linked / total * 100) if total > 0 else 0.0

    logger.info("=" * 60)
    logger.info("MLB LINKER — STATUS")
    logger.info("=" * 60)
    logger.info(f"  Total rows:     {total:,}")
    logger.info(f"  Linked:         {linked:,} ({pct:.1f}%)")
    logger.info(f"  Unlinked:       {unlinked:,}")
    logger.info("=" * 60)


# ============================================================================
# BACKFILL MODE
# ============================================================================


def link_backfill(batch_delay: float = 0):
    """One-time backfill — processes ALL unlinked rows with progress logging."""
    engine = get_engine()

    logger.info("=" * 60)
    logger.info("MLB LINKER — BACKFILL MODE")
    logger.info("=" * 60)

    # Build lookups
    logger.info("[1/3] Building lookups...")
    game_lookup = build_game_lookup(engine)
    player_lookup = build_player_lookup(engine)
    team_id_lookup = build_team_id_lookup(engine)

    # Count total and unlinked for progress context on restart
    with engine.connect() as conn:
        total_rows = conn.execute(text("SELECT COUNT(*) FROM mlb_raw_player_props")).scalar()
        total_unlinked = conn.execute(
            text("SELECT COUNT(*) FROM mlb_raw_player_props WHERE player_id IS NULL")
        ).scalar()

    already_linked = total_rows - total_unlinked
    linked_pct = (already_linked / total_rows * 100) if total_rows > 0 else 0.0
    logger.info(
        f"Total rows: {total_rows:,} | Already linked: {already_linked:,} ({linked_pct:.1f}%) "
        f"| Remaining: {total_unlinked:,}"
    )
    if total_unlinked == 0:
        logger.info("Nothing to backfill — done.")
        return

    if batch_delay > 0:
        logger.info(f"Batch delay: {batch_delay}s between batches (I/O throttling)")

    # Process all in batches (no limit)
    logger.info(f"[2/3] Processing all {total_unlinked:,} unlinked rows...")

    batch_size = LINKER_BATCH_SIZE
    total_game = 0
    total_player = 0
    total_team = 0
    processed = 0
    fuzzy_cache: dict[str, int | None] = {}

    consecutive_errors = 0
    max_retries = 20  # High count to survive laptop sleep/wake cycles

    while True:
        try:
            # Always fetch from offset 0 since previous batch got linked
            with engine.connect() as conn:
                batch_df = pd.read_sql(
                    text("""
                        SELECT staging_id, api_player_name, home_team, away_team, commence_time
                        FROM mlb_raw_player_props
                        WHERE player_id IS NULL
                        ORDER BY staging_id
                        LIMIT :lim
                    """),
                    conn,
                    params={"lim": batch_size},
                )

            if batch_df.empty:
                break

            batch_df = match_batch(batch_df, game_lookup, player_lookup, team_id_lookup, fuzzy_cache)
            g, p, t = apply_updates(engine, batch_df)
            total_game += g
            total_player += p
            total_team += t
            processed += len(batch_df)
            consecutive_errors = 0  # Reset on success

            # If no new player matches, remaining rows are unmatchable
            if p == 0:
                logger.info("No new player matches in last batch — remaining rows are unmatchable.")
                break

            logger.info(
                f"Backfill progress: {processed:,}/{total_unlinked:,} — "
                f"games={total_game:,} players={total_player:,} teams={total_team:,}"
            )

            if batch_delay > 0:
                time.sleep(batch_delay)

        except Exception as e:
            consecutive_errors += 1
            if consecutive_errors > max_retries:
                logger.error(f"Failed {max_retries} consecutive times, giving up. Last error: {e}")
                break
            wait = min(60, 10 * consecutive_errors)
            logger.warning(f"Connection error (attempt {consecutive_errors}/{max_retries}): {e}")
            logger.info(f"Recycling connection pool and retrying in {wait}s...")
            engine.dispose()
            time.sleep(wait)

    # Summary
    try:
        logger.info("[3/3] Backfill summary")
        logger.info("=" * 60)
        logger.info(f"  Rows processed:  {processed:,}")
        logger.info(f"  Games matched:   {total_game:,}")
        logger.info(f"  Players matched: {total_player:,}")
        logger.info(f"  Teams matched:   {total_team:,}")

        with engine.connect() as conn:
            remaining = conn.execute(
                text("SELECT COUNT(*) FROM mlb_raw_player_props WHERE player_id IS NULL")
            ).scalar()
        logger.info(f"  Remaining unlinked: {remaining:,}")
    except Exception:
        logger.info("  (Could not query remaining count — connection unavailable)")
    logger.info("=" * 60)


# ============================================================================
# CLI
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="MLB Data Linker")
    parser.add_argument(
        "mode",
        choices=["status", "incremental", "backfill"],
        help="status = check progress; incremental = daily batched linking; backfill = process all unlinked rows",
    )
    parser.add_argument("--batch-size", type=int, default=LINKER_BATCH_SIZE, help="Batch size (default 50000)")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process (incremental only)")
    parser.add_argument(
        "--delay", type=float, default=0, help="Seconds to sleep between batches for I/O throttling (default 0)"
    )

    args = parser.parse_args()

    start = datetime.now()

    if args.mode == "status":
        show_status()
    elif args.mode == "incremental":
        link_incremental(batch_size=args.batch_size, limit=args.limit)
    else:
        link_backfill(batch_delay=args.delay)

    elapsed = datetime.now() - start
    logger.info(f"Total elapsed: {elapsed}")


if __name__ == "__main__":
    main()
