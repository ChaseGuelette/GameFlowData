"""
RapidAPI Injury Data Linker
===========================
Links player names and team names in `rapidapi_injuries` to NBA integer IDs
(player_id, nba_team_id) using normalization, manual overrides, and fuzzy matching.

Usage:
    python src/processing/link_injury_data.py                  # Full run
    python src/processing/link_injury_data.py --dry-run        # Preview without DB writes
    python src/processing/link_injury_data.py --report-only    # Just output unmatched report
    python src/processing/link_injury_data.py --threshold 0.85 # Custom fuzzy threshold

Environment:
    DATABASE_URL - PostgreSQL connection string (required)
"""

import argparse
import csv
import logging
import sys
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
from sqlalchemy import text

from src.db.client import get_engine
from src.processing.nba_linker_local import normalize_team

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("InjuryLinker")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANUAL_MAPPINGS_PATH = PROJECT_ROOT / "data" / "linker_data" / "player_mappings.csv"
UNMATCHED_REPORT_PATH = PROJECT_ROOT / "data" / "linker_data" / "unmatched_injury_players.csv"

FUZZY_THRESHOLD = 0.80
LAST_NAME_BONUS = 0.15
BATCH_SIZE = 5000


# --- DDL Migration ---

DDL_ADD_COLUMNS = """
ALTER TABLE public.rapidapi_injuries ADD COLUMN IF NOT EXISTS player_id bigint;
ALTER TABLE public.rapidapi_injuries ADD COLUMN IF NOT EXISTS nba_team_id bigint;
CREATE INDEX IF NOT EXISTS idx_rapidapi_inj_player_id ON public.rapidapi_injuries(player_id);
CREATE INDEX IF NOT EXISTS idx_rapidapi_inj_team_id ON public.rapidapi_injuries(nba_team_id);
"""


def ensure_columns(engine) -> None:
    """Add player_id and nba_team_id columns to rapidapi_injuries if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text(DDL_ADD_COLUMNS))
    logger.info("Ensured player_id and nba_team_id columns exist.")


# --- Normalization ---

def normalize_player(name: str) -> str | None:
    """
    Strip accents, lowercase, remove punctuation and suffixes.
    Reimplemented from nba_linker_local.process_local() (line 343-352)
    since it's defined inside that function and cannot be imported.
    """
    if not name or not isinstance(name, str):
        return None
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    name = name.lower().strip()
    for old, new in [(".", ""), ("'", ""), ("-", " "), (" jr", ""), (" iii", ""), (" ii", "")]:
        name = name.replace(old, new)
    return " ".join(name.split())


# --- Reference Data Loading ---

def load_manual_mappings(path: Path) -> dict[str, int]:
    """
    Load api_name -> player_id mappings from the manual overrides CSV.

    Returns:
        Dict mapping exact API name strings to player_id integers.
    """
    if not path.exists():
        logger.warning(f"Manual mappings file not found: {path}. Continuing without overrides.")
        return {}

    mappings = {}
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            api_name = row.get("api_name", "").strip()
            pid_str = row.get("player_id", "").strip()
            if api_name and pid_str:
                try:
                    mappings[api_name] = int(pid_str)
                except ValueError:
                    logger.warning(f"Invalid player_id in mappings: {pid_str} for '{api_name}'")

    logger.info(f"Loaded {len(mappings)} manual overrides from {path.name}")
    return mappings


def load_reference_data(engine) -> tuple[dict[str, int], dict[str, int], dict[int, str]]:
    """
    Load teams and players from the database.

    Returns:
        team_lookup:   normalized_team_name -> team_id
        player_lookup: normalized_player_name -> player_id
        player_names:  player_id -> original player_name (for reporting)
    """
    # Teams
    team_lookup: dict[str, int] = {}
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT team_id, team_name FROM teams")).fetchall()
    for team_id, team_name in rows:
        if team_name:
            # Store both the raw name and the normalized name as keys
            team_lookup[team_name.strip()] = int(team_id)
            normed = normalize_team(team_name.strip())
            if normed:
                team_lookup[normed] = int(team_id)

    # Players
    player_lookup: dict[str, int] = {}
    player_names: dict[int, str] = {}
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT player_id, player_name FROM players")).fetchall()
    for player_id, player_name in rows:
        pid = int(player_id)
        if player_name:
            player_names[pid] = player_name
            normed = normalize_player(player_name)
            if normed:
                player_lookup[normed] = pid

    return team_lookup, player_lookup, player_names


# --- Fuzzy Matching ---

def fuzzy_match_player(
    name: str,
    player_lookup: dict[str, int],
    player_names: dict[int, str],
    threshold: float = FUZZY_THRESHOLD,
    top_n: int = 3,
) -> tuple[int | None, list[tuple[int, str, float]]]:
    """
    Fuzzy match a player name against the lookup dict.
    Uses SequenceMatcher + last-name bonus (same algorithm as nba_linker_local).

    Returns:
        (best_player_id or None, top_n suggestions as [(pid, display_name, score), ...])
    """
    norm = normalize_player(name)
    if not norm:
        return None, []

    scores: list[tuple[int, str, float]] = []
    for db_norm, pid in player_lookup.items():
        if not db_norm or not isinstance(db_norm, str):
            continue
        score = SequenceMatcher(None, norm, db_norm).ratio()
        # Last-name bonus
        norm_parts = norm.split()
        db_parts = db_norm.split()
        if norm_parts and db_parts and norm_parts[-1] == db_parts[-1]:
            score += LAST_NAME_BONUS
        scores.append((pid, player_names.get(pid, db_norm), score))

    scores.sort(key=lambda x: x[2], reverse=True)
    top = scores[:top_n]

    if top and top[0][2] >= threshold:
        return top[0][0], top
    return None, top


# --- Core Matching Logic ---

def match_team(team_text: str | None, team_lookup: dict[str, int]) -> int | None:
    """Match a team name string to an NBA team_id."""
    if not team_text:
        return None
    # Try exact match first
    tid = team_lookup.get(team_text.strip())
    if tid is not None:
        return tid
    # Try after alias normalization
    normed = normalize_team(team_text.strip())
    if normed:
        tid = team_lookup.get(normed)
    return tid


def match_player(
    player_text: str,
    player_lookup: dict[str, int],
    player_names: dict[int, str],
    manual_mappings: dict[str, int],
    threshold: float = FUZZY_THRESHOLD,
) -> tuple[int | None, str, list[tuple[int, str, float]]]:
    """
    Match a player name string to an NBA player_id using the priority cascade:
      1. Manual override (exact api_name)
      2. Exact normalized match
      3. Fuzzy match above threshold

    Returns:
        (player_id or None, match_method, top_suggestions)
    """
    # Priority 1: Manual override
    pid = manual_mappings.get(player_text)
    if pid is not None:
        return pid, "manual", []

    # Also try stripped version
    pid = manual_mappings.get(player_text.strip())
    if pid is not None:
        return pid, "manual", []

    # Priority 2: Exact normalized match
    normed = normalize_player(player_text)
    if normed:
        pid = player_lookup.get(normed)
        if pid is not None:
            return pid, "exact", []

    # Priority 3: Fuzzy match
    pid, suggestions = fuzzy_match_player(
        player_text, player_lookup, player_names, threshold=threshold
    )
    if pid is not None:
        return pid, "fuzzy", suggestions

    return None, "unmatched", suggestions


def build_injury_mappings(
    engine,
    team_lookup: dict[str, int],
    player_lookup: dict[str, int],
    player_names: dict[int, str],
    manual_mappings: dict[str, int],
    threshold: float = FUZZY_THRESHOLD,
) -> tuple[dict[tuple[str, str | None], tuple[int | None, int | None]], list[dict]]:
    """
    Fetch distinct (player, team) pairs from rapidapi_injuries WHERE player_id IS NULL,
    and match each to (player_id, nba_team_id).

    Returns:
        mapping:   {(player_text, team_text): (player_id, nba_team_id)}
        unmatched: list of dicts for the unmatched report
    """
    # Fetch distinct pairs with row counts
    query = text("""
        SELECT player, team, COUNT(*) as row_count
        FROM rapidapi_injuries
        WHERE player_id IS NULL
        GROUP BY player, team
        ORDER BY COUNT(*) DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(query).fetchall()

    logger.info(f"Found {len(rows)} distinct (player, team) pairs to match.")

    mapping: dict[tuple[str, str | None], tuple[int | None, int | None]] = {}
    unmatched: list[dict] = []

    match_stats = {"manual": 0, "exact": 0, "fuzzy": 0, "unmatched": 0, "team_matched": 0, "team_null": 0}

    for player_text, team_text, row_count in rows:
        # Match team
        nba_team_id = match_team(team_text, team_lookup)
        if nba_team_id is not None:
            match_stats["team_matched"] += 1
        elif team_text is None:
            match_stats["team_null"] += 1

        # Match player
        player_id, method, suggestions = match_player(
            player_text, player_lookup, player_names, manual_mappings, threshold=threshold
        )
        match_stats[method] += 1

        mapping[(player_text, team_text)] = (player_id, nba_team_id)

        if player_id is None:
            entry = {
                "api_name": player_text,
                "team": team_text or "",
                "row_count": row_count,
                "suggestion_1": suggestions[0][1] if len(suggestions) > 0 else "",
                "score_1": f"{suggestions[0][2]:.3f}" if len(suggestions) > 0 else "",
                "suggestion_2": suggestions[1][1] if len(suggestions) > 1 else "",
                "score_2": f"{suggestions[1][2]:.3f}" if len(suggestions) > 1 else "",
                "suggestion_3": suggestions[2][1] if len(suggestions) > 2 else "",
                "score_3": f"{suggestions[2][2]:.3f}" if len(suggestions) > 2 else "",
            }
            unmatched.append(entry)

    logger.info("Match results:")
    logger.info(f"  Players — manual: {match_stats['manual']}, exact: {match_stats['exact']}, "
                f"fuzzy: {match_stats['fuzzy']}, unmatched: {match_stats['unmatched']}")
    logger.info(f"  Teams — matched: {match_stats['team_matched']}, null input: {match_stats['team_null']}")

    return mapping, unmatched


# --- Bulk Update ---

def bulk_update_injuries(
    engine,
    mapping: dict[tuple[str, str | None], tuple[int | None, int | None]],
    dry_run: bool = False,
) -> int:
    """
    Apply the mapping to rapidapi_injuries in batched UPDATEs.
    Only updates rows WHERE player_id IS NULL.

    Returns:
        Total rows updated.
    """
    # Filter to pairs where we have at least one ID to set
    items = [
        (player_text, team_text, pid, tid)
        for (player_text, team_text), (pid, tid) in mapping.items()
        if pid is not None or tid is not None
    ]

    if not items:
        logger.info("No matches to write.")
        return 0

    if dry_run:
        logger.info(f"DRY RUN: Would update {len(items)} distinct (player, team) groups.")
        return 0

    total_updated = 0

    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i : i + BATCH_SIZE]
        batch_updated = 0

        with engine.begin() as conn:
            for player_text, team_text, player_id, nba_team_id in batch:
                set_parts: list[str] = []
                params: dict = {"player_text": player_text}

                if player_id is not None:
                    set_parts.append("player_id = :player_id")
                    params["player_id"] = player_id
                if nba_team_id is not None:
                    set_parts.append("nba_team_id = :nba_team_id")
                    params["nba_team_id"] = nba_team_id

                if not set_parts:
                    continue

                if team_text is None:
                    where_team = "team IS NULL"
                else:
                    where_team = "team = :team_text_where"
                    params["team_text_where"] = team_text

                sql = f"""
                    UPDATE rapidapi_injuries
                    SET {', '.join(set_parts)}
                    WHERE player = :player_text
                      AND {where_team}
                      AND player_id IS NULL
                """
                result = conn.execute(text(sql), params)
                batch_updated += result.rowcount

        total_updated += batch_updated
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"  Batch {batch_num}/{total_batches}: {batch_updated:,} rows updated")

    return total_updated


# --- Team Backfill from Game Data ---

def backfill_team_from_games(engine, dry_run: bool = False, max_days: int = 30) -> int:
    """
    For injury rows that have a player_id but no nba_team_id (because the API
    returned a null team), look up the player's team from player_game_stats
    by finding the closest game within ±max_days of the report_date.

    Returns:
        Number of rows updated.
    """
    # Find how many rows need backfilling
    with engine.connect() as conn:
        count = conn.execute(text("""
            SELECT COUNT(*)
            FROM rapidapi_injuries
            WHERE player_id IS NOT NULL AND nba_team_id IS NULL
        """)).scalar()

    if count == 0:
        logger.info("No rows need team backfill from game data.")
        return 0

    logger.info(f"Found {count:,} rows with player_id but no nba_team_id. "
                f"Searching player_game_stats (±{max_days} days)...")

    if dry_run:
        # Preview what would match
        with engine.connect() as conn:
            preview = conn.execute(text("""
                SELECT COUNT(DISTINCT ri.id)
                FROM rapidapi_injuries ri
                CROSS JOIN LATERAL (
                    SELECT pgs.team_id
                    FROM player_game_stats pgs
                    WHERE pgs.player_id = ri.player_id
                      AND pgs.game_date BETWEEN ri.report_date - INTERVAL ':max_days days'
                                             AND ri.report_date + INTERVAL ':max_days days'
                    ORDER BY ABS(pgs.game_date - ri.report_date)
                    LIMIT 1
                ) nearest
                WHERE ri.player_id IS NOT NULL AND ri.nba_team_id IS NULL
            """.replace(":max_days", str(max_days)))).scalar()
        logger.info(f"DRY RUN: Would backfill nba_team_id for {preview:,} rows.")
        return 0

    # Perform the update using a lateral join to find the nearest game
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE rapidapi_injuries ri
            SET nba_team_id = nearest.team_id
            FROM (
                SELECT DISTINCT ON (ri2.id) ri2.id, pgs.team_id
                FROM rapidapi_injuries ri2
                JOIN player_game_stats pgs
                  ON pgs.player_id = ri2.player_id
                 AND pgs.game_date BETWEEN ri2.report_date - INTERVAL ':max_days days'
                                        AND ri2.report_date + INTERVAL ':max_days days'
                WHERE ri2.player_id IS NOT NULL
                  AND ri2.nba_team_id IS NULL
                ORDER BY ri2.id, ABS(pgs.game_date - ri2.report_date)
            ) nearest
            WHERE ri.id = nearest.id
        """.replace(":max_days", str(max_days))))
        updated = result.rowcount

    logger.info(f"Backfilled nba_team_id for {updated:,} rows from player_game_stats.")
    return updated


# --- Unmatched Report ---

def write_unmatched_report(unmatched: list[dict], path: Path) -> None:
    """Write unmatched players with fuzzy suggestions to CSV for manual review."""
    if not unmatched:
        logger.info("All players matched. No unmatched report needed.")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "api_name", "team", "row_count",
        "suggestion_1", "score_1",
        "suggestion_2", "score_2",
        "suggestion_3", "score_3",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unmatched)

    logger.info(f"Wrote {len(unmatched)} unmatched players to {path}")
    logger.info(f"To fix: add confirmed mappings to {MANUAL_MAPPINGS_PATH}")


# --- CLI ---

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Link RapidAPI injury player/team names to NBA integer IDs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Full link run:
    python src/processing/link_injury_data.py

  Preview without writing to DB:
    python src/processing/link_injury_data.py --dry-run

  Only generate unmatched report:
    python src/processing/link_injury_data.py --report-only

  Custom fuzzy threshold:
    python src/processing/link_injury_data.py --threshold 0.85
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show matching results without writing to the database",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Only generate the unmatched player report (no DB updates)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=FUZZY_THRESHOLD,
        help=f"Fuzzy match score threshold (default: {FUZZY_THRESHOLD})",
    )
    parser.add_argument(
        "--mappings",
        type=str,
        default=str(MANUAL_MAPPINGS_PATH),
        help=f"Path to manual player_mappings.csv (default: {MANUAL_MAPPINGS_PATH})",
    )
    return parser.parse_args()


# --- Main ---

def main() -> None:
    args = parse_args()
    engine = get_engine()

    # 1. DDL migration
    logger.info("=" * 70)
    logger.info("RapidAPI Injury Data Linker")
    logger.info("=" * 70)
    ensure_columns(engine)

    # 2. Load reference data
    logger.info("Loading reference data...")
    team_lookup, player_lookup, player_names = load_reference_data(engine)
    logger.info(f"  {len(team_lookup)} team name variants, {len(player_lookup)} players")

    manual_mappings = load_manual_mappings(Path(args.mappings))

    # 3. Build mappings
    logger.info("Matching injury records to NBA IDs...")
    mapping, unmatched = build_injury_mappings(
        engine, team_lookup, player_lookup, player_names, manual_mappings,
        threshold=args.threshold,
    )

    matched_players = sum(1 for (pid, _) in mapping.values() if pid is not None)
    matched_teams = sum(1 for (_, tid) in mapping.values() if tid is not None)
    total_pairs = len(mapping)
    logger.info(f"Summary: {matched_players}/{total_pairs} players matched, "
                f"{matched_teams}/{total_pairs} teams matched, "
                f"{len(unmatched)} unmatched players")

    # 4. Write unmatched report
    write_unmatched_report(unmatched, UNMATCHED_REPORT_PATH)

    if args.report_only:
        logger.info("Report-only mode. Skipping DB updates.")
        return

    # 5. Bulk update
    logger.info("Writing matched IDs to database...")
    total = bulk_update_injuries(engine, mapping, dry_run=args.dry_run)
    logger.info(f"Updated {total:,} rows in rapidapi_injuries.")

    # 6. Backfill missing team IDs from player_game_stats
    logger.info("Backfilling missing team IDs from game data...")
    backfill_team_from_games(engine, dry_run=args.dry_run)

    # 7. Verification
    if not args.dry_run:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COUNT(*) as total,
                    COUNT(player_id) as has_player_id,
                    COUNT(nba_team_id) as has_team_id,
                    COUNT(*) - COUNT(player_id) as missing_player_id,
                    COUNT(*) - COUNT(nba_team_id) as missing_team_id
                FROM rapidapi_injuries
            """)).fetchone()
        logger.info("=" * 70)
        logger.info("Verification:")
        logger.info(f"  Total rows:         {result[0]:,}")
        logger.info(f"  Has player_id:      {result[1]:,} ({result[1] / result[0] * 100:.1f}%)")
        logger.info(f"  Has nba_team_id:    {result[2]:,} ({result[2] / result[0] * 100:.1f}%)")
        logger.info(f"  Missing player_id:  {result[3]:,}")
        logger.info(f"  Missing nba_team_id: {result[4]:,}")
        logger.info("=" * 70)


if __name__ == "__main__":
    main()
