"""
Database Integration for ESPN Injury Scraper
Handles storage, retrieval, and change tracking using SQLAlchemy
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text

from src.db.client import get_engine
from src.scrapers.espn_injury_scraper import InjuryRecord


class InjuryDatabase:
    """
    Manages injury data storage and retrieval in SQL Database

    Features:
    - Upsert with deduplication
    - Historical tracking
    - Change detection queries
    - Data validation
    """

    def __init__(self, engine=None):
        """
        Initialize database connection
        """
        self.logger = logging.getLogger(__name__)
        self.engine = engine or get_engine()
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create espn_injuries table if it doesn't exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS public.espn_injuries (
            id bigserial primary key,
            espn_injury_id text not null,
            espn_player_id text not null,
            player_name text not null,
            team_id text not null,
            team_name text null,
            status text not null,
            injury_type text null,
            injury_location text null,
            injury_side text null,
            injury_detail text null,
            return_date text null,
            date_reported text null,
            short_comment text null,
            long_comment text null,
            fantasy_status text null,
            scrape_timestamp timestamp with time zone not null,
            created_at timestamp with time zone default now()
        );
        CREATE INDEX IF NOT EXISTS idx_injuries_player ON public.espn_injuries(espn_player_id);
        CREATE INDEX IF NOT EXISTS idx_injuries_team ON public.espn_injuries(team_id);
        CREATE INDEX IF NOT EXISTS idx_injuries_scrape_ts ON public.espn_injuries(scrape_timestamp);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_injuries_unique_snapshot ON public.espn_injuries(espn_injury_id, scrape_timestamp);
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))

    def store_injuries(self, injuries: list[InjuryRecord]) -> tuple[int, int]:
        """
        Store injury records in database.
        Since we want history, we insert new rows for each scrape timestamp.
        The unique constraint is (espn_injury_id, scrape_timestamp).
        """
        if not injuries:
            self.logger.warning("No injuries to store")
            return 0, 0

        inserted = 0
        # updated = 0  # Not really updating, just inserting history

        # Convert to list of dicts
        records = [inj.to_dict() for inj in injuries]

        # Prepare insert query
        cols = [
            "espn_injury_id",
            "espn_player_id",
            "player_name",
            "team_id",
            "team_name",
            "status",
            "injury_type",
            "injury_location",
            "injury_side",
            "injury_detail",
            "return_date",
            "date_reported",
            "short_comment",
            "long_comment",
            "fantasy_status",
            "scrape_timestamp",
        ]

        col_str = ", ".join(cols)
        val_str = ", ".join([f":{c}" for c in cols])

        # We use ON CONFLICT DO NOTHING to deduplicate if run multiple times same second
        stmt = text(f"""
            INSERT INTO espn_injuries ({col_str})
            VALUES ({val_str})
            ON CONFLICT (espn_injury_id, scrape_timestamp) DO NOTHING
        """)

        with self.engine.begin() as conn:
            for record in records:
                result = conn.execute(stmt, record)
                if result.rowcount > 0:
                    inserted += 1
                else:
                    # It was a duplicate
                    pass

        self.logger.info(f"Stored injuries - Inserted: {inserted}")
        return inserted, 0

    def get_latest_injuries(self) -> list[dict]:
        """Get most recent injury snapshot."""
        try:
            # Get latest timestamp
            with self.engine.connect() as conn:
                res = conn.execute(text("SELECT MAX(scrape_timestamp) FROM espn_injuries")).scalar()

                if not res:
                    return []

                timestamp = res

                # Fetch records
                query = text("SELECT * FROM espn_injuries WHERE scrape_timestamp = :ts")
                rows = conn.execute(query, {"ts": timestamp}).mappings().all()
                return [dict(r) for r in rows]

        except Exception as e:
            self.logger.error(f"Error retrieving latest injuries: {e}")
            return []

    def get_injuries_for_date(self, date_obj: datetime) -> list[dict]:
        """Get injuries active on a specific date (latest scrape of that day)."""
        try:
            # Define day range
            start_of_day = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = date_obj.replace(hour=23, minute=59, second=59, microsecond=999999)

            with self.engine.connect() as conn:
                # Find latest scrape in that day
                ts_query = text("""
                    SELECT MAX(scrape_timestamp)
                    FROM espn_injuries
                    WHERE scrape_timestamp >= :start AND scrape_timestamp <= :end
                """)
                ts = conn.execute(ts_query, {"start": start_of_day, "end": end_of_day}).scalar()

                if not ts:
                    # Fallback: Find latest scrape BEFORE that day if none on that day?
                    # Or just return empty. Let's return empty if no scrape that day.
                    return []

                query = text("SELECT * FROM espn_injuries WHERE scrape_timestamp = :ts")
                rows = conn.execute(query, {"ts": ts}).mappings().all()
                return [dict(r) for r in rows]

        except Exception as e:
            self.logger.error(f"Error retrieving injuries for date: {e}")
            return []
