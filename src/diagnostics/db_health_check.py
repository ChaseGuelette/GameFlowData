#!/usr/bin/env python3
"""
Database Health Check Script
=============================
Validates data integrity, freshness, and linkage across all tables.

Usage:
    python src/diagnostics/db_health_check.py [--days 7] [--verbose] [--json]

Exit codes:
    0 = All checks passed
    1 = Warnings present
    2 = Critical errors found
"""

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from sqlalchemy import text

# Add project root to path
sys.path.insert(0, str(__file__).replace("\\", "/").rsplit("/src/", 1)[0])

from src.db.client import get_engine


@dataclass
class CheckResult:
    """Result of a single health check."""
    name: str
    status: str  # "pass", "warn", "fail"
    message: str
    details: dict = field(default_factory=dict)


class DatabaseHealthChecker:
    """Comprehensive database health checker."""

    def __init__(self, engine, days_back: int = 7, verbose: bool = False):
        self.engine = engine
        self.days_back = days_back
        self.verbose = verbose
        self.today = date.today()
        self.cutoff_date = self.today - timedelta(days=days_back)
        self.results: list[CheckResult] = []

    def run_all_checks(self) -> list[CheckResult]:
        """Run all health checks and return results."""
        print("=" * 60)
        print(f"DATABASE HEALTH CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking last {self.days_back} days (since {self.cutoff_date})")
        print("=" * 60)
        print()

        self._check_data_freshness()
        self._check_game_data_completeness()
        self._check_prop_linking()
        self._check_aggregation_sync()
        self._check_injury_linking()
        self._check_position_history()
        self._check_prediction_coverage()
        self._check_prediction_game_times()
        self._check_foreign_keys()

        self._print_summary()
        return self.results

    def _check_data_freshness(self):
        """Check 1: Data freshness across key tables."""
        print("[1/9] DATA FRESHNESS")

        queries = {
            "player_game_stats": "SELECT MAX(game_date)::text FROM player_game_stats",
            "team_game_stats": "SELECT MAX(game_date)::text FROM team_game_stats",
            "daily_predictions": "SELECT MAX(prediction_date)::text FROM daily_predictions",
            "rapidapi_injuries": "SELECT MAX(report_date)::text FROM rapidapi_injuries",
        }

        details = {}
        has_warning = False

        for table, query in queries.items():
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text(query)).fetchone()
                    latest = result[0] if result and result[0] else "No data"

                if latest != "No data":
                    latest_date = datetime.strptime(latest, "%Y-%m-%d").date()
                    days_ago = (self.today - latest_date).days
                    status_icon = "✓" if days_ago <= 1 else "⚠️"
                    if days_ago > 1:
                        has_warning = True
                    print(f"  {status_icon} {table}: {latest} ({days_ago} day(s) ago)")
                else:
                    print(f"  ✗ {table}: No data")
                    has_warning = True

                details[table] = latest
            except Exception as e:
                print(f"  ✗ {table}: Error - {e}")
                details[table] = f"Error: {e}"
                has_warning = True

        self.results.append(CheckResult(
            name="data_freshness",
            status="warn" if has_warning else "pass",
            message="Data freshness check",
            details=details
        ))
        print()

    def _check_game_data_completeness(self):
        """Check 2: Game data completeness in recent days."""
        print("[2/9] GAME DATA COMPLETENESS")

        query = text("""
            SELECT
                game_date::text,
                COUNT(DISTINCT game_id) as games,
                COUNT(DISTINCT player_id) as players
            FROM player_game_stats
            WHERE game_date >= :cutoff
            GROUP BY game_date
            ORDER BY game_date DESC
        """)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"cutoff": self.cutoff_date}).fetchall()

            details = {}
            has_warning = False

            if not rows:
                print("  ⚠️ No game data in the last {self.days_back} days")
                has_warning = True
            else:
                for row in rows:
                    game_date, games, players = row
                    avg_players_per_game = players / games if games > 0 else 0
                    status = "✓" if avg_players_per_game >= 20 else "⚠️"
                    if avg_players_per_game < 20:
                        has_warning = True
                    print(f"  {status} {game_date}: {games} games, {players} players ({avg_players_per_game:.1f}/game)")
                    details[game_date] = {"games": games, "players": players}

            self.results.append(CheckResult(
                name="game_data_completeness",
                status="warn" if has_warning else "pass",
                message="Game data completeness",
                details=details
            ))
        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.results.append(CheckResult(
                name="game_data_completeness",
                status="fail",
                message=f"Error: {e}",
                details={}
            ))
        print()

    def _check_prop_linking(self):
        """Check 3: Prop linking health (using staging_id ranges to avoid timeouts)."""
        print("[3/9] PROP LINKING HEALTH")

        # Get staging_id range for recent data using a faster query
        try:
            with self.engine.connect() as conn:
                # Get max staging_id
                max_id = conn.execute(text(
                    "SELECT MAX(staging_id) FROM raw_player_props_combined"
                )).fetchone()[0]

                if not max_id:
                    print("  ⚠️ No prop data found")
                    self.results.append(CheckResult(
                        name="prop_linking",
                        status="warn",
                        message="No prop data",
                        details={}
                    ))
                    print()
                    return

                # Sample recent ~500k rows (staging_id based)
                recent_threshold = max(0, max_id - 500000)

                # Count totals and linking status
                result = conn.execute(text("""
                    SELECT
                        COUNT(*) as total,
                        COUNT(game_id) as has_game_id,
                        COUNT(player_id) as has_player_id,
                        COUNT(team_id) as has_team_id,
                        COUNT(CASE WHEN game_id IS NOT NULL AND player_id IS NOT NULL THEN 1 END) as fully_linked
                    FROM raw_player_props_combined
                    WHERE staging_id >= :threshold
                """), {"threshold": recent_threshold}).fetchone()

                total, has_game, has_player, has_team, fully_linked = result

                pct_linked = (fully_linked / total * 100) if total > 0 else 0
                pct_game = (has_game / total * 100) if total > 0 else 0
                pct_player = (has_player / total * 100) if total > 0 else 0

                has_warning = pct_linked < 90

                print("  Recent props (last ~500k rows):")
                print(f"    Total: {total:,}")
                print(f"    {'✓' if pct_game >= 90 else '⚠️'} Has game_id: {has_game:,} ({pct_game:.1f}%)")
                print(f"    {'✓' if pct_player >= 90 else '⚠️'} Has player_id: {has_player:,} ({pct_player:.1f}%)")
                print(f"    {'✓' if pct_linked >= 90 else '⚠️'} Fully linked: {fully_linked:,} ({pct_linked:.1f}%)")

                self.results.append(CheckResult(
                    name="prop_linking",
                    status="warn" if has_warning else "pass",
                    message="Prop linking health",
                    details={
                        "total_sampled": total,
                        "pct_has_game_id": round(pct_game, 1),
                        "pct_has_player_id": round(pct_player, 1),
                        "pct_fully_linked": round(pct_linked, 1)
                    }
                ))

        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.results.append(CheckResult(
                name="prop_linking",
                status="fail",
                message=f"Error: {e}",
                details={}
            ))
        print()

    def _check_aggregation_sync(self):
        """Check 4: Aggregation tables are in sync with source data."""
        print("[4/9] AGGREGATION TABLE SYNC")

        checks = [
            {
                "name": "player_average_game_stats",
                "query": text("""
                    SELECT COUNT(DISTINCT (pgs.player_id, pgs.game_id)) as missing
                    FROM player_game_stats pgs
                    LEFT JOIN player_average_game_stats pags
                        ON pgs.player_id = pags.player_id AND pgs.game_id = pags.game_id
                    WHERE pgs.game_date >= :cutoff
                      AND pags.player_id IS NULL
                      AND pgs.did_not_play = false
                """)
            },
            {
                "name": "team_allowed_by_position",
                "query": text("""
                    SELECT COUNT(DISTINCT (tgs.team_id, tgs.game_id)) as missing
                    FROM team_game_stats tgs
                    LEFT JOIN team_allowed_by_position tabp
                        ON tgs.team_id = tabp.team_id AND tgs.game_id = tabp.game_id
                    WHERE tgs.game_date >= :cutoff
                      AND tabp.team_id IS NULL
                """)
            }
        ]

        details = {}
        has_warning = False

        for check in checks:
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(check["query"], {"cutoff": self.cutoff_date}).fetchone()
                    missing = result[0] if result else 0

                status = "✓" if missing == 0 else "⚠️"
                if missing > 0:
                    has_warning = True
                print(f"  {status} {check['name']}: {missing} missing entries")
                details[check["name"]] = missing

            except Exception as e:
                print(f"  ✗ {check['name']}: Error - {e}")
                details[check["name"]] = f"Error: {e}"
                has_warning = True

        self.results.append(CheckResult(
            name="aggregation_sync",
            status="warn" if has_warning else "pass",
            message="Aggregation table sync",
            details=details
        ))
        print()

    def _check_injury_linking(self):
        """Check 5: Injury data linking health."""
        print("[5/9] INJURY DATA LINKING")

        query = text("""
            SELECT
                COUNT(*) as total,
                COUNT(player_id) as linked,
                COUNT(*) - COUNT(player_id) as unlinked
            FROM rapidapi_injuries
            WHERE report_date >= :cutoff
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"cutoff": self.cutoff_date}).fetchone()
                total, linked, unlinked = result

            pct_linked = (linked / total * 100) if total > 0 else 100
            has_warning = pct_linked < 80

            print(f"  Total injuries (last {self.days_back} days): {total:,}")
            print(f"  {'✓' if pct_linked >= 80 else '⚠️'} Linked: {linked:,} ({pct_linked:.1f}%)")
            print(f"  Unlinked: {unlinked:,}")

            self.results.append(CheckResult(
                name="injury_linking",
                status="warn" if has_warning else "pass",
                message="Injury data linking",
                details={
                    "total": total,
                    "linked": linked,
                    "pct_linked": round(pct_linked, 1)
                }
            ))

        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.results.append(CheckResult(
                name="injury_linking",
                status="fail",
                message=f"Error: {e}",
                details={}
            ))
        print()

    def _check_position_history(self):
        """Check 6: Position history coverage for active players."""
        print("[6/9] POSITION HISTORY COVERAGE")

        query = text("""
            SELECT COUNT(DISTINCT pgs.player_id) as active_players,
                   COUNT(DISTINCT pph.player_id) as has_position
            FROM player_game_stats pgs
            LEFT JOIN player_position_history pph
                ON pgs.player_id = pph.player_id
                AND pph.snapshot_date >= :cutoff
            WHERE pgs.game_date >= :cutoff
              AND pgs.did_not_play = false
        """)

        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {"cutoff": self.cutoff_date}).fetchone()
                active, has_pos = result

            missing = active - has_pos
            pct_coverage = (has_pos / active * 100) if active > 0 else 100
            has_warning = pct_coverage < 95

            print(f"  Active players (last {self.days_back} days): {active:,}")
            print(f"  {'✓' if pct_coverage >= 95 else '⚠️'} With position history: {has_pos:,} ({pct_coverage:.1f}%)")
            if missing > 0:
                print(f"  Missing position data: {missing}")

            self.results.append(CheckResult(
                name="position_history",
                status="warn" if has_warning else "pass",
                message="Position history coverage",
                details={
                    "active_players": active,
                    "has_position": has_pos,
                    "pct_coverage": round(pct_coverage, 1)
                }
            ))

        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.results.append(CheckResult(
                name="position_history",
                status="fail",
                message=f"Error: {e}",
                details={}
            ))
        print()

    def _check_prediction_coverage(self):
        """Check 7: Prediction coverage for recent games."""
        print("[7/9] PREDICTION COVERAGE")

        query = text("""
            SELECT
                pgs.game_date::text,
                COUNT(DISTINCT pgs.game_id) as games,
                COUNT(DISTINCT dp.game_id) as games_with_predictions
            FROM player_game_stats pgs
            LEFT JOIN daily_predictions dp
                ON pgs.game_id = dp.game_id
                AND pgs.game_date = dp.prediction_date
            WHERE pgs.game_date >= :cutoff
            GROUP BY pgs.game_date
            ORDER BY pgs.game_date DESC
        """)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"cutoff": self.cutoff_date}).fetchall()

            details = {}
            has_warning = False

            for row in rows:
                game_date, games, with_preds = row
                status = "✓" if with_preds > 0 else "⚠️"
                if with_preds == 0:
                    has_warning = True
                print(f"  {status} {game_date}: {with_preds}/{games} games have predictions")
                details[game_date] = {"games": games, "with_predictions": with_preds}

            self.results.append(CheckResult(
                name="prediction_coverage",
                status="warn" if has_warning else "pass",
                message="Prediction coverage",
                details=details
            ))

        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.results.append(CheckResult(
                name="prediction_coverage",
                status="fail",
                message=f"Error: {e}",
                details={}
            ))
        print()

    def _check_prediction_game_times(self):
        """Check 8: Game time enrichment for predictions."""
        print("[8/9] PREDICTION GAME TIMES")

        query = text("""
            SELECT
                prediction_date::text,
                COUNT(*) as total,
                COUNT(game_time) as with_time,
                COUNT(*) - COUNT(game_time) as missing_time
            FROM daily_predictions
            WHERE prediction_date >= :cutoff
            GROUP BY prediction_date
            ORDER BY prediction_date DESC
        """)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"cutoff": self.cutoff_date}).fetchall()

            details = {}
            has_warning = False

            if not rows:
                print("  ⚠️ No predictions in recent days")
                has_warning = True
            else:
                for row in rows:
                    pred_date, total, with_time, missing = row
                    pct = (with_time / total * 100) if total > 0 else 0
                    status = "✓" if pct >= 95 else "⚠️"
                    if pct < 95:
                        has_warning = True
                    print(f"  {status} {pred_date}: {with_time}/{total} predictions have game_time ({pct:.0f}%)")
                    details[pred_date] = {
                        "total": total,
                        "with_game_time": with_time,
                        "pct_with_time": round(pct, 1)
                    }

            self.results.append(CheckResult(
                name="prediction_game_times",
                status="warn" if has_warning else "pass",
                message="Prediction game time enrichment",
                details=details
            ))

        except Exception as e:
            print(f"  ✗ Error: {e}")
            self.results.append(CheckResult(
                name="prediction_game_times",
                status="fail",
                message=f"Error: {e}",
                details={}
            ))
        print()

    def _check_foreign_keys(self):
        """Check 9: Soft foreign key integrity."""
        print("[9/9] FOREIGN KEY INTEGRITY")

        checks = [
            {
                "name": "player_game_stats.player_id → players",
                "query": text("""
                    SELECT COUNT(DISTINCT pgs.player_id)
                    FROM player_game_stats pgs
                    LEFT JOIN players p ON pgs.player_id = p.player_id
                    WHERE pgs.game_date >= :cutoff
                      AND p.player_id IS NULL
                """)
            },
            {
                "name": "player_game_stats.team_id → teams",
                "query": text("""
                    SELECT COUNT(DISTINCT pgs.team_id)
                    FROM player_game_stats pgs
                    LEFT JOIN teams t ON pgs.team_id = t.team_id
                    WHERE pgs.game_date >= :cutoff
                      AND pgs.team_id IS NOT NULL
                      AND t.team_id IS NULL
                """)
            }
        ]

        details = {}
        has_warning = False

        for check in checks:
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(check["query"], {"cutoff": self.cutoff_date}).fetchone()
                    orphans = result[0] if result else 0

                status = "✓" if orphans == 0 else "⚠️"
                if orphans > 0:
                    has_warning = True
                print(f"  {status} {check['name']}: {orphans} orphan(s)")
                details[check["name"]] = orphans

            except Exception as e:
                print(f"  ✗ {check['name']}: Error - {e}")
                details[check["name"]] = f"Error: {e}"

        self.results.append(CheckResult(
            name="foreign_key_integrity",
            status="warn" if has_warning else "pass",
            message="Foreign key integrity",
            details=details
        ))
        print()

    def _print_summary(self):
        """Print overall summary."""
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)

        passed = sum(1 for r in self.results if r.status == "pass")
        warnings = sum(1 for r in self.results if r.status == "warn")
        failed = sum(1 for r in self.results if r.status == "fail")

        print(f"  ✓ Passed: {passed}")
        print(f"  ⚠️ Warnings: {warnings}")
        print(f"  ✗ Failed: {failed}")
        print()

        if warnings > 0 or failed > 0:
            print("Issues found:")
            for r in self.results:
                if r.status != "pass":
                    icon = "⚠️" if r.status == "warn" else "✗"
                    print(f"  {icon} {r.name}: {r.message}")

    def get_exit_code(self) -> int:
        """Return exit code based on results."""
        if any(r.status == "fail" for r in self.results):
            return 2
        if any(r.status == "warn" for r in self.results):
            return 1
        return 0

    def to_json(self) -> str:
        """Export results as JSON."""
        return json.dumps({
            "timestamp": datetime.now().isoformat(),
            "days_checked": self.days_back,
            "results": [
                {
                    "name": r.name,
                    "status": r.status,
                    "message": r.message,
                    "details": r.details
                }
                for r in self.results
            ]
        }, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Database Health Check Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to check (default: 7)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed breakdowns",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    args = parser.parse_args()

    engine = get_engine()
    checker = DatabaseHealthChecker(engine, days_back=args.days, verbose=args.verbose)
    checker.run_all_checks()

    if args.json:
        print("\n" + checker.to_json())

    sys.exit(checker.get_exit_code())


if __name__ == "__main__":
    main()
