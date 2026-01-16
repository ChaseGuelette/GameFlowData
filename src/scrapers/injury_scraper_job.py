"""
Injury Scraper Scheduler
Runs scraping job with monitoring, error handling, and alerting
"""

import json
import logging
import os
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from espn_injury_scraper import ESPNInjuryScraper, InjuryChangeDetector
from injury_database import InjuryDatabase

load_dotenv()


class InjuryScraperJob:
    """
    Scheduled job runner for ESPN injury scraping

    Features:
    - Automated scraping
    - Error handling and recovery
    - Change detection and alerts
    - Performance monitoring
    - Logging
    """

    def __init__(
        self,
        log_dir: str = "./logs",
        alert_email: str | None = None,
    ):
        """
        Initialize job runner

        Args:
            log_dir: Directory for log files
            alert_email: Email address for alerts (optional)
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self.alert_email = alert_email or os.environ.get("ALERT_EMAIL")

        # Setup logging
        self.setup_logging()

        # Initialize components
        self.scraper = ESPNInjuryScraper(min_request_interval=2.0)
        self.db = InjuryDatabase()
        self.change_detector = InjuryChangeDetector()

        self.logger = logging.getLogger(__name__)

    def setup_logging(self):
        """Setup file and console logging"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"injury_scraper_{timestamp}.log"

        # Create formatters and handlers
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # Root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

    def run(self) -> dict:
        """
        Execute scraping job

        Returns:
            Dictionary with job results and statistics
        """
        self.logger.info("=" * 80)
        self.logger.info("ESPN INJURY SCRAPER JOB STARTED")
        self.logger.info("=" * 80)

        start_time = datetime.now(UTC)
        results = {
            "success": False,
            "start_time": start_time.isoformat(),
            "errors": [],
            "warnings": [],
            "stats": {},
        }

        try:
            # Step 1: Get previous injuries for change detection
            self.logger.info("Step 1: Retrieving previous injuries")
            previous_injuries_data = self.db.get_latest_injuries()

            # Step 2: Scrape current injuries
            self.logger.info("Step 2: Scraping ESPN for current injuries")
            current_injuries = self.scraper.scrape()

            if not current_injuries:
                error_msg = "No injuries retrieved from ESPN"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                self.send_alert("Scraping Failed", error_msg)
                return results

            # Step 3: Store in database
            self.logger.info("Step 3: Storing injuries in database")
            inserted, updated = self.db.store_injuries(current_injuries)

            # Step 4: Detect changes (if we have previous data)
            changes = None
            if previous_injuries_data:
                self.logger.info("Step 4: Detecting changes")
                # Convert previous data back to InjuryRecord objects (simplified)
                from espn_injury_scraper import InjuryRecord

                previous_injuries = [
                    InjuryRecord(**{k: v for k, v in record.items() if k != "id"})
                    for record in previous_injuries_data
                ]
                changes = self.change_detector.detect_changes(previous_injuries, current_injuries)

                # Log significant changes
                if changes["new"]:
                    self.logger.info(f"New injuries: {len(changes['new'])}")
                    for injury in changes["new"][:5]:  # Log first 5
                        self.logger.info(
                            f"  - {injury.player_name} ({injury.team_name}): {injury.injury_type}"
                        )

                if changes["resolved"]:
                    self.logger.info(f"Resolved injuries: {len(changes['resolved'])}")

                if changes["updated"]:
                    self.logger.info(f"Updated injuries: {len(changes['updated'])}")

            # Step 5: Gather statistics
            self.logger.info("Step 5: Gathering statistics")
            stats = {
                "total_injuries": len(current_injuries),
                "inserted": inserted,
                "updated": updated,
                "new_injuries": len(changes["new"]) if changes else 0,
                "resolved_injuries": len(changes["resolved"]) if changes else 0,
                "status_updated": len(changes["updated"]) if changes else 0,
            }

            # Status distribution
            from collections import Counter

            status_dist = Counter(injury.status for injury in current_injuries)
            stats["status_distribution"] = dict(status_dist)

            # Team with most injuries
            team_counts = Counter(injury.team_name for injury in current_injuries)
            if team_counts:
                most_injured = team_counts.most_common(1)[0]
                stats["most_injured_team"] = {"name": most_injured[0], "count": most_injured[1]}

            results["stats"] = stats
            results["success"] = True

            # Step 6: Send success notification if significant changes
            if changes and (changes["new"] or changes["resolved"]):
                self.send_change_notification(changes, stats)

        except Exception as e:
            error_msg = f"Job failed with error: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            results["errors"].append(error_msg)
            self.send_alert("Job Failed", f"{error_msg}\n\n{traceback.format_exc()}")

        finally:
            end_time = datetime.now(UTC)
            duration = (end_time - start_time).total_seconds()
            results["end_time"] = end_time.isoformat()
            results["duration_seconds"] = duration

            self.logger.info("-" * 80)
            self.logger.info(f"JOB COMPLETED IN {duration:.2f}s")
            self.logger.info(f"Success: {results['success']}")
            if results["stats"]:
                self.logger.info(f"Total injuries: {results['stats']['total_injuries']}")
                self.logger.info(f"New records: {results['stats']['inserted']}")
                self.logger.info(f"Updated records: {results['stats']['updated']}")
            self.logger.info("=" * 80)

            # Save results to file
            self.save_results(results)

        return results

    def save_results(self, results: dict):
        """Save job results to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_file = self.log_dir / f"results_{timestamp}.json"

            with open(results_file, "w") as f:
                json.dump(results, f, indent=2)

            self.logger.debug(f"Results saved to {results_file}")
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")

    def send_alert(self, subject: str, message: str):
        """
        Send email alert (if configured)

        Args:
            subject: Email subject
            message: Email body
        """
        if not self.alert_email:
            return

        try:
            # This is a placeholder - implement with your email service
            # Options: SendGrid, AWS SES, SMTP, etc.
            self.logger.info(f"ALERT: {subject}")
            self.logger.debug(f"Alert message: {message}")

            # TODO: Implement actual email sending
            # Example with SMTP:
            # smtp_server = os.environ.get('SMTP_SERVER')
            # smtp_user = os.environ.get('SMTP_USER')
            # smtp_pass = os.environ.get('SMTP_PASS')
            # ... send email ...

        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")

    def send_change_notification(self, changes: dict, stats: dict):
        """
        Send notification about significant injury changes

        Args:
            changes: Dictionary with injury changes
            stats: Job statistics
        """
        if not self.alert_email:
            return

        # Build notification message
        message_parts = [
            "NBA Injury Update",
            "=" * 50,
            f"Total Injuries: {stats['total_injuries']}",
            "",
        ]

        if changes["new"]:
            message_parts.append(f"New Injuries ({len(changes['new'])}):")
            for injury in changes["new"][:10]:  # Top 10
                message_parts.append(
                    f"  - {injury.player_name} ({injury.team_name}): "
                    f"{injury.injury_type or 'Unknown'}"
                )
            if len(changes["new"]) > 10:
                message_parts.append(f"  ... and {len(changes['new']) - 10} more")
            message_parts.append("")

        if changes["resolved"]:
            message_parts.append(f"Resolved Injuries ({len(changes['resolved'])}):")
            for injury in changes["resolved"][:10]:
                message_parts.append(f"  - {injury.player_name} ({injury.team_name})")
            if len(changes["resolved"]) > 10:
                message_parts.append(f"  ... and {len(changes['resolved']) - 10} more")

        message = "\n".join(message_parts)
        self.send_alert("NBA Injury Changes Detected", message)


def run_scheduled_job():
    """Entry point for scheduled job execution"""
    job = InjuryScraperJob(
        log_dir=os.environ.get("LOG_DIR", "./logs"), alert_email=os.environ.get("ALERT_EMAIL")
    )

    results = job.run()

    # Exit with appropriate code
    sys.exit(0 if results["success"] else 1)


if __name__ == "__main__":
    run_scheduled_job()
