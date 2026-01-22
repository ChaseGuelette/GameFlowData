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

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.scrapers.espn_injury_scraper import ESPNInjuryScraper, InjuryChangeDetector
from src.scrapers.injury_database import InjuryDatabase


class InjuryScraperJob:
    """
    Scheduled job runner for ESPN injury scraping
    """

    def __init__(self, log_dir: str = "./logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        self.scraper = ESPNInjuryScraper(min_request_interval=2.0)
        self.db = InjuryDatabase() # Uses get_engine() internally
        self.change_detector = InjuryChangeDetector()
        
        self.logger = logging.getLogger(__name__)

    def run(self) -> dict:
        self.logger.info("=" * 80)
        self.logger.info("ESPN INJURY SCRAPER JOB STARTED")
        self.logger.info("=" * 80)

        start_time = datetime.now(UTC)
        results = {"success": False, "stats": {}}

        try:
            # 1. Get previous
            previous_injuries_data = self.db.get_latest_injuries()
            
            # 2. Scrape
            self.logger.info("Scraping ESPN...")
            current_injuries = self.scraper.scrape()
            
            if not current_injuries:
                self.logger.error("No injuries retrieved")
                return results

            # 3. Store
            self.logger.info("Storing in DB...")
            inserted, _ = self.db.store_injuries(current_injuries)
            
            # 4. Detect Changes (simplified logic for logging)
            if previous_injuries_data:
                # Basic count check for logs
                self.logger.info(f"Previous count: {len(previous_injuries_data)}, Current count: {len(current_injuries)}")

            stats = {
                "total_injuries": len(current_injuries),
                "inserted": inserted
            }
            results["stats"] = stats
            results["success"] = True

        except Exception as e:
            self.logger.error(f"Job failed: {e}")
            self.logger.error(traceback.format_exc())
        
        finally:
            duration = (datetime.now(UTC) - start_time).total_seconds()
            self.logger.info(f"JOB COMPLETED IN {duration:.2f}s")
            
        return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    job = InjuryScraperJob()
    job.run()