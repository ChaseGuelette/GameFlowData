"""
Database Integration for ESPN Injury Scraper
Handles storage, retrieval, and change tracking in Supabase
"""

import os
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone, timedelta
import logging
from supabase import create_client, Client
from espn_injury_scraper import InjuryRecord


class InjuryDatabase:
    """
    Manages injury data storage and retrieval in Supabase
    
    Features:
    - Upsert with deduplication
    - Historical tracking
    - Change detection queries
    - Data validation
    """
    
    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        """
        Initialize database connection
        
        Args:
            supabase_url: Supabase project URL (or from env var)
            supabase_key: Supabase API key (or from env var)
        """
        self.logger = logging.getLogger(__name__)
        
        # Get credentials from environment if not provided
        url = supabase_url or os.environ.get('SUPABASE_URL')
        key = supabase_key or os.environ.get('SUPABASE_KEY')
        
        if not url or not key:
            raise ValueError(
                "Supabase credentials required. Set SUPABASE_URL and SUPABASE_KEY "
                "environment variables or pass them to constructor."
            )
        
        self.client: Client = create_client(url, key)
        self.logger.info("Connected to Supabase")
    
    def store_injuries(self, injuries: List[InjuryRecord]) -> Tuple[int, int]:
        """
        Store injury records in database with deduplication
        
        Args:
            injuries: List of InjuryRecord objects
            
        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not injuries:
            self.logger.warning("No injuries to store")
            return 0, 0
        
        inserted = 0
        updated = 0
        
        for injury in injuries:
            try:
                # Check if record already exists
                existing = self.client.table('espn_injuries').select('*').eq(
                    'espn_injury_id', injury.espn_injury_id
                ).eq(
                    'scrape_timestamp', injury.scrape_timestamp
                ).execute()
                
                if existing.data:
                    # Skip if exact record already exists
                    continue
                
                # Check if we need to update or insert
                latest = self.client.table('espn_injuries').select('*').eq(
                    'espn_injury_id', injury.espn_injury_id
                ).order('scrape_timestamp', desc=True).limit(1).execute()
                
                injury_dict = injury.to_dict()
                
                if latest.data and self._has_changed(latest.data[0], injury_dict):
                    # Insert new version with updated data
                    self.client.table('espn_injuries').insert(injury_dict).execute()
                    updated += 1
                elif not latest.data:
                    # New injury - insert
                    self.client.table('espn_injuries').insert(injury_dict).execute()
                    inserted += 1
                
            except Exception as e:
                self.logger.error(f"Error storing injury {injury.espn_injury_id}: {e}")
                continue
        
        self.logger.info(f"Stored injuries - Inserted: {inserted}, Updated: {updated}")
        return inserted, updated
    
    def _has_changed(self, old_record: Dict, new_record: Dict) -> bool:
        """
        Check if injury record has meaningfully changed
        
        Args:
            old_record: Previous database record
            new_record: New injury record
            
        Returns:
            True if meaningful change detected
        """
        # Fields to check for changes
        check_fields = [
            'status', 'return_date', 'injury_type', 
            'injury_location', 'short_comment', 'long_comment'
        ]
        
        for field in check_fields:
            if old_record.get(field) != new_record.get(field):
                return True
        
        return False
    
    def get_latest_injuries(self) -> List[Dict]:
        """
        Get most recent injury snapshot
        
        Returns:
            List of latest injury records
        """
        try:
            # Get the latest scrape timestamp
            latest_scrape = self.client.table('espn_injuries').select(
                'scrape_timestamp'
            ).order('scrape_timestamp', desc=True).limit(1).execute()
            
            if not latest_scrape.data:
                self.logger.warning("No injury records found")
                return []
            
            timestamp = latest_scrape.data[0]['scrape_timestamp']
            
            # Get all injuries from that scrape
            injuries = self.client.table('espn_injuries').select('*').eq(
                'scrape_timestamp', timestamp
            ).execute()
            
            self.logger.info(f"Retrieved {len(injuries.data)} latest injuries")
            return injuries.data
            
        except Exception as e:
            self.logger.error(f"Error retrieving latest injuries: {e}")
            return []
    
    def get_injuries_for_date(self, date: datetime) -> List[Dict]:
        """
        Get injuries for a specific date
        
        Args:
            date: Date to retrieve injuries for
            
        Returns:
            List of injury records
        """
        try:
            # Find scrape closest to the requested date
            start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            injuries = self.client.table('espn_injuries').select('*').gte(
                'scrape_timestamp', start_of_day.isoformat()
            ).lte(
                'scrape_timestamp', end_of_day.isoformat()
            ).order('scrape_timestamp', desc=True).execute()
            
            self.logger.info(f"Retrieved {len(injuries.data)} injuries for {date.date()}")
            return injuries.data
            
        except Exception as e:
            self.logger.error(f"Error retrieving injuries for date: {e}")
            return []
    
    def get_active_injuries_for_team(
        self, 
        team_id: str, 
        as_of_date: datetime = None
    ) -> List[Dict]:
        """
        Get active injuries for a specific team
        
        Args:
            team_id: ESPN team ID
            as_of_date: Date to check injuries (defaults to now)
            
        Returns:
            List of active injury records for team
        """
        if as_of_date is None:
            as_of_date = datetime.now(timezone.utc)
        
        try:
            injuries = self.get_injuries_for_date(as_of_date)
            
            # Filter for team and active statuses
            active_statuses = {'out', 'doubtful', 'questionable', 'day-to-day'}
            team_injuries = [
                inj for inj in injuries 
                if inj['team_id'] == team_id and inj['status'] in active_statuses
            ]
            
            self.logger.info(f"Found {len(team_injuries)} active injuries for team {team_id}")
            return team_injuries
            
        except Exception as e:
            self.logger.error(f"Error retrieving team injuries: {e}")
            return []
    
    def get_injury_history(
        self, 
        espn_player_id: str,
        days_back: int = 30
    ) -> List[Dict]:
        """
        Get injury history for a specific player
        
        Args:
            espn_player_id: ESPN player ID
            days_back: Number of days to look back
            
        Returns:
            List of injury records for player
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            injuries = self.client.table('espn_injuries').select('*').eq(
                'espn_player_id', espn_player_id
            ).gte(
                'scrape_timestamp', cutoff_date.isoformat()
            ).order('scrape_timestamp', desc=True).execute()
            
            self.logger.info(f"Retrieved {len(injuries.data)} injury records for player {espn_player_id}")
            return injuries.data
            
        except Exception as e:
            self.logger.error(f"Error retrieving player injury history: {e}")
            return []
    
    def get_scrape_stats(self, days_back: int = 7) -> Dict:
        """
        Get statistics about recent scrapes
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Dictionary with scrape statistics
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)
            
            # Get unique scrape timestamps
            scrapes = self.client.table('espn_injuries').select(
                'scrape_timestamp'
            ).gte(
                'scrape_timestamp', cutoff_date.isoformat()
            ).execute()
            
            unique_scrapes = set(record['scrape_timestamp'] for record in scrapes.data)
            
            # Get total injuries per scrape
            injuries_per_scrape = []
            for timestamp in unique_scrapes:
                count_result = self.client.table('espn_injuries').select(
                    '*', count='exact'
                ).eq('scrape_timestamp', timestamp).execute()
                injuries_per_scrape.append(count_result.count)
            
            stats = {
                'total_scrapes': len(unique_scrapes),
                'total_records': len(scrapes.data),
                'avg_injuries_per_scrape': sum(injuries_per_scrape) / len(injuries_per_scrape) if injuries_per_scrape else 0,
                'min_injuries': min(injuries_per_scrape) if injuries_per_scrape else 0,
                'max_injuries': max(injuries_per_scrape) if injuries_per_scrape else 0,
                'period_days': days_back,
            }
            
            self.logger.info(f"Scrape stats: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Error retrieving scrape stats: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep: int = 90) -> int:
        """
        Remove old injury data beyond retention period
        
        Args:
            days_to_keep: Number of days of data to retain
            
        Returns:
            Number of records deleted
        """
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
            
            result = self.client.table('espn_injuries').delete().lt(
                'scrape_timestamp', cutoff_date.isoformat()
            ).execute()
            
            deleted_count = len(result.data) if result.data else 0
            self.logger.info(f"Cleaned up {deleted_count} old injury records")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
            return 0


# Example usage
if __name__ == "__main__":
    import logging
    from espn_injury_scraper import ESPNInjuryScraper
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize components
    scraper = ESPNInjuryScraper()
    db = InjuryDatabase()
    
    # Scrape and store
    print("Scraping ESPN injuries...")
    injuries = scraper.scrape()
    
    print(f"\nStoring {len(injuries)} injuries in database...")
    inserted, updated = db.store_injuries(injuries)
    
    print(f"\nResults:")
    print(f"  - Inserted: {inserted}")
    print(f"  - Updated: {updated}")
    
    # Test retrieval
    print("\nRetrieving latest injuries...")
    latest = db.get_latest_injuries()
    print(f"Found {len(latest)} injuries in latest scrape")
    
    # Show stats
    print("\nScrape statistics (last 7 days):")
    stats = db.get_scrape_stats(days_back=7)
    for key, value in stats.items():
        print(f"  {key}: {value}")