"""
ESPN Injury Scraper for NBA
Fetches current injury data and stores in Supabase with change tracking
"""

import requests
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum
import json


class InjuryStatus(Enum):
    """Standardized injury statuses"""
    OUT = "out"
    DOUBTFUL = "doubtful"
    QUESTIONABLE = "questionable"
    PROBABLE = "probable"
    DAY_TO_DAY = "day-to-day"
    ACTIVE = "active"


@dataclass
class InjuryRecord:
    """Structured injury data"""
    espn_injury_id: str
    espn_player_id: str
    player_name: str
    team_id: str
    team_name: str
    status: str
    injury_type: Optional[str]
    injury_location: Optional[str]
    injury_side: Optional[str]
    injury_detail: Optional[str]
    return_date: Optional[str]
    date_reported: str
    short_comment: Optional[str]
    long_comment: Optional[str]
    fantasy_status: Optional[str]
    scrape_timestamp: str
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for database insertion"""
        return {
            'espn_injury_id': self.espn_injury_id,
            'espn_player_id': self.espn_player_id,
            'player_name': self.player_name,
            'team_id': self.team_id,
            'team_name': self.team_name,
            'status': self.status,
            'injury_type': self.injury_type,
            'injury_location': self.injury_location,
            'injury_side': self.injury_side,
            'injury_detail': self.injury_detail,
            'return_date': self.return_date,
            'date_reported': self.date_reported,
            'short_comment': self.short_comment,
            'long_comment': self.long_comment,
            'fantasy_status': self.fantasy_status,
            'scrape_timestamp': self.scrape_timestamp,
        }


class ESPNInjuryScraper:
    """
    Scrapes current NBA injury data from ESPN's unofficial API
    
    Features:
    - Rate limiting and respectful scraping
    - Retry logic with exponential backoff
    - Change detection
    - Comprehensive error handling
    - Structured data extraction
    """
    
    BASE_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
    
    def __init__(
        self,
        min_request_interval: float = 2.0,
        max_retries: int = 3,
        timeout: int = 15,
    ):
        """
        Initialize scraper
        
        Args:
            min_request_interval: Minimum seconds between requests (be respectful)
            max_retries: Maximum retry attempts on failure
            timeout: Request timeout in seconds
        """
        self.min_request_interval = min_request_interval
        self.max_retries = max_retries
        self.timeout = timeout
        
        # Setup session with appropriate headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
        })
        
        self.last_request_time = None
        self.logger = logging.getLogger(__name__)
        
    def _rate_limit(self):
        """Enforce rate limiting between requests"""
        if self.last_request_time:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_request_interval:
                sleep_time = self.min_request_interval - elapsed
                self.logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """
        Retry function with exponential backoff
        
        Args:
            func: Function to retry
            *args, **kwargs: Arguments to pass to function
            
        Returns:
            Function result or None on failure
        """
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except requests.RequestException as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed after {self.max_retries} attempts: {e}")
                    return None
                
                wait_time = (2 ** attempt) * self.min_request_interval
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s")
                time.sleep(wait_time)
        
        return None
    
    def fetch_current_injuries(self) -> Optional[Dict]:
        """
        Fetch current injury data from ESPN API
        
        Returns:
            Dictionary with injury data or None on failure
        """
        self._rate_limit()
        
        def _fetch():
            response = self.session.get(self.BASE_URL, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        
        data = self._retry_with_backoff(_fetch)
        
        if data:
            self.logger.info(f"Successfully fetched injury data at {datetime.now(timezone.utc)}")
        else:
            self.logger.error("Failed to fetch injury data")
            
        return data
    
    def parse_injuries(self, data: Dict) -> List[InjuryRecord]:
        """
        Parse raw ESPN data into structured injury records
        
        Args:
            data: Raw ESPN API response
            
        Returns:
            List of InjuryRecord objects
        """
        if not data or 'injuries' not in data:
            self.logger.warning("No injury data found in response")
            return []
        
        injuries = []
        scrape_timestamp = datetime.now(timezone.utc).isoformat()
        
        for team in data['injuries']:
            team_id = str(team.get('id', ''))
            team_name = team.get('displayName', '')
            
            for injury in team.get('injuries', []):
                try:
                    athlete = injury.get('athlete', {})
                    details = injury.get('details', {})
                    fantasy = details.get('fantasyStatus', {})
                    
                    # Extract status type
                    injury_type_obj = injury.get('type', {})
                    status_description = injury_type_obj.get('description', 'unknown')
                    
                    # Create injury record
                    record = InjuryRecord(
                        espn_injury_id=str(injury.get('id', '')),
                        espn_player_id=str(athlete.get('id', '')),
                        player_name=athlete.get('displayName', ''),
                        team_id=team_id,
                        team_name=team_name,
                        status=status_description,
                        injury_type=details.get('type'),
                        injury_location=details.get('location'),
                        injury_side=details.get('side'),
                        injury_detail=details.get('detail'),
                        return_date=details.get('returnDate'),
                        date_reported=injury.get('date'),
                        short_comment=injury.get('shortComment'),
                        long_comment=injury.get('longComment'),
                        fantasy_status=fantasy.get('description') if fantasy else None,
                        scrape_timestamp=scrape_timestamp,
                    )
                    
                    injuries.append(record)
                    
                except Exception as e:
                    self.logger.error(f"Error parsing injury record: {e}")
                    self.logger.debug(f"Problematic injury data: {json.dumps(injury, indent=2)}")
                    continue
        
        self.logger.info(f"Parsed {len(injuries)} injury records")
        return injuries
    
    def scrape(self) -> List[InjuryRecord]:
        """
        Main scraping method - fetch and parse injuries
        
        Returns:
            List of InjuryRecord objects
        """
        self.logger.info("Starting ESPN injury scrape")
        
        raw_data = self.fetch_current_injuries()
        if not raw_data:
            return []
        
        injuries = self.parse_injuries(raw_data)
        
        return injuries
    
    def get_active_injury_ids(self, injuries: List[InjuryRecord]) -> Set[str]:
        """
        Get set of currently active ESPN injury IDs
        
        Args:
            injuries: List of current injury records
            
        Returns:
            Set of injury IDs
        """
        return {injury.espn_injury_id for injury in injuries}


class InjuryChangeDetector:
    """
    Detects changes in injury status between scrapes
    Tracks: new injuries, status changes, and resolved injuries
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def detect_changes(
        self,
        previous_injuries: List[InjuryRecord],
        current_injuries: List[InjuryRecord]
    ) -> Dict[str, List[InjuryRecord]]:
        """
        Detect changes between two injury snapshots
        
        Args:
            previous_injuries: Previous scrape results
            current_injuries: Current scrape results
            
        Returns:
            Dictionary with 'new', 'updated', 'resolved' injury lists
        """
        # Create lookup dicts
        prev_dict = {inj.espn_injury_id: inj for inj in previous_injuries}
        curr_dict = {inj.espn_injury_id: inj for inj in current_injuries}
        
        prev_ids = set(prev_dict.keys())
        curr_ids = set(curr_dict.keys())
        
        # Find changes
        new_ids = curr_ids - prev_ids
        resolved_ids = prev_ids - curr_ids
        potential_updates = prev_ids & curr_ids
        
        changes = {
            'new': [curr_dict[id] for id in new_ids],
            'resolved': [prev_dict[id] for id in resolved_ids],
            'updated': []
        }
        
        # Check for status updates
        for injury_id in potential_updates:
            prev = prev_dict[injury_id]
            curr = curr_dict[injury_id]
            
            # Check if meaningful fields changed
            if (prev.status != curr.status or
                prev.return_date != curr.return_date or
                prev.injury_type != curr.injury_type):
                changes['updated'].append(curr)
        
        self.logger.info(
            f"Changes detected - New: {len(changes['new'])}, "
            f"Updated: {len(changes['updated'])}, "
            f"Resolved: {len(changes['resolved'])}"
        )
        
        return changes


# Example usage and testing
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize scraper
    scraper = ESPNInjuryScraper(min_request_interval=2.0)
    
    # Scrape current injuries
    injuries = scraper.scrape()
    
    print(f"\n{'='*80}")
    print(f"SCRAPE RESULTS: {len(injuries)} injuries found")
    print(f"{'='*80}\n")
    
    # Display sample injuries
    for i, injury in enumerate(injuries[:5], 1):
        print(f"{i}. {injury.player_name} ({injury.team_name})")
        print(f"   Status: {injury.status}")
        print(f"   Type: {injury.injury_type} - {injury.injury_location}")
        print(f"   Return: {injury.return_date}")
        print(f"   Comment: {injury.short_comment[:100] if injury.short_comment else 'N/A'}")
        print()
    
    if len(injuries) > 5:
        print(f"... and {len(injuries) - 5} more injuries\n")
    
    # Show distribution by status
    from collections import Counter
    status_counts = Counter(injury.status for injury in injuries)
    print(f"\nInjury Status Distribution:")
    for status, count in status_counts.most_common():
        print(f"  {status}: {count}")