#!/usr/bin/env python3
"""
NBA Injury/Transaction Scraper (Playwright Edition)

Scrapes injury, personal, and disciplinary data from prosportstransactions.com

Usage:
    python nba_injury_scraper.py

Configuration:
    Edit the CONFIGURATION section below to customize date ranges, delays, etc.
"""

from playwright.sync_api import sync_playwright, Browser, Page
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import json
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine


# =============================================================================
# CONFIGURATION - EDIT THESE VALUES
# =============================================================================

START_DATE = "2009-01-01"  # Format: YYYY-MM-DD
END_DATE = "2026-01-07"    # Format: YYYY-MM-DD

# Conservative delay range (seconds) between requests
MIN_DELAY = 3
MAX_DELAY = 5

# Browser configuration
HEADLESS_MODE = False  # Set True for headless (faster but more detectable)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 1080

# Future stealth configuration (currently inactive)
ENABLE_STEALTH = False  # Set True when playwright-stealth is installed
# USE_PROXY = False       # Set True to enable proxy rotation
# PROXY_SERVER = None     # e.g., 'http://proxy.example.com:8080'

# Checkpoint save frequency (save after every N teams)
CHECKPOINT_FREQUENCY = 1  # Save after each team

# Output files
CHECKPOINT_FILE = "injury_data_checkpoint.csv"
FINAL_FILE = "injury_data_final.csv"
PROGRESS_FILE = "scrape_progress.json"  # Tracks completed teams for resume

# Database table name
DB_TABLE_NAME = "nba_injury_transactions"

# Resume behavior
RESUME_ENABLED = True  # Set to False to force fresh start (ignores progress file)

# =============================================================================
# NBA TEAMS
# =============================================================================

NBA_TEAMS = {
    # Eastern Conference - Atlantic
    "Boston Celtics": "Celtics",
    "Brooklyn Nets": "Nets",
    "New Jersey Nets": "Nets",  # Historical (pre-2012)
    "New York Knicks": "Knicks",
    "Philadelphia 76ers": "76ers",
    "Toronto Raptors": "Raptors",
    
    # Eastern Conference - Central
    "Chicago Bulls": "Bulls",
    "Cleveland Cavaliers": "Cavaliers",
    "Detroit Pistons": "Pistons",
    "Indiana Pacers": "Pacers",
    "Milwaukee Bucks": "Bucks",
    
    # Eastern Conference - Southeast
    "Atlanta Hawks": "Hawks",
    "Charlotte Hornets": "Hornets",
    "Charlotte Bobcats": "Bobcats",  # Historical (2004-2014)
    "Miami Heat": "Heat",
    "Orlando Magic": "Magic",
    "Washington Wizards": "Wizards",
    
    # Western Conference - Northwest
    "Denver Nuggets": "Nuggets",
    "Minnesota Timberwolves": "Timberwolves",
    "Oklahoma City Thunder": "Thunder",
    "Portland Trail Blazers": "Trail Blazers",
    "Utah Jazz": "Jazz",
    
    # Western Conference - Pacific
    "Golden State Warriors": "Warriors",
    "Los Angeles Clippers": "Clippers",
    "Los Angeles Lakers": "Lakers",
    "Phoenix Suns": "Suns",
    "Sacramento Kings": "Kings",
    
    # Western Conference - Southwest
    "Dallas Mavericks": "Mavericks",
    "Houston Rockets": "Rockets",
    "Memphis Grizzlies": "Grizzlies",
    "New Orleans Pelicans": "Pelicans",
    "New Orleans Hornets": "Hornets",  # Historical (2002-2013)
    "San Antonio Spurs": "Spurs",
}

# Deduplicated list for scraping
TEAMS_TO_SCRAPE = sorted(list(set(NBA_TEAMS.values())))


# =============================================================================
# BROWSER MANAGER CLASS
# =============================================================================

class BrowserManager:
    """Manages Playwright browser instances with anti-detection configurations."""
    
    def __init__(self, headless: bool = False, user_agent: str = None):
        self.headless = headless
        self.user_agent = user_agent or USER_AGENT
        self.playwright = None
        self.browser = None
        self.context = None
        
    def launch_browser(self):
        """Launch the browser with configured settings."""
        self.playwright = sync_playwright().start()
        
        # Launch args to reduce bot detection
        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-web-security',
        ]
        
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=launch_args
        )
        
        # Create context with realistic settings
        self.context = self.browser.new_context(
            viewport={'width': VIEWPORT_WIDTH, 'height': VIEWPORT_HEIGHT},
            user_agent=self.user_agent,
            locale='en-US',
            timezone_id='America/New_York',
            # Future: Add proxy here when enabled
            # proxy={'server': PROXY_SERVER} if USE_PROXY else None,
        )
        
        # Override navigator.webdriver property (basic anti-detection)
        self.context.add_init_script("""Object.defineProperty(navigator, 'webdriver', {get: () => undefined})""")
        
        print("[BROWSER] Launched successfully")
        
    def create_page(self) -> Page:
        """Create a new page in the current context."""
        if not self.context:
            self.launch_browser()
        return self.context.new_page()
    
    def fetch_url(self, url: str, retries: int = 3, timeout: int = 30000) -> str:
        """
        Navigate to URL and return HTML content.
        
        Args:
            url: Target URL
            retries: Number of retry attempts
            timeout: Timeout in milliseconds
        
        Returns:
            HTML content as string, or None if failed
        """
        page = None
        
        for attempt in range(retries):
            try:
                page = self.create_page()
                
                # Navigate with timeout
                response = page.goto(url, timeout=timeout, wait_until='domcontentloaded')
                
                # Check if we got a successful response
                if response and response.status == 403:
                    raise Exception(f"403 Forbidden: {url}")
                
                # Small random delay to appear more human
                time.sleep(random.uniform(0.5, 1.5))
                
                # Get HTML content
                html = page.content()
                
                page.close()
                return html
                
            except Exception as e:
                print(f"    [ERROR] Request failed (attempt {attempt + 1}/{retries}): {e}")
                
                if page:
                    page.close()
                
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) * 5
                    print(f"    [RETRY] Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    print(f"    [FAILED] All retries exhausted for URL: {url}")
                    return None
        
        return None
    
    def apply_stealth_config(self):
        """
        PLACEHOLDER: Apply stealth configurations to evade detection.
        
        To enable stealth mode in the future:
        1. Install: pip install playwright-stealth
        2. Set ENABLE_STEALTH = True in configuration
        3. Uncomment the stealth_sync call below
        """
        if ENABLE_STEALTH:
            try:
                from playwright_stealth import stealth_sync
                # Apply stealth to all pages in context
                for page in self.context.pages:
                    stealth_sync(page)
                print("[STEALTH] Stealth mode enabled")
            except ImportError:
                print("[WARNING] playwright-stealth not installed. Run: pip install playwright-stealth")
    
    def close(self):
        """Close browser and cleanup resources."""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("[BROWSER] Closed successfully")


# =============================================================================
# SCRAPER CLASS
# =============================================================================

class NBAInjuryScraper:
    BASE_URL = "https://prosportstransactions.com/basketball/Search/SearchResults.php"
    
    def __init__(self, start_date: str, end_date: str, headless: bool = None):
        self.start_date = start_date
        self.end_date = end_date
        
        # Use BrowserManager instead of requests.Session
        headless_mode = headless if headless is not None else HEADLESS_MODE
        self.browser_manager = BrowserManager(headless=headless_mode)
        self.browser_manager.launch_browser()
        
        # Apply stealth if enabled
        if ENABLE_STEALTH:
            self.browser_manager.apply_stealth_config()
        
        self.all_data = []
        self.completed_teams = set()
        self._load_progress()
        
    def _load_progress(self):
        """Load progress from previous run if resume is enabled."""
        if not RESUME_ENABLED:
            print("[RESUME] Resume disabled - starting fresh")
            self.completed_teams = set()
            return
            
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    progress_data = json.load(f)
                    
                # Verify date range matches
                if (progress_data.get('start_date') == self.start_date and 
                    progress_data.get('end_date') == self.end_date):
                    self.completed_teams = set(progress_data.get('completed_teams', []))
                    print(f"[RESUME] Loaded progress: {len(self.completed_teams)} teams already completed")
                    if self.completed_teams:
                        print(f"[RESUME] Completed teams: {sorted(self.completed_teams)}")
                else:
                    print("[RESUME] Date range changed - starting fresh scrape")
                    self.completed_teams = set()
            except (json.JSONDecodeError, KeyError) as e:
                print(f"[RESUME] Could not load progress file: {e}")
                self.completed_teams = set()
        else:
            print("[RESUME] No progress file found - starting fresh")
            self.completed_teams = set()
            
        # Load existing checkpoint data if resuming
        if self.completed_teams and os.path.exists(CHECKPOINT_FILE):
            try:
                existing_df = pd.read_csv(CHECKPOINT_FILE)
                self.all_data = existing_df.to_dict('records')
                print(f"[RESUME] Loaded {len(self.all_data)} existing rows from checkpoint")
            except Exception as e:
                print(f"[RESUME] Could not load checkpoint data: {e}")
                self.all_data = []
    
    def _save_progress(self):
        """Save current progress to file."""
        progress_data = {
            'start_date': self.start_date,
            'end_date': self.end_date,
            'completed_teams': list(self.completed_teams),
            'last_updated': datetime.now().isoformat(),
            'total_rows': len(self.all_data),
        }
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress_data, f, indent=2)
    
    def _mark_team_completed(self, team: str):
        """Mark a team as completed and save progress."""
        self.completed_teams.add(team)
        self._save_progress()
        
    def _build_url(self, team: str, start: int = 0) -> str:
        """Build the URL for a specific team and pagination offset."""
        params = {
            'Player': '',
            'Team': team,
            'BeginDate': self.start_date,
            'EndDate': self.end_date,
            'InjuriesChkBx': 'yes',
            'PersonalChkBx': 'yes',
            'DisciplinaryChkBx': 'yes',
            'Submit': 'Search',
        }
        if start > 0:
            params['start'] = start
            
        param_str = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{self.BASE_URL}?{param_str}"
    
    def _random_delay(self):
        """Sleep for a random duration within the configured range."""
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        time.sleep(delay)
        
    def _fetch_page(self, url: str, retries: int = 3):
        """Fetch a page using Playwright with retry logic and exponential backoff."""
        html = self.browser_manager.fetch_url(url, retries=retries)
        if html:
            return BeautifulSoup(html, 'html.parser')
        return None
    
    def _get_total_pages(self, soup) -> int:
        """Parse the pagination section to determine total number of pages."""
        paging_tables = soup.find_all('table', {'width': '75%'})
        
        for table in paging_tables:
            links = table.find_all('a')
            page_numbers = []
            
            for link in links:
                text = link.get_text(strip=True)
                if text.lower() in ['previous', 'next']:
                    continue
                try:
                    page_num = int(text)
                    page_numbers.append(page_num)
                except ValueError:
                    continue
            
            if page_numbers:
                return max(page_numbers)
        
        return 1
    
    def _parse_table(self, soup, team: str) -> list:
        """Parse the data table from the page."""
        rows = []
        
        table = soup.find('table', class_='datatable')
        if not table:
            return rows
            
        tr_elements = table.find_all('tr')
        
        for tr in tr_elements:
            if tr.find('td', class_='DraftTableLabel') or tr.find('tr', class_='DraftTableLabel'):
                continue
            if 'DraftTableLabel' in tr.get('class', []):
                continue
                
            tds = tr.find_all('td')
            if len(tds) >= 5:
                date_text = tds[0].get_text(strip=True)
                team_text = tds[1].get_text(strip=True)
                acquired_text = tds[2].get_text(strip=True)
                relinquished_text = tds[3].get_text(strip=True)
                notes_text = tds[4].get_text(strip=True)
                
                if date_text.lower() == 'date':
                    continue
                
                if date_text:
                    rows.append({
                        'date': date_text,
                        'team': team_text if team_text else team,
                        'acquired': acquired_text,
                        'relinquished': relinquished_text,
                        'notes': notes_text,
                        'scrape_timestamp': datetime.now().isoformat(),
                    })
        
        return rows
    
    def _check_for_no_results(self, soup) -> bool:
        """Check if the page indicates no results found."""
        page_text = soup.get_text()
        if "No records found" in page_text or "0 records" in page_text:
            return True
        table = soup.find('table', class_='datatable')
        if not table:
            return True
        rows = table.find_all('tr')
        data_rows = [r for r in rows if not r.find('td', class_='DraftTableLabel') 
                     and 'DraftTableLabel' not in r.get('class', [])]
        return len(data_rows) <= 1
    
    def scrape_team(self, team: str) -> list:
        """Scrape all pages for a single team."""
        print(f"\n[TEAM] Scraping: {team}")
        team_data = []
        
        url = self._build_url(team, start=0)
        print(f"  [PAGE 1] Fetching: {url[:80]}...")
        
        soup = self._fetch_page(url)
        if not soup:
            print(f"  [ERROR] Failed to fetch first page for {team}")
            return team_data
        
        if self._check_for_no_results(soup):
            print(f"  [INFO] No results found for {team}")
            return team_data
        
        total_pages = self._get_total_pages(soup)
        print(f"  [INFO] Total pages detected: {total_pages}")
        
        page_rows = self._parse_table(soup, team)
        team_data.extend(page_rows)
        print(f"  [PAGE 1] Extracted {len(page_rows)} rows")
        
        for page_num in range(2, total_pages + 1):
            self._random_delay()
            
            start_offset = (page_num - 1) * 25
            url = self._build_url(team, start=start_offset)
            
            print(f"  [PAGE {page_num}/{total_pages}] Fetching...")
            
            soup = self._fetch_page(url)
            if not soup:
                print(f"  [ERROR] Failed to fetch page {page_num}")
                continue
                
            page_rows = self._parse_table(soup, team)
            team_data.extend(page_rows)
            print(f"  [PAGE {page_num}] Extracted {len(page_rows)} rows")
            
            if len(page_rows) == 0:
                print(f"  [INFO] No more data, stopping pagination")
                break
        
        print(f"  [TEAM COMPLETE] {team}: {len(team_data)} total rows")
        return team_data
    
    def scrape_all_teams(self, teams: list = None):
        """Scrape all teams (or a specified subset)."""
        if teams is None:
            teams = TEAMS_TO_SCRAPE
        
        teams_to_process = [t for t in teams if t not in self.completed_teams]
        skipped_count = len(teams) - len(teams_to_process)
            
        print("=" * 60)
        print("NBA INJURY TRANSACTION SCRAPER")
        print("=" * 60)
        print(f"Date Range: {self.start_date} to {self.end_date}")
        print(f"Total teams: {len(teams)}")
        if skipped_count > 0:
            print(f"Already completed (skipping): {skipped_count}")
        print(f"Teams to scrape this run: {len(teams_to_process)}")
        print(f"Delay between requests: {MIN_DELAY}-{MAX_DELAY} seconds")
        print(f"Headless mode: {HEADLESS_MODE}")
        print(f"Resume enabled: {RESUME_ENABLED}")
        print("=" * 60)
        
        if not teams_to_process:
            print("\n[INFO] All teams already scraped! Nothing to do.")
            print("[INFO] To force a fresh scrape, set RESUME_ENABLED = False")
            return self.get_dataframe()
        
        for idx, team in enumerate(teams_to_process, 1):
            print(f"\n[PROGRESS] Team {idx}/{len(teams_to_process)} (Overall: {len(self.completed_teams) + idx}/{len(teams)})")
            
            team_data = self.scrape_team(team)
            self.all_data.extend(team_data)
            
            self._mark_team_completed(team)
            
            if idx % CHECKPOINT_FREQUENCY == 0:
                self._save_checkpoint()
            
            if idx < len(teams_to_process):
                self._random_delay()
        
        print("\n" + "=" * 60)
        print("SCRAPING COMPLETE")
        print(f"Total rows collected: {len(self.all_data)}")
        print(f"Teams completed: {len(self.completed_teams)}")
        print("=" * 60)
        
        return self.get_dataframe()
    
    def close(self):
        """Close the browser and cleanup resources."""
        if hasattr(self, 'browser_manager'):
            self.browser_manager.close()
    
    def __del__(self):
        """Destructor to ensure browser is closed."""
        self.close()
    
    def _save_checkpoint(self):
        """Save current data to checkpoint file."""
        if self.all_data:
            df = pd.DataFrame(self.all_data)
            df.to_csv(CHECKPOINT_FILE, index=False)
            print(f"  [CHECKPOINT] Saved {len(self.all_data)} rows to {CHECKPOINT_FILE}")
    
    def get_dataframe(self) -> pd.DataFrame:
        """Return collected data as a DataFrame."""
        if not self.all_data:
            return pd.DataFrame()
        return pd.DataFrame(self.all_data)
    
    def save_to_csv(self, filename: str = None):
        """Save data to CSV file."""
        if filename is None:
            filename = FINAL_FILE
        df = self.get_dataframe()
        if not df.empty:
            df.to_csv(filename, index=False)
            print(f"[SAVED] {len(df)} rows to {filename}")
        else:
            print("[WARNING] No data to save")
        return df


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def save_to_database(df: pd.DataFrame, table_name: str = DB_TABLE_NAME):
    """Save DataFrame to Supabase database."""
    print("\n[DATABASE] Connecting to database...")
    
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        print("[ERROR] DATABASE_URL not found in environment variables")
        print("[INFO] Data saved to CSV only. Set DATABASE_URL to enable database saving.")
        return False
    
    try:
        engine = create_engine(database_url)
        
        # Clean the dataframe for postgres
        df_clean = df.copy()
        
        # Convert date column to proper datetime
        df_clean['date'] = pd.to_datetime(df_clean['date'], errors='coerce')
        
        # Replace NaN with None for proper NULL handling
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # Replace empty strings with None
        df_clean = df_clean.replace('', None)
        
        # Clean the bullet point characters from player names
        if 'acquired' in df_clean.columns:
            df_clean['acquired'] = df_clean['acquired'].str.replace('•', '').str.strip()
        if 'relinquished' in df_clean.columns:
            df_clean['relinquished'] = df_clean['relinquished'].str.replace('•', '').str.strip()
        
        print(f"[DATABASE] Saving {len(df_clean)} rows to table '{table_name}'...")
        
        df_clean.to_sql(
            table_name,
            engine,
            if_exists='append',  # Use 'replace' if you want to overwrite
            index=False
        )
        
        print(f"[DATABASE] Successfully saved to '{table_name}'")
        
        # Verify the save
        row_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", engine)
        print(f"[DATABASE] Total rows in table: {row_count['count'].iloc[0]}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Database save failed: {e}")
        print("[INFO] Data preserved in CSV checkpoint file")
        return False


def reset_progress():
    """Delete progress and checkpoint files to start fresh."""
    files_to_delete = [PROGRESS_FILE, CHECKPOINT_FILE]
    for f in files_to_delete:
        if os.path.exists(f):
            os.remove(f)
            print(f"[RESET] Deleted {f}")
        else:
            print(f"[RESET] {f} does not exist")
    print("[RESET] Progress reset complete - next run will start fresh")


def show_progress():
    """Display current scraping progress."""
    print("=" * 60)
    print("SCRAPING PROGRESS STATUS")
    print("=" * 60)
    
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            progress = json.load(f)
        
        completed = progress.get('completed_teams', [])
        total_teams = len(TEAMS_TO_SCRAPE)
        
        print(f"Date Range: {progress.get('start_date')} to {progress.get('end_date')}")
        print(f"Last Updated: {progress.get('last_updated')}")
        print(f"Teams Completed: {len(completed)}/{total_teams}")
        print(f"Total Rows Collected: {progress.get('total_rows', 0)}")
        
        remaining = [t for t in TEAMS_TO_SCRAPE if t not in completed]
        if remaining:
            print(f"\nRemaining teams ({len(remaining)}):")
            for t in sorted(remaining):
                print(f"  - {t}")
        else:
            print("\nAll teams completed!")
    else:
        print("No progress file found - scraping has not started yet")
    
    print("=" * 60)


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("NBA INJURY SCRAPER - PLAYWRIGHT EDITION")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  Date Range: {START_DATE} to {END_DATE}")
    print(f"  Headless Mode: {HEADLESS_MODE}")
    print(f"  Stealth Mode: {ENABLE_STEALTH}")
    print(f"  Resume Enabled: {RESUME_ENABLED}")
    print("=" * 60 + "\n")
    
    # Check current progress
    if os.path.exists(PROGRESS_FILE):
        show_progress()
        print()
    
    # Initialize scraper
    scraper = None
    try:
        scraper = NBAInjuryScraper(START_DATE, END_DATE)
        
        # Run scraping
        df = scraper.scrape_all_teams()
        
        # Save to CSV
        scraper.save_to_csv()
        
        # Optionally save to database
        if not df.empty:
            save_to_database(df)
        
        print("\n[SUCCESS] Scraping completed successfully!")
        print(f"[INFO] Data saved to {FINAL_FILE}")
        print(f"[INFO] Total rows: {len(df)}")
        
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Scraping interrupted by user")
        print("[INFO] Progress has been saved - run again to resume")
    except Exception as e:
        print(f"\n[ERROR] Fatal error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if scraper:
            scraper.close()
            print("[CLEANUP] Browser closed successfully")


if __name__ == "__main__":
    main()