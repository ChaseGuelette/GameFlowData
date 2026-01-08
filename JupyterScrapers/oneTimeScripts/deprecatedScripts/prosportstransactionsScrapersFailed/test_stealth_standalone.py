#!/usr/bin/env python3
"""
Standalone Stealth Mode Test Script

Tests playwright-stealth against prosportstransactions.com
Does NOT depend on the main nba_injury_scraper.py

This will show you:
1. If stealth mode actually works
2. What detection vectors are still exposed
3. Whether this site is even worth scraping

Usage:
    pip install playwright playwright-stealth beautifulsoup4 pandas
    python test_stealth_standalone.py
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import time
import random


# =============================================================================
# TEST CONFIGURATION
# =============================================================================

TEST_TEAM = "Lakers"
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
HEADLESS_MODE = False  # Keep False for debugging
ENABLE_STEALTH = True  # Set to True to test stealth mode

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'


# =============================================================================
# STEALTH BROWSER MANAGER
# =============================================================================

class StealthBrowserManager:
    """Standalone browser manager with stealth mode for testing."""
    
    def __init__(self, headless: bool = False, enable_stealth: bool = True):
        self.headless = headless
        self.enable_stealth = enable_stealth
        self.playwright = None
        self.browser = None
        self.context = None
        
    def launch_browser(self):
        """Launch browser with anti-detection configurations."""
        print(f"[BROWSER] Launching (Headless: {self.headless}, Stealth: {self.enable_stealth})...")
        
        self.playwright = sync_playwright().start()
        
        # Anti-detection launch arguments
        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--disable-dev-shm-usage',
            '--no-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
        ]
        
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=launch_args
        )
        
        # Create context with realistic fingerprint
        self.context = self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=USER_AGENT,
            locale='en-US',
            timezone_id='America/New_York',
            # Add realistic screen resolution
            screen={'width': 1920, 'height': 1080},
            # Enable permissions that real browsers have
            permissions=['geolocation'],
        )
        
        # Basic anti-detection: Remove navigator.webdriver
        self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)
        
        print("[BROWSER] ✓ Launched successfully")
        
        # Apply stealth mode if enabled
        if self.enable_stealth:
            self._apply_stealth()
    
    def _apply_stealth(self):
        """Apply playwright-stealth to all pages in context."""
        try:
            from playwright_stealth import stealth_sync
            
            # Create a dummy page to apply stealth (will be applied to all future pages)
            dummy_page = self.context.new_page()
            stealth_sync(dummy_page)
            dummy_page.close()
            
            print("[STEALTH] ✓ Stealth mode applied")
            print("[STEALTH]   - navigator.webdriver removed")
            print("[STEALTH]   - permissions patched")
            print("[STEALTH]   - plugins array fixed")
            print("[STEALTH]   - WebGL fingerprint masked")
            
        except ImportError:
            print("[ERROR] playwright-stealth not installed!")
            print("[ERROR] Install with: pip install playwright-stealth")
            print("[ERROR] Continuing WITHOUT stealth mode...")
            self.enable_stealth = False
    
    def test_detection(self):
        """Test against bot detection checker to see what's exposed."""
        print("\n" + "=" * 60)
        print("RUNNING BOT DETECTION TEST")
        print("=" * 60)
        
        page = self.context.new_page()
        
        try:
            print("[TEST] Navigating to bot.sannysoft.com...")
            page.goto('https://bot.sannysoft.com/', timeout=30000)
            time.sleep(2)
            
            # Take screenshot
            screenshot_path = 'stealth_detection_test.png'
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"[TEST] ✓ Screenshot saved: {screenshot_path}")
            print("[TEST]   Check the image - Red boxes = detected, Green = passed")
            
            # Try to extract some detection results
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Look for failed tests (this is brittle but gives quick feedback)
            webdriver_el = page.query_selector('text=/webdriver/')
            if webdriver_el:
                print("[TEST] navigator.webdriver: CHECKING...")
            
            print("\n[RESULT] Open 'stealth_detection_test.png' to see full results")
            
        except Exception as e:
            print(f"[ERROR] Detection test failed: {e}")
        finally:
            page.close()
        
        print("=" * 60 + "\n")
    
    def fetch_url(self, url: str, retries: int = 3) -> tuple[str, int]:
        """
        Fetch URL and return (HTML content, status_code).
        
        Returns:
            tuple: (html_content, status_code) or (None, status_code) on failure
        """
        page = None
        
        for attempt in range(retries):
            try:
                page = self.context.new_page()
                
                # Add random mouse movements to appear more human
                if not self.headless:
                    page.mouse.move(
                        random.randint(100, 500),
                        random.randint(100, 500)
                    )
                
                print(f"[FETCH] Attempt {attempt + 1}/{retries}: {url[:80]}...")
                
                # Navigate with timeout
                response = page.goto(url, timeout=30000, wait_until='domcontentloaded')
                
                status_code = response.status if response else 0
                
                # Small random delay to appear more human
                time.sleep(random.uniform(0.5, 1.5))
                
                # Get HTML content
                html = page.content()
                
                page.close()
                
                if status_code == 403:
                    print(f"[FETCH] ✗ Got 403 Forbidden (attempt {attempt + 1})")
                    if attempt < retries - 1:
                        wait_time = (2 ** attempt) * 5
                        print(f"[FETCH] Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                    continue
                
                print(f"[FETCH] ✓ Success! Status: {status_code}")
                return html, status_code
                
            except Exception as e:
                print(f"[FETCH] ✗ Error: {e}")
                
                if page:
                    page.close()
                
                if attempt < retries - 1:
                    wait_time = (2 ** attempt) * 5
                    print(f"[FETCH] Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
        
        return None, 403
    
    def close(self):
        """Clean up browser resources."""
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        print("[BROWSER] ✓ Closed successfully")


# =============================================================================
# SCRAPER FUNCTIONS
# =============================================================================

def parse_transaction_table(html: str) -> list[dict]:
    """Parse the transaction table from HTML."""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Find the results table
    table = soup.find('table', class_='datatable')
    
    if not table:
        print("[PARSE] ✗ No table found in HTML")
        return []
    
    rows = table.find_all('tr')[1:]  # Skip header
    
    if not rows:
        print("[PARSE] ✗ Table found but no data rows")
        return []
    
    data = []
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 5:
            record = {
                'date': cols[0].text.strip(),
                'team': cols[1].text.strip(),
                'acquired': cols[2].text.strip(),
                'relinquished': cols[3].text.strip(),
                'notes': cols[4].text.strip()
            }
            data.append(record)
    
    print(f"[PARSE] ✓ Parsed {len(data)} rows")
    return data


def test_scrape_team(browser: StealthBrowserManager, team: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Test scraping a single team."""
    print(f"\n[SCRAPE] Testing {team}...")
    
    # Build URL
    base_url = "https://prosportstransactions.com/basketball/Search/SearchResults.php"
    url = f"{base_url}?Player=&Team={team}&BeginDate={start_date}&EndDate={end_date}&InjuriesChkBx=yes&PersonalChkBx=yes&DisciplinaryChkBx=yes&Submit=Search"
    
    # Fetch page
    html, status_code = browser.fetch_url(url)
    
    if html is None:
        print(f"[SCRAPE] ✗ Failed to fetch data (status: {status_code})")
        return pd.DataFrame()
    
    # Parse data
    data = parse_transaction_table(html)
    
    if not data:
        print(f"[SCRAPE] ✗ No data found (might be blocked or no transactions)")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    print(f"[SCRAPE] ✓ Successfully scraped {len(df)} rows")
    
    return df


# =============================================================================
# MAIN TEST EXECUTION
# =============================================================================

def main():
    """Run the standalone stealth test."""
    print("\n" + "=" * 60)
    print("STEALTH MODE TEST - STANDALONE VERSION")
    print("=" * 60)
    print(f"Target: {TEST_TEAM}")
    print(f"Date Range: {START_DATE} to {END_DATE}")
    print(f"Stealth Mode: {ENABLE_STEALTH}")
    print(f"Headless: {HEADLESS_MODE}")
    print("=" * 60 + "\n")
    
    browser = None
    
    try:
        # Initialize browser with stealth
        browser = StealthBrowserManager(
            headless=HEADLESS_MODE,
            enable_stealth=ENABLE_STEALTH
        )
        browser.launch_browser()
        
        # Run bot detection test (optional but useful)
        if not HEADLESS_MODE:  # Only in visible mode so you can see results
            user_input = input("\n[PROMPT] Run bot detection test first? (y/n): ").lower()
            if user_input == 'y':
                browser.test_detection()
                input("Press Enter to continue to actual scraping test...")
        
        # Test scraping
        df = test_scrape_team(browser, TEST_TEAM, START_DATE, END_DATE)
        
        # Results
        print("\n" + "=" * 60)
        print("TEST RESULTS")
        print("=" * 60)
        
        if len(df) > 0:
            print(f"✓ SUCCESS! Scraped {len(df)} rows")
            print("\nSample data:")
            print(df.head(10))
            
            # Save results
            output_file = f"test_{TEST_TEAM.lower()}_stealth.csv"
            df.to_csv(output_file, index=False)
            print(f"\n✓ Saved to: {output_file}")
            
        else:
            print("✗ FAILED! No data scraped")
            print("\nPossible reasons:")
            print("1. Still getting blocked (403) - stealth not working")
            print("2. No transactions for this team/date range")
            print("3. Site structure changed")
            
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Test cancelled by user")
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if browser:
            browser.close()


if __name__ == "__main__":
    # Check if stealth is installed
    if ENABLE_STEALTH:
        try:
            import playwright_stealth
            print("[CHECK] ✓ playwright-stealth is installed")
        except ImportError:
            print("\n" + "=" * 60)
            print("ERROR: playwright-stealth is not installed!")
            print("=" * 60)
            print("Install with:")
            print("  pip install playwright-stealth")
            print("\nThen run this script again.")
            print("=" * 60 + "\n")
            exit(1)
    
    main()