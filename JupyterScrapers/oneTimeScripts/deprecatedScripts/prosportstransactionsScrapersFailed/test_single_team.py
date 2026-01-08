#!/usr/bin/env python3
"""
Quick Test Script - Scrape Single Team

This script tests the scraper with just one team to verify everything works.
Run this BEFORE running the full scraper.

Usage:
    python test_single_team.py
"""

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
import time

# Test configuration
TEST_TEAM = "Lakers"
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
HEADLESS_MODE = False  # Keep browser visible for debugging

print("=" * 60)
print("NBA INJURY SCRAPER - SINGLE TEAM TEST")
print("=" * 60)
print(f"Testing with: {TEST_TEAM}")
print(f"Date range: {START_DATE} to {END_DATE}")
print(f"Headless mode: {HEADLESS_MODE}")
print("=" * 60 + "\n")

def test_scraper():
    """Test the scraper with a single team."""
    
    # Import from main script
    import sys
    import os
    
    # Add current directory to path so we can import from nba_injury_scraper.py
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    try:
        from nba_injury_scraper import NBAInjuryScraper
        
        print("[TEST] Initializing scraper...")
        scraper = NBAInjuryScraper(START_DATE, END_DATE, headless=HEADLESS_MODE)
        
        print(f"[TEST] Scraping {TEST_TEAM}...")
        df = scraper.scrape_all_teams(teams=[TEST_TEAM])
        
        print(f"\n[TEST] Results:")
        print(f"  Rows scraped: {len(df)}")
        
        if len(df) > 0:
            print(f"\n[TEST] Sample data:")
            print(df.head(10))
            
            # Save test results
            test_file = f"test_{TEST_TEAM.lower()}.csv"
            df.to_csv(test_file, index=False)
            print(f"\n[TEST] Data saved to: {test_file}")
            
            print("\n" + "=" * 60)
            print("✅ SUCCESS! Scraper is working correctly.")
            print("=" * 60)
            print("\nNext steps:")
            print("1. Check the CSV file to verify data quality")
            print("2. If everything looks good, run the full scraper:")
            print("   python nba_injury_scraper.py")
            
        else:
            print("\n" + "=" * 60)
            print("⚠️ WARNING: No data scraped")
            print("=" * 60)
            print("\nPossible reasons:")
            print("1. No injury transactions for this team in date range")
            print("2. Site is blocking requests (try enabling stealth mode)")
            print("3. Network issue")
            
        scraper.close()
        
    except ImportError:
        print("\n[ERROR] Could not import nba_injury_scraper.py")
        print("Make sure nba_injury_scraper.py is in the same directory!")
        return False
        
    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


if __name__ == "__main__":
    try:
        test_scraper()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Test cancelled by user")