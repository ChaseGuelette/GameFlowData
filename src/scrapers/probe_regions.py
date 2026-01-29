"""
Probe The Odds API for player prop coverage in us2 and us_ex regions.

Checks a few recent historical dates to see which bookmakers return
player prop data, how many players are covered, and which markets exist.

Usage:
    python src/scrapers/probe_regions.py
"""

import os
import sys
import time
from collections import defaultdict

import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")

if not API_KEY:
    print("ERROR: ODDS_API_KEY not found in .env")
    sys.exit(1)

session = requests.Session()

# Dates to probe (pick recent game dates across different days of week)
PROBE_DATES = [
    "2025-10-28T23:00:00Z",  # Mon
    "2025-10-30T23:00:00Z",  # Wed
    "2025-11-01T23:00:00Z",  # Sat
]

REGIONS_TO_PROBE = ["us2", "us_ex"]
BASELINE_REGION = "us"

TARGET_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
]


def get_historical_events(date_str):
    """Get game IDs for a historical date."""
    url = "https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events"
    params = {"apiKey": API_KEY, "date": date_str}
    try:
        resp = session.get(url, params=params, timeout=15)
        credits = resp.headers.get("x-requests-last", "?")
        print(f"  [events] credits used: {credits}")
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except Exception as e:
        print(f"  ERROR fetching events: {e}")
    return []


def get_event_props(date_str, event_id, region):
    """Get player props for a single event + region."""
    url = f"https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events/{event_id}/odds"
    params = {
        "apiKey": API_KEY,
        "date": date_str,
        "regions": region,
        "markets": ",".join(TARGET_MARKETS),
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    try:
        resp = session.get(url, params=params, timeout=15)
        credits = resp.headers.get("x-requests-last", "?")
        if resp.status_code == 200:
            return resp.json().get("data", {}), int(credits) if credits != "?" else 0
        elif resp.status_code == 422:
            return None, 0
        else:
            print(f"    HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"    ERROR: {e}")
    return None, 0


def analyze_response(data):
    """Extract bookmaker and market coverage stats from a props response."""
    if not data or "bookmakers" not in data:
        return {}

    results = {}
    for bk in data["bookmakers"]:
        bk_key = bk["key"]
        markets_found = set()
        player_count = 0
        players_seen = set()

        for market in bk.get("markets", []):
            market_key = market["key"]
            markets_found.add(market_key)
            for outcome in market.get("outcomes", []):
                players_seen.add(outcome.get("description", ""))

        results[bk_key] = {
            "markets": markets_found,
            "players": len(players_seen),
        }

    return results


def main():
    total_credits = 0

    for region in REGIONS_TO_PROBE:
        print(f"\n{'='*60}")
        print(f"REGION: {region}")
        print(f"{'='*60}")

        # Aggregate across all dates
        bookmaker_stats = defaultdict(lambda: {"dates_seen": 0, "total_players": 0, "markets": set()})

        for date_str in PROBE_DATES:
            date_label = date_str[:10]
            print(f"\n--- {date_label} ---")

            events = get_historical_events(date_str)
            total_credits += 1  # events endpoint cost
            print(f"  Found {len(events)} games")

            if not events:
                continue

            # Probe first 3 games per date to conserve credits
            sample_events = events[:3]
            print(f"  Probing {len(sample_events)} games...")

            for event in sample_events:
                event_id = event["id"]
                home = event.get("home_team", "?")
                away = event.get("away_team", "?")
                print(f"  Game: {away} @ {home}")

                time.sleep(0.2)  # rate limit
                data, credits = get_event_props(date_str, event_id, region)
                total_credits += credits

                if data is None:
                    print(f"    -> No data for region={region}")
                    continue

                bk_results = analyze_response(data)
                if not bk_results:
                    print(f"    -> Empty response (no bookmakers)")
                    continue

                for bk_key, info in bk_results.items():
                    print(f"    {bk_key}: {info['players']} players, markets={info['markets']}")
                    bookmaker_stats[bk_key]["dates_seen"] += 1
                    bookmaker_stats[bk_key]["total_players"] += info["players"]
                    bookmaker_stats[bk_key]["markets"].update(info["markets"])

        # Summary for this region
        print(f"\n--- SUMMARY for region={region} ---")
        if bookmaker_stats:
            for bk_key, stats in sorted(bookmaker_stats.items(), key=lambda x: -x[1]["total_players"]):
                avg_players = stats["total_players"] / max(stats["dates_seen"], 1)
                print(
                    f"  {bk_key}: seen {stats['dates_seen']}/{len(PROBE_DATES)*3} games, "
                    f"avg {avg_players:.0f} players/game, "
                    f"markets={stats['markets']}"
                )
        else:
            print("  NO bookmakers returned player prop data")

    # Also probe baseline (us) for one game to compare
    print(f"\n{'='*60}")
    print(f"BASELINE: {BASELINE_REGION} (1 game for comparison)")
    print(f"{'='*60}")
    events = get_historical_events(PROBE_DATES[0])
    total_credits += 1
    if events:
        data, credits = get_event_props(PROBE_DATES[0], events[0]["id"], BASELINE_REGION)
        total_credits += credits
        if data:
            bk_results = analyze_response(data)
            for bk_key, info in sorted(bk_results.items(), key=lambda x: -x[1]["players"]):
                print(f"  {bk_key}: {info['players']} players, markets={info['markets']}")

    print(f"\n{'='*60}")
    print(f"TOTAL API CREDITS USED: ~{total_credits}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
