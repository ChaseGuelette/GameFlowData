"""MLB Weather Forecast Job — fetches today's game weather from Open-Meteo (free, no key needed)."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.scrapers.mlb.mlb_weather_scraper import fetch_today_forecast

engine = get_engine()
saved = fetch_today_forecast(engine)
print(f"MLB weather: saved {saved} game records")
