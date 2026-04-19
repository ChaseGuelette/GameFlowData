"""MLB Weather Forecast Job — fetches today's game weather from OpenWeatherMap."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.db.client import get_engine
from src.scrapers.mlb.mlb_weather_scraper import fetch_today_forecast, get_api_key

engine = get_engine()
api_key = get_api_key()
saved = fetch_today_forecast(engine, api_key)
print(f"MLB weather: saved {saved} game records")
