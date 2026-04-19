"""
MLB Weather Scraper
====================
Fetches weather data for MLB games from OpenWeatherMap API.

Computes derived features:
  - air_density_idx: normalized air density (1.0 = 68°F, 50% RH, sea level)
    Lower = thinner air = more ball carry = more HRs.
  - wind_out_mph: tailwind component toward outfield (positive = HR-favorable)
  - has_precip: boolean precipitation indicator

Usage:
    python -m src.scrapers.mlb.mlb_weather_scraper --backfill --start 2022-04-01 --end 2025-12-31
    python -m src.scrapers.mlb.mlb_weather_scraper --today
"""

import argparse
import logging
import math
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.db.client import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MLBWeatherScraper")

# ---------------------------------------------------------------------------
# Venue lookup tables (all 30 MLB parks)
# ---------------------------------------------------------------------------

# (lat, lng) for OpenWeatherMap calls
PARK_LAT_LNG: dict[int, tuple[float, float]] = {
    15:   (33.4453, -112.0667),  # Chase Field (ARI)
    2:    (42.3467, -71.0972),   # Fenway Park (BOS)
    4705: (43.0280, -88.0970),   # American Family Field (MIL)
    17:   (41.9484, -87.6553),   # Wrigley Field (CHC)
    4:    (41.8300, -87.6340),   # Guaranteed Rate Field (CWS)
    27:   (39.0974, -84.5069),   # Great American Ball Park (CIN)
    5:    (41.4962, -81.6852),   # Progressive Field (CLE)
    19:   (39.7559, -104.9942),  # Coors Field (COL)
    2394: (42.3390, -83.0485),   # Comerica Park (DET)
    2392: (29.7572, -95.3553),   # Minute Maid Park (HOU)
    7:    (39.0517, -94.4803),   # Kauffman Stadium (KC)
    1:    (33.8003, -117.8827),  # Angel Stadium (LAA)
    22:   (34.0739, -118.2400),  # Dodger Stadium (LAD)
    4169: (25.7781, -80.2197),   # loanDepot park (MIA)
    3289: (40.7571, -73.8458),   # Citi Field (NYM)
    3313: (40.8296, -73.9262),   # Yankee Stadium (NYY)
    10:   (37.7516, -122.2005),  # Oakland Coliseum (OAK)
    2681: (39.9061, -75.1665),   # Citizens Bank Park (PHI)
    31:   (40.4469, -80.0057),   # PNC Park (PIT)
    2680: (32.7076, -117.1570),  # Petco Park (SD)
    2395: (37.7786, -122.3893),  # Oracle Park (SF)
    680:  (47.5914, -122.3324),  # T-Mobile Park (SEA)
    2889: (38.6226, -90.1928),   # Busch Stadium (STL)
    12:   (27.7683, -82.6534),   # Tropicana Field (TB)
    13:   (32.7473, -97.0832),   # Globe Life Field (TEX)
    14:   (43.6414, -79.3894),   # Rogers Centre (TOR)
    3309: (38.8730, -77.0074),   # Nationals Park (WSH)
    2862: (44.9817, -93.2776),   # Target Field (MIN)
    5325: (33.8908, -84.4678),   # Truist Park (ATL)
    # Baltimore (Camden Yards) — venue_id varies; add if needed
}

# Compass bearing from home plate toward center field (degrees, 0=N, 90=E)
# Used to compute tailwind component toward outfield
PARK_OF_BEARING: dict[int, int] = {
    15:   0,    # Chase Field — NNE
    2:    35,   # Fenway Park — NNE
    4705: 355,  # American Family Field — N
    17:   45,   # Wrigley Field — NE
    4:    130,  # Guaranteed Rate Field — SE
    27:   330,  # Great American Ball Park — NNW
    5:    335,  # Progressive Field — NNW
    19:   350,  # Coors Field — N
    2394: 220,  # Comerica Park — SW
    2392: 80,   # Minute Maid Park — E
    7:    10,   # Kauffman Stadium — N
    1:    180,  # Angel Stadium — S
    22:   0,    # Dodger Stadium — N
    4169: 55,   # loanDepot park — NE
    3289: 175,  # Citi Field — S
    3313: 330,  # Yankee Stadium — NNW
    10:   270,  # Oakland Coliseum — W
    2681: 20,   # Citizens Bank Park — NNE
    31:   330,  # PNC Park — NNW
    2680: 315,  # Petco Park — NW
    2395: 5,    # Oracle Park — N
    680:  10,   # T-Mobile Park — N
    2889: 15,   # Busch Stadium — N
    12:   180,  # Tropicana Field — dome
    13:   45,   # Globe Life Field — NE
    14:   90,   # Rogers Centre — E (dome)
    3309: 345,  # Nationals Park — NNW
    2862: 355,  # Target Field — N
    5325: 25,   # Truist Park — NNE
}

# Dome / retractable roof venues — weather overridden to neutral values
DOME_VENUE_IDS: set[int] = {
    12,    # Tropicana Field (TB) — fixed dome
    4169,  # loanDepot park (MIA) — retractable (usually closed)
    14,    # Rogers Centre (TOR) — retractable
    13,    # Globe Life Field (TEX) — retractable
    2392,  # Minute Maid Park (HOU) — retractable
    4705,  # American Family Field (MIL) — retractable
    680,   # T-Mobile Park (SEA) — retractable
}

# Neutral weather values for dome stadiums
DOME_NEUTRAL = {
    "temp_f": 72.0,
    "humidity_pct": 50.0,
    "pressure_hpa": 1013.25,
    "air_density_idx": 1.0,
    "wind_speed_mph": 0.0,
    "wind_dir_deg": 0,
    "wind_out_mph": 0.0,
    "has_precip": False,
    "is_dome": True,
    "condition": "dome",
}

# ---------------------------------------------------------------------------
# Physics helpers
# ---------------------------------------------------------------------------


def compute_air_density_idx(temp_f: float, humidity_pct: float, pressure_hpa: float) -> float:
    """Compute air density index normalized to 1.0 at standard conditions.

    Standard: 68°F (20°C), 50% RH, 1013.25 hPa.
    Lower value = thinner air = more ball carry = more HRs.

    Physics: density ∝ P / (T * (1 + 0.378 * Pv / P))
    where Pv = partial pressure of water vapor (Magnus formula).
    """
    temp_c = (temp_f - 32) * 5 / 9
    temp_k = temp_c + 273.15

    # Saturation vapor pressure (Magnus formula, hPa)
    pv_sat = 6.1078 * math.exp(17.27 * temp_c / (temp_c + 237.3))
    pv = (humidity_pct / 100.0) * pv_sat

    density_ratio = pressure_hpa / (temp_k * (1 + 0.378 * pv / pressure_hpa))

    # Standard conditions: 20°C, 50% RH, 1013.25 hPa
    std_pv_sat = 6.1078 * math.exp(17.27 * 20.0 / (20.0 + 237.3))
    std_pv = 0.50 * std_pv_sat
    std_ratio = 1013.25 / (293.15 * (1 + 0.378 * std_pv / 1013.25))

    return round(density_ratio / std_ratio, 4)


def compute_wind_out_mph(wind_speed_mph: float, wind_dir_deg: int, venue_id: int) -> float:
    """Compute tailwind component toward outfield.

    wind_dir_deg: meteorological convention (direction wind is coming FROM).
    Positive result = wind blowing toward OF = HR-favorable.
    Negative = headwind into home plate = HR-suppressing.
    """
    of_bearing = PARK_OF_BEARING.get(venue_id, 0)
    # Wind is moving TOWARD (wind_dir_deg + 180). Tailwind toward OF:
    #   component = cos(angle between wind_to_direction and of_bearing)
    wind_to_deg = (wind_dir_deg + 180) % 360
    angle_diff = math.radians(wind_to_deg - of_bearing)
    return round(math.cos(angle_diff) * wind_speed_mph, 2)


# ---------------------------------------------------------------------------
# Weather fetching
# ---------------------------------------------------------------------------

OWM_TIMEMACHINE = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
OWM_FORECAST = "https://api.openweathermap.org/data/2.5/forecast"


def _parse_owm_data(data: dict) -> dict:
    """Extract weather fields from an OWM API response dict."""
    # Handle both onecall/timemachine (data[0]) and forecast (list[0]) formats
    current = data.get("current") or (data.get("data") or [{}])[0]
    temp_f = float(current.get("temp", 70))
    humidity = float(current.get("humidity", 50))
    pressure = float(current.get("pressure", 1013.25))
    wind_speed = float(current.get("wind_speed", 0))
    wind_deg = int(current.get("wind_deg", 0))
    weather_list = current.get("weather", [])
    condition = weather_list[0].get("description", "") if weather_list else ""
    precip_mains = {w.get("main", "").lower() for w in weather_list}
    has_precip = bool(precip_mains & {"rain", "snow", "thunderstorm", "drizzle"})
    return {
        "temp_f": round(temp_f, 1),
        "humidity_pct": round(humidity, 1),
        "pressure_hpa": round(pressure, 2),
        "wind_speed_mph": round(wind_speed, 1),
        "wind_dir_deg": wind_deg,
        "has_precip": has_precip,
        "condition": condition,
    }


def fetch_weather_for_game(
    game_pk: int,
    game_dt: datetime,
    venue_id: int,
    api_key: str,
) -> Optional[dict]:
    """Fetch and compute weather for a single game.

    Returns a dict ready for upsert into mlb_game_weather, or None on failure.
    """
    if venue_id in DOME_VENUE_IDS:
        return {"game_pk": game_pk, "game_date": game_dt.date(), "venue_id": venue_id, **DOME_NEUTRAL}

    lat, lng = PARK_LAT_LNG.get(venue_id, (None, None))
    if lat is None:
        logger.warning("No lat/lng for venue_id=%d (game_pk=%d), skipping", venue_id, game_pk)
        return None

    unix_ts = int(game_dt.timestamp())
    try:
        resp = requests.get(
            OWM_TIMEMACHINE,
            params={"lat": lat, "lon": lng, "dt": unix_ts, "appid": api_key, "units": "imperial"},
            timeout=15,
        )
        resp.raise_for_status()
        raw = _parse_owm_data(resp.json())
    except Exception as e:
        logger.warning("OWM request failed game_pk=%d venue=%d: %s", game_pk, venue_id, e)
        return None

    air_density = compute_air_density_idx(raw["temp_f"], raw["humidity_pct"], raw["pressure_hpa"])
    wind_out = compute_wind_out_mph(raw["wind_speed_mph"], raw["wind_dir_deg"], venue_id)

    return {
        "game_pk": game_pk,
        "game_date": game_dt.date(),
        "venue_id": venue_id,
        "temp_f": raw["temp_f"],
        "humidity_pct": raw["humidity_pct"],
        "pressure_hpa": raw["pressure_hpa"],
        "air_density_idx": air_density,
        "wind_speed_mph": raw["wind_speed_mph"],
        "wind_dir_deg": raw["wind_dir_deg"],
        "wind_out_mph": wind_out,
        "has_precip": raw["has_precip"],
        "is_dome": False,
        "condition": raw["condition"],
    }


# ---------------------------------------------------------------------------
# DB I/O
# ---------------------------------------------------------------------------


def upsert_weather(engine, records: list[dict]) -> int:
    """Upsert weather records into mlb_game_weather."""
    if not records:
        return 0
    with engine.begin() as conn:
        for r in records:
            conn.execute(
                text("""
                    INSERT INTO mlb_game_weather
                        (game_pk, game_date, venue_id, temp_f, humidity_pct, pressure_hpa,
                         air_density_idx, wind_speed_mph, wind_dir_deg, wind_out_mph,
                         has_precip, is_dome, condition)
                    VALUES
                        (:game_pk, :game_date, :venue_id, :temp_f, :humidity_pct, :pressure_hpa,
                         :air_density_idx, :wind_speed_mph, :wind_dir_deg, :wind_out_mph,
                         :has_precip, :is_dome, :condition)
                    ON CONFLICT (game_pk) DO UPDATE SET
                        temp_f = EXCLUDED.temp_f,
                        humidity_pct = EXCLUDED.humidity_pct,
                        pressure_hpa = EXCLUDED.pressure_hpa,
                        air_density_idx = EXCLUDED.air_density_idx,
                        wind_speed_mph = EXCLUDED.wind_speed_mph,
                        wind_dir_deg = EXCLUDED.wind_dir_deg,
                        wind_out_mph = EXCLUDED.wind_out_mph,
                        has_precip = EXCLUDED.has_precip,
                        is_dome = EXCLUDED.is_dome,
                        condition = EXCLUDED.condition,
                        fetched_at = NOW()
                """),
                r,
            )
    return len(records)


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------


def backfill_weather(engine, api_key: str, start_date: str, end_date: str, delay_s: float = 1.1):
    """Backfill weather for completed games between start_date and end_date.

    Skips games that already have weather data. Processes in date order.
    """
    query = text("""
        SELECT gs.game_id          AS game_pk,
               gs.game_date,
               gs.venue_id,
               gs.game_datetime
        FROM mlb_game_schedule gs
        LEFT JOIN mlb_game_weather gw ON gw.game_pk = gs.game_id
        WHERE gs.game_date BETWEEN :start AND :end
          AND gs.game_type = 'R'
          AND gw.game_pk IS NULL
        ORDER BY gs.game_date
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"start": start_date, "end": end_date}).fetchall()

    logger.info("Backfill: %d games need weather data", len(rows))

    batch: list[dict] = []
    for i, row in enumerate(rows):
        game_dt = row.game_datetime
        if game_dt is None:
            # Default to 7 PM local time if no datetime stored
            game_dt = datetime(row.game_date.year, row.game_date.month, row.game_date.day, 19, 0, 0)
        elif not isinstance(game_dt, datetime):
            game_dt = datetime(game_dt.year, game_dt.month, game_dt.day, 19, 0, 0)

        record = fetch_weather_for_game(row.game_pk, game_dt, row.venue_id or 0, api_key)
        if record:
            batch.append(record)

        if len(batch) >= 50:
            saved = upsert_weather(engine, batch)
            logger.info("  Saved %d records (processed %d/%d)", saved, i + 1, len(rows))
            batch = []

        time.sleep(delay_s)

    if batch:
        upsert_weather(engine, batch)

    logger.info("Backfill complete.")


# ---------------------------------------------------------------------------
# Daily forecast
# ---------------------------------------------------------------------------


def fetch_today_forecast(engine, api_key: str) -> int:
    """Fetch forecast weather for today's scheduled games and upsert."""
    today = date.today().isoformat()
    query = text("""
        SELECT gs.game_id  AS game_pk,
               gs.game_date,
               gs.venue_id,
               gs.game_datetime
        FROM mlb_game_schedule gs
        WHERE gs.game_date = :today
          AND gs.game_type = 'R'
        ORDER BY gs.game_datetime NULLS LAST
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"today": today}).fetchall()

    logger.info("Forecast: %d games scheduled for %s", len(rows), today)
    records: list[dict] = []
    for row in rows:
        game_dt = row.game_datetime
        if game_dt is None:
            t = date.today()
            game_dt = datetime(t.year, t.month, t.day, 19, 0, 0)
        elif not isinstance(game_dt, datetime):
            game_dt = datetime(game_dt.year, game_dt.month, game_dt.day, 19, 0, 0)

        record = fetch_weather_for_game(row.game_pk, game_dt, row.venue_id or 0, api_key)
        if record:
            records.append(record)
        time.sleep(0.5)

    saved = upsert_weather(engine, records)
    logger.info("Saved weather for %d games", saved)
    return saved


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def get_api_key() -> str:
    """Get OpenWeatherMap API key from environment."""
    key = os.environ.get("OPENWEATHERMAP_API_KEY", "")
    if not key:
        raise RuntimeError("OPENWEATHERMAP_API_KEY not set in environment")
    return key


def main():
    parser = argparse.ArgumentParser(description="MLB Weather Scraper")
    parser.add_argument("--backfill", action="store_true", help="Backfill historical weather")
    parser.add_argument("--today", action="store_true", help="Fetch today's forecast")
    parser.add_argument("--start", default="2022-04-01", help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument(
        "--end", default=date.today().isoformat(), help="Backfill end date (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    engine = get_engine()
    api_key = get_api_key()

    if args.backfill:
        backfill_weather(engine, api_key, args.start, args.end)
    elif args.today:
        fetch_today_forecast(engine, api_key)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
