"""
MLB Weather Scraper
====================
Fetches weather data for MLB games from Open-Meteo (free, no API key required).

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
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

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

# (lat, lng) for Open-Meteo calls
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
    15:   0,    # Chase Field — N
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
# Open-Meteo weather fetching (free, no API key required)
# ---------------------------------------------------------------------------

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"

# Open-Meteo hourly variable names (same for both archive and forecast endpoints)
HOURLY_VARS = (
    "temperature_2m,relative_humidity_2m,precipitation,"
    "wind_speed_10m,wind_direction_10m,surface_pressure"
)


def _parse_open_meteo_response(data: dict, target_dt: datetime) -> dict | None:
    """Extract the weather closest to target_dt from an Open-Meteo hourly response.

    Open-Meteo returns hourly arrays in UTC. target_dt should be UTC.
    """
    hourly = data.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return None

    # Find index of closest time to target_dt (naive datetime, UTC)
    target_naive = target_dt.replace(tzinfo=None) if target_dt.tzinfo else target_dt
    best_idx = 0
    best_diff = float("inf")
    for i, t_str in enumerate(times):
        try:
            t_dt = datetime.strptime(t_str, "%Y-%m-%dT%H:%M")
            diff = abs((t_dt - target_naive).total_seconds())
            if diff < best_diff:
                best_diff = diff
                best_idx = i
        except ValueError:
            continue
    idx = best_idx

    def _get(key: str, default: float) -> float:
        arr = hourly.get(key, [])
        val = arr[idx] if arr and idx < len(arr) else None
        return float(val) if val is not None else default

    temp_c = _get("temperature_2m", 20.0)
    humidity = _get("relative_humidity_2m", 50.0)
    precip_mm = _get("precipitation", 0.0)
    wind_kmh = _get("wind_speed_10m", 0.0)
    wind_deg = int(_get("wind_direction_10m", 0))
    pressure = _get("surface_pressure", 1013.25)

    # Convert units
    temp_f = temp_c * 9 / 5 + 32
    wind_mph = wind_kmh * 0.621371
    has_precip = precip_mm > 0.1  # >0.1 mm threshold

    condition = f"{temp_f:.0f}°F, {wind_mph:.0f} mph wind"
    if has_precip:
        condition += f", precip {precip_mm:.1f}mm"

    return {
        "temp_f": round(temp_f, 1),
        "humidity_pct": round(humidity, 1),
        "pressure_hpa": round(pressure, 2),
        "wind_speed_mph": round(wind_mph, 1),
        "wind_dir_deg": wind_deg,
        "has_precip": has_precip,
        "condition": condition,
    }


def fetch_weather_for_game(
    game_pk: int,
    game_dt: datetime,
    venue_id: int,
) -> dict | None:
    """Fetch and compute weather for a single game via Open-Meteo (no API key needed).

    Uses the archive endpoint for past games and the forecast endpoint for future games.
    Returns a dict ready for upsert into mlb_game_weather, or None on failure.
    """
    if venue_id in DOME_VENUE_IDS:
        return {"game_pk": game_pk, "game_date": game_dt.date(), "venue_id": venue_id, **DOME_NEUTRAL}

    lat, lng = PARK_LAT_LNG.get(venue_id, (None, None))
    if lat is None:
        logger.warning("No lat/lng for venue_id=%d (game_pk=%d), skipping", venue_id, game_pk)
        return None

    game_date_str = game_dt.strftime("%Y-%m-%d")
    now_utc = datetime.utcnow()
    is_historical = game_dt.replace(tzinfo=None) < now_utc - timedelta(hours=6)

    if is_historical:
        url = OPEN_METEO_ARCHIVE
        params = {
            "latitude": lat,
            "longitude": lng,
            "start_date": game_date_str,
            "end_date": game_date_str,
            "hourly": HOURLY_VARS,
            "wind_speed_unit": "kmh",
            "timezone": "UTC",
        }
    else:
        url = OPEN_METEO_FORECAST
        params = {
            "latitude": lat,
            "longitude": lng,
            "start_date": game_date_str,
            "end_date": (game_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            "hourly": HOURLY_VARS,
            "wind_speed_unit": "kmh",
            "timezone": "UTC",
            "forecast_days": 3,
        }

    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        raw = _parse_open_meteo_response(resp.json(), game_dt)
    except Exception as e:
        logger.warning("Open-Meteo request failed game_pk=%d venue=%d: %s", game_pk, venue_id, e)
        return None

    if raw is None:
        logger.warning("No hourly data returned for game_pk=%d venue=%d", game_pk, venue_id)
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
# Backfill — batched by venue × season (120 API calls for 4-year full backfill)
# ---------------------------------------------------------------------------


def _fetch_open_meteo_range(lat: float, lng: float, start_date: str, end_date: str) -> dict | None:
    """Fetch a range of hourly weather data from Open-Meteo archive.

    Returns the raw JSON response (hourly arrays), or None on failure.
    One call covers an entire season — caller extracts individual game hours.
    """
    try:
        resp = requests.get(
            OPEN_METEO_ARCHIVE,
            params={
                "latitude": lat,
                "longitude": lng,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": HOURLY_VARS,
                "wind_speed_unit": "kmh",
                "timezone": "UTC",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("Open-Meteo range fetch failed (%s to %s lat=%.3f): %s", start_date, end_date, lat, e)
        return None


def backfill_weather(engine, start_date: str, end_date: str, delay_s: float = 0.5):
    """Backfill weather for completed games between start_date and end_date.

    Strategy: batch by venue × month to use ~1 API call per venue-month instead of
    1 call per game. For a full 2022–2025 backfill (~9,700 games) this uses
    ~120–150 API calls instead of ~9,700 — well within Open-Meteo's free tier.

    Skips games that already have weather data.
    """
    query = text("""
        SELECT gs.game_id          AS game_pk,
               gs.game_date,
               gs.venue_id,
               gs.game_time_utc
        FROM mlb_game_schedule gs
        LEFT JOIN mlb_game_weather gw ON gw.game_pk = gs.game_id
        WHERE gs.game_date BETWEEN :start AND :end
          AND gs.game_type = 'R'
          AND gw.game_pk IS NULL
        ORDER BY gs.venue_id, gs.game_date
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"start": start_date, "end": end_date}).fetchall()

    logger.info("Backfill: %d games need weather data", len(rows))
    if not rows:
        return

    # Group games by venue_id → then process in monthly chunks per venue
    from collections import defaultdict
    by_venue: dict[int, list] = defaultdict(list)
    for row in rows:
        by_venue[row.venue_id or 0].append(row)

    total_saved = 0
    for venue_id, venue_rows in by_venue.items():
        # Dome venues: no API call needed
        if venue_id in DOME_VENUE_IDS:
            dome_records = []
            for row in venue_rows:
                gd = row.game_date
                game_dt = datetime(gd.year, gd.month, gd.day, 19, 0, 0)
                dome_records.append(
                    {"game_pk": row.game_pk, "game_date": gd, "venue_id": venue_id, **DOME_NEUTRAL}
                )
            total_saved += upsert_weather(engine, dome_records)
            logger.info("  Venue %d (dome): saved %d records", venue_id, len(dome_records))
            continue

        lat, lng = PARK_LAT_LNG.get(venue_id, (None, None))
        if lat is None:
            logger.warning("  No lat/lng for venue_id=%d, skipping %d games", venue_id, len(venue_rows))
            continue

        # Group this venue's games by year-month for chunked fetching
        by_month: dict[str, list] = defaultdict(list)
        for row in venue_rows:
            key = row.game_date.strftime("%Y-%m")
            by_month[key].append(row)

        for month_key, month_rows in sorted(by_month.items()):
            # Fetch the entire month in one API call
            year, mon = int(month_key[:4]), int(month_key[5:])
            # Last day of month
            if mon == 12:
                last_day = date(year + 1, 1, 1) - timedelta(days=1)
            else:
                last_day = date(year, mon + 1, 1) - timedelta(days=1)

            month_start = f"{month_key}-01"
            month_end = last_day.strftime("%Y-%m-%d")

            logger.info(
                "  Venue %d: fetching %s → %s (%d games)",
                venue_id, month_start, month_end, len(month_rows),
            )
            hourly_data = _fetch_open_meteo_range(lat, lng, month_start, month_end)
            if hourly_data is None:
                logger.warning("    Skipping month %s for venue %d", month_key, venue_id)
                time.sleep(delay_s)
                continue

            records = []
            for row in month_rows:
                game_dt = row.game_time_utc
                if game_dt is None:
                    gd = row.game_date
                    game_dt = datetime(gd.year, gd.month, gd.day, 19, 0, 0)
                elif not isinstance(game_dt, datetime):
                    game_dt = datetime(game_dt.year, game_dt.month, game_dt.day, 19, 0, 0)

                raw = _parse_open_meteo_response(hourly_data, game_dt)
                if raw is None:
                    continue

                air_density = compute_air_density_idx(raw["temp_f"], raw["humidity_pct"], raw["pressure_hpa"])
                wind_out = compute_wind_out_mph(raw["wind_speed_mph"], raw["wind_dir_deg"], venue_id)

                records.append({
                    "game_pk": row.game_pk,
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
                })

            saved = upsert_weather(engine, records)
            total_saved += saved
            logger.info("    Saved %d records", saved)
            time.sleep(delay_s)

    logger.info("Backfill complete: %d total records saved.", total_saved)


# ---------------------------------------------------------------------------
# Daily forecast
# ---------------------------------------------------------------------------


def fetch_today_forecast(engine) -> int:
    """Fetch forecast weather for today's scheduled games and upsert."""
    today = date.today().isoformat()
    query = text("""
        SELECT gs.game_id  AS game_pk,
               gs.game_date,
               gs.venue_id,
               gs.game_time_utc
        FROM mlb_game_schedule gs
        WHERE gs.game_date = :today
          AND gs.game_type = 'R'
        ORDER BY gs.game_time_utc NULLS LAST
    """)
    with engine.connect() as conn:
        rows = conn.execute(query, {"today": today}).fetchall()

    logger.info("Forecast: %d games scheduled for %s", len(rows), today)
    records: list[dict] = []
    for row in rows:
        game_dt = row.game_time_utc
        if game_dt is None:
            t = date.today()
            game_dt = datetime(t.year, t.month, t.day, 19, 0, 0)
        elif not isinstance(game_dt, datetime):
            game_dt = datetime(game_dt.year, game_dt.month, game_dt.day, 19, 0, 0)

        record = fetch_weather_for_game(row.game_pk, game_dt, row.venue_id or 0)
        if record:
            records.append(record)
        time.sleep(0.15)

    saved = upsert_weather(engine, records)
    logger.info("Saved weather for %d games", saved)
    return saved


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="MLB Weather Scraper (Open-Meteo, no key needed)")
    parser.add_argument("--backfill", action="store_true", help="Backfill historical weather")
    parser.add_argument("--today", action="store_true", help="Fetch today's forecast")
    parser.add_argument("--start", default="2022-04-01", help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument(
        "--end", default=date.today().isoformat(), help="Backfill end date (YYYY-MM-DD)"
    )
    args = parser.parse_args()

    engine = get_engine()

    if args.backfill:
        backfill_weather(engine, args.start, args.end)
    elif args.today:
        fetch_today_forecast(engine)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
