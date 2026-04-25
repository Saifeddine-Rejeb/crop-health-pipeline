"""
ingestion/weather_ingestor.py
------------------------------
Fetches hourly weather data from the Open-Meteo API (free, no auth).
Variables: temperature_2m, precipitation, shortwave_radiation (solar).
Writes a parquet per AOI centroid for later enrichment with satellite data.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Centroids of the study AOIs  (lat, lon)
DEFAULT_LOCATIONS = [
    {"name": "AOI-1", "lat": 32.75, "lon": -91.0},
    {"name": "AOI-2", "lat": 32.0, "lon": -90.4},
]

HOURLY_VARS = [
    "temperature_2m",
    "precipitation",
    "shortwave_radiation",
    "relative_humidity_2m",
    "wind_speed_10m",
]


def fetch_weather(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch hourly weather for a single coordinate from Open-Meteo.
    Retries on transient failures (retry on critical step ✅).
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_VARS),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "UTC",
    }

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Open-Meteo request lat=%.3f lon=%.3f attempt=%d", lat, lon, attempt
            )
            resp = httpx.get(OPEN_METEO_URL, params=params, timeout=30)
            resp.raise_for_status()
            break
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            logger.warning("Weather request failed (attempt %d): %s", attempt, exc)
            if attempt == max_retries:
                logger.error("Max retries exceeded for lat=%s lon=%s", lat, lon)
                raise
            time.sleep(2**attempt)

    data = resp.json()
    hourly = data.get("hourly", {})
    df = pd.DataFrame(hourly)
    df.rename(columns={"time": "datetime"}, inplace=True)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["lat"] = lat
    df["lon"] = lon
    return df


def ingest_weather(
    output_path: str = "data/weather.parquet",
    locations: Optional[list] = None,
    days_back: int = 7,
) -> str:
    """
    Main ingestion entry point called by the Airflow DAG.

    Returns:
        output_path (for XCom passing in Airflow)
    """
    locations = locations or DEFAULT_LOCATIONS
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    all_dfs = []
    for loc in locations:
        try:
            df = fetch_weather(loc["lat"], loc["lon"], start_date, end_date)
            df["location_name"] = loc["name"]
            all_dfs.append(df)
            logger.info("Fetched %d hourly rows for %s", len(df), loc["name"])
        except Exception as exc:
            logger.error("Failed fetching weather for %s: %s", loc["name"], exc)

    if not all_dfs:
        logger.warning("No weather data fetched — writing empty file")
        combined = pd.DataFrame()
    else:
        combined = pd.concat(all_dfs, ignore_index=True)

    combined.to_parquet(output_path, index=False)
    logger.info("Wrote %d weather rows to %s", len(combined), output_path)
    return output_path


if __name__ == "__main__":
    ingest_weather()
