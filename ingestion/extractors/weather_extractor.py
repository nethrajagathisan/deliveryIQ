"""Extractor for Indian city weather data from the Open-Meteo archive API.

Fetches daily historical weather for the six Indian cities used across the
DeliveryIQ pipeline and lands flattened Parquet files for the Bronze layer.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ingestion.utils.logger import get_logger

if TYPE_CHECKING:
    from ingestion.loaders.gcs_loader import GCSLoader

log = get_logger(__name__)

INDIAN_CITIES: dict[str, dict] = {
    "delhi": {"lat": 28.6448, "lon": 77.2167, "timezone": "Asia/Kolkata"},
    "mumbai": {"lat": 18.9388, "lon": 72.8354, "timezone": "Asia/Kolkata"},
    "bangalore": {"lat": 12.9716, "lon": 77.5946, "timezone": "Asia/Kolkata"},
    "chennai": {"lat": 13.0827, "lon": 80.2707, "timezone": "Asia/Kolkata"},
    "hyderabad": {"lat": 17.3850, "lon": 78.4867, "timezone": "Asia/Kolkata"},
    "pune": {"lat": 18.5204, "lon": 73.8567, "timezone": "Asia/Kolkata"},
}

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
    "weathercode",
]

WMO_WEATHER_CODES: dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Foggy",
    48: "Icy fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    80: "Slight showers",
    81: "Moderate showers",
    82: "Violent showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail",
    99: "Severe thunderstorm",
}

RAINY_PRECIPITATION_THRESHOLD_MM = 2.5
HEAVY_RAIN_PRECIPITATION_THRESHOLD_MM = 10.0
EXTREME_HEAT_TEMPERATURE_THRESHOLD_C = 40.0

_INTER_CITY_SLEEP_SECONDS = 1

_API_VERSION = "archive-api.open-meteo.com"


class IndianCityWeatherExtractor:
    """Extracts daily historical weather for Indian cities from Open-Meteo."""

    def __init__(self, gcs_loader: "GCSLoader | None" = None) -> None:
        self.gcs_loader = gcs_loader
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "DeliveryIQ-Analytics/1.0 (portfolio project)"}
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30))
    def fetch_city_weather(self, city: str, start_date: str, end_date: str) -> dict:
        """Fetch raw daily weather for a single city from the Open-Meteo archive API.

        Retries up to 3 times with exponential backoff on request failures.

        Raises:
            ValueError: If the response does not contain a "daily" key.

        Returns:
            The raw JSON response as a dict (untransformed).
        """
        log.info(
            f"fetching weather for {city} from {start_date} to {end_date}",
            extra={
                "extra_fields": {
                    "city": city,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            },
        )

        coords = INDIAN_CITIES[city]
        response = self.session.get(
            self.base_url,
            params={
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "start_date": start_date,
                "end_date": end_date,
                "timezone": coords["timezone"],
                "daily": DAILY_VARIABLES,
            },
        )
        response.raise_for_status()
        data = response.json()

        if "daily" not in data:
            raise ValueError(f"Open-Meteo response for {city} is missing 'daily' key")

        return data

    def fetch_all_cities(self, start_date: str, end_date: str) -> dict[str, dict]:
        """Fetch raw weather for every city in INDIAN_CITIES.

        Sleeps 1 second between cities to respect API rate limits. If a
        city-level fetch fails after retries, logs the error and continues
        with the remaining cities.

        Returns:
            {city_name: raw_api_response} for cities that succeeded.
        """
        results: dict[str, dict] = {}
        cities = list(INDIAN_CITIES.keys())

        for i, city in enumerate(cities):
            try:
                results[city] = self.fetch_city_weather(city, start_date, end_date)
            except Exception:
                log.exception(
                    "failed to fetch weather for city",
                    extra={"extra_fields": {"city": city}},
                )

            if i < len(cities) - 1:
                time.sleep(_INTER_CITY_SLEEP_SECONDS)

        return results

    def flatten_to_dataframe(self, city: str, raw_response: dict) -> pd.DataFrame:
        """Flatten an Open-Meteo daily response into one row per date.

        Adds derived columns ``is_rainy``, ``weather_category``,
        ``weather_description``, plus ingestion metadata.
        """
        daily = raw_response["daily"]

        df = pd.DataFrame(
            {
                "city": city,
                "weather_date": daily["time"],
                "temperature_max": daily["temperature_2m_max"],
                "temperature_min": daily["temperature_2m_min"],
                "precipitation_sum": daily["precipitation_sum"],
                "windspeed_max": daily["windspeed_10m_max"],
                "weather_code": daily["weathercode"],
            }
        )

        df["is_rainy"] = df["precipitation_sum"] > RAINY_PRECIPITATION_THRESHOLD_MM
        df["weather_category"] = df.apply(
            lambda row: self._classify_weather(row["precipitation_sum"], row["temperature_max"]),
            axis=1,
        )
        df["weather_description"] = df["weather_code"].map(WMO_WEATHER_CODES)

        df["_fetched_at"] = datetime.now(timezone.utc).isoformat()
        df["_api_version"] = _API_VERSION

        return df

    @staticmethod
    def _classify_weather(precipitation_sum: float, temperature_max: float) -> str:
        """Classify a day's weather into clear/light_rain/heavy_rain/extreme_heat."""
        if precipitation_sum > HEAVY_RAIN_PRECIPITATION_THRESHOLD_MM:
            return "heavy_rain"
        if precipitation_sum > RAINY_PRECIPITATION_THRESHOLD_MM:
            return "light_rain"
        if temperature_max > EXTREME_HEAT_TEMPERATURE_THRESHOLD_C:
            return "extreme_heat"
        return "clear"

    def extract_and_save(self, start_date: str, end_date: str, output_dir: Path) -> dict:
        """Fetch weather for all cities and save flattened Parquet files.

        Writes ``{output_dir}/{city}/date_range={start_date}_{end_date}/weather.parquet``
        for each city that was successfully fetched.

        Returns:
            {city: {rows, date_range, file_path}} for cities that were saved.
        """
        output_dir = Path(output_dir)
        date_range = f"{start_date}_{end_date}"

        raw_responses = self.fetch_all_cities(start_date, end_date)

        manifest: dict[str, dict] = {}
        for city, raw_response in raw_responses.items():
            df = self.flatten_to_dataframe(city, raw_response)

            city_dir = output_dir / city / f"date_range={date_range}"
            city_dir.mkdir(parents=True, exist_ok=True)

            file_path = city_dir / "weather.parquet"
            df.to_parquet(file_path, index=False)

            manifest[city] = {
                "rows": len(df),
                "date_range": date_range,
                "file_path": str(file_path),
            }

        log.info(
            "extracted and saved weather data",
            extra={"extra_fields": {"cities": list(manifest.keys()), "date_range": date_range}},
        )
        return manifest
