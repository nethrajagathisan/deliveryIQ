"""Unit tests for IndianCityWeatherExtractor. No real HTTP calls."""

from __future__ import annotations

import requests
from tenacity import RetryError

from ingestion.extractors.weather_extractor import (
    RAINY_PRECIPITATION_THRESHOLD_MM,
    IndianCityWeatherExtractor,
)


def _raw_response(
    dates: list[str],
    temp_max: list[float],
    temp_min: list[float],
    precipitation: list[float],
    windspeed: list[float],
    weathercode: list[int],
) -> dict:
    return {
        "daily": {
            "time": dates,
            "temperature_2m_max": temp_max,
            "temperature_2m_min": temp_min,
            "precipitation_sum": precipitation,
            "windspeed_10m_max": windspeed,
            "weathercode": weathercode,
        }
    }


class TestFlattenToDataframe:
    def test_flatten_to_dataframe_produces_correct_columns(self):
        extractor = IndianCityWeatherExtractor()
        raw = _raw_response(
            dates=["2023-01-01", "2023-01-02"],
            temp_max=[30.0, 32.0],
            temp_min=[20.0, 21.0],
            precipitation=[0.0, 5.0],
            windspeed=[10.0, 15.0],
            weathercode=[0, 61],
        )

        df = extractor.flatten_to_dataframe("bangalore", raw)

        expected_columns = [
            "city",
            "weather_date",
            "temperature_max",
            "temperature_min",
            "precipitation_sum",
            "windspeed_max",
            "weather_code",
            "is_rainy",
            "weather_category",
            "weather_description",
            "_fetched_at",
            "_api_version",
        ]
        for col in expected_columns:
            assert col in df.columns

        assert len(df) == 2
        assert (df["city"] == "bangalore").all()
        assert (df["_api_version"] == "archive-api.open-meteo.com").all()

    def test_is_rainy_flag_set_correctly(self):
        extractor = IndianCityWeatherExtractor()
        raw = _raw_response(
            dates=["2023-01-01", "2023-01-02", "2023-01-03"],
            temp_max=[28.0, 28.0, 28.0],
            temp_min=[20.0, 20.0, 20.0],
            precipitation=[0.0, RAINY_PRECIPITATION_THRESHOLD_MM, 5.0],
            windspeed=[10.0, 10.0, 10.0],
            weathercode=[0, 0, 61],
        )

        df = extractor.flatten_to_dataframe("mumbai", raw)

        assert df.loc[0, "is_rainy"] is False or df.loc[0, "is_rainy"] == False  # noqa: E712
        assert not df.loc[1, "is_rainy"]
        assert df.loc[2, "is_rainy"]

    def test_weather_category_classification(self):
        extractor = IndianCityWeatherExtractor()
        raw = _raw_response(
            dates=["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
            temp_max=[28.0, 28.0, 28.0, 42.0],
            temp_min=[20.0, 20.0, 20.0, 30.0],
            precipitation=[0.0, 5.0, 15.0, 0.0],
            windspeed=[10.0, 10.0, 10.0, 10.0],
            weathercode=[0, 61, 65, 0],
        )

        df = extractor.flatten_to_dataframe("chennai", raw)

        assert df.loc[0, "weather_category"] == "clear"
        assert df.loc[1, "weather_category"] == "light_rain"
        assert df.loc[2, "weather_category"] == "heavy_rain"
        assert df.loc[3, "weather_category"] == "extreme_heat"


class TestFetchCityWeather:
    def test_fetch_city_weather_retries_on_connection_error(self, mocker):
        extractor = IndianCityWeatherExtractor()

        success_response = mocker.Mock()
        success_response.raise_for_status.return_value = None
        success_response.json.return_value = {"daily": {"time": ["2023-01-01"]}}

        mock_get = mocker.patch.object(
            extractor.session,
            "get",
            side_effect=[
                requests.ConnectionError("boom"),
                requests.ConnectionError("boom"),
                success_response,
            ],
        )

        result = extractor.fetch_city_weather("bangalore", "2023-01-01", "2023-01-07")

        assert result == {"daily": {"time": ["2023-01-01"]}}
        assert mock_get.call_count == 3

    def test_fetch_city_weather_raises_value_error_when_daily_missing(self, mocker):
        extractor = IndianCityWeatherExtractor()

        bad_response = mocker.Mock()
        bad_response.raise_for_status.return_value = None
        bad_response.json.return_value = {"error": "bad request"}

        mocker.patch.object(extractor.session, "get", return_value=bad_response)

        # The @retry decorator exhausts all attempts on ValueError and
        # re-raises it wrapped in a tenacity.RetryError.
        try:
            extractor.fetch_city_weather("bangalore", "2023-01-01", "2023-01-07")
            assert False, "expected RetryError"
        except RetryError as exc:
            assert isinstance(exc.last_attempt.exception(), ValueError)
