"""GSOD (Global Summary of the Day) data fetching for USAF-WBAN stations."""

import csv
import logging
from datetime import date
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.download import download_with_retry
from get_weather_data.weather.units import KNOTS_TO_MS, f_to_c

logger = logging.getLogger("get_weather_data")

GSOD_URL = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/{station_id}.csv"

GSOD_COLUMNS = [
    ("TEMP", "temp"),
    ("DEWP", "dewpoint"),
    ("SLP", "sea_level_pressure"),
    ("STP", "station_pressure"),
    ("VISIB", "visibility"),
    ("WDSP", "wind_speed"),
    ("MXSPD", "max_wind_speed"),
    ("GUST", "gust"),
    ("MAX", "max_temp"),
    ("MIN", "min_temp"),
    ("PRCP", "precipitation"),
    ("SNDP", "snow_depth"),
]


def _get_gsod_file_path(station_id: str, year: int) -> Path:
    """Get path to cached GSOD file."""
    config = get_config()
    return config.gsod_cache_dir / str(year) / f"{station_id}.csv"


def _ensure_gsod_file(station_id: str, year: int) -> Path | None:
    """Download GSOD file if not cached."""
    file_path = _get_gsod_file_path(station_id, year)

    if file_path.exists():
        return file_path

    file_path.parent.mkdir(parents=True, exist_ok=True)

    url = GSOD_URL.format(year=year, station_id=station_id.replace("-", ""))
    result = download_with_retry(url, file_path)
    return result


def get_gsod_data(
    station_id: str,
    target_date: date,
    convert_units: bool = True,
) -> dict[str, float | None]:
    """Get GSOD data for a station and date.

    Args:
        station_id: USAF-WBAN station ID (e.g., "722950-23174").
        target_date: Date to get data for.
        convert_units: If True, convert temperature to Celsius and wind to m/s.

    Returns:
        Dict mapping field names to values.
    """
    year = target_date.year
    file_path = _ensure_gsod_file(station_id, year)

    if file_path is None:
        return {col[1]: None for col in GSOD_COLUMNS}

    date_str = target_date.strftime("%Y-%m-%d")
    values: dict[str, float | None] = {col[1]: None for col in GSOD_COLUMNS}

    with open(file_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("DATE") == date_str:
                for gsod_name, field_name in GSOD_COLUMNS:
                    raw = row.get(gsod_name, "").strip()
                    if raw and raw not in ("9999.9", "999.9", "99.99"):
                        try:
                            value = float(raw)
                            if convert_units:
                                if field_name in (
                                    "temp",
                                    "max_temp",
                                    "min_temp",
                                    "dewpoint",
                                ):
                                    value = f_to_c(value)
                                elif field_name in (
                                    "wind_speed",
                                    "max_wind_speed",
                                    "gust",
                                ):
                                    value = value * KNOTS_TO_MS
                            values[field_name] = value
                        except ValueError:
                            pass
                break

    return values
