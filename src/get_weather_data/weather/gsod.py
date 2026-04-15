"""GSOD (Global Summary of the Day) data fetching for USAF-WBAN stations."""

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.download import download_with_retry

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


@dataclass
class GSODData:
    """GSOD weather observation data."""

    station_id: str
    date: date
    temp: float | None = None
    dewpoint: float | None = None
    sea_level_pressure: float | None = None
    station_pressure: float | None = None
    visibility: float | None = None
    wind_speed: float | None = None
    max_wind_speed: float | None = None
    gust: float | None = None
    max_temp: float | None = None
    min_temp: float | None = None
    precipitation: float | None = None
    snow_depth: float | None = None


def _f2c(f: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return (f - 32) * 5.0 / 9.0


def _kn2ms(kn: float) -> float:
    """Convert knots to meters per second."""
    return 0.51444 * kn


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
    import csv

    year = target_date.year
    file_path = _ensure_gsod_file(station_id, year)

    if file_path is None:
        return {col[1]: None for col in GSOD_COLUMNS}

    date_str = target_date.strftime("%Y-%m-%d")
    values: dict[str, float | None] = {col[1]: None for col in GSOD_COLUMNS}

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
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
                                    value = _f2c(value)
                                elif field_name in (
                                    "wind_speed",
                                    "max_wind_speed",
                                    "gust",
                                ):
                                    value = _kn2ms(value)
                            values[field_name] = value
                        except ValueError:
                            pass
                break

    return values


def get_gsod_data_range(
    station_id: str,
    start_date: date,
    end_date: date,
    convert_units: bool = True,
) -> list[dict]:
    """Get GSOD data for a station over a date range.

    Args:
        station_id: USAF-WBAN station ID.
        start_date: Start date.
        end_date: End date.
        convert_units: If True, convert units to metric.

    Returns:
        List of dicts with 'date' and 'values' keys.
    """
    from datetime import timedelta

    results = []
    current = start_date
    while current <= end_date:
        values = get_gsod_data(station_id, current, convert_units)
        results.append({"date": current, "values": values})
        current += timedelta(days=1)

    return results


def get_gsod_data_object(
    station_id: str,
    target_date: date,
    convert_units: bool = True,
) -> GSODData:
    """Get GSOD data as a dataclass object.

    Args:
        station_id: USAF-WBAN station ID.
        target_date: Date to get data for.
        convert_units: If True, convert units to metric.

    Returns:
        GSODData object with weather values.
    """
    values = get_gsod_data(station_id, target_date, convert_units)
    return GSODData(station_id=station_id, date=target_date, **values)
