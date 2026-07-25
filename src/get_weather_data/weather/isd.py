"""ISD-Lite hourly data for USAF-WBAN stations.

Integrated Surface Database (ISD) Lite ships one whitespace-delimited,
gzipped file per station-year at NCEI. Each row is one hour (UTC) with
air temperature, dew point, sea-level pressure, wind direction and
speed, a sky-cover code, and 1- and 6-hour precipitation. Numeric
fields are scaled by 10 (temp ``78`` = 7.8 °C) and ``-9999`` marks a
missing value.
"""

import gzip
import logging
from datetime import UTC, date, datetime
from pathlib import Path

from get_weather_data.core.cache import is_fresh, year_is_immutable
from get_weather_data.core.config import get_config
from get_weather_data.core.download import download_with_retry

logger = logging.getLogger("get_weather_data")

ISD_LITE_URL = (
    "https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/{year}/{station_id}-{year}.gz"
)

# (column index, field name, scale). Missing = -9999. wind_direction and
# sky_condition are unscaled integers; the rest are tenths.
_TENTHS = 10.0
_FIELDS: list[tuple[int, str, float]] = [
    (4, "temp", _TENTHS),
    (5, "dewpoint", _TENTHS),
    (6, "sea_level_pressure", _TENTHS),
    (7, "wind_direction", 1.0),
    (8, "wind_speed", _TENTHS),
    (9, "sky_condition", 1.0),
    (10, "precip_1h", _TENTHS),
    (11, "precip_6h", _TENTHS),
]
_INT_FIELDS = frozenset({"wind_direction", "sky_condition"})


def _get_isd_file_path(station_id: str, year: int) -> Path:
    """Path to the cached ISD-Lite file for a station-year."""
    config = get_config()
    return config.cache_dir / "isd" / str(year) / f"{station_id}.txt.gz"


def _ensure_isd_file(station_id: str, year: int) -> Path | None:
    """Download the ISD-Lite file if not cached (or stale for a live year).

    Args:
        station_id: USAF-WBAN station ID (e.g. "725030-14732").
        year: Calendar year to ensure.

    Returns:
        Path to the cached file, or None if it could not be downloaded.
    """
    file_path = _get_isd_file_path(station_id, year)

    if file_path.exists() and (
        year_is_immutable(year) or is_fresh(file_path, get_config().cache_max_age_days)
    ):
        return file_path

    file_path.parent.mkdir(parents=True, exist_ok=True)
    url = ISD_LITE_URL.format(year=year, station_id=station_id)
    return download_with_retry(url, file_path)


def _parse_line(
    line: str, target_date: date
) -> dict[str, float | int | datetime | None] | None:
    """Parse one ISD-Lite row into a metric hourly record for a date.

    Args:
        line: One whitespace-delimited ISD-Lite row.
        target_date: Only rows on this UTC date are returned.

    Returns:
        A record dict (metric units, missing fields None) with an
        ``observed_at`` UTC datetime, or None if the row is malformed or
        falls on another date.
    """
    parts = line.split()
    if len(parts) < 12:
        return None
    try:
        year, month, day, hour = (int(parts[i]) for i in range(4))
    except ValueError:
        return None
    if (year, month, day) != (target_date.year, target_date.month, target_date.day):
        return None

    record: dict[str, float | int | datetime | None] = {
        "observed_at": datetime(year, month, day, hour, tzinfo=UTC)
    }
    for index, field_name, scale in _FIELDS:
        raw = parts[index]
        if raw == "-9999":
            record[field_name] = None
            continue
        try:
            value = int(raw) / scale
        except ValueError:
            record[field_name] = None
            continue
        record[field_name] = int(value) if field_name in _INT_FIELDS else value
    return record


def get_isd_hourly(
    station_id: str, target_date: date
) -> list[dict[str, float | int | datetime | None]]:
    """Get one day's hourly ISD-Lite records for a station, in metric.

    Args:
        station_id: USAF-WBAN station ID (e.g. "725030-14732").
        target_date: UTC date to read.

    Returns:
        One record dict per available hour (sorted by time), each with
        an ``observed_at`` UTC datetime and metric values (missing
        fields are None). Empty if the station-year file is unavailable.
    """
    file_path = _ensure_isd_file(station_id, target_date.year)
    if file_path is None:
        return []

    records: list[dict[str, float | int | datetime | None]] = []
    with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            record = _parse_line(line, target_date)
            if record is not None:
                records.append(record)
    records.sort(key=lambda r: r["observed_at"])  # type: ignore[arg-type,return-value]
    return records
