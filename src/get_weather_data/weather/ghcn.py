"""GHCN (Global Historical Climatology Network) daily data fetching."""

import csv
import gzip
import logging
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.download import download_with_retry

logger = logging.getLogger("get_weather_data")

GHCN_BY_YEAR_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"
GHCN_STATION_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/all/{station_id}.dly"

GHCN_ELEMENTS = [
    "AWND",  # Average daily wind speed
    "PRCP",  # Precipitation
    "SNOW",  # Snowfall
    "SNWD",  # Snow depth
    "TMAX",  # Maximum temperature
    "TMIN",  # Minimum temperature
    "TOBS",  # Temperature at observation time
    "TAVG",  # Average temperature
]


@dataclass
class GHCNData:
    """GHCN weather observation data."""

    station_id: str
    date: date
    element: str
    value: float | None
    m_flag: str
    q_flag: str
    s_flag: str


def _get_ghcn_db_path(year: int) -> Path:
    """Get path to GHCN database for a year."""
    config = get_config()
    return config.ghcn_cache_dir / f"ghcn_{year}.sqlite3"


def _ensure_ghcn_database(year: int) -> Path:
    """Ensure GHCN database exists for a year, downloading if needed."""
    db_path = _get_ghcn_db_path(year)

    if db_path.exists():
        return db_path

    config = get_config()
    gz_path = config.ghcn_cache_dir / f"{year}.csv.gz"

    if not gz_path.exists():
        url = GHCN_BY_YEAR_URL.format(year=year)
        result = download_with_retry(url, gz_path)
        if result is None:
            raise RuntimeError(f"Failed to download GHCN data for {year}")

    logger.info(f"Building GHCN database for {year}...")

    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS ghcn_{year} (
                id VARCHAR(12) NOT NULL,
                date VARCHAR(8) NOT NULL,
                element VARCHAR(4),
                value VARCHAR(6),
                m_flag VARCHAR(1),
                q_flag VARCHAR(1),
                s_flag VARCHAR(1),
                obs_time VARCHAR(4)
            )
        """)
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_id_date ON ghcn_{year} (id, date)")
        c.execute("PRAGMA journal_mode = OFF")
        c.execute("PRAGMA synchronous = OFF")
        c.execute("PRAGMA cache_size = 1000000")

        with gzip.open(gz_path, "rt") as f:
            reader = csv.reader(f)
            c.executemany(
                f"""INSERT OR IGNORE INTO ghcn_{year}
                    (id, date, element, value, m_flag, q_flag, s_flag, obs_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                reader,
            )
        conn.commit()
    finally:
        conn.close()

    logger.info(f"GHCN database for {year} ready")
    return db_path


def get_ghcn_data(
    station_id: str,
    target_date: date,
    elements: list[str] | None = None,
) -> dict[str, float | None]:
    """Get GHCN data for a station and date.

    Args:
        station_id: GHCN station ID (e.g., "USW00094728").
        target_date: Date to get data for.
        elements: List of elements to retrieve. Uses default set if None.

    Returns:
        Dict mapping element names to values (tenths of units, or None if missing).
    """
    if elements is None:
        elements = GHCN_ELEMENTS

    year = target_date.year
    db_path = _ensure_ghcn_database(year)

    date_str = target_date.strftime("%Y%m%d")

    values: dict[str, float | None] = {e: None for e in elements}

    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()
        c.execute(
            f"SELECT element, value FROM ghcn_{year} WHERE id = ? AND date = ?",
            (station_id, date_str),
        )
        for row in c:
            element, value = row
            if element in elements and value and value != "-9999":
                values[element] = float(value)
    finally:
        conn.close()

    return values


def get_ghcn_data_range(
    station_id: str,
    start_date: date,
    end_date: date,
    elements: list[str] | None = None,
) -> list[dict]:
    """Get GHCN data for a station over a date range.

    Args:
        station_id: GHCN station ID.
        start_date: Start date.
        end_date: End date.
        elements: List of elements to retrieve.

    Returns:
        List of dicts with 'date' and 'values' keys.
    """
    from datetime import timedelta

    if elements is None:
        elements = GHCN_ELEMENTS

    results = []
    current = start_date
    while current <= end_date:
        values = get_ghcn_data(station_id, current, elements)
        results.append({"date": current, "values": values})
        current += timedelta(days=1)

    return results


def get_ghcn_data_from_file(
    station_id: str,
    target_date: date,
    elements: list[str] | None = None,
) -> dict[str, float | None]:
    """Get GHCN data from per-station .dly file (slower but doesn't require full year).

    Args:
        station_id: GHCN station ID.
        target_date: Date to get data for.
        elements: List of elements to retrieve.

    Returns:
        Dict mapping element names to values.
    """
    if elements is None:
        elements = GHCN_ELEMENTS

    config = get_config()
    dly_path = config.ghcn_cache_dir / f"{station_id}.dly"

    if not dly_path.exists():
        url = GHCN_STATION_URL.format(station_id=station_id)
        result = download_with_retry(url, dly_path)
        if result is None:
            return {e: None for e in elements}

    year = target_date.year
    month = target_date.month
    day = target_date.day

    search_prefix = f"{station_id}{year:04d}{month:02d}"
    values: dict[str, float | None] = {e: None for e in elements}

    with open(dly_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if line[:17] == search_prefix:
                element = line[17:21]
                if element in elements:
                    offset = 21 + (day - 1) * 8
                    value_str = line[offset : offset + 5].strip()
                    if value_str and value_str != "-9999":
                        values[element] = float(value_str)
            elif line[:11] > station_id + str(year):
                break

    return values
