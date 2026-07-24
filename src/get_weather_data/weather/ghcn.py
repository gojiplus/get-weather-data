"""GHCN (Global Historical Climatology Network) daily data fetching."""

import csv
import gzip
import logging
import sqlite3
from datetime import date
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.download import download_with_retry

logger = logging.getLogger("get_weather_data")

GHCN_BY_YEAR_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"

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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",  # noqa: S608 - int year
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

    values: dict[str, float | None] = dict.fromkeys(elements)

    conn = sqlite3.connect(db_path)
    try:
        c = conn.cursor()
        c.execute(
            f"SELECT element, value FROM ghcn_{year} "  # noqa: S608 - int year
            "WHERE id = ? AND date = ?",
            (station_id, date_str),
        )
        for row in c:
            element, value = row
            if element in elements and value and value != "-9999":
                values[element] = float(value)
    finally:
        conn.close()

    return values
