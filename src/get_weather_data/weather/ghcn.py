"""GHCN (Global Historical Climatology Network) daily data fetching."""

import csv
import gzip
import logging
import os
import sqlite3
import threading
from datetime import date
from pathlib import Path

from get_weather_data.core.cache import is_fresh, year_is_immutable
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

# One lock per year so concurrent batch threads build each yearly
# database exactly once.
_locks_guard = threading.Lock()
_year_locks: dict[int, threading.Lock] = {}

# Per-thread, per-year read-only connections (multi-GB files; opening a
# fresh connection per row lookup is wasteful).
_connections = threading.local()


def _year_lock(year: int) -> threading.Lock:
    """Get (or create) the build lock for a year."""
    with _locks_guard:
        if year not in _year_locks:
            _year_locks[year] = threading.Lock()
        return _year_locks[year]


def _get_ghcn_db_path(year: int) -> Path:
    """Get path to GHCN database for a year."""
    config = get_config()
    return config.ghcn_cache_dir / f"ghcn_{year}.sqlite3"


def _ensure_ghcn_database(year: int) -> Path:
    """Ensure the yearly GHCN database exists, downloading if needed.

    Thread-safe: a per-year lock means one thread downloads and builds
    while the rest wait. Cross-process safe: the database is built to a
    temporary path and atomically renamed, so other processes only ever
    see a complete file (worst case they duplicate work, never corrupt).

    Args:
        year: Calendar year to ensure.

    Returns:
        Path to the yearly SQLite database.

    Raises:
        RuntimeError: If the yearly file cannot be downloaded.
    """
    db_path = _get_ghcn_db_path(year)
    if _year_db_usable(db_path, year):
        return db_path

    with _year_lock(year):
        if _year_db_usable(db_path, year):  # built while we waited
            return db_path

        config = get_config()
        gz_path = config.ghcn_cache_dir / f"{year}.csv.gz"
        if not gz_path.exists():
            url = GHCN_BY_YEAR_URL.format(year=year)
            if download_with_retry(url, gz_path) is None:
                raise RuntimeError(f"Failed to download GHCN data for {year}")

        logger.info(f"Building GHCN database for {year}...")
        tmp_path = db_path.with_name(f"{db_path.name}.tmp-{os.getpid()}")
        try:
            _build_year_db(tmp_path, gz_path, year)
            os.replace(tmp_path, db_path)
        finally:
            tmp_path.unlink(missing_ok=True)

        # The extracted database supersedes the source archive
        gz_path.unlink(missing_ok=True)
        logger.info(f"GHCN database for {year} ready")
        return db_path


def _year_db_usable(db_path: Path, year: int) -> bool:
    """Whether the cached yearly database can be used as-is.

    Historical years are immutable; the current and previous year keep
    accumulating observations and refresh after the cache TTL.
    """
    if not db_path.exists():
        return False
    if year_is_immutable(year):
        return True
    return is_fresh(db_path, get_config().cache_max_age_days)


def _build_year_db(db_file: Path, gz_path: Path, year: int) -> None:
    """Load a yearly GHCN csv.gz into a SQLite file."""
    conn = sqlite3.connect(db_file)
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


def _year_connection(year: int, db_path: Path) -> sqlite3.Connection:
    """Get this thread's read-only connection for a year."""
    pool: dict[int, sqlite3.Connection] | None = getattr(_connections, "pool", None)
    if pool is None:
        pool = {}
        _connections.pool = pool
    conn = pool.get(year)
    if conn is None:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        pool[year] = conn
    return conn


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
        Dict mapping element names to raw GHCN values (None if missing).
    """
    if elements is None:
        elements = GHCN_ELEMENTS

    year = target_date.year
    db_path = _ensure_ghcn_database(year)

    date_str = target_date.strftime("%Y%m%d")
    values: dict[str, float | None] = dict.fromkeys(elements)

    conn = _year_connection(year, db_path)
    c = conn.execute(
        f"SELECT element, value FROM ghcn_{year} "  # noqa: S608 - int year
        "WHERE id = ? AND date = ?",
        (station_id, date_str),
    )
    for element, value in c:
        if element in elements and value and value != "-9999":
            values[element] = float(value)

    return values
