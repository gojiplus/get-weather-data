"""Database operations for get-weather-data."""

import logging
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from get_weather_data.core.config import get_config
from get_weather_data.core.distance import Station

logger = logging.getLogger("get_weather_data")


class Database:
    """SQLite database for weather station and ZIP code data.

    Uses connection pooling and caches station metadata for efficiency.
    """

    _local = threading.local()

    def __init__(self, path: Path | str | None = None) -> None:
        """Initialize database.

        Args:
            path: Path to SQLite database. If None, uses config default.
        """
        if path is None:
            db_path = get_config().database_path
        else:
            db_path = Path(path)
        self.path = db_path
        self._station_cache: dict[str, tuple[str, str]] | None = None
        self._zipcode_cache: dict[str, tuple[float, float]] | None = None
        self._closest_cache: dict[str, list[tuple[str, int]]] | None = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.path, check_same_thread=False, timeout=30.0
            )
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn.execute("PRAGMA cache_size=10000")
        return self._local.conn  # type: ignore[no-any-return]

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connection (uses pool)."""
        yield self._get_connection()

    def close(self) -> None:
        """Close the database connection."""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            self._local.conn = None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        """Execute SQL and return all results."""
        conn = self._get_connection()
        cursor = conn.execute(sql, params)
        return cursor.fetchall()

    def execute_many(self, sql: str, params_list: list[tuple[Any, ...]]) -> None:
        """Execute SQL with multiple parameter sets."""
        conn = self._get_connection()
        conn.executemany(sql, params_list)
        conn.commit()

    def init_schema(self) -> None:
        """Initialize database schema."""
        conn = self._get_connection()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS zipcodes (
                zipcode TEXT PRIMARY KEY,
                city TEXT,
                state TEXT,
                lat REAL,
                lon REAL,
                county TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                id TEXT PRIMARY KEY,
                name TEXT,
                state TEXT,
                lat REAL,
                lon REAL,
                elevation REAL,
                type TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS closest (
                zipcode TEXT,
                station_id TEXT,
                distance_meters INTEGER,
                PRIMARY KEY (zipcode, station_id)
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_stations_type ON stations(type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closest_zip ON closest(zipcode)")
        conn.commit()
        logger.debug("Database schema initialized")

    def _load_station_cache(self) -> None:
        """Load station metadata into memory cache."""
        if self._station_cache is not None:
            return
        self._station_cache = {}
        results = self.execute("SELECT id, name, type FROM stations")
        for row in results:
            self._station_cache[row[0]] = (row[1], row[2])
        logger.debug(f"Cached {len(self._station_cache)} stations")

    def _load_zipcode_cache(self) -> None:
        """Load ZIP code coordinates into memory cache."""
        if self._zipcode_cache is not None:
            return
        self._zipcode_cache = {}
        results = self.execute("SELECT zipcode, lat, lon FROM zipcodes")
        for row in results:
            if row[1] is not None and row[2] is not None:
                self._zipcode_cache[row[0]] = (row[1], row[2])
        logger.debug(f"Cached {len(self._zipcode_cache)} ZIP codes")

    def _load_closest_cache(self) -> None:
        """Load closest stations mapping into memory cache."""
        if self._closest_cache is not None:
            return
        self._closest_cache = {}
        results = self.execute(
            "SELECT zipcode, station_id, distance_meters FROM closest ORDER BY zipcode, distance_meters"
        )
        for zipcode, station_id, distance in results:
            if zipcode not in self._closest_cache:
                self._closest_cache[zipcode] = []
            self._closest_cache[zipcode].append((station_id, distance))
        logger.debug(
            f"Cached closest stations for {len(self._closest_cache)} ZIP codes"
        )

    def preload_caches(self) -> None:
        """Preload all caches for maximum performance."""
        self._load_station_cache()
        self._load_zipcode_cache()
        self._load_closest_cache()

    def get_station_info(self, station_id: str) -> tuple[str, str] | None:
        """Get station name and type from cache.

        Args:
            station_id: Station ID.

        Returns:
            Tuple of (name, type) or None if not found.
        """
        self._load_station_cache()
        return self._station_cache.get(station_id) if self._station_cache else None

    def insert_zipcode(
        self,
        zipcode: str,
        city: str,
        state: str,
        lat: float,
        lon: float,
        county: str = "",
    ) -> None:
        """Insert or update a ZIP code."""
        conn = self._get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO zipcodes (zipcode, city, state, lat, lon, county)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (zipcode, city, state, lat, lon, county),
        )
        conn.commit()
        if self._zipcode_cache is not None:
            self._zipcode_cache[zipcode] = (lat, lon)

    def insert_station(self, station: Station) -> None:
        """Insert or update a station."""
        conn = self._get_connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO stations (id, name, state, lat, lon, elevation, type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                station.id,
                station.name,
                station.state,
                station.lat,
                station.lon,
                station.elevation,
                station.type,
            ),
        )
        conn.commit()
        if self._station_cache is not None:
            self._station_cache[station.id] = (station.name, station.type)

    def insert_stations_bulk(self, stations: list[Station]) -> None:
        """Bulk insert stations."""
        conn = self._get_connection()
        conn.executemany(
            """
            INSERT OR IGNORE INTO stations (id, name, state, lat, lon, elevation, type)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (s.id, s.name, s.state, s.lat, s.lon, s.elevation, s.type)
                for s in stations
            ],
        )
        conn.commit()
        self._station_cache = None

    def get_stations(
        self, station_type: str | None = None, state: str | None = None
    ) -> list[Station]:
        """Get stations from database."""
        sql = (
            "SELECT id, name, state, lat, lon, elevation, type FROM stations WHERE 1=1"
        )
        params: list[Any] = []

        if station_type:
            sql += " AND type = ?"
            params.append(station_type)

        if state:
            sql += " AND state = ?"
            params.append(state)

        results = self.execute(sql, tuple(params))

        return [
            Station(
                id=row[0],
                name=row[1],
                state=row[2],
                lat=row[3],
                lon=row[4],
                elevation=row[5],
                type=row[6],
            )
            for row in results
        ]

    def get_zipcode(self, zipcode: str) -> tuple[float, float] | None:
        """Get lat/lon for a ZIP code (uses cache)."""
        self._load_zipcode_cache()
        if self._zipcode_cache:
            return self._zipcode_cache.get(zipcode)
        return None

    def get_closest_stations(self, zipcode: str) -> list[tuple[str, int]]:
        """Get cached closest stations for a ZIP code (uses cache)."""
        self._load_closest_cache()
        if self._closest_cache:
            return self._closest_cache.get(zipcode, [])
        return []

    def set_closest_stations(
        self, zipcode: str, stations: list[tuple[str, int]]
    ) -> None:
        """Cache closest stations for a ZIP code."""
        conn = self._get_connection()
        conn.execute("DELETE FROM closest WHERE zipcode = ?", (zipcode,))
        conn.executemany(
            """
            INSERT INTO closest (zipcode, station_id, distance_meters)
            VALUES (?, ?, ?)
            """,
            [(zipcode, sid, dist) for sid, dist in stations],
        )
        conn.commit()
        if self._closest_cache is not None:
            self._closest_cache[zipcode] = stations

    def count_zipcodes(self) -> int:
        """Count ZIP codes in database."""
        result = self.execute("SELECT COUNT(*) FROM zipcodes")
        return result[0][0] if result else 0

    def count_stations(self, station_type: str | None = None) -> int:
        """Count stations in database."""
        if station_type:
            result = self.execute(
                "SELECT COUNT(*) FROM stations WHERE type = ?", (station_type,)
            )
        else:
            result = self.execute("SELECT COUNT(*) FROM stations")
        return result[0][0] if result else 0

    def exists(self) -> bool:
        """Check if database file exists."""
        return self.path.exists()
