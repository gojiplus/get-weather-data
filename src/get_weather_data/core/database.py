"""Database operations for get-weather-data."""

import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from get_weather_data.core.config import get_config
from get_weather_data.core.distance import Station

logger = logging.getLogger("get_weather_data")


class Database:
    """SQLite database for weather station and ZIP code data."""

    def __init__(self, path: Path | str | None = None) -> None:
        """Initialize database connection.

        Args:
            path: Path to SQLite database. If None, uses config default.
        """
        if path is None:
            db_path = get_config().database_path
        else:
            db_path = Path(path)
        self.path = db_path
        self._conn: sqlite3.Connection | None = None

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database connection."""
        conn = sqlite3.connect(self.path)
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        """Execute SQL and return all results."""
        with self.connection() as conn:
            cursor = conn.execute(sql, params)
            return cursor.fetchall()

    def execute_many(self, sql: str, params_list: list[tuple[Any, ...]]) -> None:
        """Execute SQL with multiple parameter sets."""
        with self.connection() as conn:
            conn.executemany(sql, params_list)
            conn.commit()

    def init_schema(self) -> None:
        """Initialize database schema."""
        with self.connection() as conn:
            # ZIP codes table
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

            # Stations table
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

            # Closest stations cache
            conn.execute("""
                CREATE TABLE IF NOT EXISTS closest (
                    zipcode TEXT,
                    station_id TEXT,
                    distance_meters INTEGER,
                    PRIMARY KEY (zipcode, station_id)
                )
            """)

            # Create indices
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_stations_type ON stations(type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_closest_zip ON closest(zipcode)"
            )

            conn.commit()
            logger.debug("Database schema initialized")

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
        with self.connection() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO zipcodes (zipcode, city, state, lat, lon, county)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (zipcode, city, state, lat, lon, county),
            )
            conn.commit()

    def insert_station(self, station: Station) -> None:
        """Insert or update a station."""
        with self.connection() as conn:
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

    def insert_stations_bulk(self, stations: list[Station]) -> None:
        """Bulk insert stations."""
        with self.connection() as conn:
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

    def get_stations(
        self, station_type: str | None = None, state: str | None = None
    ) -> list[Station]:
        """Get stations from database.

        Args:
            station_type: Filter by station type (GHCND, USAF-WBAN, etc.)
            state: Filter by state abbreviation.

        Returns:
            List of Station objects.
        """
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
        """Get lat/lon for a ZIP code.

        Args:
            zipcode: 5-digit ZIP code.

        Returns:
            Tuple of (lat, lon) or None if not found.
        """
        results = self.execute(
            "SELECT lat, lon FROM zipcodes WHERE zipcode = ?",
            (zipcode,),
        )
        if results and results[0][0] is not None:
            return (results[0][0], results[0][1])
        return None

    def get_closest_stations(self, zipcode: str) -> list[tuple[str, int]]:
        """Get cached closest stations for a ZIP code.

        Args:
            zipcode: 5-digit ZIP code.

        Returns:
            List of (station_id, distance_meters) tuples.
        """
        results = self.execute(
            """
            SELECT station_id, distance_meters FROM closest
            WHERE zipcode = ?
            ORDER BY distance_meters
            """,
            (zipcode,),
        )
        return [(row[0], row[1]) for row in results]

    def set_closest_stations(
        self, zipcode: str, stations: list[tuple[str, int]]
    ) -> None:
        """Cache closest stations for a ZIP code.

        Args:
            zipcode: 5-digit ZIP code.
            stations: List of (station_id, distance_meters) tuples.
        """
        with self.connection() as conn:
            # Clear existing
            conn.execute("DELETE FROM closest WHERE zipcode = ?", (zipcode,))

            # Insert new
            conn.executemany(
                """
                INSERT INTO closest (zipcode, station_id, distance_meters)
                VALUES (?, ?, ?)
                """,
                [(zipcode, sid, dist) for sid, dist in stations],
            )
            conn.commit()

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
