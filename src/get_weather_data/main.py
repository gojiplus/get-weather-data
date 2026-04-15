"""High-level Weather API for get-weather-data."""

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from get_weather_data.core.config import Config, set_config
from get_weather_data.core.database import Database
from get_weather_data.core.logging import setup_logging
from get_weather_data.stations import (
    build_closest_index,
    import_ghcnd_stations,
    import_isd_stations,
    import_zipcodes,
)
from get_weather_data.weather.batch import process_csv as _process_csv
from get_weather_data.weather.lookup import WeatherLookup, WeatherResult

logger = logging.getLogger("get_weather_data")


@dataclass
class Weather:
    """High-level API for fetching weather data.

    This class provides a simple interface for:
    - Setting up the station database
    - Looking up weather data for ZIP codes
    - Processing CSV files

    Example:
        weather = Weather()
        weather.setup()
        result = weather.get("10001", "2024-01-15")
    """

    database_path: Path | str | None = None
    verbose: bool = False
    _db: Database | None = field(default=None, repr=False)
    _lookup: WeatherLookup | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        setup_logging(verbose=self.verbose)

        if self.database_path:
            config = Config(_database_path=Path(self.database_path))
            set_config(config)

        self._db = Database(self.database_path)

    @property
    def db(self) -> Database:
        """Get the database instance."""
        if self._db is None:
            self._db = Database(self.database_path)
        return self._db

    @property
    def lookup(self) -> WeatherLookup:
        """Get the weather lookup instance."""
        if self._lookup is None:
            self._lookup = WeatherLookup(db=self.db)
        return self._lookup

    def setup(
        self,
        force: bool = False,
        ghcn_stations: bool = True,
        usaf_stations: bool = True,
        zipcodes: bool = True,
        closest_index: bool = True,
    ) -> None:
        """Set up the database with station and ZIP code data.

        This downloads station lists and ZIP code data, then builds
        an index of closest stations for each ZIP code.

        Args:
            force: If True, rebuild even if database exists.
            ghcn_stations: Import GHCN stations.
            usaf_stations: Import USAF/WBAN (ISD) stations.
            zipcodes: Import ZIP code data.
            closest_index: Build closest stations index.
        """
        if self.db.exists() and not force:
            # Check if already set up
            if self.db.count_stations() > 0 and self.db.count_zipcodes() > 0:
                logger.info("Database already set up. Use force=True to rebuild.")
                return

        self.db.init_schema()

        if ghcn_stations:
            logger.info("Importing GHCN stations...")
            count = import_ghcnd_stations(self.db)
            logger.info(f"Imported {count} GHCN stations")

        if usaf_stations:
            logger.info("Importing USAF/WBAN stations...")
            count = import_isd_stations(self.db)
            logger.info(f"Imported {count} USAF/WBAN stations")

        if zipcodes:
            logger.info("Importing ZIP codes...")
            count = import_zipcodes(self.db)
            logger.info(f"Imported {count} ZIP codes")

        if closest_index:
            logger.info("Building closest stations index...")
            count = build_closest_index(self.db)
            logger.info(f"Indexed {count} ZIP codes")

    def get(
        self,
        zipcode: str,
        target_date: str | date,
        elements: list[str] | None = None,
    ) -> WeatherResult:
        """Get weather data for a ZIP code and date.

        Args:
            zipcode: 5-digit US ZIP code.
            target_date: Date as string (YYYY-MM-DD) or date object.
            elements: List of weather elements to retrieve.

        Returns:
            WeatherResult with available weather data.
        """
        if isinstance(target_date, str):
            target_date = date.fromisoformat(target_date)

        return self.lookup.get_weather(zipcode, target_date, elements)

    def get_range(
        self,
        zipcode: str,
        start_date: str | date,
        end_date: str | date,
        elements: list[str] | None = None,
    ) -> list[WeatherResult]:
        """Get weather data for a ZIP code over a date range.

        Args:
            zipcode: 5-digit US ZIP code.
            start_date: Start date as string (YYYY-MM-DD) or date object.
            end_date: End date as string (YYYY-MM-DD) or date object.
            elements: List of weather elements to retrieve.

        Returns:
            List of WeatherResult objects, one per day.
        """
        if isinstance(start_date, str):
            start_date = date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = date.fromisoformat(end_date)

        return self.lookup.get_weather_range(zipcode, start_date, end_date, elements)

    def process_csv(
        self,
        input_path: str | Path,
        output_path: str | Path,
        zipcode_column: str | int = "zip",
        date_column: str | int | None = None,
        year_column: str | int | None = "year",
        month_column: str | int | None = "month",
        day_column: str | int | None = "day",
    ) -> int:
        """Process a CSV file and add weather data.

        Args:
            input_path: Path to input CSV file.
            output_path: Path to output CSV file.
            zipcode_column: Column name or index for ZIP code.
            date_column: Column name or index for date (YYYY-MM-DD).
            year_column: Column for year (if no date_column).
            month_column: Column for month (if no date_column).
            day_column: Column for day (if no date_column).

        Returns:
            Number of rows processed.
        """
        return _process_csv(
            input_path=Path(input_path),
            output_path=Path(output_path),
            zipcode_column=zipcode_column,
            date_column=date_column,
            year_column=year_column,
            month_column=month_column,
            day_column=day_column,
            db=self.db,
        )

    def info(self) -> dict[str, int]:
        """Get database statistics.

        Returns:
            Dict with counts of stations and ZIP codes.
        """
        return {
            "ghcn_stations": self.db.count_stations("GHCND"),
            "usaf_stations": self.db.count_stations("USAF-WBAN"),
            "total_stations": self.db.count_stations(),
            "zipcodes": self.db.count_zipcodes(),
        }
