"""Batch processing of CSV files with locations and dates.

Rows stream through in chunks: each chunk is looked up (optionally in
parallel), written, and flushed before the next chunk is read, so
memory stays bounded and completed chunks survive a crash. A failing
row gets its error recorded in the ``weather_error`` column instead of
aborting the job.
"""

import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from itertools import islice
from pathlib import Path

from get_weather_data.core.database import Database
from get_weather_data.weather.lookup import WeatherLookup
from get_weather_data.weather.results import WeatherResult
from get_weather_data.weather.units import Units

logger = logging.getLogger("get_weather_data")

CHUNK_SIZE = 500

WEATHER_COLUMNS = [
    "station_id",
    "station_name",
    "station_type",
    "station_distance_meters",
    "tmax",
    "tmin",
    "tavg",
    "tobs",
    "prcp",
    "snow",
    "snwd",
    "awnd",
    "weather_error",
]


@dataclass
class _Row:
    """Internal row representation for parallel processing."""

    data: dict[str, str]
    location: str | tuple[float, float] | None
    target_date: date | None
    error: str | None = None


def process_csv(
    input_path: Path | str,
    output_path: Path | str,
    zipcode_column: str | int = "zip",
    lat_column: str | int | None = None,
    lon_column: str | int | None = None,
    date_column: str | int | None = None,
    year_column: str | int | None = "year",
    month_column: str | int | None = "month",
    day_column: str | int | None = "day",
    db: Database | None = None,
    units: Units = "metric",
    parallel: bool = True,
    max_workers: int | None = None,
) -> int:
    """Process a CSV file and add weather data.

    Args:
        input_path: Path to input CSV file.
        output_path: Path to output CSV file.
        zipcode_column: Column name or index for ZIP code.
        lat_column: Column name or index for latitude (with lon_column,
            takes precedence over the ZIP column when both are present).
        lon_column: Column name or index for longitude.
        date_column: Column name or index for date (YYYY-MM-DD format).
        year_column: Column name or index for year.
        month_column: Column name or index for month.
        day_column: Column name or index for day.
        db: Database instance. Uses default if None.
        units: Unit system for the output values.
        parallel: Use parallel processing for faster execution.
        max_workers: Number of worker threads (default: CPU count).

    Returns:
        Number of rows processed.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if db is None:
        db = Database()
    if max_workers is None:
        max_workers = min(os.cpu_count() or 4, 8)

    lookup = WeatherLookup(db=db, units=units)

    def parse_row(row: dict[str, str]) -> _Row:
        location: str | tuple[float, float] | None = None
        error: str | None = None
        if lat_column is not None and lon_column is not None:
            lat_raw = _get_column(row, lat_column)
            lon_raw = _get_column(row, lon_column)
            if lat_raw and lon_raw:
                try:
                    location = (float(lat_raw), float(lon_raw))
                except ValueError:
                    error = f"invalid coordinates: {lat_raw!r},{lon_raw!r}"
        if location is None and error is None:
            zipcode = _get_column(row, zipcode_column)
            if zipcode:
                location = zipcode
            else:
                error = "missing location"
        target_date = _parse_date(
            row, date_column, year_column, month_column, day_column
        )
        if target_date is None and error is None:
            error = "missing or invalid date"
        return _Row(data=row, location=location, target_date=target_date, error=error)

    def process_row(parsed: _Row) -> tuple[dict[str, str], WeatherResult | None, str]:
        if parsed.error is not None or parsed.location is None:
            return (parsed.data, None, parsed.error or "missing location")
        if parsed.target_date is None:
            return (parsed.data, None, "missing or invalid date")
        try:
            result = lookup.get_weather(parsed.location, parsed.target_date)
        except Exception as exc:
            logger.warning("Row failed: %s", exc)
            return (parsed.data, None, str(exc))
        return (parsed.data, result, "")

    processed = 0
    errors = 0

    with (
        open(input_path, encoding="utf-8", errors="replace") as infile,
        open(output_path, "w", encoding="utf-8", newline="") as outfile,
    ):
        reader = csv.DictReader(infile)
        fieldnames = list(reader.fieldnames or []) + WEATHER_COLUMNS
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        outfile.flush()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                chunk = [parse_row(row) for row in islice(reader, CHUNK_SIZE)]
                if not chunk:
                    break

                if parallel and len(chunk) > 1:
                    outputs = list(executor.map(process_row, chunk))
                else:
                    outputs = [process_row(parsed) for parsed in chunk]

                for row_data, result, error in outputs:
                    row_data.update(_result_to_dict(result, error))
                    writer.writerow(row_data)
                    if error:
                        errors += 1
                outfile.flush()

                processed += len(chunk)
                logger.info(f"Processed {processed} rows...")

    if errors:
        logger.warning(f"{errors} of {processed} rows had errors (see weather_error)")
    logger.info(f"Processed {processed} rows total")
    return processed


def _get_column(row: dict[str, str], column: str | int) -> str:
    """Get column value from row by name or index."""
    if isinstance(column, int):
        keys = list(row.keys())
        if column < len(keys):
            return row[keys[column]]
        return ""
    return row.get(str(column), "")


def _parse_date(
    row: dict[str, str],
    date_column: str | int | None,
    year_column: str | int | None,
    month_column: str | int | None,
    day_column: str | int | None,
) -> date | None:
    """Parse date from row columns."""
    if date_column is not None:
        date_str = _get_column(row, date_column)
        if date_str:
            try:
                return date.fromisoformat(date_str)
            except ValueError:
                return None

    if year_column is not None and month_column is not None and day_column is not None:
        try:
            year = int(_get_column(row, year_column))
            month = int(_get_column(row, month_column))
            day = int(_get_column(row, day_column))
            return date(year, month, day)
        except (ValueError, TypeError):
            return None

    return None


def _result_to_dict(result: WeatherResult | None, error: str) -> dict[str, str]:
    """Convert a WeatherResult (or an error) to CSV output columns."""
    if result is None:
        empty = dict.fromkeys(WEATHER_COLUMNS, "")
        empty["weather_error"] = error
        return empty
    return {
        "station_id": result.station_id or "",
        "station_name": result.station_name or "",
        "station_type": result.station_type or "",
        "station_distance_meters": (
            str(result.station_distance_meters)
            if result.station_distance_meters is not None
            else ""
        ),
        "tmax": str(result.tmax) if result.tmax is not None else "",
        "tmin": str(result.tmin) if result.tmin is not None else "",
        "tavg": str(result.tavg) if result.tavg is not None else "",
        "tobs": str(result.tobs) if result.tobs is not None else "",
        "prcp": str(result.prcp) if result.prcp is not None else "",
        "snow": str(result.snow) if result.snow is not None else "",
        "snwd": str(result.snwd) if result.snwd is not None else "",
        "awnd": str(result.awnd) if result.awnd is not None else "",
        "weather_error": error,
    }
