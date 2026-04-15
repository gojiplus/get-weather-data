"""Batch processing of CSV files with ZIP codes and dates."""

import csv
import logging
from datetime import date
from pathlib import Path

from get_weather_data.core.database import Database
from get_weather_data.weather.lookup import WeatherLookup, WeatherResult

logger = logging.getLogger("get_weather_data")


def process_csv(
    input_path: Path | str,
    output_path: Path | str,
    zipcode_column: str | int = "zip",
    date_column: str | int | None = None,
    year_column: str | int | None = "year",
    month_column: str | int | None = "month",
    day_column: str | int | None = "day",
    db: Database | None = None,
) -> int:
    """Process a CSV file and add weather data.

    The input CSV must contain a ZIP code column. Dates can be specified either
    as a single date column or as separate year/month/day columns.

    Args:
        input_path: Path to input CSV file.
        output_path: Path to output CSV file.
        zipcode_column: Column name or index for ZIP code.
        date_column: Column name or index for date (YYYY-MM-DD format).
        year_column: Column name or index for year.
        month_column: Column name or index for month.
        day_column: Column name or index for day.
        db: Database instance. Uses default if None.

    Returns:
        Number of rows processed.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    if db is None:
        db = Database()

    lookup = WeatherLookup(db=db)

    weather_columns = [
        "station_id",
        "station_name",
        "station_type",
        "station_distance_meters",
        "tmax",
        "tmin",
        "tavg",
        "prcp",
        "snow",
        "snwd",
        "awnd",
    ]

    rows_processed = 0

    with open(input_path, "r", encoding="utf-8", errors="replace") as infile:
        reader = csv.DictReader(infile)
        fieldnames = list(reader.fieldnames or []) + weather_columns

        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                zipcode = _get_column(row, zipcode_column)
                if not zipcode:
                    logger.warning(f"Missing ZIP code in row {rows_processed + 1}")
                    writer.writerow(row)
                    rows_processed += 1
                    continue

                target_date = _parse_date(
                    row, date_column, year_column, month_column, day_column
                )
                if target_date is None:
                    logger.warning(f"Missing date in row {rows_processed + 1}")
                    writer.writerow(row)
                    rows_processed += 1
                    continue

                result = lookup.get_weather(zipcode, target_date)

                row.update(_result_to_dict(result))
                writer.writerow(row)

                rows_processed += 1
                if rows_processed % 1000 == 0:
                    logger.info(f"Processed {rows_processed} rows...")

    logger.info(f"Processed {rows_processed} rows total")
    return rows_processed


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
                pass

    if year_column is not None and month_column is not None and day_column is not None:
        try:
            year = int(_get_column(row, year_column))
            month = int(_get_column(row, month_column))
            day = int(_get_column(row, day_column))
            return date(year, month, day)
        except (ValueError, TypeError):
            pass

    return None


def _result_to_dict(result: WeatherResult) -> dict[str, str]:
    """Convert WeatherResult to dict for CSV output."""
    return {
        "station_id": result.station_id or "",
        "station_name": result.station_name or "",
        "station_type": result.station_type or "",
        "station_distance_meters": str(result.station_distance_meters or ""),
        "tmax": str(result.tmax) if result.tmax is not None else "",
        "tmin": str(result.tmin) if result.tmin is not None else "",
        "tavg": str(result.tavg) if result.tavg is not None else "",
        "prcp": str(result.prcp) if result.prcp is not None else "",
        "snow": str(result.snow) if result.snow is not None else "",
        "snwd": str(result.snwd) if result.snwd is not None else "",
        "awnd": str(result.awnd) if result.awnd is not None else "",
    }
