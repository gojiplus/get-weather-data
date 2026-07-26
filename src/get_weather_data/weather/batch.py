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
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date
from itertools import islice
from pathlib import Path
from typing import Any, TextIO

from get_weather_data.core.database import Database
from get_weather_data.weather.lookup import WeatherLookup
from get_weather_data.weather.results import WeatherResult
from get_weather_data.weather.units import ELEMENTS, Units
from get_weather_data.weather.weather_types import format_weather_types

logger = logging.getLogger("get_weather_data")

CHUNK_SIZE = 500

_STATION_COLUMNS = [
    "station_id",
    "station_name",
    "station_type",
    "station_distance_meters",
]
# Value columns are derived from the element registry so they stay in
# sync as elements are added.
_VALUE_COLUMNS = [spec.field for spec in ELEMENTS.values()]
WEATHER_COLUMNS = [*_STATION_COLUMNS, *_VALUE_COLUMNS, "weather_error"]


def _weather_columns(include_weather_types: bool, explain: bool = False) -> list[str]:
    """Output weather columns, optionally with weather_types/provenance."""
    columns = [*_STATION_COLUMNS, *_VALUE_COLUMNS]
    if include_weather_types:
        columns.append("weather_types")
    if explain:
        columns += ["stations_considered", "missing"]
    columns.append("weather_error")
    return columns


# Weather columns that carry native numeric types in Parquet output.
_FLOAT_COLUMNS = frozenset(_VALUE_COLUMNS)
_INT_COLUMNS = frozenset({"station_distance_meters", "stations_considered"})


def _parquet_schema(
    passthrough: list[str], include_weather_types: bool, explain: bool
) -> "Any":
    """Build the pyarrow schema for the batch Parquet output."""
    import pyarrow as pa

    fields = [pa.field(name, pa.string()) for name in passthrough]
    for name in _weather_columns(include_weather_types, explain):
        if name in _FLOAT_COLUMNS:
            fields.append(pa.field(name, pa.float64()))
        elif name in _INT_COLUMNS:
            fields.append(pa.field(name, pa.int64()))
        else:
            fields.append(pa.field(name, pa.string()))
    return pa.schema(fields)


class _CsvSink:
    """Stream output rows to a CSV file, one chunk at a time."""

    def __init__(
        self,
        outfile: "TextIO",
        input_fieldnames: list[str],
        include_weather_types: bool,
        explain: bool,
    ) -> None:
        self._outfile = outfile
        self._iwt = include_weather_types
        self._explain = explain
        fieldnames = input_fieldnames + _weather_columns(include_weather_types, explain)
        self._writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        self._writer.writeheader()
        outfile.flush()

    def write_row(
        self, row_data: dict[str, str], result: "WeatherResult | None", error: str
    ) -> None:
        """Append one output row."""
        row_data.update(_result_to_dict(result, error, self._iwt, self._explain))
        self._writer.writerow(row_data)

    def flush_chunk(self) -> None:
        """Flush the current chunk to disk."""
        self._outfile.flush()

    def close(self) -> None:
        """Close the sink (no-op for CSV; the file is closed by caller)."""


class _ParquetSink:
    """Stream output rows to a Parquet file, one chunk per row group."""

    def __init__(
        self,
        path: Path,
        input_fieldnames: list[str],
        include_weather_types: bool,
        explain: bool,
    ) -> None:
        import pyarrow.parquet as pq

        self._iwt = include_weather_types
        self._explain = explain
        weather = set(_weather_columns(include_weather_types, explain))
        self._passthrough = [c for c in input_fieldnames if c not in weather]
        self._schema = _parquet_schema(
            self._passthrough, include_weather_types, explain
        )
        self._writer = pq.ParquetWriter(str(path), self._schema)
        self._buffer: list[dict[str, object]] = []

    def write_row(
        self, row_data: dict[str, str], result: "WeatherResult | None", error: str
    ) -> None:
        """Buffer one output row for the current chunk."""
        typed = _result_to_typed(result, error, self._iwt, self._explain)
        self._buffer.append(
            {**{c: row_data.get(c) for c in self._passthrough}, **typed}
        )

    def flush_chunk(self) -> None:
        """Write the buffered chunk as one Parquet row group."""
        import pyarrow as pa

        if not self._buffer:
            return
        columns = {
            field.name: [row.get(field.name) for row in self._buffer]
            for field in self._schema
        }
        self._writer.write_table(pa.table(columns, schema=self._schema))
        self._buffer.clear()

    def close(self) -> None:
        """Finalize the Parquet file."""
        self._writer.close()


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
    include_weather_types: bool = False,
    explain: bool = False,
    output_format: str | None = None,
    parallel: bool = True,
    max_workers: int | None = None,
) -> int:
    """Process a CSV file and add weather data.

    Args:
        input_path: Path to input CSV file.
        output_path: Path to output file (CSV or Parquet).
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
        include_weather_types: If True, add a ``weather_types`` column
            with the day's present-weather phenomena (comma-joined).
        explain: If True, add ``stations_considered`` and ``missing``
            columns explaining why any requested value is absent.
        output_format: "csv" or "parquet". Defaults to inferring from the
            output path suffix (".parquet" -> parquet, else csv). Parquet
            needs the ``parquet`` extra and gives typed, compressed
            columns for the analytical workflow.
        parallel: Use parallel processing for faster execution.
        max_workers: Number of worker threads (default: CPU count).

    Returns:
        Number of rows processed.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    if output_format is None:
        output_format = "parquet" if output_path.suffix.lower() == ".parquet" else "csv"

    if db is None:
        db = Database()
    if max_workers is None:
        max_workers = min(os.cpu_count() or 4, 8)

    lookup = WeatherLookup(
        db=db,
        units=units,
        include_weather_types=include_weather_types,
        explain=explain,
    )

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

    with ExitStack() as stack:
        infile = stack.enter_context(
            open(input_path, encoding="utf-8", errors="replace")
        )
        reader = csv.DictReader(infile)
        input_fieldnames = list(reader.fieldnames or [])

        sink: _CsvSink | _ParquetSink
        if output_format == "parquet":
            sink = _ParquetSink(
                output_path, input_fieldnames, include_weather_types, explain
            )
        else:
            outfile = stack.enter_context(
                open(output_path, "w", encoding="utf-8", newline="")
            )
            sink = _CsvSink(outfile, input_fieldnames, include_weather_types, explain)
        stack.callback(sink.close)

        executor = stack.enter_context(ThreadPoolExecutor(max_workers=max_workers))
        while True:
            chunk = [parse_row(row) for row in islice(reader, CHUNK_SIZE)]
            if not chunk:
                break

            if parallel and len(chunk) > 1:
                outputs = list(executor.map(process_row, chunk))
            else:
                outputs = [process_row(parsed) for parsed in chunk]

            for row_data, result, error in outputs:
                sink.write_row(row_data, result, error)
                if error:
                    errors += 1
            sink.flush_chunk()

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


def _cell(value: object) -> str:
    """Render a result attribute as a CSV cell (blank for None)."""
    return "" if value is None else str(value)


def _result_to_typed(
    result: WeatherResult | None,
    error: str,
    include_weather_types: bool = False,
    explain: bool = False,
) -> dict[str, object]:
    """Convert a WeatherResult (or an error) to native-typed columns.

    Value columns are float|None, station_distance_meters and
    stations_considered are int|None, and the rest are str|None. The CSV
    path stringifies this; the Parquet path keeps the native types.

    Args:
        result: The result to render, or None for an errored row.
        error: The error message (blank when the row succeeded).
        include_weather_types: Whether to add the weather_types column.
        explain: Whether to add the provenance columns.

    Returns:
        A row dict keyed by the weather output column names.
    """
    columns = _weather_columns(include_weather_types, explain)
    if result is None:
        empty: dict[str, object] = {}
        for name in columns:
            empty[name] = None
        empty["weather_error"] = error
        return empty
    row: dict[str, object] = {c: getattr(result, c) for c in _STATION_COLUMNS}
    row.update({field: getattr(result, field) for field in _VALUE_COLUMNS})
    if include_weather_types:
        row["weather_types"] = format_weather_types(result.weather_types) or None
    if explain:
        row["stations_considered"] = result.stations_considered
        row["missing"] = _format_missing(result.missing) or None
    row["weather_error"] = error
    return row


def _result_to_dict(
    result: WeatherResult | None,
    error: str,
    include_weather_types: bool = False,
    explain: bool = False,
) -> dict[str, str]:
    """Convert a WeatherResult (or an error) to CSV (string) columns."""
    typed = _result_to_typed(result, error, include_weather_types, explain)
    return {key: _cell(value) for key, value in typed.items()}


def _format_missing(missing: dict[str, str] | None) -> str:
    """Render a per-field missing-reason map as one compact CSV cell."""
    if not missing:
        return ""
    return "; ".join(f"{field}: {reason}" for field, reason in missing.items())
