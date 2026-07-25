"""Optional pandas output for weather results.

Requires the ``pandas`` extra: ``pip install get-weather-data[pandas]``.
Imports are deferred so the base package stays pandas-free.
"""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from get_weather_data.weather.results import WeatherResult
from get_weather_data.weather.units import ELEMENTS
from get_weather_data.weather.weather_types import format_weather_types

if TYPE_CHECKING:
    import pandas as pd

# Columns emitted before the weather values, in order.
_META_COLUMNS = [
    "date",
    "zipcode",
    "latitude",
    "longitude",
    "station_id",
    "station_name",
    "station_type",
    "station_distance_meters",
    "units",
]

# Weather value columns, in registry order (tmax, tmin, ...).
_VALUE_COLUMNS = [spec.field for spec in ELEMENTS.values()]


def _format_missing(missing: dict[str, str] | None) -> str:
    """Render a per-field missing-reason map as one compact cell.

    Args:
        missing: Field name -> reason, or None.

    Returns:
        "field: reason; field: reason" for each entry, or "" when the
        map is empty or None.
    """
    if not missing:
        return ""
    return "; ".join(f"{field}: {reason}" for field, reason in missing.items())


def _require_pandas() -> "Any":
    """Import pandas or raise a helpful error.

    Returns:
        The imported pandas module.

    Raises:
        ImportError: If pandas is not installed.
    """
    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - exercised via message test
        raise ImportError(
            "pandas is required for DataFrame output. Install it with "
            "'pip install get-weather-data[pandas]'."
        ) from exc
    return pd


def results_to_frame(results: Sequence[WeatherResult]) -> "pd.DataFrame":
    """Convert weather results to a tidy DataFrame.

    One row per result (per date), with metadata columns followed by the
    weather value columns. Value columns are always present even when a
    given result is missing them (as NaN), so the schema is stable.

    Args:
        results: Weather results, e.g. from ``Weather.get_range``.

    Returns:
        A DataFrame indexed by position with a ``date`` column; value
        columns carry the results' units (all results are assumed to
        share one unit system).

    Raises:
        ImportError: If pandas is not installed.
    """  # noqa: DOC502 - raised by _require_pandas
    pd = _require_pandas()
    columns = _META_COLUMNS + _VALUE_COLUMNS
    rows = [
        {column: getattr(result, column) for column in columns} for result in results
    ]
    # Present-weather phenomena are only collected on request; add the
    # column (comma-joined, sorted) only when at least one result has it.
    if any(result.weather_types is not None for result in results):
        columns = [*columns, "weather_types"]
        for row, result in zip(rows, results, strict=True):
            row["weather_types"] = format_weather_types(result.weather_types)
    # Provenance columns appear only when explain=True populated them.
    if any(result.stations_considered is not None for result in results):
        columns = [*columns, "stations_considered", "missing"]
        for row, result in zip(rows, results, strict=True):
            row["stations_considered"] = result.stations_considered
            row["missing"] = _format_missing(result.missing)
    frame = pd.DataFrame(rows, columns=columns)
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


def result_to_frame(result: WeatherResult) -> "pd.DataFrame":
    """Convert a single weather result to a one-row DataFrame.

    Args:
        result: A single weather result.

    Returns:
        A one-row DataFrame (see :func:`results_to_frame`).

    Raises:
        ImportError: If pandas is not installed.
    """  # noqa: DOC502 - raised by results_to_frame
    return results_to_frame([result])
