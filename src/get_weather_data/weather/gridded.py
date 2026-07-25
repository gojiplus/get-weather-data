"""Gridded weather lookup via NOAA nClimGrid-Daily.

nClimGrid-Daily is an authoritative 5-km daily grid of maximum, minimum,
and average temperature and precipitation for the contiguous US
(1951-present), interpolated from GHCN-Daily with thin-plate splines. It
gives *any* Lower-48 point real NOAA data with no station gaps.

Data is sliced a single grid cell at a time over THREDDS/OPeNDAP, so a
per-point time series is a tiny transfer (no multi-GB monthly
downloads). Requires the ``grid`` extra:
``pip install get-weather-data[grid]``.

Caveats: contiguous US only (no AK/HI/PR), temperature + precipitation
only, and a ~2-3 day latency for the most recent days.
"""

import logging
import math
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING

from get_weather_data.stations.zipcodes import zip_centroids
from get_weather_data.weather.location import LocationInput, parse_location
from get_weather_data.weather.results import (
    StationMeta,
    WeatherResult,
    assemble_result,
)
from get_weather_data.weather.units import Units, normalize_elements

if TYPE_CHECKING:
    import xarray as xr

logger = logging.getLogger("get_weather_data")

OPENDAP_URL = (
    "https://www.ncei.noaa.gov/thredds/dodsC/nclimgrid-daily/"
    "{year}/ncdd-{year}{month:02d}-grd-scaled.nc"
)

# Element code -> nClimGrid variable name. Values are already metric
# (temperatures in degree_Celsius, precipitation in millimeter).
GRID_VARS = {"TMAX": "tmax", "TMIN": "tmin", "TAVG": "tavg", "PRCP": "prcp"}

# Contiguous-US grid extent (south, west, north, east) in degrees.
CONUS_BOUNDS = (24.0, -125.0, 49.0, -67.0)


def in_conus(lat: float, lon: float) -> bool:
    """Whether a point falls within the nClimGrid CONUS extent.

    Args:
        lat: Latitude in degrees.
        lon: Longitude in degrees.

    Returns:
        True when the point is inside the grid domain.
    """
    south, west, north, east = CONUS_BOUNDS
    return south <= lat <= north and west <= lon <= east


def _iter_months(start: date, end: date) -> Iterator[tuple[int, int]]:
    """Yield (year, month) pairs spanned by a date range."""
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            year += 1
            month = 1


def _open_opendap(year: int, month: int) -> "xr.Dataset | None":
    """Open one monthly nClimGrid dataset over OPeNDAP."""
    import xarray as xr

    url = OPENDAP_URL.format(year=year, month=month)
    try:
        return xr.open_dataset(url)
    except (OSError, ValueError) as exc:
        logger.warning("nClimGrid unavailable for %d-%02d: %s", year, month, exc)
        return None


@dataclass
class GriddedLookup:
    """Look up weather for any CONUS point from the nClimGrid grid."""

    units: Units = "metric"
    dataset_opener: Callable[[int, int], "xr.Dataset | None"] = _open_opendap
    zip_coordinates_loader: Callable[[], dict[str, tuple[float, float]]] = zip_centroids
    _zip_coords: dict[str, tuple[float, float]] | None = field(default=None, repr=False)

    def get_weather(
        self,
        location: LocationInput,
        target_date: date,
        elements: list[str] | None = None,
    ) -> WeatherResult:
        """Get gridded weather for a location and date.

        Args:
            location: 5-digit US ZIP code, "lat,lon" string, or
                (lat, lon) tuple.
            target_date: Date to get weather for.
            elements: Element codes to retrieve (default: all; only
                TMAX/TMIN/TAVG/PRCP are available from the grid).

        Returns:
            WeatherResult with available data in the configured units.
        """
        return self.get_weather_range(location, target_date, target_date, elements)[0]

    def get_weather_range(
        self,
        location: LocationInput,
        start_date: date,
        end_date: date,
        elements: list[str] | None = None,
    ) -> list[WeatherResult]:
        """Get gridded weather for a location over a date range.

        Args:
            location: 5-digit US ZIP code, "lat,lon" string, or
                (lat, lon) tuple.
            start_date: Start date.
            end_date: End date.
            elements: Element codes to retrieve (default: all).

        Returns:
            List of WeatherResult objects, one per day.

        Raises:
            ValueError: If the location cannot be parsed.
        """  # noqa: DOC502 - raised by parse_location/normalize_elements
        requested = normalize_elements(elements)
        grid_elements = [e for e in requested if e in GRID_VARS]
        parsed = parse_location(location)

        zipcode: str | None = None
        if isinstance(parsed, str):
            zipcode = parsed
            coords = self._resolve_zip(zipcode)
        else:
            coords = parsed

        n_days = (end_date - start_date).days + 1

        def _bare(day_offset: int) -> WeatherResult:
            return WeatherResult(
                date=start_date + timedelta(days=day_offset),
                zipcode=zipcode,
                latitude=coords[0] if coords else None,
                longitude=coords[1] if coords else None,
                units=self.units,
            )

        if coords is None or not grid_elements or not in_conus(*coords):
            if coords is not None and not in_conus(*coords):
                logger.warning(
                    "Point (%.2f, %.2f) is outside the nClimGrid CONUS domain",
                    *coords,
                )
            return [_bare(i) for i in range(n_days)]

        lat, lon = coords
        by_date = self._read_cell(lat, lon, start_date, end_date, grid_elements)

        results = []
        current = start_date
        while current <= end_date:
            values = by_date.get(current, {})
            station = StationMeta(station_type="gridded" if values else None)
            results.append(
                assemble_result(
                    target_date=current,
                    metric_values=values,
                    station=station,
                    units=self.units,
                    requested=requested,
                    zipcode=zipcode,
                    latitude=lat,
                    longitude=lon,
                )
            )
            current += timedelta(days=1)
        return results

    def _resolve_zip(self, zipcode: str) -> tuple[float, float] | None:
        """Resolve a ZIP code to its centroid via GeoNames."""
        if self._zip_coords is None:
            self._zip_coords = self.zip_coordinates_loader()
        coords = self._zip_coords.get(zipcode)
        if coords is None:
            logger.warning("ZIP code %s not found in GeoNames data", zipcode)
        return coords

    def _read_cell(
        self,
        lat: float,
        lon: float,
        start_date: date,
        end_date: date,
        grid_elements: list[str],
        method: str = "nearest",
    ) -> dict[date, dict[str, float]]:
        """Slice the nearest grid cell across a range, month by month."""
        by_date: dict[date, dict[str, float]] = {}
        variables = [GRID_VARS[e] for e in grid_elements]

        for year, month in _iter_months(start_date, end_date):
            dataset = self.dataset_opener(year, month)
            if dataset is None:
                continue
            with dataset as ds:
                point = ds[variables].sel(lat=lat, lon=lon, method=method)
                # nClimGrid time decodes to datetime64; take the calendar date
                times = [
                    ts.astype("datetime64[D]").item() for ts in point["time"].to_numpy()
                ]
                for element, variable in zip(grid_elements, variables, strict=True):
                    series = point[variable].to_numpy()
                    for day, raw in zip(times, series, strict=True):
                        if start_date <= day <= end_date:
                            value = float(raw)
                            if not math.isnan(value):
                                by_date.setdefault(day, {})[element] = value
        return by_date
