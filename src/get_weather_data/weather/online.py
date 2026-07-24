"""Online weather lookup via the NOAA CDO Web Services v2 API.

Answers the same questions as WeatherLookup without the local station
database: no setup() build, but an NCDC_TOKEN and network access are
required. ZIP centroids come from a small cached GeoNames file (a few
MB, downloaded once); stations are found near the query point via the
CDO /stations endpoint, since CDO's own ZIP locations rarely contain a
GHCND station.
"""

import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from get_weather_data.api.noaa import NOAAClient, StationInfo
from get_weather_data.core.distance import meters_distance
from get_weather_data.stations.zipcodes import download_zipcodes, parse_zipcodes
from get_weather_data.weather.location import LocationInput, parse_location
from get_weather_data.weather.results import (
    StationMeta,
    WeatherResult,
    assemble_result,
)
from get_weather_data.weather.units import (
    Units,
    ghcn_raw_to_metric,
    normalize_elements,
)

logger = logging.getLogger("get_weather_data")

# Bounding-box half-sizes tried for the station search, in degrees of
# latitude (~110 km each). Widened progressively for sparse regions.
EXTENT_STEPS = (1.0, 2.0, 4.0)

_StationList = list[tuple[StationInfo, int]]


def _default_zip_coordinates() -> dict[str, tuple[float, float]]:
    """Load ZIP centroids from the cached GeoNames file."""
    path = download_zipcodes()
    return {z["zipcode"]: (z["lat"], z["lon"]) for z in parse_zipcodes(path)}


@dataclass
class OnlineLookup:
    """Look up weather for locations via the CDO API."""

    client: NOAAClient = field(default_factory=NOAAClient)
    units: Units = "metric"
    max_stations: int = 10
    zip_coordinates_loader: Callable[[], dict[str, tuple[float, float]]] | None = None
    _zip_coords: dict[str, tuple[float, float]] | None = field(default=None, repr=False)
    _station_lists: dict[tuple[float, float, int, int], _StationList] = field(
        default_factory=dict, repr=False
    )

    def get_weather(
        self,
        location: LocationInput,
        target_date: date,
        elements: list[str] | None = None,
    ) -> WeatherResult:
        """Get weather data for a location and date.

        Args:
            location: 5-digit US ZIP code, "lat,lon" string, or
                (lat, lon) tuple.
            target_date: Date to get weather for.
            elements: Element codes to retrieve (default: all).

        Returns:
            WeatherResult with available data in the configured units.
        """
        results = self.get_weather_range(location, target_date, target_date, elements)
        return results[0]

    def get_weather_range(
        self,
        location: LocationInput,
        start_date: date,
        end_date: date,
        elements: list[str] | None = None,
    ) -> list[WeatherResult]:
        """Get weather data for a location over a date range.

        The range is fetched in at most one API request per calendar
        year (CDO caps GHCND requests at one year), never per day.

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
        parsed = parse_location(location)

        zipcode: str | None = None
        if isinstance(parsed, str):
            zipcode = parsed
            coords = self._resolve_zip(zipcode)
        else:
            coords = parsed

        def _empty(day_offset: int) -> WeatherResult:
            return WeatherResult(
                date=start_date + timedelta(days=day_offset),
                zipcode=zipcode,
                latitude=coords[0] if coords else None,
                longitude=coords[1] if coords else None,
                units=self.units,
            )

        n_days = (end_date - start_date).days + 1
        if coords is None:
            return [_empty(i) for i in range(n_days)]

        lat, lon = coords
        stations = self._closest_stations(lat, lon, start_date, end_date)
        if not stations:
            return [_empty(i) for i in range(n_days)]

        station_ids = [info.id for info, _ in stations]
        records: list[dict[str, Any]] = []
        chunk_start = start_date
        while chunk_start <= end_date:
            chunk_end = min(date(chunk_start.year, 12, 31), end_date)
            records.extend(
                self.client.get_data_for_stations(station_ids, chunk_start, chunk_end)
            )
            chunk_start = chunk_end + timedelta(days=1)

        by_date: dict[date, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            record_date = _record_date(record)
            if record_date is not None:
                by_date[record_date].append(record)

        results = []
        current = start_date
        while current <= end_date:
            results.append(
                self._build_result(
                    current,
                    by_date.get(current, []),
                    stations,
                    requested,
                    zipcode,
                    lat,
                    lon,
                )
            )
            current += timedelta(days=1)
        return results

    def _resolve_zip(self, zipcode: str) -> tuple[float, float] | None:
        """Resolve a ZIP code to its centroid via GeoNames."""
        if self._zip_coords is None:
            loader = self.zip_coordinates_loader or _default_zip_coordinates
            self._zip_coords = loader()
        coords = self._zip_coords.get(zipcode)
        if coords is None:
            logger.warning("ZIP code %s not found in GeoNames data", zipcode)
        return coords

    def _closest_stations(
        self, lat: float, lon: float, start: date, end: date
    ) -> _StationList:
        """Nearest GHCND stations to a point, with distances in meters."""
        # Round the key so nearby queries and same-year ranges share the
        # station list (each /stations call spends API quota)
        cache_key = (round(lat, 2), round(lon, 2), start.year, end.year)
        if cache_key in self._station_lists:
            return self._station_lists[cache_key]

        candidates: list[StationInfo] = []
        for extent in EXTENT_STEPS:
            candidates = self.client.get_stations(
                (lat - extent, lon - extent, lat + extent, lon + extent),
                start,
                end,
            )
            if candidates:
                break
            logger.info(
                "No CDO stations within %.0f deg of (%.2f, %.2f); widening",
                extent,
                lat,
                lon,
            )

        ranked = sorted(
            (
                (info, int(meters_distance(lat, lon, info.latitude, info.longitude)))
                for info in candidates
                if info.latitude is not None and info.longitude is not None
            ),
            key=lambda pair: pair[1],
        )[: self.max_stations]
        if not ranked:
            logger.warning("No CDO stations found near (%.2f, %.2f)", lat, lon)
        self._station_lists[cache_key] = ranked
        return ranked

    def _build_result(
        self,
        target_date: date,
        records: list[dict[str, Any]],
        stations: _StationList,
        requested: list[str],
        zipcode: str | None,
        lat: float,
        lon: float,
    ) -> WeatherResult:
        """Assemble one day's result from CDO records, nearest first."""
        wanted = set(requested)
        by_station: dict[str, dict[str, float]] = defaultdict(dict)
        for record in records:
            datatype = record.get("datatype")
            station = record.get("station")
            value = record.get("value")
            if not datatype or not station or value is None or datatype not in wanted:
                continue
            by_station[station][datatype] = ghcn_raw_to_metric(datatype, float(value))

        values: dict[str, float] = {}
        meta = StationMeta()
        for info, distance in stations:
            station_values = by_station.get(info.id)
            if not station_values:
                continue
            if meta.station_id is None:
                meta = StationMeta(
                    # Bulk results carry bare GHCN ids; strip CDO's prefix
                    station_id=info.id.removeprefix("GHCND:"),
                    station_name=info.name,
                    station_type="GHCND",
                    station_distance_meters=distance,
                )
            for element, value in station_values.items():
                values.setdefault(element, value)

        return assemble_result(
            target_date=target_date,
            metric_values=values,
            station=meta,
            units=self.units,
            requested=requested,
            zipcode=zipcode,
            latitude=lat,
            longitude=lon,
        )


def _record_date(record: dict[str, Any]) -> date | None:
    """Parse the date out of a CDO /data record ("2024-01-15T00:00:00")."""
    raw = record.get("date")
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None
