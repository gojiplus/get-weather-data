"""Online weather lookup via the NOAA CDO Web Services v2 API.

Answers the same questions as WeatherLookup without the local station
database: no setup() build, but an NCDC_TOKEN and network access are
required. ZIP centroids come from a small cached GeoNames file (a few
MB, downloaded once); stations are found near the centroid via the CDO
/stations endpoint, since CDO's own ZIP locations rarely contain a
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
from get_weather_data.weather.ghcn import GHCN_ELEMENTS
from get_weather_data.weather.lookup import WeatherResult

logger = logging.getLogger("get_weather_data")

# Bounding box half-size for the station search, in degrees (~110 km of
# latitude); mirrors the bulk path's nearest-station behavior.
EXTENT_DEGREES = 1.0


def _default_zip_coordinates() -> dict[str, tuple[float, float]]:
    """Load ZIP centroids from the cached GeoNames file."""
    path = download_zipcodes()
    return {z["zipcode"]: (z["lat"], z["lon"]) for z in parse_zipcodes(path)}


@dataclass
class OnlineLookup:
    """Look up weather for ZIP codes via the CDO API."""

    client: NOAAClient = field(default_factory=NOAAClient)
    max_stations: int = 10
    zip_coordinates_loader: Callable[[], dict[str, tuple[float, float]]] | None = None
    _zip_coords: dict[str, tuple[float, float]] | None = field(default=None, repr=False)
    _station_lists: dict[str, list[tuple[StationInfo, int]]] = field(
        default_factory=dict, repr=False
    )

    def get_weather(
        self,
        zipcode: str,
        target_date: date,
        elements: list[str] | None = None,
    ) -> WeatherResult:
        """Get weather data for a ZIP code and date.

        Args:
            zipcode: 5-digit US ZIP code.
            target_date: Date to get weather for.
            elements: List of elements to retrieve.

        Returns:
            WeatherResult with available data (values in GHCN raw units).
        """
        results = self.get_weather_range(zipcode, target_date, target_date, elements)
        return results[0]

    def get_weather_range(
        self,
        zipcode: str,
        start_date: date,
        end_date: date,
        elements: list[str] | None = None,
    ) -> list[WeatherResult]:
        """Get weather data for a ZIP code over a date range.

        The range is fetched in at most one API request per calendar
        year (CDO caps GHCND requests at one year), never per day.

        Args:
            zipcode: 5-digit US ZIP code.
            start_date: Start date.
            end_date: End date.
            elements: List of elements to retrieve.

        Returns:
            List of WeatherResult objects, one per day.
        """
        zipcode = zipcode.zfill(5)
        stations = self._closest_stations(zipcode, start_date, end_date)
        if not stations:
            return [
                WeatherResult(zipcode=zipcode, date=start_date + timedelta(days=i))
                for i in range((end_date - start_date).days + 1)
            ]

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
                    zipcode, current, by_date.get(current, []), stations, elements
                )
            )
            current += timedelta(days=1)
        return results

    def _closest_stations(
        self, zipcode: str, start: date, end: date
    ) -> list[tuple[StationInfo, int]]:
        """Nearest GHCND stations to a ZIP centroid, with distances in meters."""
        cache_key = f"{zipcode}:{start.isoformat()}:{end.isoformat()}"
        if cache_key in self._station_lists:
            return self._station_lists[cache_key]

        if self._zip_coords is None:
            loader = self.zip_coordinates_loader or _default_zip_coordinates
            self._zip_coords = loader()

        coords = self._zip_coords.get(zipcode)
        if coords is None:
            logger.warning("ZIP code %s not found in GeoNames data", zipcode)
            self._station_lists[cache_key] = []
            return []

        lat, lon = coords
        candidates = self.client.get_stations(
            (
                lat - EXTENT_DEGREES,
                lon - EXTENT_DEGREES,
                lat + EXTENT_DEGREES,
                lon + EXTENT_DEGREES,
            ),
            start,
            end,
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
            logger.warning("No CDO stations found near ZIP %s", zipcode)
        self._station_lists[cache_key] = ranked
        return ranked

    def _build_result(
        self,
        zipcode: str,
        target_date: date,
        records: list[dict[str, Any]],
        stations: list[tuple[StationInfo, int]],
        elements: list[str] | None,
    ) -> WeatherResult:
        """Assemble a WeatherResult from one day's records, nearest first."""
        wanted = set(elements if elements is not None else GHCN_ELEMENTS)
        result = WeatherResult(zipcode=zipcode, date=target_date)

        by_station: dict[str, dict[str, float]] = defaultdict(dict)
        for record in records:
            datatype = record.get("datatype")
            station = record.get("station")
            value = record.get("value")
            if not datatype or not station or value is None or datatype not in wanted:
                continue
            by_station[station][datatype] = float(value)

        values: dict[str, float] = {}
        for info, distance in stations:
            station_values = by_station.get(info.id)
            if not station_values:
                continue
            if result.station_id is None:
                # Bulk results carry bare GHCN ids; strip CDO's prefix
                result.station_id = info.id.removeprefix("GHCND:")
                result.station_name = info.name
                result.station_type = "GHCND"
                result.station_distance_meters = distance
            for element, value in station_values.items():
                values.setdefault(element, value)

        result.tmax = values.get("TMAX")
        result.tmin = values.get("TMIN")
        result.tavg = values.get("TAVG")
        result.prcp = values.get("PRCP")
        result.snow = values.get("SNOW")
        result.snwd = values.get("SNWD")
        result.awnd = values.get("AWND")
        return result


def _record_date(record: dict[str, Any]) -> date | None:
    """Parse the date out of a CDO /data record ("2024-01-15T00:00:00")."""
    raw = record.get("date")
    if not raw:
        return None
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None
