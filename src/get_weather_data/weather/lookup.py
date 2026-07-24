"""Weather data lookup backed by the local station database."""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import lru_cache

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import find_closest
from get_weather_data.weather.ghcn import get_ghcn_data
from get_weather_data.weather.gsod import get_gsod_data
from get_weather_data.weather.location import LocationInput, parse_location
from get_weather_data.weather.results import (
    StationMeta,
    WeatherResult,
    assemble_result,
)
from get_weather_data.weather.units import (
    IN_TO_MM,
    Units,
    ghcn_raw_to_metric,
    normalize_elements,
)

logger = logging.getLogger("get_weather_data")

# GSOD field -> (GHCN element, converter to metric). Temps arrive in °C
# and wind in m/s (converted inside gsod.py); precip and snow depth are
# still inches. GSOD has no snowfall element, so SNOW is never filled
# from a GSOD station.
_GSOD_TO_METRIC = {
    "max_temp": ("TMAX", 1.0),
    "min_temp": ("TMIN", 1.0),
    "temp": ("TAVG", 1.0),
    "precipitation": ("PRCP", IN_TO_MM),
    "snow_depth": ("SNWD", IN_TO_MM),
    "wind_speed": ("AWND", 1.0),
}


@lru_cache(maxsize=65536)
def _cached_ghcn_data(
    station_id: str, year: int, month: int, day: int
) -> dict[str, float | None]:
    """Cached GHCN data lookup (all elements)."""
    return get_ghcn_data(station_id, date(year, month, day))


@lru_cache(maxsize=65536)
def _cached_gsod_data(
    station_id: str, year: int, month: int, day: int
) -> dict[str, float | None]:
    """Cached GSOD data lookup."""
    return get_gsod_data(station_id, date(year, month, day))


def _ghcn_metric(raw: dict[str, float | None]) -> dict[str, float]:
    """Convert a raw GHCN element dict to metric values."""
    return {
        element: ghcn_raw_to_metric(element, value)
        for element, value in raw.items()
        if value is not None
    }


def _gsod_metric(raw: dict[str, float | None]) -> dict[str, float]:
    """Convert a GSOD field dict to metric GHCN-element values."""
    values: dict[str, float] = {}
    for gsod_field, (element, factor) in _GSOD_TO_METRIC.items():
        value = raw.get(gsod_field)
        if value is not None:
            values[element] = value * factor
    return values


@dataclass
class WeatherLookup:
    """Look up weather data via the local station database.

    Uses caching for improved performance on repeated queries.
    """

    db: Database = field(default_factory=Database)
    units: Units = "metric"
    max_stations: int = 10  # Try more stations before giving up (fallback)
    max_distance_meters: int | None = None
    use_ghcn: bool = True
    use_gsod: bool = True
    use_cache: bool = True

    def __post_init__(self) -> None:
        """Preload caches and check the index version."""
        if self.db.exists():
            self.db.preload_caches()
            stored = self.db.get_meta("index_version")
            if stored != str(INDEX_VERSION):
                logger.warning(
                    "Station index was built by an older version and its "
                    "distances are unreliable. Run setup(force=True) "
                    "(CLI: get-weather setup --force) to rebuild."
                )

    def get_weather(
        self,
        location: LocationInput,
        target_date: date,
        elements: list[str] | None = None,
    ) -> WeatherResult:
        """Get weather data for a location and date.

        Walks the closest stations, nearest first, until every requested
        element has a value or the station budget runs out.

        Args:
            location: 5-digit US ZIP code, "lat,lon" string, or
                (lat, lon) tuple.
            target_date: Date to get weather for.
            elements: Element codes to retrieve (default: all).

        Returns:
            WeatherResult with available data in the configured units.

        Raises:
            ValueError: If the location cannot be parsed.
        """  # noqa: DOC502 - raised by parse_location/normalize_elements
        requested = normalize_elements(elements)
        parsed = parse_location(location)

        zipcode: str | None = None
        if isinstance(parsed, str):
            zipcode = parsed
            coords = self.db.get_zipcode(zipcode)
            if coords is None:
                logger.warning(f"ZIP code {zipcode} not found in database")
                return WeatherResult(
                    date=target_date, zipcode=zipcode, units=self.units
                )
            lat, lon = coords
            closest = self._closest_stations_for_zip(zipcode, lat, lon)
        else:
            lat, lon = parsed
            closest = self._closest_stations_for_coords(lat, lon)

        values: dict[str, float] = {}
        station = StationMeta()

        for station_id, distance in closest[: self.max_stations]:
            if self.max_distance_meters and distance > self.max_distance_meters:
                break
            if all(element in values for element in requested):
                break

            station_info = self.db.get_station_info(station_id)
            if not station_info:
                continue
            station_name, station_type = station_info

            metric = self._station_values(station_id, station_type, target_date)
            new_elements = {
                element: value
                for element, value in metric.items()
                if element in requested and element not in values
            }
            if not new_elements:
                continue

            values.update(new_elements)
            if station.station_id is None:
                station = StationMeta(
                    station_id=station_id,
                    station_name=station_name,
                    station_type=station_type,
                    station_distance_meters=distance,
                )

        return assemble_result(
            target_date=target_date,
            metric_values=values,
            station=station,
            units=self.units,
            requested=requested,
            zipcode=zipcode,
            latitude=lat,
            longitude=lon,
        )

    def _closest_stations_for_zip(
        self, zipcode: str, lat: float, lon: float
    ) -> list[tuple[str, int]]:
        """Closest stations for a ZIP, from the index or computed live."""
        closest = self.db.get_closest_stations(zipcode)
        if closest:
            return closest
        return self._closest_stations_for_coords(lat, lon)

    def _closest_stations_for_coords(
        self, lat: float, lon: float
    ) -> list[tuple[str, int]]:
        """Closest stations to a coordinate, via in-memory spatial search."""
        ghcn_stations = self.db.get_stations(station_type="GHCND")
        usaf_stations = self.db.get_stations(station_type="USAF-WBAN")
        pairs = [
            (sd.station.id, sd.distance_meters)
            for sd in find_closest(lat, lon, ghcn_stations, n=5)
        ]
        pairs.extend(
            (sd.station.id, sd.distance_meters)
            for sd in find_closest(lat, lon, usaf_stations, n=3)
        )
        pairs.sort(key=lambda pair: pair[1])
        return pairs

    def _station_values(
        self, station_id: str, station_type: str, target_date: date
    ) -> dict[str, float]:
        """Fetch one station's observations for a date, in metric units."""
        if station_type == "GHCND" and self.use_ghcn:
            if self.use_cache:
                raw = _cached_ghcn_data(
                    station_id,
                    target_date.year,
                    target_date.month,
                    target_date.day,
                )
            else:
                raw = get_ghcn_data(station_id, target_date)
            return _ghcn_metric(raw)
        if station_type == "USAF-WBAN" and self.use_gsod:
            if self.use_cache:
                raw = _cached_gsod_data(
                    station_id,
                    target_date.year,
                    target_date.month,
                    target_date.day,
                )
            else:
                raw = get_gsod_data(station_id, target_date)
            return _gsod_metric(raw)
        return {}

    def get_weather_range(
        self,
        location: LocationInput,
        start_date: date,
        end_date: date,
        elements: list[str] | None = None,
    ) -> list[WeatherResult]:
        """Get weather data for a location over a date range.

        Args:
            location: 5-digit US ZIP code, "lat,lon" string, or
                (lat, lon) tuple.
            start_date: Start date.
            end_date: End date.
            elements: Element codes to retrieve (default: all).

        Returns:
            List of WeatherResult objects, one per day.
        """
        results = []
        current = start_date
        while current <= end_date:
            results.append(self.get_weather(location, current, elements))
            current += timedelta(days=1)
        return results

    def clear_cache(self) -> None:
        """Clear the weather data cache."""
        _cached_ghcn_data.cache_clear()
        _cached_gsod_data.cache_clear()

    def cache_info(self) -> dict:
        """Get cache statistics."""
        return {
            "ghcn": _cached_ghcn_data.cache_info(),
            "gsod": _cached_gsod_data.cache_info(),
        }
