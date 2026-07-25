"""Hourly weather lookup backed by ISD-Lite and the local station DB.

Resolves a location to the nearest USAF-WBAN (ISD) station using the
same station database as the daily path, then reads hourly observations
from ISD-Lite. Station selection needs the local database (``setup()``);
there is no online (CDO) equivalent for hourly data.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from get_weather_data.core.database import Database
from get_weather_data.core.distance import find_closest
from get_weather_data.weather.isd import get_isd_hourly
from get_weather_data.weather.location import LocationInput, parse_location
from get_weather_data.weather.results import HourlyResult
from get_weather_data.weather.units import (
    HPA_TO_INHG,
    IN_TO_MM,
    MS_TO_MPH,
    Units,
)

logger = logging.getLogger("get_weather_data")

# Nearest USAF-WBAN stations to try before giving up (many stations have
# a GSOD daily record but no ISD-Lite file for a given year).
_MAX_STATIONS = 5


def _to_units(value: float | None, units: Units, factor: float) -> float | None:
    """Scale a metric value to the requested unit system."""
    if value is None:
        return None
    return value if units == "metric" else value * factor


def _c_to_f(celsius: float | None, units: Units) -> float | None:
    """Convert °C to the requested unit system."""
    if celsius is None:
        return None
    return celsius if units == "metric" else celsius * 9 / 5 + 32


@dataclass
class HourlyLookup:
    """Look up hourly weather via ISD-Lite and the local station DB."""

    db: Database = field(default_factory=Database)
    units: Units = "metric"
    max_stations: int = _MAX_STATIONS

    def get_hourly(
        self,
        location: LocationInput,
        start_date: date,
        end_date: date | None = None,
    ) -> list[HourlyResult]:
        """Get hourly observations for a location over a date range.

        Args:
            location: 5-digit US ZIP code, "lat,lon" string, or
                (lat, lon) tuple.
            start_date: First UTC date (inclusive).
            end_date: Last UTC date (inclusive); defaults to start_date.

        Returns:
            One HourlyResult per available hour across the range, in
            time order. Empty when no nearby ISD station has data.

        Raises:
            ValueError: If the location cannot be parsed.
        """  # noqa: DOC502 - raised by parse_location
        if end_date is None:
            end_date = start_date

        coords, zipcode = self._resolve(location)
        if coords is None:
            return []
        lat, lon = coords

        dates = _date_range(start_date, end_date)
        for station in self._nearest_stations(lat, lon):
            station_id, station_name, distance = station
            records: list[dict[str, float | int | datetime | None]] = []
            for day in dates:
                records.extend(get_isd_hourly(station_id, day))
            if records:
                return [
                    self._build(
                        r, zipcode, lat, lon, station_id, station_name, distance
                    )
                    for r in records
                ]

        logger.debug(
            "No ISD-Lite data near %s for %s..%s",
            zipcode or f"{lat:.3f},{lon:.3f}",
            start_date,
            end_date,
        )
        return []

    def _resolve(
        self, location: LocationInput
    ) -> tuple[tuple[float, float] | None, str | None]:
        """Resolve a location to coordinates and (if any) its ZIP code."""
        parsed = parse_location(location)
        if isinstance(parsed, str):
            coords = self.db.get_zipcode(parsed)
            if coords is None:
                logger.warning("ZIP code %s not found in database", parsed)
            return coords, parsed
        return parsed, None

    def _nearest_stations(self, lat: float, lon: float) -> list[tuple[str, str, int]]:
        """Nearest USAF-WBAN stations as (id, name, distance_meters)."""
        stations = self.db.get_stations(station_type="USAF-WBAN")
        closest = find_closest(lat, lon, stations, n=self.max_stations)
        return [(sd.station.id, sd.station.name, sd.distance_meters) for sd in closest]

    def _build(
        self,
        record: dict[str, float | int | datetime | None],
        zipcode: str | None,
        lat: float,
        lon: float,
        station_id: str,
        station_name: str,
        distance: int,
    ) -> HourlyResult:
        """Convert one metric ISD-Lite record to a HourlyResult."""
        u = self.units
        return HourlyResult(
            observed_at=record["observed_at"],  # type: ignore[arg-type]
            zipcode=zipcode,
            latitude=lat,
            longitude=lon,
            station_id=station_id,
            station_name=station_name,
            station_distance_meters=distance,
            units=u,
            temp=_c_to_f(record["temp"], u),  # type: ignore[arg-type]
            dewpoint=_c_to_f(record["dewpoint"], u),  # type: ignore[arg-type]
            sea_level_pressure=_to_units(
                record["sea_level_pressure"],  # type: ignore[arg-type]
                u,
                HPA_TO_INHG,
            ),
            wind_direction=record["wind_direction"],  # type: ignore[arg-type]
            wind_speed=_to_units(record["wind_speed"], u, MS_TO_MPH),  # type: ignore[arg-type]
            sky_condition=record["sky_condition"],  # type: ignore[arg-type]
            precip_1h=_to_units(record["precip_1h"], u, 1 / IN_TO_MM),  # type: ignore[arg-type]
            precip_6h=_to_units(record["precip_6h"], u, 1 / IN_TO_MM),  # type: ignore[arg-type]
        )


def _date_range(start: date, end: date) -> list[date]:
    """Inclusive list of dates from start to end."""
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]
