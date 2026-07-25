"""Weather result type and the single assembly/conversion boundary."""

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date as date_type

from get_weather_data.weather.units import ELEMENTS, Units, convert


@dataclass
class WeatherResult:
    """Weather data for one location and date.

    Value fields are in the unit system named by ``units``:
    metric — tmax/tmin/tavg/tobs in °C, prcp/snow/snwd in mm, awnd in m/s;
    imperial — °F, inches, mph. Fields are None when no station reported
    that element.

    Attributes:
        date: The calendar date of the observations.
        zipcode: Queried ZIP code, when the query used one.
        latitude: Latitude of the resolved query point.
        longitude: Longitude of the resolved query point.
        station_id: Station that supplied the first-found element.
        station_name: Its human-readable name.
        station_type: "GHCND" or "USAF-WBAN".
        station_distance_meters: Distance from the query point, when known.
        units: Unit system of the value fields.
        tmax: Maximum temperature.
        tmin: Minimum temperature.
        tavg: Average temperature.
        tobs: Temperature at observation time.
        prcp: Precipitation.
        snow: Snowfall (GHCN stations only; GSOD has no snowfall element).
        snwd: Snow depth.
        awnd: Average wind speed.
        wind_gust: Peak wind gust.
        dewpoint: Average dew point temperature.
        sea_level_pressure: Sea-level pressure (hPa / inHg).
        station_pressure: Station-level pressure (hPa / inHg).
        visibility: Visibility (km / mi; GSOD stations only).
        flags: Per-field GHCN quality-control flag, when include_flags is
            set; a blank flag means the value passed all QC checks
            (GHCN stations only).
        weather_types: Present-weather phenomena for the day (e.g.
            {"fog", "thunder"}), when include_weather_types is set.
        stations_considered: How many stations were examined to build
            this result, when explain is set.
        missing: Per-field reason a requested value is absent (e.g.
            {"tmax": "none of the 20 nearest stations ..."}), when
            explain is set; empty when every requested field was found.
    """

    date: date_type
    zipcode: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    station_id: str | None = None
    station_name: str | None = None
    station_type: str | None = None
    station_distance_meters: int | None = None
    units: Units = "metric"
    tmax: float | None = None
    tmin: float | None = None
    tavg: float | None = None
    tobs: float | None = None
    prcp: float | None = None
    snow: float | None = None
    snwd: float | None = None
    awnd: float | None = None
    wind_gust: float | None = None
    dewpoint: float | None = None
    sea_level_pressure: float | None = None
    station_pressure: float | None = None
    visibility: float | None = None
    flags: dict[str, str] | None = None
    weather_types: set[str] | None = None
    stations_considered: int | None = None
    missing: dict[str, str] | None = None


@dataclass
class StationMeta:
    """Identity of the station credited on a result."""

    station_id: str | None = None
    station_name: str | None = None
    station_type: str | None = None
    station_distance_meters: int | None = None


@dataclass
class Coverage:
    """How well a location is covered over a date range.

    Attributes:
        total_days: Number of days in the range.
        station_id: The station credited on the most days.
        station_name: Its name.
        station_distance_meters: Its distance from the query point.
        available: Per-field count of days with a value.
    """

    total_days: int
    station_id: str | None = None
    station_name: str | None = None
    station_distance_meters: int | None = None
    available: dict[str, int] = field(default_factory=dict)

    def fraction(self, element_field: str) -> float:
        """Fraction of days with data for one field (0.0 to 1.0).

        Args:
            element_field: A weather value field name (e.g. "tmax").

        Returns:
            Days-present / total-days, or 0.0 for an empty range.
        """
        if self.total_days == 0:
            return 0.0
        return self.available.get(element_field, 0) / self.total_days


def summarize_coverage(results: Sequence["WeatherResult"]) -> Coverage:
    """Summarize per-element availability across a range of results.

    Args:
        results: One result per day, e.g. from ``get_range``.

    Returns:
        A Coverage report crediting the most-frequent station.
    """
    fields = [spec.field for spec in ELEMENTS.values()]
    available = {
        f: sum(1 for r in results if getattr(r, f) is not None) for f in fields
    }
    station_counts: Counter[str] = Counter(
        r.station_id for r in results if r.station_id is not None
    )
    top_id = station_counts.most_common(1)[0][0] if station_counts else None
    credited = next((r for r in results if r.station_id == top_id), None)
    return Coverage(
        total_days=len(results),
        station_id=top_id,
        station_name=credited.station_name if credited else None,
        station_distance_meters=(
            credited.station_distance_meters if credited else None
        ),
        available=available,
    )


def assemble_result(
    target_date: date_type,
    metric_values: dict[str, float],
    station: StationMeta,
    units: Units,
    requested: list[str],
    zipcode: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
) -> WeatherResult:
    """Build a WeatherResult from metric element values.

    This is the only place unit conversion and element filtering happen;
    both the local and online lookup paths go through it.

    Args:
        target_date: Date of the observations.
        metric_values: Element code -> value in metric units.
        station: Credited station identity.
        units: Requested unit system.
        requested: Element codes to include (already normalized).
        zipcode: Originating ZIP code, if any.
        latitude: Resolved query latitude.
        longitude: Resolved query longitude.

    Returns:
        WeatherResult with converted, filtered values.
    """
    result = WeatherResult(
        date=target_date,
        zipcode=zipcode,
        latitude=latitude,
        longitude=longitude,
        station_id=station.station_id,
        station_name=station.station_name,
        station_type=station.station_type,
        station_distance_meters=station.station_distance_meters,
        units=units,
    )
    for element in requested:
        value = metric_values.get(element)
        if value is not None:
            setattr(result, ELEMENTS[element].field, convert(element, value, units))
    return result
