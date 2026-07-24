"""Weather result type and the single assembly/conversion boundary."""

from dataclasses import dataclass
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


@dataclass
class StationMeta:
    """Identity of the station credited on a result."""

    station_id: str | None = None
    station_name: str | None = None
    station_type: str | None = None
    station_distance_meters: int | None = None


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
