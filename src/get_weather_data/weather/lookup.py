"""Weather data lookup by ZIP code."""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

from get_weather_data.core.database import Database
from get_weather_data.core.distance import find_closest
from get_weather_data.weather.ghcn import GHCN_ELEMENTS, get_ghcn_data
from get_weather_data.weather.gsod import get_gsod_data

logger = logging.getLogger("get_weather_data")


@dataclass
class WeatherResult:
    """Weather data result for a ZIP code and date."""

    zipcode: str
    date: date
    station_id: str | None = None
    station_name: str | None = None
    station_type: str | None = None
    station_distance_meters: int | None = None
    tmax: float | None = None
    tmin: float | None = None
    tavg: float | None = None
    prcp: float | None = None
    snow: float | None = None
    snwd: float | None = None
    awnd: float | None = None


@dataclass
class WeatherLookup:
    """Look up weather data for ZIP codes."""

    db: Database = field(default_factory=Database)
    max_stations: int = 5
    max_distance_meters: int | None = None
    use_ghcn: bool = True
    use_gsod: bool = True

    def get_weather(
        self,
        zipcode: str,
        target_date: date,
        elements: list[str] | None = None,
    ) -> WeatherResult:
        """Get weather data for a ZIP code and date.

        Searches closest stations until data is found.

        Args:
            zipcode: 5-digit US ZIP code.
            target_date: Date to get weather for.
            elements: List of elements to retrieve.

        Returns:
            WeatherResult with available data.
        """
        zipcode = zipcode.zfill(5)

        if elements is None:
            elements = GHCN_ELEMENTS

        result = WeatherResult(zipcode=zipcode, date=target_date)

        coords = self.db.get_zipcode(zipcode)
        if coords is None:
            logger.warning(f"ZIP code {zipcode} not found in database")
            return result

        lat, lon = coords

        closest = self.db.get_closest_stations(zipcode)
        if not closest:
            ghcn_stations = self.db.get_stations(station_type="GHCND")
            usaf_stations = self.db.get_stations(station_type="USAF-WBAN")

            ghcn_closest = find_closest(lat, lon, ghcn_stations, n=3)
            usaf_closest = find_closest(lat, lon, usaf_stations, n=2)

            closest = [(sd.station.id, sd.distance_meters) for sd in ghcn_closest]
            closest.extend([(sd.station.id, sd.distance_meters) for sd in usaf_closest])
            closest.sort(key=lambda x: x[1])

        values: dict[str, float | None] = {}
        found_elements: set[str] = set()

        for station_id, distance in closest[: self.max_stations]:
            if self.max_distance_meters and distance > self.max_distance_meters:
                break

            if elements and len(found_elements) >= len(elements):
                break

            stations = self.db.execute(
                "SELECT name, type FROM stations WHERE id = ?", (station_id,)
            )
            if not stations:
                continue

            station_name, station_type = stations[0]

            if station_type == "GHCND" and self.use_ghcn:
                data = get_ghcn_data(station_id, target_date, elements)
            elif station_type == "USAF-WBAN" and self.use_gsod:
                gsod_data = get_gsod_data(station_id, target_date)
                data = {
                    "TMAX": gsod_data.get("max_temp"),
                    "TMIN": gsod_data.get("min_temp"),
                    "TAVG": gsod_data.get("temp"),
                    "PRCP": gsod_data.get("precipitation"),
                    "SNWD": gsod_data.get("snow_depth"),
                    "AWND": gsod_data.get("wind_speed"),
                }
                data = {k: v * 10 if v else None for k, v in data.items()}
            else:
                continue

            for elem, val in data.items():
                if elem not in found_elements and val is not None:
                    values[elem] = val
                    found_elements.add(elem)

                    if result.station_id is None:
                        result.station_id = station_id
                        result.station_name = station_name
                        result.station_type = station_type
                        result.station_distance_meters = distance

        result.tmax = values.get("TMAX")
        result.tmin = values.get("TMIN")
        result.tavg = values.get("TAVG")
        result.prcp = values.get("PRCP")
        result.snow = values.get("SNOW")
        result.snwd = values.get("SNWD")
        result.awnd = values.get("AWND")

        return result

    def get_weather_range(
        self,
        zipcode: str,
        start_date: date,
        end_date: date,
        elements: list[str] | None = None,
    ) -> list[WeatherResult]:
        """Get weather data for a ZIP code over a date range.

        Args:
            zipcode: 5-digit US ZIP code.
            start_date: Start date.
            end_date: End date.
            elements: List of elements to retrieve.

        Returns:
            List of WeatherResult objects, one per day.
        """
        results = []
        current = start_date
        while current <= end_date:
            results.append(self.get_weather(zipcode, current, elements))
            current += timedelta(days=1)
        return results
