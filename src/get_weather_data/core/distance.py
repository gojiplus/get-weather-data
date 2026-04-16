"""Geographic distance calculations for get-weather-data."""

import math
from dataclasses import dataclass
from typing import Any, Sequence, TypeVar

# Constants for distance calculation
NAUTICAL_MILE_PER_LAT = 60.00721
NAUTICAL_MILE_PER_LON = 60.10793
RAD = math.pi / 180.0
METERS_PER_NAUTICAL_MILE = 1852

# Try to import scipy for KDTree optimization
try:
    import numpy as np
    from scipy.spatial import cKDTree

    KDTREE_AVAILABLE = True
except ImportError:
    KDTREE_AVAILABLE = False
    np = None  # type: ignore[assignment]
    cKDTree = None  # type: ignore[assignment, misc]


def meters_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two lat/lon coordinates in meters.

    Uses a simplified spherical approximation suitable for distances
    within the continental US.

    Args:
        lat1: Latitude of first point.
        lon1: Longitude of first point.
        lat2: Latitude of second point.
        lon2: Longitude of second point.

    Returns:
        Distance in meters.
    """
    y_distance = (lat2 - lat1) * NAUTICAL_MILE_PER_LAT
    x_distance = (
        (math.cos(lat1 * RAD) + math.cos(lat2 * RAD))
        * (lon2 - lon1)
        * (NAUTICAL_MILE_PER_LON / 2)
    )

    distance = math.sqrt(y_distance**2 + x_distance**2)
    return distance * METERS_PER_NAUTICAL_MILE


@dataclass
class Station:
    """A weather station with location."""

    id: str
    name: str
    lat: float
    lon: float
    type: str
    state: str = ""
    elevation: float | None = None


@dataclass
class StationDistance:
    """A station with its distance from a reference point."""

    station: Station
    distance_meters: int


T = TypeVar("T")


class StationIndex:
    """Pre-built spatial index for fast nearest-neighbor queries.

    Build once, query many times. Uses KDTree when scipy is available.
    """

    def __init__(self, stations: Sequence[Station]) -> None:
        """Build the spatial index from a list of stations."""
        self.stations = [s for s in stations if s.lat is not None and s.lon is not None]
        self._tree: Any = None
        if KDTREE_AVAILABLE and self.stations:
            coords = np.array([(s.lat, s.lon) for s in self.stations])
            self._tree = cKDTree(coords)

    def find_closest(
        self,
        lat: float,
        lon: float,
        n: int,
        max_distance_km: float | None = None,
    ) -> list[StationDistance]:
        """Query the pre-built index for closest stations.

        Args:
            lat: Latitude of reference point.
            lon: Longitude of reference point.
            n: Maximum number of stations to return.
            max_distance_km: Maximum distance in kilometers.

        Returns:
            List of StationDistance objects, sorted by distance.
        """
        if not self.stations:
            return []

        if self._tree is not None:
            k = min(n, len(self.stations))
            distances, indices = self._tree.query([lat, lon], k=k)

            # Handle single result case
            if isinstance(distances, float):
                distances = [distances]
                indices = [indices]

            results = []
            for dist_deg, idx in zip(distances, indices):
                station = self.stations[idx]
                dist_m = int(dist_deg * 111000)

                if max_distance_km is not None and dist_m > max_distance_km * 1000:
                    break

                results.append(StationDistance(station=station, distance_meters=dist_m))

            return results

        # Fallback to brute force
        return _find_closest_brute(lat, lon, self.stations, n, max_distance_km)


def find_closest(
    lat: float,
    lon: float,
    stations: Sequence[Station],
    n: int | None = None,
    max_distance_km: float | None = None,
) -> list[StationDistance]:
    """Find closest stations to a given coordinate.

    Uses KDTree for O(log n) lookup when scipy is available and
    there are many stations. Falls back to brute force O(n) otherwise.

    Args:
        lat: Latitude of reference point.
        lon: Longitude of reference point.
        stations: Sequence of Station objects to search.
        n: Maximum number of stations to return (None = all).
        max_distance_km: Maximum distance in kilometers (None = no limit).

    Returns:
        List of StationDistance objects, sorted by distance.
    """
    if not stations:
        return []

    # Filter out stations without coordinates
    valid_stations = [s for s in stations if s.lat is not None and s.lon is not None]

    if not valid_stations:
        return []

    # Use KDTree for large datasets
    if KDTREE_AVAILABLE and len(valid_stations) > 100:
        return _find_closest_kdtree(lat, lon, valid_stations, n, max_distance_km)

    return _find_closest_brute(lat, lon, valid_stations, n, max_distance_km)


def _find_closest_kdtree(
    lat: float,
    lon: float,
    stations: list[Station],
    n: int | None,
    max_distance_km: float | None,
) -> list[StationDistance]:
    """Find closest stations using KDTree (scipy)."""
    coords = np.array([(s.lat, s.lon) for s in stations])
    tree = cKDTree(coords)

    k = len(stations) if n is None else min(n, len(stations))
    distances, indices = tree.query([lat, lon], k=k)

    # Handle single result case
    if isinstance(distances, float):
        distances = [distances]
        indices = [indices]

    results = []
    for dist_deg, idx in zip(distances, indices):
        station = stations[idx]
        # Convert degree distance to meters (approximate)
        dist_m = int(dist_deg * 111000)

        if max_distance_km is not None and dist_m > max_distance_km * 1000:
            break

        results.append(StationDistance(station=station, distance_meters=dist_m))

    return results


def _find_closest_brute(
    lat: float,
    lon: float,
    stations: list[Station],
    n: int | None,
    max_distance_km: float | None,
) -> list[StationDistance]:
    """Find closest stations using brute force O(n) search."""
    results = []

    for station in stations:
        try:
            dist = meters_distance(lat, lon, station.lat, station.lon)
        except Exception:
            continue

        if max_distance_km is not None and dist > max_distance_km * 1000:
            continue

        results.append(StationDistance(station=station, distance_meters=int(dist)))

    # Sort by distance
    results.sort(key=lambda x: x.distance_meters)

    if n is not None:
        results = results[:n]

    return results


def f2c(fahrenheit: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return (fahrenheit - 32) * 5.0 / 9.0


def c2f(celsius: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return celsius * 9.0 / 5.0 + 32


def knots_to_ms(knots: float) -> float:
    """Convert knots to meters per second."""
    return 0.51444 * knots


def ms_to_knots(ms: float) -> float:
    """Convert meters per second to knots."""
    return ms / 0.51444
