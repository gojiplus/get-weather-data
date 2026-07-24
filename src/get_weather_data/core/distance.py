"""Geographic distance calculations for get-weather-data."""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

# Constants for distance calculation
NAUTICAL_MILE_PER_LAT = 60.00721
NAUTICAL_MILE_PER_LON = 60.10793
RAD = math.pi / 180.0
METERS_PER_NAUTICAL_MILE = 1852

# Try to import scipy for KDTree optimization
try:
    import numpy as np
    from scipy.spatial import KDTree

    KDTREE_AVAILABLE = True
except ImportError:
    KDTREE_AVAILABLE = False
    np = None  # type: ignore[assignment]
    KDTree = None  # type: ignore[assignment, misc]


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


# Extra neighbors fetched from the tree before re-ranking. Chord ranking
# is exact for great-circle distance; the margin absorbs the tiny
# difference between great-circle and the equirectangular meters_distance
# used for final ordering.
KDTREE_OVERSAMPLE = 5


def _project(lat: float, lon: float) -> tuple[float, float, float]:
    """Project degrees onto the 3D unit sphere.

    Euclidean (chord) distance between projected points is monotone in
    great-circle distance, so nearest-neighbor ranking in this space is
    exact everywhere — no flat-map distortion.
    """
    lat_r = lat * RAD
    lon_r = lon * RAD
    cos_lat = math.cos(lat_r)
    return (cos_lat * math.cos(lon_r), cos_lat * math.sin(lon_r), math.sin(lat_r))


def _rank_candidates(
    lat: float,
    lon: float,
    candidates: list[Station],
    n: int | None,
    max_distance_km: float | None,
) -> list[StationDistance]:
    """Order candidate stations by true distance and apply limits."""
    ranked = sorted(
        (
            StationDistance(
                station=s,
                distance_meters=int(meters_distance(lat, lon, s.lat, s.lon)),
            )
            for s in candidates
        ),
        key=lambda sd: sd.distance_meters,
    )
    if max_distance_km is not None:
        ranked = [sd for sd in ranked if sd.distance_meters <= max_distance_km * 1000]
    if n is not None:
        ranked = ranked[:n]
    return ranked


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

logger = logging.getLogger("get_weather_data")


class StationIndex:
    """Pre-built spatial index for fast nearest-neighbor queries.

    Build once, query many times. Uses KDTree when scipy is available.
    """

    def __init__(self, stations: Sequence[Station]) -> None:
        """Build the spatial index from a list of stations."""
        self.stations = [s for s in stations if s.lat is not None and s.lon is not None]
        self._tree: Any = None
        if self.stations and np is not None and KDTree is not None:
            coords = np.array([_project(s.lat, s.lon) for s in self.stations])
            self._tree = KDTree(coords)

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

        if self._tree is not None and np is not None:
            # Oversample to absorb residual projection error, then re-rank
            # candidates by true distance
            k = min(n + KDTREE_OVERSAMPLE, len(self.stations))
            _, raw_indices = self._tree.query(_project(lat, lon), k=k)
            indices = [int(i) for i in np.atleast_1d(raw_indices)]
            return _rank_candidates(
                lat, lon, [self.stations[i] for i in indices], n, max_distance_km
            )

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
    if np is None or KDTree is None:
        raise RuntimeError("scipy is required for KDTree search")
    coords = np.array([_project(s.lat, s.lon) for s in stations])
    tree = KDTree(coords)

    k = len(stations) if n is None else min(n + KDTREE_OVERSAMPLE, len(stations))
    _, raw_indices = tree.query(_project(lat, lon), k=k)
    indices = [int(i) for i in np.atleast_1d(raw_indices)]
    return _rank_candidates(
        lat, lon, [stations[i] for i in indices], n, max_distance_km
    )


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
            logger.debug("Skipping station %s: bad coordinates", station.id)
            continue

        if max_distance_km is not None and dist > max_distance_km * 1000:
            continue

        results.append(StationDistance(station=station, distance_meters=int(dist)))

    # Sort by distance
    results.sort(key=lambda x: x.distance_meters)

    if n is not None:
        results = results[:n]

    return results
