"""Tests for nearest-station ranking and distance math."""

import random

from get_weather_data.core.distance import (
    Station,
    StationIndex,
    _find_closest_brute,
    meters_distance,
)


def _station(sid: str, lat: float, lon: float) -> Station:
    return Station(id=sid, name=sid, lat=lat, lon=lon, type="GHCND")


class TestKDTreeRanking:
    """The KDTree path must rank by true distance, not raw degrees."""

    def test_east_west_station_not_penalized(self):
        """At 45N, 1.0 deg east (~79 km) is closer than 0.8 deg north (~89 km).

        A tree built on raw degrees would pick the northern station
        (0.8 < 1.0 in degree space); the cos-lat projection must not.
        """
        query_lat, query_lon = 45.0, -100.0
        east = _station("EAST", 45.0, -99.0)
        north = _station("NORTH", 45.8, -100.0)
        assert meters_distance(query_lat, query_lon, east.lat, east.lon) < (
            meters_distance(query_lat, query_lon, north.lat, north.lon)
        )

        index = StationIndex([east, north])
        results = index.find_closest(query_lat, query_lon, n=1)
        assert results[0].station.id == "EAST"

    def test_distances_match_meters_distance(self):
        query_lat, query_lon = 40.75, -73.99
        stations = [
            _station("A", 40.78, -73.97),
            _station("B", 40.78, -73.88),
            _station("C", 41.2, -74.5),
        ]
        index = StationIndex(stations)
        for sd in index.find_closest(query_lat, query_lon, n=3):
            expected = meters_distance(
                query_lat, query_lon, sd.station.lat, sd.station.lon
            )
            assert abs(sd.distance_meters - expected) <= 1

    def test_kdtree_agrees_with_brute_force(self):
        rng = random.Random(42)  # noqa: S311 - deterministic test fixture
        stations = [
            _station(
                f"S{i}",
                rng.uniform(25.0, 49.0),
                rng.uniform(-124.0, -67.0),
            )
            for i in range(500)
        ]
        index = StationIndex(stations)
        for _ in range(25):
            lat = rng.uniform(25.0, 49.0)
            lon = rng.uniform(-124.0, -67.0)
            tree_ids = [sd.station.id for sd in index.find_closest(lat, lon, n=5)]
            brute_ids = [
                sd.station.id for sd in _find_closest_brute(lat, lon, stations, 5, None)
            ]
            assert tree_ids == brute_ids

    def test_max_distance_filter(self):
        near = _station("NEAR", 40.76, -73.99)
        far = _station("FAR", 42.0, -74.0)
        index = StationIndex([near, far])
        results = index.find_closest(40.75, -73.99, n=5, max_distance_km=10)
        assert [sd.station.id for sd in results] == ["NEAR"]


class TestMetersDistance:
    """Sanity checks for the equirectangular approximation."""

    def test_known_distance(self):
        # NYC City Hall to Newark Penn Station is roughly 14 km
        d = meters_distance(40.7128, -74.0060, 40.7357, -74.1724)
        assert 13000 < d < 15500

    def test_symmetry(self):
        d1 = meters_distance(40.7, -74.0, 34.05, -118.24)
        d2 = meters_distance(34.05, -118.24, 40.7, -74.0)
        assert abs(d1 - d2) < 1
