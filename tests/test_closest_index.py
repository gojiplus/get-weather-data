"""Tests for building the closest-stations index."""

import pytest

from get_weather_data.core.database import Database
from get_weather_data.core.distance import Station
from get_weather_data.stations.closest import build_closest_index


@pytest.fixture
def seeded_db(tmp_path) -> Database:
    db = Database(tmp_path / "idx.sqlite")
    db.init_schema()
    # Two ZIPs; one has coords, one is missing lat (skipped)
    db.insert_zipcode("10001", "NYC", "NY", 40.75, -73.99)
    db.insert_zipcode("00000", "NoCoords", "NA", None, None)  # type: ignore[arg-type]
    # GHCND stations: near and far from NYC
    db.insert_station(
        Station(id="NEAR_G", name="near", lat=40.76, lon=-73.98, type="GHCND")
    )
    db.insert_station(
        Station(id="FAR_G", name="far", lat=42.0, lon=-75.0, type="GHCND")
    )
    # A USAF-WBAN station
    db.insert_station(
        Station(id="A-1", name="airport", lat=40.78, lon=-73.87, type="USAF-WBAN")
    )
    return db


class TestBuildClosestIndex:
    def test_selects_nearest_by_type(self, seeded_db):
        count = build_closest_index(seeded_db, ghcn_count=1, usaf_count=1)
        assert count == 1  # only the ZIP with coords

        closest = seeded_db.get_closest_stations("10001")
        ids = [sid for sid, _dist in closest]
        # nearest GHCND is NEAR_G (not FAR_G); the USAF station is included
        assert "NEAR_G" in ids
        assert "FAR_G" not in ids
        assert "A-1" in ids
        # distances are real meters, ascending
        dists = [d for _sid, d in closest]
        assert dists == sorted(dists)
        assert all(d >= 0 for d in dists)

    def test_zero_counts(self, seeded_db):
        count = build_closest_index(seeded_db, ghcn_count=0, usaf_count=0)
        assert count == 1
        assert seeded_db.get_closest_stations("10001") == []
