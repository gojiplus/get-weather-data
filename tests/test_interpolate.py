"""Tests for inverse-distance weighting and the interpolation path."""

from datetime import date

import pytest

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.interpolate import (
    LAPSE_RATE_C_PER_M,
    Sample,
    idw,
)
from get_weather_data.weather.lookup import WeatherLookup

DAY = date(2024, 1, 15)


class TestIDW:
    def test_empty(self):
        assert idw([]) is None

    def test_single_sample(self):
        assert idw([Sample(1000.0, 5.0)]) == 5.0

    def test_midpoint_equal_distance(self):
        # Equal distances -> simple average
        result = idw([Sample(1000.0, 0.0), Sample(1000.0, 10.0)])
        assert result == pytest.approx(5.0)

    def test_closer_station_dominates(self):
        # Nearer station pulls the estimate toward its value
        result = idw([Sample(100.0, 0.0), Sample(1000.0, 10.0)], power=2.0)
        assert result < 1.0  # heavily weighted to the near (0.0) station

    def test_coincident_wins(self):
        result = idw([Sample(0.0, 7.0), Sample(500.0, 10.0)])
        assert result == 7.0

    def test_lapse_correction(self):
        # Station 1000 m below the query point; temperature should be
        # cooled by lapse * 1000 m before weighting.
        sample = Sample(distance_meters=500.0, value=20.0, elevation_meters=0.0)
        result = idw(
            [sample],
            target_elevation_meters=1000.0,
            is_temperature=True,
        )
        assert result == pytest.approx(20.0 - 1000.0 * LAPSE_RATE_C_PER_M)

    def test_no_lapse_without_flag(self):
        sample = Sample(distance_meters=500.0, value=20.0, elevation_meters=0.0)
        assert idw([sample], target_elevation_meters=1000.0) == 20.0


@pytest.fixture
def two_station_db(tmp_path) -> Database:
    """Two GHCN stations bracketing a ZIP, at different distances."""
    db = Database(tmp_path / "two.sqlite")
    db.init_schema()
    db.insert_zipcode("10001", "NYC", "NY", 40.75, -74.0)
    db.insert_station(
        Station(id="NEAR", name="near", lat=40.76, lon=-74.0, type="GHCND")
    )
    db.insert_station(Station(id="FAR", name="far", lat=41.0, lon=-74.0, type="GHCND"))
    db.set_closest_stations_bulk({"10001": [("NEAR", 1000), ("FAR", 28000)]})
    db.set_meta("index_version", str(INDEX_VERSION))
    return db


class TestInterpolatedLookup:
    def test_blends_two_stations(self, two_station_db, monkeypatch):
        def fake_ghcn(station_id, target_date):
            return {"TMAX": 0.0} if station_id == "NEAR" else {"TMAX": 100.0}

        monkeypatch.setattr(lookup_module, "get_ghcn_data", fake_ghcn)
        lookup = WeatherLookup(
            db=two_station_db, use_cache=False, use_gsod=False, interpolate=True
        )
        result = lookup.get_weather("10001", DAY)

        # Between the two, but far closer to NEAR (0.0)
        assert result.tmax is not None
        assert 0.0 < result.tmax < 50.0
        assert result.station_type == "interpolated"
        assert result.station_distance_meters == 1000

    def test_matches_nearest_when_only_one_has_data(self, two_station_db, monkeypatch):
        # raw GHCN tenths: 70 -> 7.0 °C
        def fake_ghcn(station_id, target_date):
            return {"TMAX": 70.0} if station_id == "NEAR" else {}

        monkeypatch.setattr(lookup_module, "get_ghcn_data", fake_ghcn)
        lookup = WeatherLookup(
            db=two_station_db, use_cache=False, use_gsod=False, interpolate=True
        )
        result = lookup.get_weather("10001", DAY)
        assert result.tmax == pytest.approx(7.0)
