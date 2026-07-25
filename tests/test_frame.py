"""Tests for pandas DataFrame output."""

from datetime import date

import pytest

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.frame import result_to_frame, results_to_frame
from get_weather_data.weather.lookup import WeatherLookup
from get_weather_data.weather.results import WeatherResult

DAY = date(2024, 1, 15)


@pytest.fixture
def city_db(tmp_path) -> Database:
    db = Database(tmp_path / "city.sqlite")
    db.init_schema()
    db.insert_zipcode("10001", "New York", "NY", 40.7484, -73.9967)
    db.insert_station(
        Station(id="GHCN1", name="GHCN STATION", lat=40.78, lon=-73.97, type="GHCND")
    )
    db.set_closest_stations_bulk({"10001": [("GHCN1", 4000)]})
    db.set_meta("index_version", str(INDEX_VERSION))
    return db


class TestResultsToFrame:
    def test_columns_and_types(self, city_db, monkeypatch):
        monkeypatch.setattr(
            lookup_module,
            "get_ghcn_data",
            lambda station_id, target_date: {"TMAX": -16.0, "PRCP": 0.0},
        )
        lookup = WeatherLookup(db=city_db, use_cache=False, use_gsod=False)
        results = lookup.get_weather_range("10001", DAY, date(2024, 1, 17))

        frame = results_to_frame(results)

        assert len(frame) == 3
        for column in ("date", "station_id", "units", "tmax", "tmin", "prcp"):
            assert column in frame.columns
        # metadata comes before value columns
        assert list(frame.columns).index("tmax") > list(frame.columns).index("units")
        assert str(frame["date"].dtype).startswith("datetime64")
        assert frame["tmax"].iloc[0] == -1.6
        # zero survives, missing element is NaN
        assert frame["prcp"].iloc[0] == 0.0
        assert frame["tavg"].isna().all()

    def test_single_result(self):
        result = WeatherResult(date=DAY, zipcode="10001", tmax=1.0)
        frame = result_to_frame(result)
        assert len(frame) == 1
        assert frame["tmax"].iloc[0] == 1.0

    def test_empty(self):
        frame = results_to_frame([])
        assert len(frame) == 0
        assert "tmax" in frame.columns
