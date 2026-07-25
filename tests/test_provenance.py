"""Tests for explain=True missing-data provenance."""

from datetime import date

import pytest

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.lookup import WeatherLookup
from get_weather_data.weather.online import OnlineLookup

DAY = date(2022, 1, 1)


@pytest.fixture
def city_db(tmp_path) -> Database:
    db = Database(tmp_path / "city.sqlite")
    db.init_schema()
    db.insert_zipcode("10001", "New York", "NY", 40.7484, -73.9967)
    db.insert_station(
        Station(id="S1", name="PRECIP ONLY", lat=40.78, lon=-73.97, type="GHCND")
    )
    db.set_closest_stations_bulk({"10001": [("S1", 4000)]})
    db.set_meta("index_version", str(INDEX_VERSION))
    return db


class TestStationProvenance:
    def test_off_by_default(self, city_db, monkeypatch):
        monkeypatch.setattr(lookup_module, "get_ghcn_data", lambda s, d: {"PRCP": 0.0})
        r = WeatherLookup(db=city_db, use_cache=False).get_weather("10001", DAY)
        assert r.stations_considered is None
        assert r.missing is None

    def test_missing_field_explained(self, city_db, monkeypatch):
        # Station reports only precipitation; temperature is unexplained None.
        monkeypatch.setattr(lookup_module, "get_ghcn_data", lambda s, d: {"PRCP": 0.0})
        r = WeatherLookup(db=city_db, use_cache=False, explain=True).get_weather(
            "10001", DAY
        )
        assert r.tmax is None
        assert r.stations_considered == 1
        assert "tmax" in r.missing
        assert "2022-01-01" in r.missing["tmax"]
        # A field that WAS found must not appear in missing.
        assert "prcp" not in r.missing

    def test_all_found_empty_missing(self, city_db, monkeypatch):
        full = dict.fromkeys(("TMAX", "TMIN", "TAVG", "PRCP", "SNOW", "SNWD"), 0.0)
        monkeypatch.setattr(lookup_module, "get_ghcn_data", lambda s, d: full)
        r = WeatherLookup(db=city_db, use_cache=False, explain=True).get_weather(
            "10001", DAY, elements=["TMAX", "PRCP"]
        )
        assert r.missing == {}
        assert r.stations_considered == 1

    def test_max_distance_reason(self, city_db, monkeypatch):
        monkeypatch.setattr(lookup_module, "get_ghcn_data", lambda s, d: {"PRCP": 0.0})
        # The only station (4 km) is past a 1 km cap -> zero considered.
        r = WeatherLookup(
            db=city_db,
            use_cache=False,
            explain=True,
            max_distance_meters=1000,
        ).get_weather("10001", DAY)
        assert r.stations_considered == 0
        assert "no weather stations" in r.missing["tmax"]

    def test_unknown_zip_reason(self, city_db):
        r = WeatherLookup(db=city_db, use_cache=False, explain=True).get_weather(
            "00000", DAY
        )
        assert r.stations_considered == 0
        assert "not in the station database" in r.missing["tmax"]


class TestOnlineProvenance:
    def _online(self, **kwargs) -> OnlineLookup:
        from get_weather_data.api.noaa import NOAAClient

        kwargs.setdefault("client", NOAAClient(token="test"))
        return OnlineLookup(**kwargs)

    def _stations(self):
        from get_weather_data.api.noaa import StationInfo

        return [(StationInfo(id="S1", name="A", latitude=40.0, longitude=-73.0), 100)]

    def test_missing_field_explained(self):
        records = [
            {
                "date": "2022-01-01T00:00:00",
                "datatype": "PRCP",
                "station": "S1",
                "value": 0.0,
            }
        ]
        online = self._online(explain=True)
        r = online._build_result(
            DAY, records, self._stations(), ["TMAX", "PRCP"], None, 40.0, -73.0
        )
        assert r.stations_considered == 1
        assert "tmax" in r.missing
        assert "prcp" not in r.missing

    def test_off_by_default(self):
        online = self._online()
        r = online._build_result(DAY, [], self._stations(), ["TMAX"], None, 40.0, -73.0)
        assert r.stations_considered is None
        assert r.missing is None


class TestOutputColumns:
    def test_frame_provenance_columns(self):
        pytest.importorskip("pandas")
        from get_weather_data.weather.frame import results_to_frame
        from get_weather_data.weather.results import WeatherResult

        plain = results_to_frame([WeatherResult(date=DAY)])
        assert "stations_considered" not in plain.columns

        explained = results_to_frame(
            [
                WeatherResult(
                    date=DAY,
                    stations_considered=3,
                    missing={"tmax": "none of the 3 nearest stations ..."},
                )
            ]
        )
        assert explained["stations_considered"].iloc[0] == 3
        assert "tmax:" in explained["missing"].iloc[0]

    def test_batch_columns(self):
        from get_weather_data.weather.batch import _format_missing, _weather_columns

        cols = _weather_columns(False, explain=True)
        assert "stations_considered" in cols
        assert cols.index("missing") == cols.index("weather_error") - 1
        assert "stations_considered" not in _weather_columns(False, explain=False)

        assert _format_missing(None) == ""
        assert _format_missing({"tmax": "no station"}) == "tmax: no station"
