"""Tests for the local lookup path: units parity, zeros, elements."""

from datetime import date

import pytest

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.lookup import WeatherLookup

DAY = date(2024, 1, 15)


@pytest.fixture
def city_db(tmp_path) -> Database:
    """A DB with one ZIP, one GHCN station, and one GSOD station."""
    db = Database(tmp_path / "city.sqlite")
    db.init_schema()
    db.insert_zipcode("10001", "New York", "NY", 40.7484, -73.9967)
    db.insert_station(
        Station(id="GHCN1", name="GHCN STATION", lat=40.78, lon=-73.97, type="GHCND")
    )
    db.insert_station(
        Station(
            id="725030-14732",
            name="GSOD STATION",
            lat=40.78,
            lon=-73.88,
            type="USAF-WBAN",
        )
    )
    db.set_closest_stations_bulk({"10001": [("GHCN1", 4000), ("725030-14732", 9000)]})
    db.set_meta("index_version", str(INDEX_VERSION))
    return db


def _mock_ghcn(monkeypatch, values: dict[str, float | None]):
    monkeypatch.setattr(
        lookup_module, "get_ghcn_data", lambda station_id, target_date: dict(values)
    )


def _mock_gsod(monkeypatch, values: dict[str, float | None]):
    monkeypatch.setattr(
        lookup_module, "get_gsod_data", lambda station_id, target_date: dict(values)
    )


def _lookup(db: Database, **kwargs) -> WeatherLookup:
    kwargs.setdefault("use_cache", False)
    return WeatherLookup(db=db, **kwargs)


class TestSourceParity:
    """The same physical observation must yield the same output value
    whether it came from a GHCN or a GSOD station."""

    def test_precipitation_parity(self, city_db, monkeypatch):
        # 0.5 inches of rain: GHCN reports 127 (tenths of mm), GSOD 0.5 in
        _mock_ghcn(monkeypatch, {"PRCP": 127.0})
        _mock_gsod(monkeypatch, {"precipitation": 0.5})

        from_ghcn = _lookup(city_db, use_gsod=False).get_weather("10001", DAY)
        from_gsod = _lookup(city_db, use_ghcn=False).get_weather("10001", DAY)

        assert from_ghcn.prcp == pytest.approx(12.7)
        assert from_gsod.prcp == pytest.approx(12.7)

    def test_temperature_parity(self, city_db, monkeypatch):
        # -1.6 degC: GHCN raw -16 tenths; GSOD already converted to degC
        _mock_ghcn(monkeypatch, {"TMAX": -16.0})
        _mock_gsod(monkeypatch, {"max_temp": -1.6})

        from_ghcn = _lookup(city_db, use_gsod=False).get_weather("10001", DAY)
        from_gsod = _lookup(city_db, use_ghcn=False).get_weather("10001", DAY)

        assert from_ghcn.tmax == pytest.approx(-1.6)
        assert from_gsod.tmax == pytest.approx(-1.6)

    def test_snow_depth_parity(self, city_db, monkeypatch):
        # 150 mm snow depth: GHCN raw 150 (whole mm), GSOD ~5.9 in
        _mock_ghcn(monkeypatch, {"SNWD": 150.0})
        _mock_gsod(monkeypatch, {"snow_depth": 150.0 / 25.4})

        from_ghcn = _lookup(city_db, use_gsod=False).get_weather("10001", DAY)
        from_gsod = _lookup(city_db, use_ghcn=False).get_weather("10001", DAY)

        assert from_ghcn.snwd == pytest.approx(150.0)
        assert from_gsod.snwd == pytest.approx(150.0)

    def test_gsod_never_fills_snowfall(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {})
        _mock_gsod(monkeypatch, {"snow_depth": 2.0, "max_temp": 1.0})
        result = _lookup(city_db, use_ghcn=False).get_weather("10001", DAY)
        assert result.snow is None
        assert result.snwd is not None


class TestZeroHandling:
    """Genuine 0.0 readings are data, not missing values."""

    def test_zero_values_survive(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {"TMAX": 0.0, "PRCP": 0.0, "AWND": 0.0})
        result = _lookup(city_db, use_gsod=False).get_weather("10001", DAY)
        assert result.tmax == 0.0
        assert result.prcp == 0.0
        assert result.awnd == 0.0

    def test_gsod_zero_values_survive(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {})
        _mock_gsod(monkeypatch, {"max_temp": 0.0, "precipitation": 0.0})
        result = _lookup(city_db, use_ghcn=False).get_weather("10001", DAY)
        assert result.tmax == 0.0
        assert result.prcp == 0.0


class TestElements:
    """elements= restricts both retrieval targets and output."""

    def test_output_filtered(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {"TMAX": 100.0, "TMIN": 50.0, "PRCP": 30.0})
        result = _lookup(city_db).get_weather("10001", DAY, elements=["TMAX"])
        assert result.tmax == 10.0
        assert result.tmin is None
        assert result.prcp is None

    def test_unknown_element_raises(self, city_db):
        with pytest.raises(ValueError, match="Unknown weather element"):
            _lookup(city_db).get_weather("10001", DAY, elements=["NOPE"])

    def test_second_station_fills_missing(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {"TMAX": -16.0})
        _mock_gsod(monkeypatch, {"precipitation": 0.5})
        result = _lookup(city_db).get_weather("10001", DAY)
        assert result.tmax == pytest.approx(-1.6)
        assert result.prcp == pytest.approx(12.7)
        # station credit goes to the nearest contributor
        assert result.station_id == "GHCN1"


class TestUnitsOption:
    """Imperial output converts at the assembly boundary."""

    def test_imperial(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {"TMAX": 0.0, "PRCP": 254.0, "SNOW": 25.4})
        result = _lookup(city_db, units="imperial", use_gsod=False).get_weather(
            "10001", DAY
        )
        assert result.units == "imperial"
        assert result.tmax == 32.0
        assert result.prcp == pytest.approx(1.0)
        assert result.snow == pytest.approx(1.0)


class TestLocationInput:
    """Coordinates and ZIPs resolve through the same lookup."""

    def test_coords_direct(self, city_db, monkeypatch):
        _mock_ghcn(monkeypatch, {"TMAX": -16.0})
        result = _lookup(city_db, use_gsod=False).get_weather((40.7484, -73.9967), DAY)
        assert result.zipcode is None
        assert result.latitude == 40.7484
        assert result.tmax == pytest.approx(-1.6)
        assert result.station_id == "GHCN1"

    def test_unknown_zip(self, city_db):
        result = _lookup(city_db).get_weather("99999", DAY)
        assert result.zipcode == "99999"
        assert result.station_id is None
