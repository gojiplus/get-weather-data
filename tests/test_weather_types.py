"""Tests for present-weather phenomena (WT** / FRSHTT) end to end."""

import csv
import gzip
import io
from datetime import date

import pytest
import respx
from httpx import Response

from get_weather_data.core.config import Config, set_config
from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import ghcn
from get_weather_data.weather import gsod as gsod_module
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.ghcn import get_ghcn_weather_types
from get_weather_data.weather.gsod import get_gsod_weather_types
from get_weather_data.weather.lookup import WeatherLookup
from get_weather_data.weather.online import OnlineLookup
from get_weather_data.weather.weather_types import (
    format_weather_types,
    ghcn_weather_type,
    parse_frshtt,
)

DAY = date(2010, 1, 15)


class TestParsingHelpers:
    def test_ghcn_weather_type_known_and_unknown(self):
        assert ghcn_weather_type("WT01") == "fog"
        assert ghcn_weather_type("WT03") == "thunder"
        assert ghcn_weather_type("TMAX") is None

    def test_parse_frshtt(self):
        # FRSHTT order: fog, rain, snow, hail, thunder, tornado
        assert parse_frshtt("010001") == {"rain", "tornado"}
        assert parse_frshtt("100010") == {"fog", "thunder"}
        assert parse_frshtt("000000") == set()

    def test_parse_frshtt_handles_short_or_blank(self):
        assert parse_frshtt("") == set()
        assert parse_frshtt("1") == {"fog"}

    def test_format_weather_types_sorted_and_joined(self):
        assert format_weather_types({"thunder", "fog"}) == "fog,thunder"

    def test_format_weather_types_empty(self):
        assert format_weather_types(set()) == ""
        assert format_weather_types(None) == ""


@pytest.fixture(autouse=True)
def _isolated_ghcn(tmp_path, monkeypatch):
    """Point GHCN caches at a temp dir and reset module-level pools."""
    monkeypatch.delenv("NCDC_TOKEN", raising=False)
    set_config(Config(ncdc_token=None, data_dir=tmp_path, cache_dir=tmp_path))
    ghcn._year_locks.clear()
    if hasattr(ghcn._connections, "pool"):
        del ghcn._connections.pool
    yield
    if hasattr(ghcn._connections, "pool"):
        del ghcn._connections.pool


def _year_gz_bytes(rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    csv.writer(buf).writerows(rows)
    return gzip.compress(buf.getvalue().encode())


class TestGhcnWeatherTypes:
    @respx.mock
    def test_reads_wt_occurrence_codes(self):
        rows = [
            ("USW00094728", "20100115", "TMAX", "-10", "", "", "W", ""),
            ("USW00094728", "20100115", "WT01", "1", "", "", "W", ""),
            ("USW00094728", "20100115", "WT03", "1", "", "", "W", ""),
            # A non-WT element must be ignored
            ("USW00094728", "20100115", "PRCP", "0", "", "", "W", ""),
        ]
        respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2010)).mock(
            return_value=Response(200, content=_year_gz_bytes(rows))
        )
        assert get_ghcn_weather_types("USW00094728", DAY) == {"fog", "thunder"}

    @respx.mock
    def test_absent_day_is_empty(self):
        rows = [("USW00094728", "20100115", "WT01", "1", "", "", "W", "")]
        respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2010)).mock(
            return_value=Response(200, content=_year_gz_bytes(rows))
        )
        assert get_ghcn_weather_types("USW00094728", date(2010, 6, 1)) == set()


GSOD_HEADER = "STATION,DATE,TEMP,FRSHTT\n"


class TestGsodWeatherTypes:
    @pytest.fixture
    def gsod_file(self, tmp_path, monkeypatch):
        path = tmp_path / "725030.csv"
        # 2010-01-15: rain + snow + thunder (FRSHTT = 011010)
        path.write_text(
            GSOD_HEADER
            + "725030,2010-01-15,40.0,011010\n"
            + "725030,2010-01-16,50.0,000000\n"
        )
        monkeypatch.setattr(gsod_module, "_ensure_gsod_file", lambda sid, yr: path)
        return path

    def test_parses_frshtt(self, gsod_file):
        assert get_gsod_weather_types("725030", DAY) == {"rain", "snow", "thunder"}

    def test_clear_day_empty(self, gsod_file):
        assert get_gsod_weather_types("725030", date(2010, 1, 16)) == set()

    def test_missing_file_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gsod_module, "_ensure_gsod_file", lambda sid, yr: None)
        assert get_gsod_weather_types("725030", DAY) == set()


@pytest.fixture
def city_db(tmp_path) -> Database:
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


class TestLookupPlumbing:
    def test_off_by_default(self, city_db, monkeypatch):
        monkeypatch.setattr(
            lookup_module, "get_ghcn_data", lambda sid, d: {"TMAX": -100.0}
        )
        result = WeatherLookup(db=city_db, use_cache=False).get_weather("10001", DAY)
        assert result.weather_types is None

    def test_collects_from_ghcn(self, city_db, monkeypatch):
        monkeypatch.setattr(
            lookup_module, "get_ghcn_data", lambda sid, d: {"TMAX": -100.0}
        )
        monkeypatch.setattr(
            lookup_module,
            "get_ghcn_weather_types",
            lambda sid, d: {"fog", "thunder"},
        )
        result = WeatherLookup(
            db=city_db, use_cache=False, include_weather_types=True
        ).get_weather("10001", DAY)
        assert result.weather_types == {"fog", "thunder"}

    def test_unions_across_stations(self, city_db, monkeypatch):
        # GHCN supplies no values so the loop keeps walking to the GSOD
        # station; phenomena from both must union.
        monkeypatch.setattr(lookup_module, "get_ghcn_data", lambda sid, d: {})
        monkeypatch.setattr(lookup_module, "get_gsod_data", lambda sid, d: {})
        monkeypatch.setattr(
            lookup_module, "get_ghcn_weather_types", lambda sid, d: {"fog"}
        )
        monkeypatch.setattr(
            lookup_module, "get_gsod_weather_types", lambda sid, d: {"snow"}
        )
        result = WeatherLookup(
            db=city_db, use_cache=False, include_weather_types=True
        ).get_weather("10001", DAY)
        assert result.weather_types == {"fog", "snow"}


class TestOnlinePlumbing:
    def _records(self):
        return [
            {
                "date": "2010-01-15T00:00:00",
                "datatype": "TMAX",
                "station": "S1",
                "value": -100.0,
            },
            {
                "date": "2010-01-15T00:00:00",
                "datatype": "WT01",
                "station": "S1",
                "value": 1.0,
            },
            {
                "date": "2010-01-15T00:00:00",
                "datatype": "WT03",
                "station": "S1",
                "value": 1.0,
            },
        ]

    def _online(self, **kwargs) -> OnlineLookup:
        from get_weather_data.api.noaa import NOAAClient

        kwargs.setdefault("client", NOAAClient(token="test"))
        return OnlineLookup(**kwargs)

    def _stations(self):
        from get_weather_data.api.noaa import StationInfo

        return [(StationInfo(id="S1", name="A", latitude=40.0, longitude=-73.0), 100)]

    def test_collects_wt_datatypes(self):
        online = self._online(include_weather_types=True)
        result = online._build_result(
            date(2010, 1, 15),
            self._records(),
            self._stations(),
            ["TMAX"],
            None,
            40.0,
            -73.0,
        )
        assert result.weather_types == {"fog", "thunder"}

    def test_off_by_default(self):
        online = self._online()
        result = online._build_result(
            date(2010, 1, 15),
            self._records(),
            self._stations(),
            ["TMAX"],
            None,
            40.0,
            -73.0,
        )
        assert result.weather_types is None


class TestOutputColumns:
    def test_frame_column_only_when_present(self):
        pd = pytest.importorskip("pandas")
        from get_weather_data.weather.frame import results_to_frame
        from get_weather_data.weather.results import WeatherResult

        without = results_to_frame([WeatherResult(date=DAY)])
        assert "weather_types" not in without.columns

        with_types = results_to_frame(
            [WeatherResult(date=DAY, weather_types={"fog", "thunder"})]
        )
        assert with_types["weather_types"].iloc[0] == "fog,thunder"
        assert pd.notna(with_types["weather_types"].iloc[0])

    def test_batch_columns(self):
        from get_weather_data.weather.batch import _weather_columns

        assert "weather_types" not in _weather_columns(False)
        cols = _weather_columns(True)
        assert cols.index("weather_types") == cols.index("weather_error") - 1
