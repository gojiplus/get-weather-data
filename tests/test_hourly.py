"""Tests for hourly ISD-Lite parsing, lookup, and output."""

import gzip
from datetime import UTC, date, datetime

import pytest
import respx
from httpx import Response

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import hourly as hourly_module
from get_weather_data.weather import isd as isd_module
from get_weather_data.weather.hourly import HourlyLookup
from get_weather_data.weather.isd import _parse_line, get_isd_hourly
from get_weather_data.weather.results import HourlyResult

DAY = date(2023, 1, 1)

# Real ISD-Lite rows (LaGuardia 2023): year mo dy hr temp dewp slp dir spd sky p1 p6
SAMPLE = (
    "2023 01 01 00    78    72 10098    70    15     9     8 -9999\n"
    "2023 01 01 01    83    72 10086     0     0     9    13 -9999\n"
    "2023 01 01 02   122   111 -9999   240    31 -9999 -9999 -9999\n"
    "2023 01 02 00    61   -44 10153   330    46 -9999     0 -9999\n"
)


class TestParseLine:
    def test_scaling_and_fields(self):
        r = _parse_line(
            "2023 01 01 00    78    72 10098    70    15     9     8 5", DAY
        )
        assert r is not None
        assert r["observed_at"] == datetime(2023, 1, 1, 0, tzinfo=UTC)
        assert r["temp"] == pytest.approx(7.8)
        assert r["dewpoint"] == pytest.approx(7.2)
        assert r["sea_level_pressure"] == pytest.approx(1009.8)
        assert r["wind_direction"] == 70  # unscaled int
        assert r["wind_speed"] == pytest.approx(1.5)
        assert r["sky_condition"] == 9  # unscaled int
        assert r["precip_1h"] == pytest.approx(0.8)
        assert r["precip_6h"] == pytest.approx(0.5)

    def test_missing_sentinel(self):
        r = _parse_line(
            "2023 01 01 02   122   111 -9999   240    31 -9999 -9999 -9999", DAY
        )
        assert r is not None
        assert r["sea_level_pressure"] is None
        assert r["sky_condition"] is None
        assert r["precip_1h"] is None
        assert r["temp"] == pytest.approx(12.2)

    def test_other_date_skipped(self):
        assert _parse_line("2023 01 02 00    61   -44 10153 330 46 0 0 0", DAY) is None

    def test_malformed_skipped(self):
        assert _parse_line("garbage row", DAY) is None
        assert _parse_line("2023 01 01", DAY) is None


class TestGetIsdHourly:
    @pytest.fixture
    def isd_file(self, tmp_path, monkeypatch):
        path = tmp_path / "725030-14732.txt.gz"
        path.write_bytes(gzip.compress(SAMPLE.encode()))
        monkeypatch.setattr(isd_module, "_ensure_isd_file", lambda sid, yr: path)
        return path

    def test_returns_day_sorted(self, isd_file):
        rows = get_isd_hourly("725030-14732", DAY)
        assert len(rows) == 3  # three rows on 2023-01-01, one on 01-02 excluded
        hours = [r["observed_at"].hour for r in rows]
        assert hours == [0, 1, 2]

    def test_missing_file_empty(self, tmp_path, monkeypatch):
        monkeypatch.setattr(isd_module, "_ensure_isd_file", lambda sid, yr: None)
        assert get_isd_hourly("725030-14732", DAY) == []


class TestEnsureIsdFile:
    @pytest.fixture(autouse=True)
    def _isolated(self, tmp_path, monkeypatch):
        from get_weather_data.core.config import Config, set_config

        set_config(Config(ncdc_token=None, data_dir=tmp_path, cache_dir=tmp_path))

    @respx.mock
    def test_downloads_then_caches(self):
        url = isd_module.ISD_LITE_URL.format(year=2023, station_id="725030-14732")
        route = respx.get(url).mock(
            return_value=Response(200, content=gzip.compress(SAMPLE.encode()))
        )
        first = isd_module._ensure_isd_file("725030-14732", 2023)
        assert first is not None
        # A historical year is immutable: the cache hit skips a second GET.
        second = isd_module._ensure_isd_file("725030-14732", 2023)
        assert second == first
        assert route.call_count == 1

    @respx.mock
    def test_missing_file_returns_none(self):
        url = isd_module.ISD_LITE_URL.format(year=2023, station_id="000000-00000")
        respx.get(url).mock(return_value=Response(404))
        assert isd_module._ensure_isd_file("000000-00000", 2023) is None


@pytest.fixture
def city_db(tmp_path) -> Database:
    db = Database(tmp_path / "city.sqlite")
    db.init_schema()
    db.insert_zipcode("11371", "New York", "NY", 40.7747, -73.8724)
    db.insert_station(
        Station(
            id="725030-14732",
            name="LA GUARDIA",
            lat=40.779,
            lon=-73.88,
            type="USAF-WBAN",
        )
    )
    db.set_meta("index_version", str(INDEX_VERSION))
    return db


def _canned(sid, day):
    return [
        {
            "observed_at": datetime(day.year, day.month, day.day, 0, tzinfo=UTC),
            "temp": 10.0,
            "dewpoint": 5.0,
            "sea_level_pressure": 1013.0,
            "wind_direction": 180,
            "wind_speed": 5.0,
            "sky_condition": 4,
            "precip_1h": 2.54,
            "precip_6h": None,
        }
    ]


class TestHourlyLookup:
    def test_metric_passthrough(self, city_db, monkeypatch):
        monkeypatch.setattr(hourly_module, "get_isd_hourly", _canned)
        res = HourlyLookup(db=city_db).get_hourly("11371", DAY)
        assert len(res) == 1
        r = res[0]
        assert r.temp == pytest.approx(10.0)
        assert r.station_id == "725030-14732"
        assert r.station_distance_meters is not None
        assert r.precip_1h == pytest.approx(2.54)

    def test_imperial_conversion(self, city_db, monkeypatch):
        monkeypatch.setattr(hourly_module, "get_isd_hourly", _canned)
        res = HourlyLookup(db=city_db, units="imperial").get_hourly("11371", DAY)
        r = res[0]
        assert r.temp == pytest.approx(50.0)  # 10 C -> 50 F
        assert r.wind_speed == pytest.approx(5.0 * 2.2369362920544)
        assert r.precip_1h == pytest.approx(2.54 / 25.4)  # 2.54 mm -> 0.1 in
        assert r.wind_direction == 180  # unchanged

    def test_station_walk_skips_empty(self, city_db, monkeypatch):
        # First (only) station returns nothing -> overall empty, no crash.
        monkeypatch.setattr(hourly_module, "get_isd_hourly", lambda sid, day: [])
        assert HourlyLookup(db=city_db).get_hourly("11371", DAY) == []

    def test_unknown_zip_empty(self, city_db, monkeypatch):
        monkeypatch.setattr(hourly_module, "get_isd_hourly", _canned)
        assert HourlyLookup(db=city_db).get_hourly("00000", DAY) == []

    def test_date_range(self, city_db, monkeypatch):
        monkeypatch.setattr(hourly_module, "get_isd_hourly", _canned)
        res = HourlyLookup(db=city_db).get_hourly("11371", DAY, date(2023, 1, 3))
        assert len(res) == 3  # one canned hour per day, 3 days


class TestFacadeGuard:
    def test_online_mode_rejects_hourly(self, tmp_path, monkeypatch):
        from get_weather_data.core.config import Config, set_config
        from get_weather_data.main import Weather

        set_config(
            Config(ncdc_token="test-token", data_dir=tmp_path, cache_dir=tmp_path)
        )
        w = Weather(online=True)
        with pytest.raises(ValueError, match="requires the local database"):
            w.get_hourly("11371", "2023-01-01")


class TestHourlyFrame:
    def test_frame_shape(self):
        pytest.importorskip("pandas")
        from get_weather_data.weather.frame import hourly_results_to_frame

        r = HourlyResult(
            observed_at=datetime(2023, 1, 1, 0, tzinfo=UTC),
            temp=10.0,
            units="metric",
        )
        df = hourly_results_to_frame([r])
        assert next(iter(df.columns)) == "observed_at"
        assert "temp" in df.columns
        assert "wind_direction" in df.columns
        assert df["temp"].iloc[0] == 10.0

    def test_empty_frame_has_columns(self):
        pytest.importorskip("pandas")
        from get_weather_data.weather.frame import hourly_results_to_frame

        df = hourly_results_to_frame([])
        assert "temp" in df.columns
        assert len(df) == 0
