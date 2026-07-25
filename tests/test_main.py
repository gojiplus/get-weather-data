"""Tests for the Weather facade dispatch (backends stubbed, no network)."""

from datetime import date

import pytest

from get_weather_data import Weather
from get_weather_data.weather.results import WeatherResult

DAY = date(2024, 1, 15)


def _result(**kw) -> WeatherResult:
    return WeatherResult(date=DAY, **kw)


class _Stub:
    """Records calls and returns canned results."""

    def __init__(self, single=None, ranged=None):
        self.single = single
        self.ranged = ranged
        self.calls: list[tuple] = []

    def get_weather(self, location, target_date, elements=None):
        self.calls.append(("get", location, target_date, elements))
        return self.single

    def get_weather_range(self, location, start, end, elements=None):
        self.calls.append(("range", location, start, end, elements))
        return self.ranged


def _weather(source="station", **stubs) -> Weather:
    """A Weather with its lookup/grid/online backends replaced by stubs."""
    w = Weather.__new__(Weather)  # skip __post_init__ (no DB/network)
    w.online = stubs.get("online", False)
    w.units = "metric"
    w.include_flags = False
    w.interpolate = False
    w.source = source
    w._db = None
    w._lookup = stubs.get("lookup")
    w._grid = stubs.get("grid")
    w._online_lookup = stubs.get("online_lookup")
    return w


class TestDispatch:
    def test_station_source(self):
        lookup = _Stub(single=_result(tmax=1.0))
        w = _weather("station", lookup=lookup)
        assert w.get("10001", DAY).tmax == 1.0
        assert lookup.calls[0][0] == "get"

    def test_grid_source(self):
        grid = _Stub(single=_result(tmax=2.0, station_type="gridded"))
        lookup = _Stub(single=_result(tmax=1.0))
        w = _weather("grid", grid=grid, lookup=lookup)
        assert w.get("10001", DAY).tmax == 2.0
        assert not lookup.calls  # station not consulted

    def test_auto_uses_station_when_it_has_data(self):
        lookup = _Stub(single=_result(tmax=1.0, station_id="S"))
        grid = _Stub(single=_result(tmax=2.0))
        w = _weather("auto", lookup=lookup, grid=grid)
        assert w.get("10001", DAY).tmax == 1.0
        assert not grid.calls  # no fallback needed

    def test_auto_falls_back_to_grid_when_station_empty(self):
        lookup = _Stub(single=_result())  # no data
        grid = _Stub(single=_result(tmax=2.0, station_type="gridded"))
        w = _weather("auto", lookup=lookup, grid=grid)
        assert w.get("10001", DAY).tmax == 2.0
        assert grid.calls  # fallback fired

    def test_auto_range_fills_empty_days_from_grid(self):
        s0, s1 = _result(tmax=1.0, station_id="S"), _result()  # day2 empty
        g0, g1 = _result(tmax=9.0), _result(tmax=8.0, station_type="gridded")
        lookup = _Stub(ranged=[s0, s1])
        grid = _Stub(ranged=[g0, g1])
        w = _weather("auto", lookup=lookup, grid=grid)
        out = w.get_range("10001", DAY, date(2024, 1, 16))
        assert out[0].tmax == 1.0  # station kept
        assert out[1].tmax == 8.0  # grid filled

    def test_online_routing(self):
        online = _Stub(single=_result(tmax=5.0))
        w = _weather(online=True, online_lookup=online)
        assert w.get("10001", DAY).tmax == 5.0
        assert online.calls

    def test_date_string_coercion(self):
        lookup = _Stub(single=_result(tmax=1.0))
        w = _weather("station", lookup=lookup)
        w.get("10001", "2024-01-15")
        assert lookup.calls[0][2] == DAY  # coerced to date


class TestCoverageAndFrame:
    def test_coverage(self):
        lookup = _Stub(
            ranged=[
                _result(tmax=1.0, station_id="A", station_distance_meters=100),
                _result(station_id="A", station_distance_meters=100),
            ]
        )
        w = _weather("station", lookup=lookup)
        cov = w.coverage("10001", DAY, date(2024, 1, 16))
        assert cov.total_days == 2
        assert cov.fraction("tmax") == 0.5
        assert cov.station_id == "A"

    def test_get_frame(self):
        pytest.importorskip("pandas")
        lookup = _Stub(ranged=[_result(tmax=1.0), _result(tmax=2.0)])
        w = _weather("station", lookup=lookup)
        df = w.get_frame("10001", DAY, date(2024, 1, 16))
        assert list(df["tmax"]) == [1.0, 2.0]


class TestGuards:
    def test_process_csv_online_raises(self):
        w = _weather(online=True, online_lookup=_Stub())
        with pytest.raises(ValueError, match="local database"):
            w.process_csv("in.csv", "out.csv")

    def test_info_online_raises(self):
        w = _weather(online=True, online_lookup=_Stub())
        with pytest.raises(RuntimeError, match="online"):
            w.info()


class TestSetupAndProcess:
    def test_setup_wiring(self, tmp_path, monkeypatch):
        import get_weather_data.main as main_module

        calls = []
        monkeypatch.setattr(
            main_module, "import_ghcnd_stations", lambda db, force: calls.append("ghcn")
        )
        monkeypatch.setattr(
            main_module, "import_isd_stations", lambda db, force: calls.append("isd")
        )
        monkeypatch.setattr(
            main_module, "import_zipcodes", lambda db, force: calls.append("zip")
        )
        monkeypatch.setattr(
            main_module, "build_closest_index", lambda db: calls.append("index")
        )
        w = Weather(database_path=tmp_path / "db.sqlite")
        w.setup()
        assert calls == ["ghcn", "isd", "zip", "index"]
        assert w.db.get_meta("index_version") is not None

    def test_process_csv_forwards(self, tmp_path, monkeypatch):
        import get_weather_data.main as main_module

        recorded = {}

        def fake_process(**kwargs):
            recorded.update(kwargs)
            return 7

        monkeypatch.setattr(main_module, "_process_csv", fake_process)
        w = Weather(database_path=tmp_path / "db.sqlite")
        n = w.process_csv("in.csv", "out.csv", lat_column="lat", lon_column="lon")
        assert n == 7
        assert str(recorded["input_path"]).endswith("in.csv")
        assert recorded["lat_column"] == "lat"
        assert recorded["units"] == "metric"
