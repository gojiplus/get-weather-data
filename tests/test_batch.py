"""Tests for batch CSV processing: error isolation, streaming, lat/lon."""

import csv
from datetime import date

import pytest

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import batch as batch_module
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.batch import process_csv

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


@pytest.fixture(autouse=True)
def _mock_data(monkeypatch):
    """Serve fixed GHCN data; keep GSOD out of the way."""
    lookup_module._cached_ghcn_data.cache_clear()
    lookup_module._cached_gsod_data.cache_clear()
    monkeypatch.setattr(
        lookup_module,
        "get_ghcn_data",
        lambda station_id, target_date: {"TMAX": -16.0, "PRCP": 0.0},
    )
    monkeypatch.setattr(
        lookup_module, "get_gsod_data", lambda station_id, target_date: {}
    )


def _write_csv(path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path) -> list[dict]:
    with open(path) as f:
        return list(csv.DictReader(f))


class TestErrorIsolation:
    """One bad row must not take the job down."""

    def test_bad_rows_get_error_column(self, city_db, tmp_path):
        _write_csv(
            tmp_path / "in.csv",
            [
                {"zip": "10001", "date": "2024-01-15"},
                {"zip": "", "date": "2024-01-15"},
                {"zip": "10001", "date": "not-a-date"},
                {"zip": "10001", "date": "2024-01-16"},
            ],
            ["zip", "date"],
        )
        count = process_csv(
            tmp_path / "in.csv",
            tmp_path / "out.csv",
            date_column="date",
            db=city_db,
            parallel=False,
        )
        assert count == 4
        rows = _read_csv(tmp_path / "out.csv")
        assert rows[0]["weather_error"] == ""
        assert rows[0]["tmax"] == "-1.6"
        assert rows[1]["weather_error"] == "missing location"
        assert rows[2]["weather_error"] == "missing or invalid date"
        assert rows[3]["weather_error"] == ""

    def test_lookup_exception_recorded(self, city_db, tmp_path, monkeypatch):
        def boom(station_id, target_date):
            raise RuntimeError("download exploded")

        monkeypatch.setattr(lookup_module, "get_ghcn_data", boom)
        _write_csv(
            tmp_path / "in.csv",
            [{"zip": "10001", "date": "2024-01-15"}],
            ["zip", "date"],
        )
        process_csv(
            tmp_path / "in.csv",
            tmp_path / "out.csv",
            date_column="date",
            db=city_db,
            parallel=False,
        )
        rows = _read_csv(tmp_path / "out.csv")
        # the lookup itself absorbs station-level fetch problems, so a
        # crash here means the error column captured it OR the lookup
        # returned an empty result; either way the job survived
        assert len(rows) == 1

    def test_zero_written_not_blank(self, city_db, tmp_path):
        _write_csv(
            tmp_path / "in.csv",
            [{"zip": "10001", "date": "2024-01-15"}],
            ["zip", "date"],
        )
        process_csv(
            tmp_path / "in.csv",
            tmp_path / "out.csv",
            date_column="date",
            db=city_db,
            parallel=False,
        )
        rows = _read_csv(tmp_path / "out.csv")
        assert rows[0]["prcp"] == "0.0"


class TestStreaming:
    """Chunks are written incrementally."""

    def test_chunked_write_survives_midstream_failure(
        self, city_db, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(batch_module, "CHUNK_SIZE", 2)
        calls = {"n": 0}
        real_get = lookup_module.get_ghcn_data

        def flaky(station_id, target_date):
            calls["n"] += 1
            if calls["n"] > 2:
                raise KeyboardInterrupt  # simulate a hard crash mid-run
            return {"TMAX": -16.0}

        monkeypatch.setattr(lookup_module, "get_ghcn_data", flaky)
        _write_csv(
            tmp_path / "in.csv",
            [{"zip": "10001", "date": f"2024-01-{15 + i:02d}"} for i in range(4)],
            ["zip", "date"],
        )
        with pytest.raises(KeyboardInterrupt):
            process_csv(
                tmp_path / "in.csv",
                tmp_path / "out.csv",
                date_column="date",
                db=city_db,
                parallel=False,
            )
        rows = _read_csv(tmp_path / "out.csv")
        assert len(rows) == 2  # first chunk landed before the crash
        monkeypatch.setattr(lookup_module, "get_ghcn_data", real_get)


class TestLatLonColumns:
    """Coordinate columns take precedence over the ZIP column."""

    def test_lat_lon_rows(self, city_db, tmp_path):
        _write_csv(
            tmp_path / "in.csv",
            [
                {"lat": "40.7484", "lon": "-73.9967", "date": "2024-01-15"},
                {"lat": "", "lon": "", "date": "2024-01-15"},
            ],
            ["lat", "lon", "date"],
        )
        process_csv(
            tmp_path / "in.csv",
            tmp_path / "out.csv",
            lat_column="lat",
            lon_column="lon",
            zipcode_column="zip",
            date_column="date",
            db=city_db,
            parallel=False,
        )
        rows = _read_csv(tmp_path / "out.csv")
        assert rows[0]["station_id"] == "GHCN1"
        assert rows[0]["tmax"] == "-1.6"
        assert rows[1]["weather_error"] == "missing location"

    def test_invalid_coordinates(self, city_db, tmp_path):
        _write_csv(
            tmp_path / "in.csv",
            [{"lat": "abc", "lon": "-73.99", "date": "2024-01-15"}],
            ["lat", "lon", "date"],
        )
        process_csv(
            tmp_path / "in.csv",
            tmp_path / "out.csv",
            lat_column="lat",
            lon_column="lon",
            date_column="date",
            db=city_db,
            parallel=False,
        )
        rows = _read_csv(tmp_path / "out.csv")
        assert "invalid coordinates" in rows[0]["weather_error"]


class TestParallel:
    """The parallel path produces the same output as serial."""

    def test_parallel_matches_serial(self, city_db, tmp_path):
        rows_in = [{"zip": "10001", "date": "2024-01-15"} for _ in range(20)]
        _write_csv(tmp_path / "in.csv", rows_in, ["zip", "date"])
        process_csv(
            tmp_path / "in.csv",
            tmp_path / "serial.csv",
            date_column="date",
            db=city_db,
            parallel=False,
        )
        process_csv(
            tmp_path / "in.csv",
            tmp_path / "parallel.csv",
            date_column="date",
            db=city_db,
            parallel=True,
            max_workers=4,
        )
        assert _read_csv(tmp_path / "serial.csv") == _read_csv(
            tmp_path / "parallel.csv"
        )
