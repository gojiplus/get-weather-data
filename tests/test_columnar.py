"""Tests for Parquet export and the DuckDB query surface."""

import csv
from datetime import date

import pytest

from get_weather_data.core.database import INDEX_VERSION, Database
from get_weather_data.core.distance import Station
from get_weather_data.weather import lookup as lookup_module
from get_weather_data.weather.batch import process_csv
from get_weather_data.weather.columnar import query_weather

pa = pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

DAY = date(2024, 1, 15)


@pytest.fixture
def city_db(tmp_path) -> Database:
    db = Database(tmp_path / "city.sqlite")
    db.init_schema()
    db.insert_zipcode("10001", "New York", "NY", 40.7484, -73.9967)
    db.insert_zipcode("60601", "Chicago", "IL", 41.8855, -87.6221)
    db.insert_station(
        Station(id="GHCN1", name="GHCN NY", lat=40.78, lon=-73.97, type="GHCND")
    )
    db.insert_station(
        Station(id="GHCN2", name="GHCN IL", lat=41.88, lon=-87.62, type="GHCND")
    )
    db.set_closest_stations_bulk(
        {"10001": [("GHCN1", 4000)], "60601": [("GHCN2", 3000)]}
    )
    db.set_meta("index_version", str(INDEX_VERSION))
    return db


@pytest.fixture(autouse=True)
def _mock_data(monkeypatch):
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


def _write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class TestBatchParquet:
    def _run(self, city_db, tmp_path, **kwargs):
        inp = tmp_path / "in.csv"
        _write_csv(
            inp,
            [
                {"zip": "10001", "date": "2024-01-15"},
                {"zip": "60601", "date": "2024-01-15"},
            ],
            ["zip", "date"],
        )
        out = tmp_path / "out.parquet"
        n = process_csv(
            inp, out, db=city_db, date_column="date", year_column=None, **kwargs
        )
        return out, n

    def test_infers_parquet_from_suffix(self, city_db, tmp_path):
        import pyarrow.parquet as pq

        out, n = self._run(city_db, tmp_path)
        assert n == 2
        table = pq.read_table(out)
        assert table.num_rows == 2
        # passthrough input columns preserved
        assert "zip" in table.column_names
        # value columns are typed (float), not strings
        assert table.schema.field("tmax").type == pa.float64()
        tmax = table.column("tmax").to_pylist()
        assert tmax == [-1.6, -1.6]  # raw -16.0 tenths degC -> -1.6 degC

    def test_explicit_format_overrides_suffix(self, city_db, tmp_path):
        import pyarrow.parquet as pq

        inp = tmp_path / "in.csv"
        _write_csv(inp, [{"zip": "10001", "date": "2024-01-15"}], ["zip", "date"])
        out = tmp_path / "out.dat"  # non-.parquet suffix
        process_csv(
            inp,
            out,
            db=city_db,
            date_column="date",
            year_column=None,
            output_format="parquet",
        )
        assert pq.read_table(out).num_rows == 1

    def test_missing_value_is_null_not_blank(self, city_db, tmp_path, monkeypatch):
        # Station reports no snow -> snow column should be null in Parquet.
        import pyarrow.parquet as pq

        out, _ = self._run(city_db, tmp_path)
        table = pq.read_table(out)
        assert table.column("snow").to_pylist() == [None, None]


class TestQueryWeather:
    def _make_parquet(self, tmp_path):
        import pyarrow.parquet as pq

        table = pa.table(
            {
                "zip": ["10001", "60601", "94103"],
                "tmax": [1.0, -5.0, 12.0],
                "prcp": [0.0, 3.0, 0.0],
            }
        )
        path = tmp_path / "w.parquet"
        pq.write_table(table, path)
        return path

    def test_query_returns_rows(self, tmp_path):
        path = self._make_parquet(tmp_path)
        rows = query_weather(
            "SELECT zip, tmax FROM t WHERE prcp = 0 ORDER BY tmax DESC",
            tables={"t": str(path)},
        )
        assert rows == [
            {"zip": "94103", "tmax": 12.0},
            {"zip": "10001", "tmax": 1.0},
        ]

    def test_query_aggregate(self, tmp_path):
        path = self._make_parquet(tmp_path)
        rows = query_weather(
            "SELECT count(*) AS n, max(tmax) AS hi FROM t", tables={"t": str(path)}
        )
        assert rows[0]["n"] == 3
        assert rows[0]["hi"] == 12.0

    def test_glob_across_files(self, tmp_path):
        import pyarrow.parquet as pq

        for i in range(3):
            pq.write_table(pa.table({"v": [i]}), tmp_path / f"part{i}.parquet")
        rows = query_weather(
            "SELECT sum(v) AS total FROM t", tables={"t": str(tmp_path / "*.parquet")}
        )
        assert rows[0]["total"] == 3

    def test_as_frame(self, tmp_path):
        pytest.importorskip("pandas")
        path = self._make_parquet(tmp_path)
        df = query_weather("SELECT * FROM t", tables={"t": str(path)}, as_frame=True)
        assert list(df["zip"]) == ["10001", "60601", "94103"]

    def test_invalid_view_name_rejected(self, tmp_path):
        path = self._make_parquet(tmp_path)
        with pytest.raises(ValueError, match="invalid view name"):
            query_weather("SELECT 1", tables={"bad name": str(path)})

    def test_path_with_quote_escaped(self, tmp_path):
        import pyarrow.parquet as pq

        # A directory whose name contains a single quote must not break SQL.
        d = tmp_path / "o'brien"
        d.mkdir()
        path = d / "w.parquet"
        pq.write_table(pa.table({"v": [7]}), path)
        rows = query_weather("SELECT v FROM t", tables={"t": str(path)})
        assert rows == [{"v": 7}]
