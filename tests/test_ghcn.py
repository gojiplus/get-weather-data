"""Tests for the yearly GHCN database build (locking, atomicity)."""

import csv
import gzip
import io
import sqlite3
import threading
from datetime import date

import pytest
import respx
from httpx import Response

from get_weather_data.core.config import Config, set_config
from get_weather_data.weather import ghcn


@pytest.fixture(autouse=True)
def _isolated_config(tmp_path, monkeypatch):
    """Point caches at a temp dir and reset module-level pools."""
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
    writer = csv.writer(buf)
    writer.writerows(rows)
    return gzip.compress(buf.getvalue().encode())


ROWS = [
    ("USW00094728", "20100115", "TMAX", "-10", "", "", "W", ""),
    ("USW00094728", "20100115", "PRCP", "0", "", "", "W", ""),
]


class TestConcurrentBuild:
    """Many threads must produce exactly one download and a valid DB."""

    @respx.mock
    def test_threads_share_one_build(self):
        route = respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2010)).mock(
            return_value=Response(200, content=_year_gz_bytes(ROWS))
        )

        errors: list[Exception] = []

        def fetch():
            try:
                ghcn.get_ghcn_data("USW00094728", date(2010, 1, 15))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=fetch) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert route.call_count == 1

        # DB is valid and queryable; the source archive was cleaned up
        db_path = ghcn._get_ghcn_db_path(2010)
        assert db_path.exists()
        assert not (db_path.parent / "2010.csv.gz").exists()
        values = ghcn.get_ghcn_data("USW00094728", date(2010, 1, 15))
        assert values["TMAX"] == -10.0
        assert values["PRCP"] == 0.0

    @respx.mock
    def test_zero_value_survives_fetch(self):
        respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2011)).mock(
            return_value=Response(
                200,
                content=_year_gz_bytes(
                    [("USW00094728", "20110115", "PRCP", "0", "", "", "W", "")]
                ),
            )
        )
        values = ghcn.get_ghcn_data("USW00094728", date(2011, 1, 15))
        assert values["PRCP"] == 0.0

    @respx.mock
    def test_failed_download_leaves_no_partial_db(self):
        respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2012)).mock(
            return_value=Response(404)
        )
        with pytest.raises(RuntimeError, match="Failed to download"):
            ghcn.get_ghcn_data("USW00094728", date(2012, 1, 15))
        assert not ghcn._get_ghcn_db_path(2012).exists()
        leftovers = list(ghcn._get_ghcn_db_path(2012).parent.glob("*.tmp-*"))
        assert leftovers == []

    @respx.mock
    def test_historical_year_not_redownloaded(self):
        route = respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2010)).mock(
            return_value=Response(200, content=_year_gz_bytes(ROWS))
        )
        ghcn.get_ghcn_data("USW00094728", date(2010, 1, 15))
        ghcn.get_ghcn_data("USW00094728", date(2010, 1, 16))
        assert route.call_count == 1


class TestReadOnlyPool:
    """Connections are pooled per thread and opened read-only."""

    @respx.mock
    def test_connection_reused(self):
        respx.get(ghcn.GHCN_BY_YEAR_URL.format(year=2010)).mock(
            return_value=Response(200, content=_year_gz_bytes(ROWS))
        )
        ghcn.get_ghcn_data("USW00094728", date(2010, 1, 15))
        pool = ghcn._connections.pool
        conn = pool[2010]
        ghcn.get_ghcn_data("USW00094728", date(2010, 1, 16))
        assert ghcn._connections.pool[2010] is conn
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("CREATE TABLE nope (x)")
