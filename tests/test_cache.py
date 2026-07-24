"""Tests for cache freshness and disk management."""

import os
import time
from datetime import date

import pytest
import respx
from httpx import Response

from get_weather_data.core.cache import (
    clear_cache,
    ensure_fresh_download,
    is_fresh,
    year_is_immutable,
)
from get_weather_data.core.config import Config, set_config


@pytest.fixture(autouse=True)
def _isolated_config(tmp_path):
    set_config(Config(ncdc_token=None, data_dir=tmp_path, cache_dir=tmp_path))


def _age(path, days: float) -> None:
    """Backdate a file's mtime."""
    past = time.time() - days * 86400
    os.utime(path, (past, past))


class TestFreshness:
    def test_missing_file_not_fresh(self, tmp_path):
        assert not is_fresh(tmp_path / "nope.txt", 30)

    def test_new_file_fresh(self, tmp_path):
        f = tmp_path / "f.txt"
        f.write_text("x")
        assert is_fresh(f, 30)

    def test_old_file_stale(self, tmp_path):
        f = tmp_path / "f.txt"
        f.write_text("x")
        _age(f, 31)
        assert not is_fresh(f, 30)


class TestEnsureFreshDownload:
    @respx.mock
    def test_fresh_file_not_redownloaded(self, tmp_path):
        f = tmp_path / "stations.txt"
        f.write_text("cached")
        route = respx.get("https://example.com/stations.txt").mock(
            return_value=Response(200, content=b"new")
        )
        ensure_fresh_download("https://example.com/stations.txt", f)
        assert route.call_count == 0
        assert f.read_text() == "cached"

    @respx.mock
    def test_stale_file_redownloaded(self, tmp_path):
        f = tmp_path / "stations.txt"
        f.write_text("old")
        _age(f, 60)
        respx.get("https://example.com/stations.txt").mock(
            return_value=Response(200, content=b"new")
        )
        ensure_fresh_download("https://example.com/stations.txt", f)
        assert f.read_text() == "new"

    @respx.mock
    def test_failed_refresh_keeps_stale_copy(self, tmp_path, monkeypatch):
        monkeypatch.setattr("time.sleep", lambda _s: None)
        f = tmp_path / "stations.txt"
        f.write_text("old")
        _age(f, 60)
        respx.get("https://example.com/stations.txt").mock(return_value=Response(500))
        result = ensure_fresh_download(
            "https://example.com/stations.txt", f, max_age_days=30
        )
        assert result == f
        assert f.read_text() == "old"

    @respx.mock
    def test_force_redownloads(self, tmp_path):
        f = tmp_path / "stations.txt"
        f.write_text("cached")
        respx.get("https://example.com/stations.txt").mock(
            return_value=Response(200, content=b"new")
        )
        ensure_fresh_download("https://example.com/stations.txt", f, force=True)
        assert f.read_text() == "new"


class TestImmutableYears:
    def test_historical_year(self):
        assert year_is_immutable(2020, today=date(2026, 7, 24))

    def test_previous_year_mutable(self):
        assert not year_is_immutable(2025, today=date(2026, 7, 24))

    def test_current_year_mutable(self):
        assert not year_is_immutable(2026, today=date(2026, 7, 24))


class TestClearCache:
    def test_clear_accounts_bytes(self, tmp_path):
        from get_weather_data.core.config import get_config

        ghcn_dir = get_config().ghcn_cache_dir
        (ghcn_dir / "ghcn_2020.sqlite3").write_bytes(b"x" * 1000)
        freed = clear_cache(ghcn=True)
        assert freed == 1000
        assert not ghcn_dir.exists()

    def test_clear_nothing_selected(self):
        assert clear_cache() == 0
