"""Tests for download_with_retry."""

import httpx
import pytest
import respx
from httpx import Response

from get_weather_data.core.download import download_with_retry

URL = "https://example.com/file.gz"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _s: None)


class TestDownloadWithRetry:
    @respx.mock
    def test_success_first_try(self, tmp_path):
        respx.get(URL).mock(return_value=Response(200, content=b"data"))
        out = download_with_retry(URL, tmp_path / "f.gz")
        assert out is not None
        assert (tmp_path / "f.gz").read_bytes() == b"data"

    @respx.mock
    def test_404_returns_none_no_retry(self, tmp_path):
        route = respx.get(URL).mock(return_value=Response(404))
        assert download_with_retry(URL, tmp_path / "f.gz") is None
        assert route.call_count == 1

    @respx.mock
    def test_retries_then_succeeds(self, tmp_path):
        route = respx.get(URL).mock(
            side_effect=[
                Response(500),
                Response(500),
                Response(200, content=b"ok"),
            ]
        )
        out = download_with_retry(URL, tmp_path / "f.gz", max_retries=3)
        assert out is not None
        assert route.call_count == 3

    @respx.mock
    def test_exhausts_retries(self, tmp_path):
        respx.get(URL).mock(return_value=Response(500))
        assert download_with_retry(URL, tmp_path / "f.gz", max_retries=2) is None

    @respx.mock
    def test_transport_error_returns_none(self, tmp_path):
        respx.get(URL).mock(side_effect=httpx.ConnectError("boom"))
        assert download_with_retry(URL, tmp_path / "f.gz", max_retries=2) is None
