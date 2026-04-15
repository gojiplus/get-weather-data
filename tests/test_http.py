"""Tests for HTTP operations with mocking."""

import pytest
import respx
from httpx import Response

from get_weather_data.core.download import download, download_with_retry


class TestDownload:
    """Tests for download function."""

    @respx.mock
    def test_download_success(self, tmp_path):
        """Test successful download."""
        test_url = "https://example.com/data.csv"
        test_content = b"col1,col2\nval1,val2"
        output_file = tmp_path / "data.csv"

        respx.get(test_url).mock(return_value=Response(200, content=test_content))

        download(test_url, output_file)

        assert output_file.exists()
        assert output_file.read_bytes() == test_content

    @respx.mock
    def test_download_404(self, tmp_path):
        """Test 404 response raises error."""
        import httpx

        test_url = "https://example.com/missing.csv"
        output_file = tmp_path / "missing.csv"

        respx.get(test_url).mock(return_value=Response(404))

        with pytest.raises(httpx.HTTPStatusError):
            download(test_url, output_file)


class TestDownloadWithRetry:
    """Tests for download_with_retry function."""

    @respx.mock
    def test_download_with_retry_success(self, tmp_path):
        """Test successful download on first try."""
        test_url = "https://example.com/data.csv"
        test_content = b"col1,col2\nval1,val2"
        output_file = tmp_path / "data.csv"

        respx.get(test_url).mock(return_value=Response(200, content=test_content))

        result = download_with_retry(test_url, output_file, max_retries=1)

        assert result is not None
        assert output_file.exists()
        assert output_file.read_bytes() == test_content

    @respx.mock
    def test_download_with_retry_404_returns_none(self, tmp_path):
        """Test 404 response returns None without retry."""
        test_url = "https://example.com/missing.csv"
        output_file = tmp_path / "missing.csv"

        respx.get(test_url).mock(return_value=Response(404))

        result = download_with_retry(test_url, output_file, max_retries=3)

        assert result is None
        assert not output_file.exists()
