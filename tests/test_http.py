"""Tests for HTTP operations with mocking."""

from __future__ import annotations


import pytest
import respx
from httpx import Response


class TestDownload:
    """Tests for download function."""

    @respx.mock
    def test_download_success(self, tmp_path):
        """Test successful download."""
        from zip2ws.zip2ws import download

        test_url = "https://example.com/data.csv"
        test_content = b"col1,col2\nval1,val2"
        output_file = tmp_path / "data.csv"

        respx.get(test_url).mock(return_value=Response(200, content=test_content))

        download(test_url, str(output_file))

        assert output_file.exists()
        assert output_file.read_bytes() == test_content

    @respx.mock
    def test_download_404(self, tmp_path):
        """Test 404 response raises error."""
        import httpx

        from zip2ws.zip2ws import download

        test_url = "https://example.com/missing.csv"
        output_file = tmp_path / "missing.csv"

        respx.get(test_url).mock(return_value=Response(404))

        with pytest.raises(httpx.HTTPStatusError):
            download(test_url, str(output_file))


class TestNoaawebGetContent:
    """Tests for noaaweb get_content function."""

    @respx.mock
    def test_get_content_success(self):
        """Test successful XML content retrieval."""
        import time

        from noaaweb.noaaweb import get_content

        test_url = "https://example.com/api/data.xml"
        test_xml = b"<root><data>test</data></root>"

        respx.get(test_url).mock(return_value=Response(200, content=test_xml))

        original_sleep = time.sleep
        time.sleep = lambda x: None

        try:
            result = get_content(test_url)
            assert result.tag == "root"
            assert result.find("data").text == "test"
        finally:
            time.sleep = original_sleep
