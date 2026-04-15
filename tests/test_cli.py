"""Tests for CLI commands."""

from __future__ import annotations

from click.testing import CliRunner

from noaaweb.noaaweb import cli as noaaweb_cli
from zip2ws.zip2ws import cli as zip2ws_cli


class TestZip2wsCli:
    """Tests for zip2ws CLI."""

    def test_help(self):
        """Test --help flag."""
        runner = CliRunner()
        result = runner.invoke(zip2ws_cli, ["--help"])
        assert result.exit_code == 0
        assert "ZIP to Weather Station mapper" in result.output

    def test_import_help(self):
        """Test import --help."""
        runner = CliRunner()
        result = runner.invoke(zip2ws_cli, ["import", "--help"])
        assert result.exit_code == 0
        assert "Import ZIP and station data" in result.output

    def test_closest_help(self):
        """Test closest --help."""
        runner = CliRunner()
        result = runner.invoke(zip2ws_cli, ["closest", "--help"])
        assert result.exit_code == 0
        assert "Calculate closest stations" in result.output

    def test_export_help(self):
        """Test export --help."""
        runner = CliRunner()
        result = runner.invoke(zip2ws_cli, ["export", "--help"])
        assert result.exit_code == 0
        assert "Export closest stations to CSV" in result.output


class TestNoaawebCli:
    """Tests for noaaweb CLI."""

    def test_help(self):
        """Test --help flag."""
        runner = CliRunner()
        result = runner.invoke(noaaweb_cli, ["--help"])
        assert result.exit_code == 0
        assert "Get weather data from NOAA" in result.output

    def test_missing_input_file(self):
        """Test error when input file is missing."""
        runner = CliRunner()
        result = runner.invoke(noaaweb_cli, [])
        assert result.exit_code != 0
