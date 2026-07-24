"""Tests for CLI commands."""

import respx
from click.testing import CliRunner
from httpx import Response

from get_weather_data.api.noaa import CDO_BASE_URL
from get_weather_data.cli import cli
from get_weather_data.core.config import Config, set_config


class TestCli:
    """Tests for get-weather CLI."""

    def test_help(self):
        """Test --help flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Get weather data for US ZIP codes" in result.output

    def test_version(self):
        """Test --version flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "version" in result.output

    def test_setup_help(self):
        """Test setup --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["setup", "--help"])
        assert result.exit_code == 0
        assert "Set up the database" in result.output

    def test_get_help(self):
        """Test get --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["get", "--help"])
        assert result.exit_code == 0
        assert "Get weather data for a location" in result.output

    def test_get_help_mentions_online(self):
        """Test that get --help documents the --online flag."""
        runner = CliRunner()
        result = runner.invoke(cli, ["get", "--help"])
        assert result.exit_code == 0
        assert "--online" in result.output

    @respx.mock
    def test_get_online(self, monkeypatch):
        """Test online get against a mocked CDO API."""
        monkeypatch.setenv("NCDC_TOKEN", "test-token")
        monkeypatch.setattr(
            "get_weather_data.weather.online._default_zip_coordinates",
            lambda: {"10001": (40.7484, -73.9967)},
        )
        set_config(Config())
        station = "GHCND:USW00094728"
        respx.get(f"{CDO_BASE_URL}/stations").mock(
            return_value=Response(
                200,
                json={
                    "metadata": {"resultset": {"offset": 1, "count": 1, "limit": 1000}},
                    "results": [
                        {
                            "id": station,
                            "name": "NY CITY CENTRAL PARK",
                            "latitude": 40.78,
                            "longitude": -73.97,
                        }
                    ],
                },
            )
        )
        respx.get(f"{CDO_BASE_URL}/data").mock(
            return_value=Response(
                200,
                json={
                    "metadata": {"resultset": {"offset": 1, "count": 1, "limit": 1000}},
                    "results": [
                        {
                            "date": "2024-01-15T00:00:00",
                            "datatype": "TMAX",
                            "station": station,
                            "attributes": "",
                            "value": 44,
                        }
                    ],
                },
            )
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["get", "10001", "2024-01-15", "--online"])
        assert result.exit_code == 0
        assert "NY CITY CENTRAL PARK" in result.output

    def test_get_online_without_token(self, monkeypatch):
        """Test online get fails cleanly without a token."""
        monkeypatch.delenv("NCDC_TOKEN", raising=False)
        set_config(Config(ncdc_token=None))
        runner = CliRunner()
        result = runner.invoke(cli, ["get", "10001", "2024-01-15", "--online"])
        assert result.exit_code == 1
        assert "Error:" in result.output
        assert "cdo-web/token" in result.output

    def test_cache_info_help(self):
        """Test cache info --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "info", "--help"])
        assert result.exit_code == 0
        assert "disk usage" in result.output.lower()

    def test_cache_clear_requires_selection(self):
        """Test cache clear with nothing selected exits 1."""
        runner = CliRunner()
        result = runner.invoke(cli, ["cache", "clear", "--yes"])
        assert result.exit_code == 1
        assert "Nothing selected" in result.output

    def test_get_units_flag_in_help(self):
        """Test get --help shows units and elements options."""
        runner = CliRunner()
        result = runner.invoke(cli, ["get", "--help"])
        assert "--units" in result.output
        assert "--elements" in result.output
        assert "lat,lon" in result.output

    def test_info_without_database(self, tmp_path, monkeypatch):
        """Test info fails helpfully when no database exists."""
        monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
        runner = CliRunner()
        result = runner.invoke(cli, ["-d", str(tmp_path / "nope.sqlite"), "info"])
        assert result.exit_code == 1
        assert "setup" in result.output

    def test_process_help(self):
        """Test process --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["process", "--help"])
        assert result.exit_code == 0
        assert "Process a CSV file" in result.output

    def test_info_help(self):
        """Test info --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["info", "--help"])
        assert result.exit_code == 0
        assert "Show database statistics" in result.output
