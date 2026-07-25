"""Tests for CLI commands."""

from datetime import UTC, date

import respx
from click.testing import CliRunner
from httpx import Response

from get_weather_data import cli as cli_module
from get_weather_data.api.noaa import CDO_BASE_URL
from get_weather_data.cli import cli
from get_weather_data.core import cache as cache_module
from get_weather_data.core.cache import CacheEntry
from get_weather_data.core.config import Config, set_config
from get_weather_data.weather.results import WeatherResult


class _FakeWeather:
    """Stand-in for Weather in CLI tests (no DB/network)."""

    last_kwargs: dict = {}

    def __init__(self, **kwargs):
        _FakeWeather.last_kwargs = kwargs

    def setup(self, **kwargs):
        return None

    def info(self):
        return {
            "ghcn_stations": 90000,
            "usaf_stations": 9000,
            "total_stations": 99000,
            "zipcodes": 41000,
        }

    def get(self, location, target_date, elements=None):
        return WeatherResult(
            date=date(2024, 1, 15),
            station_id="USW1",
            station_name="TEST STATION",
            station_type="GHCND",
            station_distance_meters=4000,
            tmax=1.5,
            prcp=0.0,
        )

    def process_csv(self, **kwargs):
        _FakeWeather.last_kwargs = kwargs
        return 3

    def get_hourly(self, location, start_date, end_date=None):
        from datetime import datetime

        from get_weather_data.weather.results import HourlyResult

        return [
            HourlyResult(
                observed_at=datetime(2023, 7, 16, 0, tzinfo=UTC),
                station_id="725030-14732",
                station_name="LA GUARDIA",
                station_distance_meters=942,
                units="metric",
                temp=27.2,
                dewpoint=21.0,
                wind_speed=4.6,
                wind_direction=170,
            )
        ]


class TestCliCommands:
    def test_setup(self, monkeypatch):
        monkeypatch.setattr(cli_module, "Weather", _FakeWeather)
        result = CliRunner().invoke(cli, ["setup"])
        assert result.exit_code == 0
        assert "Setup complete" in result.output
        assert "90,000" in result.output

    def test_get_table(self, monkeypatch):
        monkeypatch.setattr(cli_module, "Weather", _FakeWeather)
        result = CliRunner().invoke(
            cli, ["get", "10001", "2024-01-15", "--units", "imperial"]
        )
        assert result.exit_code == 0
        assert "TEST STATION" in result.output
        assert "Maximum temperature" in result.output
        # zero precip renders as a value, not N/A
        assert "0.0" in result.output

    def test_hourly_table(self, monkeypatch):
        monkeypatch.setattr(cli_module, "Weather", _FakeWeather)
        result = CliRunner().invoke(cli, ["hourly", "11371", "2023-07-16"])
        assert result.exit_code == 0
        assert "LA GUARDIA" in result.output
        assert "2023-07-16 00:00" in result.output

    def test_hourly_empty(self, monkeypatch):
        class NoData(_FakeWeather):
            def get_hourly(self, *a, **k):
                return []

        monkeypatch.setattr(cli_module, "Weather", NoData)
        result = CliRunner().invoke(cli, ["hourly", "11371", "2023-07-16"])
        assert result.exit_code == 0
        assert "No hourly data" in result.output

    def test_get_error(self, monkeypatch):
        class Boom(_FakeWeather):
            def get(self, *a, **k):
                raise RuntimeError("kaboom")

        monkeypatch.setattr(cli_module, "Weather", Boom)
        result = CliRunner().invoke(cli, ["get", "10001", "2024-01-15"])
        assert result.exit_code == 1
        assert "Error:" in result.output

    def test_process(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cli_module, "Weather", _FakeWeather)
        infile = tmp_path / "in.csv"
        infile.write_text("zip,date\n10001,2024-01-15\n")
        result = CliRunner().invoke(
            cli,
            [
                "process",
                str(infile),
                str(tmp_path / "out.csv"),
                "--date-column",
                "date",
            ],
        )
        assert result.exit_code == 0
        assert "Processed 3 rows" in result.output
        # date-column given -> year/month/day nulled
        assert _FakeWeather.last_kwargs["year_column"] is None

    def test_cache_info(self, monkeypatch):
        monkeypatch.setattr(
            cache_module,
            "cache_info",
            lambda: [CacheEntry("ghcn", "/tmp/ghcn", 3, 2_000_000)],
        )
        result = CliRunner().invoke(cli, ["cache", "info"])
        assert result.exit_code == 0
        assert "ghcn" in result.output
        assert "2.0 MB" in result.output

    def test_cache_clear(self, monkeypatch):
        called = {}

        def fake_clear(**kwargs):
            called.update(kwargs)
            return 5_000_000

        monkeypatch.setattr(cache_module, "clear_cache", fake_clear)
        result = CliRunner().invoke(cli, ["cache", "clear", "--gsod", "--yes"])
        assert result.exit_code == 0
        assert "Freed" in result.output
        assert called["gsod"] is True


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
