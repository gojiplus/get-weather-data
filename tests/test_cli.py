"""Tests for CLI commands."""

from click.testing import CliRunner

from get_weather_data.cli import cli


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
        assert "3.0.0" in result.output

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
        assert "Get weather data for a ZIP code" in result.output

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
