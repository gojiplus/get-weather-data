"""Tests for NOAAWEB."""

from __future__ import annotations

import os

import pytest
from click.testing import CliRunner

from noaaweb.noaaweb import cli

NCDC_TOKEN = os.environ.get("NCDC_TOKEN")


def test_cli_help():
    """Test CLI help output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Get weather data from NOAA" in result.output


def test_missing_input():
    """Test error when input file is missing."""
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code != 0


@pytest.fixture
def input_file(tmp_path):
    """Create a temporary input CSV file."""
    input_path = tmp_path / "input.csv"
    input_path.write_text("""no,uniqid,zip,year,month,day
2000,2,70503,1999,12,15""")
    return input_path


@pytest.fixture
def output_file(tmp_path):
    """Create a path for the output CSV file."""
    return tmp_path / "output.csv"


@pytest.mark.skipif(NCDC_TOKEN is None, reason="No NCDC token found in environment.")
def test_noaaweb_with_token(input_file, output_file):
    """Test noaaweb with actual NCDC token."""
    runner = CliRunner()
    result = runner.invoke(cli, [str(input_file), "-o", str(output_file)])
    assert result.exit_code == 0 or "values" in result.output
