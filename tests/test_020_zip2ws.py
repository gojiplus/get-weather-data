"""Tests for ZIP2WS."""

from __future__ import annotations


import pytest
from click.testing import CliRunner

from zip2ws.zip2ws import cli


@pytest.fixture
def clean_database(tmp_path):
    """Create a clean database path for tests."""
    db_path = tmp_path / "test.sqlite"
    yield db_path


def test_import_help():
    """Test import command help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["import", "--help"])
    assert result.exit_code == 0
    assert "Import ZIP and station data" in result.output


def test_cli_help():
    """Test main CLI help."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "ZIP to Weather Station mapper" in result.output
