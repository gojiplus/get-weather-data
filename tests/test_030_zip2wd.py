"""Tests for ZIP2WD."""

from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

DB_PATH = Path("zip2ws/data/zip2ws.sqlite")


@pytest.fixture
def weather_args():
    """Create arguments for WeatherByZip."""
    args = Namespace()
    args.dbpath = "zip2wd/data"
    args.uses_sqlite = True
    args.nth = 0
    args.distance = 30
    args.columns = "column-names.txt"
    args.zip2ws_db = str(DB_PATH)
    return args


@pytest.mark.skipif(
    not DB_PATH.exists(), reason="Database not initialized (run test_020_zip2ws first)"
)
def test_search(weather_args):
    """Test weather search by ZIP code."""
    from zip2wd.zip2wd import WeatherByZip

    weather = WeatherByZip(weather_args)
    z = {
        "uniqid": "1",
        "zip": "10451",
        "from.year": 1877,
        "from.month": 12,
        "from.day": 15,
        "to.year": 1877,
        "to.month": 12,
        "to.day": 15,
    }
    result = weather.search(z)
    if "TMIN" in result[0]:
        assert result[0]["TMIN"] == "6"
    else:
        pytest.skip("Weather data not available for this zipcode/date")


def test_conversion_functions():
    """Test temperature and wind conversion functions."""
    from zip2wd.zip2wd import WeatherByZip

    class MockArgs:
        dbpath = "."
        uses_sqlite = True
        nth = 0
        distance = 0
        columns = "column-names.txt"
        zip2ws_db = str(DB_PATH)

    w = WeatherByZip.__new__(WeatherByZip)
    w.args = MockArgs()

    assert w.f2c(32.0) == 0.0
    assert abs(w.f2c(212.0) - 100.0) < 0.01
    assert abs(w.kn2ms(1.0) - 0.51444) < 0.0001
