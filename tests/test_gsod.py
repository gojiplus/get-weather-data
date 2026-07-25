"""Tests for the GSOD CSV parser and unit conversion."""

from datetime import date

import pytest

from get_weather_data.weather import gsod as gsod_module
from get_weather_data.weather.gsod import get_gsod_data
from get_weather_data.weather.units import KNOTS_TO_MS

DAY = date(2024, 1, 15)

HEADER = "STATION,DATE,TEMP,DEWP,SLP,STP,VISIB,WDSP,MXSPD,GUST,MAX,MIN,PRCP,SNDP\n"
# TEMP 50F, DEWP 41F, SLP 1013.2, STP 987.6, VISIB 10, WDSP 10kn, MXSPD 999.9(missing),
# GUST 20kn, MAX 68F, MIN 32F, PRCP 0.50, SNDP 999.9(missing)
ROW = (
    "725030,2024-01-15,50.0,41.0,1013.2,987.6,10.0,10.0,999.9,20.0,"
    "68.0,32.0,0.50,999.9\n"
)
OTHER = (
    "725030,2024-01-16,60.0,50.0,1010.0,985.0,9.0,8.0,12.0,15.0,70.0,40.0,0.00,0.0\n"
)


@pytest.fixture
def gsod_file(tmp_path, monkeypatch):
    path = tmp_path / "725030.csv"
    path.write_text(HEADER + ROW + OTHER)
    monkeypatch.setattr(gsod_module, "_ensure_gsod_file", lambda sid, yr: path)
    return path


class TestGetGsodData:
    def test_unit_conversion(self, gsod_file):
        data = get_gsod_data("725030", DAY)
        assert data["temp"] == pytest.approx(10.0)  # 50F -> 10C
        assert data["max_temp"] == pytest.approx(20.0)  # 68F
        assert data["min_temp"] == pytest.approx(0.0)  # 32F
        assert data["dewpoint"] == pytest.approx(5.0)  # 41F
        assert data["wind_speed"] == pytest.approx(10.0 * KNOTS_TO_MS)
        assert data["gust"] == pytest.approx(20.0 * KNOTS_TO_MS)
        # pass-through fields
        assert data["sea_level_pressure"] == pytest.approx(1013.2)
        assert data["visibility"] == pytest.approx(10.0)
        assert data["precipitation"] == pytest.approx(0.5)

    def test_sentinels_become_none(self, gsod_file):
        data = get_gsod_data("725030", DAY)
        assert data["max_wind_speed"] is None  # 999.9
        assert data["snow_depth"] is None  # 999.9

    def test_no_conversion(self, gsod_file):
        data = get_gsod_data("725030", DAY, convert_units=False)
        assert data["temp"] == 50.0
        assert data["wind_speed"] == 10.0

    def test_missing_date_returns_none(self, gsod_file):
        data = get_gsod_data("725030", date(2024, 6, 1))
        assert all(v is None for v in data.values())

    def test_no_file_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(gsod_module, "_ensure_gsod_file", lambda sid, yr: None)
        data = get_gsod_data("725030", DAY)
        assert all(v is None for v in data.values())
