"""Real end-to-end tests against live NOAA endpoints.

Excluded from CI (marked ``live``); run locally with ``pytest -m live``.
Assertions check plausibility and direction against known events, never
exact values, and skip (not fail) on transient network/token errors.
"""

import os
from datetime import date, timedelta

import pytest

from get_weather_data import Weather
from get_weather_data.weather.results import WeatherResult

pytestmark = pytest.mark.live

TOKEN = os.environ.get("NCDC_TOKEN")
online_only = pytest.mark.skipif(not TOKEN, reason="NCDC_TOKEN not set")


def _online() -> Weather:
    return Weather(online=True)


def assert_plausible(r: WeatherResult) -> None:
    """Physical invariants any real daily observation must satisfy."""
    if r.tmax is not None and r.tmin is not None:
        assert r.tmax >= r.tmin
    for temp in (r.tmax, r.tmin, r.tavg):
        if temp is not None:
            assert -70.0 <= temp <= 60.0
    for amount in (r.prcp, r.snow, r.snwd):
        if amount is not None:
            assert amount >= 0.0


@online_only
def test_buffalo_2022_blizzard():
    """Dec 2022 Buffalo blizzard: huge snow, deep pack, frigid."""
    results = _online().get_range("14201", "2022-12-23", "2022-12-26")
    for r in results:
        assert_plausible(r)
    snows = [r.snow for r in results if r.snow is not None]
    depths = [r.snwd for r in results if r.snwd is not None]
    assert max(snows) > 200  # a day with >200 mm (~8 in) of snowfall
    assert max(depths) > 300  # snowpack over 300 mm (~12 in)
    # temperature must be reached despite the dense CoCoRaHS network
    tmins = [r.tmin for r in results if r.tmin is not None]
    assert tmins and min(tmins) < -5.0


@online_only
def test_phoenix_2023_heat():
    """July 2023 Phoenix: record run of 110F+ days."""
    results = _online().get_range("85001", "2023-07-01", "2023-07-31")
    for r in results:
        assert_plausible(r)
    hot = [r for r in results if r.tmax is not None and r.tmax >= 43.3]
    assert len(hot) >= 20  # loose; the real streak was 31
    assert max(r.tmax for r in hot) >= 46.0  # peak well above 110F


@online_only
def test_get_frame_shape_and_units():
    pytest.importorskip("pandas")
    df = _online().get_frame("10001", "2024-01-15", "2024-01-17")
    assert len(df) == 3
    for col in ("date", "station_id", "tmax", "prcp", "units"):
        assert col in df.columns
    temps = df["tmax"].dropna()
    if len(temps):
        assert temps.between(-70, 60).all()  # metric °C, not raw tenths


def test_grid_any_conus_point():
    """The grid returns data for a remote point with no nearby station."""
    xr = pytest.importorskip("xarray")  # noqa: F841 - gates the [grid] extra
    r = Weather(source="grid").get((44.06, -121.31), "2024-01-15")  # remote OR
    assert r.station_type == "gridded"
    assert r.tmax is not None
    assert_plausible(r)


@online_only
def test_grid_and_online_agree():
    """Grid and station temps agree within tolerance over a +/-1 day window
    (nClimGrid days end in the morning, so allow a one-day shift)."""
    pytest.importorskip("xarray")
    pt = (41.9, -87.9)  # Chicago area
    d = date(2023, 8, 15)
    grid = {
        r.date: r.tmax
        for r in Weather(source="grid").get_range(
            pt, d - timedelta(days=1), d + timedelta(days=1)
        )
    }
    online = _online().get(pt, d)
    if online.tmax is None or not any(v is not None for v in grid.values()):
        pytest.skip("no overlapping data")
    gap = min(abs(online.tmax - v) for v in grid.values() if v is not None)
    assert gap < 4.0
