"""Tests for the nClimGrid gridded backend (synthetic netCDF, no network)."""

from datetime import date

import numpy as np
import pytest

pytest.importorskip("xarray")
import xarray as xr

from get_weather_data.weather.gridded import (
    GRID_VARS,
    OPENDAP_URL,
    GriddedLookup,
    _iter_months,
    in_conus,
)

LAT = np.array([40.0, 40.5, 41.0])
LON = np.array([-74.0, -73.5, -73.0])


def _month_dataset(year: int, month: int) -> xr.Dataset:
    """A small nClimGrid-shaped month where tmax encodes the cell + day."""
    if month == 12:
        n_days = 31
    else:
        n_days = (date(year, month + 1, 1) - date(year, month, 1)).days
    times = np.array(
        [np.datetime64(f"{year}-{month:02d}-{d:02d}") for d in range(1, n_days + 1)]
    )
    shape = (n_days, len(LAT), len(LON))
    # tmax[t,i,j] = 100*i + 10*j + day  -> lets us assert the chosen cell
    tmax = np.zeros(shape)
    for t in range(n_days):
        for i in range(len(LAT)):
            for j in range(len(LON)):
                tmax[t, i, j] = 100 * i + 10 * j + (t + 1)
    data = {
        "tmax": (("time", "lat", "lon"), tmax),
        "tmin": (("time", "lat", "lon"), tmax - 5.0),
        "tavg": (("time", "lat", "lon"), tmax - 2.0),
        "prcp": (("time", "lat", "lon"), np.zeros(shape)),
    }
    return xr.Dataset(data, coords={"time": times, "lat": LAT, "lon": LON})


def _lookup(**kwargs) -> GriddedLookup:
    kwargs.setdefault("dataset_opener", lambda y, m: _month_dataset(y, m))
    kwargs.setdefault("zip_coordinates_loader", lambda: {"10001": (40.4, -73.6)})
    return GriddedLookup(**kwargs)


class TestHelpers:
    def test_url_pattern(self):
        url = OPENDAP_URL.format(year=2024, month=1)
        assert url.endswith("/2024/ncdd-202401-grd-scaled.nc")

    def test_conus_bounds(self):
        assert in_conus(40.0, -100.0)
        assert not in_conus(61.0, -150.0)  # Alaska
        assert not in_conus(21.3, -157.8)  # Hawaii

    def test_iter_months_across_year(self):
        months = list(_iter_months(date(2023, 11, 5), date(2024, 2, 3)))
        assert months == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]


class TestGriddedLookup:
    def test_selects_nearest_cell(self):
        # query (40.4, -73.6): nearest lat=40.5 (i=1), lon=-73.5 (j=1)
        result = _lookup().get_weather((40.4, -73.6), date(2024, 1, 15))
        assert result.station_type == "gridded"
        assert result.tmax == 100 * 1 + 10 * 1 + 15  # 125
        assert result.tmin == 125 - 5
        assert result.tavg == 125 - 2

    def test_zip_resolves(self):
        result = _lookup().get_weather("10001", date(2024, 1, 15))
        assert result.tmax == 125
        assert result.zipcode == "10001"

    def test_range_spans_month(self):
        results = _lookup().get_weather_range(
            (40.4, -73.6), date(2024, 1, 14), date(2024, 1, 16)
        )
        assert [r.tmax for r in results] == [124, 125, 126]

    def test_range_across_month_boundary(self):
        results = _lookup().get_weather_range(
            (40.4, -73.6), date(2024, 1, 31), date(2024, 2, 2)
        )
        assert [r.date.day for r in results] == [31, 1, 2]
        assert results[0].tmax == 100 + 10 + 31  # Jan 31
        assert results[1].tmax == 100 + 10 + 1  # Feb 1

    def test_out_of_conus_returns_bare(self):
        result = _lookup().get_weather((61.2, -149.9), date(2024, 1, 15))  # Anchorage
        assert result.tmax is None
        assert result.station_type is None

    def test_non_grid_elements_are_none(self):
        result = _lookup().get_weather((40.4, -73.6), date(2024, 1, 15))
        # grid has no wind/snow
        assert result.awnd is None
        assert result.snow is None

    def test_imperial_units(self):
        lookup = _lookup(units="imperial")
        result = lookup.get_weather((40.4, -73.6), date(2024, 1, 15))
        # 125 °C -> °F
        assert result.tmax == pytest.approx(125 * 9 / 5 + 32)

    def test_only_grid_elements_requested(self):
        # requesting a non-grid element yields no grid fetch, bare result
        result = _lookup().get_weather((40.4, -73.6), date(2024, 1, 15), ["AWND"])
        assert result.tmax is None
        assert result.awnd is None

    def test_missing_month_dataset(self):
        lookup = _lookup(dataset_opener=lambda y, m: None)
        result = lookup.get_weather((40.4, -73.6), date(2024, 1, 15))
        assert result.tmax is None


def test_grid_vars_cover_core_elements():
    assert set(GRID_VARS) == {"TMAX", "TMIN", "TAVG", "PRCP"}
