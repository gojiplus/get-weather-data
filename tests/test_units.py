"""Tests for the element registry and unit conversions."""

import pytest

from get_weather_data.weather.units import (
    ELEMENTS,
    convert,
    ghcn_raw_to_metric,
    normalize_elements,
    unit_label,
)


class TestRawToMetric:
    """GHCN raw scales differ by element."""

    def test_temperature_is_tenths(self):
        assert ghcn_raw_to_metric("TMAX", 283) == 28.3
        assert ghcn_raw_to_metric("TMIN", -43) == -4.3

    def test_precipitation_is_tenths_mm(self):
        assert ghcn_raw_to_metric("PRCP", 127) == 12.7

    def test_snow_is_whole_mm(self):
        # GHCN stores SNOW and SNWD in whole millimeters, not tenths
        assert ghcn_raw_to_metric("SNOW", 25) == 25.0
        assert ghcn_raw_to_metric("SNWD", 150) == 150.0

    def test_wind_is_tenths(self):
        assert ghcn_raw_to_metric("AWND", 23) == 2.3

    def test_zero_survives(self):
        assert ghcn_raw_to_metric("TMAX", 0) == 0.0
        assert ghcn_raw_to_metric("PRCP", 0) == 0.0


class TestImperial:
    """Metric -> imperial conversions."""

    def test_temperature(self):
        assert convert("TMAX", 0.0, "imperial") == 32.0
        assert convert("TMAX", 100.0, "imperial") == 212.0

    def test_precipitation(self):
        assert convert("PRCP", 25.4, "imperial") == pytest.approx(1.0)

    def test_wind(self):
        assert convert("AWND", 10.0, "imperial") == pytest.approx(22.37, abs=0.01)

    def test_metric_passthrough(self):
        assert convert("TMAX", -4.3, "metric") == -4.3

    def test_labels(self):
        assert unit_label("TMAX", "metric") == "°C"
        assert unit_label("TMAX", "imperial") == "°F"
        assert unit_label("SNOW", "imperial") == "in"
        assert unit_label("AWND", "imperial") == "mph"


class TestNormalizeElements:
    """Element validation."""

    def test_default_is_all(self):
        assert normalize_elements(None) == list(ELEMENTS)

    def test_case_insensitive(self):
        assert normalize_elements(["tmax", "Prcp"]) == ["TMAX", "PRCP"]

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown weather element"):
            normalize_elements(["TMAX", "BOGUS"])
