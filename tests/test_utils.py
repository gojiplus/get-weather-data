"""Tests for utility functions."""

from get_weather_data.core.distance import meters_distance


class TestMetersDistance:
    """Tests for meters_distance function."""

    def test_same_point_returns_zero(self):
        """Distance between a point and itself should be zero."""
        result = meters_distance(40.7, -74.0, 40.7, -74.0)
        assert result == 0.0

    def test_known_distance_nyc_to_la(self):
        """Test approximate distance between NYC and LA."""
        nyc_lat, nyc_lon = 40.7128, -74.0060
        la_lat, la_lon = 34.0522, -118.2437
        result = meters_distance(nyc_lat, nyc_lon, la_lat, la_lon)
        assert 3900000 < result < 4100000

    def test_short_distance(self):
        """Test a short distance between nearby points."""
        lat1, lon1 = 40.7128, -74.0060
        lat2, lon2 = 40.7138, -74.0070
        result = meters_distance(lat1, lon1, lat2, lon2)
        assert 100 < result < 200

    def test_symmetry(self):
        """Distance should be symmetric."""
        lat1, lon1 = 40.7128, -74.0060
        lat2, lon2 = 34.0522, -118.2437
        d1 = meters_distance(lat1, lon1, lat2, lon2)
        d2 = meters_distance(lat2, lon2, lat1, lon1)
        assert abs(d1 - d2) < 1


def f2c(f: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return (f - 32) * 5.0 / 9.0


def kn2ms(kn: float) -> float:
    """Convert Knots to m/s."""
    return 0.51444 * kn


class TestTemperatureConversion:
    """Tests for temperature conversion functions."""

    def test_f2c_freezing(self):
        """32F should equal 0C."""
        result = f2c(32.0)
        assert result == 0.0

    def test_f2c_boiling(self):
        """212F should equal 100C."""
        result = f2c(212.0)
        assert abs(result - 100.0) < 0.01


class TestKnotConversion:
    """Tests for knot to m/s conversion."""

    def test_kn2ms_one_knot(self):
        """1 knot should be approximately 0.51444 m/s."""
        result = kn2ms(1.0)
        assert abs(result - 0.51444) < 0.0001

    def test_kn2ms_zero(self):
        """0 knots should be 0 m/s."""
        result = kn2ms(0.0)
        assert result == 0.0
