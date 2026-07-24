"""Tests for location parsing."""

import pytest

from get_weather_data.weather.location import parse_location


class TestParseLocation:
    """ZIP strings, coordinate strings, and tuples."""

    def test_zip(self):
        assert parse_location("10001") == "10001"

    def test_zip_zero_fill(self):
        assert parse_location("7001") == "07001"

    def test_zip_whitespace(self):
        assert parse_location("  10001 ") == "10001"

    def test_coord_string(self):
        assert parse_location("40.75,-73.99") == (40.75, -73.99)

    def test_coord_string_with_space(self):
        assert parse_location("40.75, -73.99") == (40.75, -73.99)

    def test_coord_tuple(self):
        assert parse_location((40.75, -73.99)) == (40.75, -73.99)

    def test_latitude_out_of_range(self):
        with pytest.raises(ValueError, match="Latitude"):
            parse_location((91.0, 0.0))

    def test_longitude_out_of_range(self):
        with pytest.raises(ValueError, match="Longitude"):
            parse_location("40.75,-181")

    def test_garbage(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_location("new york")

    def test_too_many_parts(self):
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_location("1,2,3")
