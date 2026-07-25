"""Tests for coverage reporting."""

from datetime import date

from get_weather_data.weather.results import WeatherResult, summarize_coverage


def _day(offset: int, **values) -> WeatherResult:
    return WeatherResult(date=date(2024, 1, 1 + offset), **values)


class TestSummarizeCoverage:
    def test_fractions_and_station(self):
        results = [
            _day(0, station_id="A", station_distance_meters=1000, tmax=1.0, prcp=0.0),
            _day(1, station_id="A", station_distance_meters=1000, tmax=2.0),
            _day(2, station_id="B", station_distance_meters=9000, tmax=3.0),
            _day(3),  # no data
        ]
        cov = summarize_coverage(results)

        assert cov.total_days == 4
        assert cov.fraction("tmax") == 0.75
        assert cov.fraction("prcp") == 0.25
        assert cov.fraction("snow") == 0.0
        # station credited on the most days
        assert cov.station_id == "A"
        assert cov.station_distance_meters == 1000

    def test_empty_range(self):
        cov = summarize_coverage([])
        assert cov.total_days == 0
        assert cov.fraction("tmax") == 0.0
        assert cov.station_id is None
