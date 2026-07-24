"""Tests for the NOAA CDO v2 client and online lookup (all mocked)."""

from datetime import date

import pytest
import respx
from httpx import Response

from get_weather_data.api.noaa import (
    CDO_BASE_URL,
    NOAAAPIError,
    NOAAClient,
    StationInfo,
)
from get_weather_data.core.config import Config, set_config
from get_weather_data.main import Weather
from get_weather_data.weather.online import OnlineLookup

DATA_URL = f"{CDO_BASE_URL}/data"
STATION = "GHCND:USW00094728"
DAY = "2024-01-15T00:00:00"


@pytest.fixture(autouse=True)
def _reset_config(monkeypatch, tmp_path):
    """Isolate the global config (and any NCDC_TOKEN from the host env)."""
    monkeypatch.delenv("NCDC_TOKEN", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    set_config(Config(ncdc_token=None, data_dir=tmp_path, cache_dir=tmp_path))
    yield
    set_config(Config(ncdc_token=None, data_dir=tmp_path, cache_dir=tmp_path))


def _client(**kwargs) -> NOAAClient:
    """Client with test token and no sleeping."""
    kwargs.setdefault("token", "test-token")
    kwargs.setdefault("retry_delay", 0.0)
    kwargs.setdefault("min_request_interval", 0.0)
    return NOAAClient(**kwargs)


def _record(
    datatype: str,
    value: float,
    station: str = STATION,
    day: str = DAY,
) -> dict:
    return {
        "date": day,
        "datatype": datatype,
        "station": station,
        "attributes": ",,W,2400",
        "value": value,
    }


def _data_payload(results: list[dict], count: int | None = None) -> dict:
    return {
        "metadata": {
            "resultset": {
                "offset": 1,
                "count": count if count is not None else len(results),
                "limit": 1000,
            }
        },
        "results": results,
    }


def _station_payload(
    station_id: str = STATION, name: str = "NY CITY CENTRAL PARK"
) -> dict:
    return {
        "id": station_id,
        "name": name,
        "latitude": 40.77898,
        "longitude": -73.96925,
        "elevation": 42.7,
        "mindate": "1869-01-01",
        "maxdate": "2024-06-01",
        "datacoverage": 1,
        "elevationUnit": "METERS",
    }


class TestNOAAClientAuth:
    """Token handling."""

    def test_missing_token_raises(self):
        with pytest.raises(ValueError, match="cdo-web/token"):
            NOAAClient()

    @respx.mock
    def test_token_sent_as_header(self):
        route = respx.get(DATA_URL).mock(
            return_value=Response(200, json=_data_payload([]))
        )
        _client().get_data("10001", date(2024, 1, 15), date(2024, 1, 15))
        assert route.calls.last.request.headers["token"] == "test-token"


class TestNOAAClientRequest:
    """Retry and error behavior."""

    @respx.mock
    def test_rate_limit_then_success(self):
        respx.get(DATA_URL).mock(
            side_effect=[
                Response(429),
                Response(200, json=_data_payload([_record("TMAX", 39)])),
            ]
        )
        records = _client().get_data("10001", date(2024, 1, 15), date(2024, 1, 15))
        assert records[0]["value"] == 39

    @respx.mock
    def test_retries_exhausted(self):
        respx.get(DATA_URL).mock(return_value=Response(429))
        with pytest.raises(NOAAAPIError, match="failed after 3 attempts"):
            _client(max_retries=2).get_data(
                "10001", date(2024, 1, 15), date(2024, 1, 15)
            )

    @respx.mock
    def test_bad_token_no_retry(self):
        route = respx.get(DATA_URL).mock(return_value=Response(401))
        with pytest.raises(NOAAAPIError, match="rejected the token"):
            _client().get_data("10001", date(2024, 1, 15), date(2024, 1, 15))
        assert route.call_count == 1

    @respx.mock
    def test_empty_body_means_no_results(self):
        respx.get(DATA_URL).mock(return_value=Response(200, content=b""))
        records = _client().get_data("99999", date(2024, 1, 15), date(2024, 1, 15))
        assert records == []


class TestNOAAClientPagination:
    """Offset pagination on /data."""

    @respx.mock
    def test_two_pages(self):
        page1 = _data_payload([_record("TMAX", 39)], count=1500)
        page1["results"] = [_record("TMAX", float(i)) for i in range(1000)]
        page2 = _data_payload(
            [_record("TMIN", float(i)) for i in range(500)], count=1500
        )
        route = respx.get(DATA_URL).mock(
            side_effect=[Response(200, json=page1), Response(200, json=page2)]
        )
        records = _client().get_data("10001", date(2024, 1, 15), date(2024, 1, 15))
        assert len(records) == 1500
        assert route.call_count == 2
        second = route.calls[1].request.url
        assert second.params["offset"] == "1001"


class TestNOAAClientStation:
    """Station metadata endpoint."""

    @respx.mock
    def test_station_parsed(self):
        respx.get(f"{CDO_BASE_URL}/stations/{STATION}").mock(
            return_value=Response(200, json=_station_payload())
        )
        info = _client().get_station(STATION)
        assert info == StationInfo(
            id=STATION,
            name="NY CITY CENTRAL PARK",
            latitude=40.77898,
            longitude=-73.96925,
            elevation=42.7,
        )

    @respx.mock
    def test_unknown_station_is_none(self):
        respx.get(f"{CDO_BASE_URL}/stations/GHCND:NOPE").mock(
            return_value=Response(404)
        )
        assert _client().get_station("GHCND:NOPE") is None


class TestOnlineLookup:
    """ZIP-level lookup built on the client."""

    def _lookup(self, **kwargs) -> OnlineLookup:
        kwargs.setdefault("client", _client())
        kwargs.setdefault(
            "zip_coordinates_loader", lambda: {"10001": (40.7484, -73.9967)}
        )
        return OnlineLookup(**kwargs)

    def _mock_stations(self, stations: list[dict]) -> respx.Route:
        return respx.get(f"{CDO_BASE_URL}/stations").mock(
            return_value=Response(
                200,
                json={
                    "metadata": {
                        "resultset": {
                            "offset": 1,
                            "count": len(stations),
                            "limit": 1000,
                        }
                    },
                    "results": stations,
                },
            )
        )

    @staticmethod
    def _station_entry(station_id: str, name: str, lat: float, lon: float) -> dict:
        return {
            "id": station_id,
            "name": name,
            "latitude": lat,
            "longitude": lon,
            "elevation": 40.0,
        }

    @respx.mock
    def test_nearest_station_with_data_wins(self):
        near = self._station_entry(STATION, "NY CITY CENTRAL PARK", 40.78, -73.97)
        far = self._station_entry(
            "GHCND:USW00014732", "LAGUARDIA AIRPORT", 40.78, -73.88
        )
        self._mock_stations([far, near])
        respx.get(DATA_URL).mock(
            return_value=Response(
                200,
                json=_data_payload(
                    [
                        _record("TMAX", -16, station="GHCND:USW00014732"),
                        _record("TMAX", -10),
                        _record("TMIN", -43),
                    ]
                ),
            )
        )
        result = self._lookup().get_weather("10001", date(2024, 1, 15))
        assert result.station_id == "USW00094728"
        assert result.station_name == "NY CITY CENTRAL PARK"
        assert result.station_type == "GHCND"
        assert result.station_distance_meters is not None
        assert result.station_distance_meters > 0
        assert result.units == "metric"
        assert result.tmax == -1.0  # raw tenths -> deg C
        assert result.tmin == -4.3

    @respx.mock
    def test_missing_elements_filled_from_farther_stations(self):
        near = self._station_entry(STATION, "NY CITY CENTRAL PARK", 40.78, -73.97)
        far = self._station_entry(
            "GHCND:USW00014732", "LAGUARDIA AIRPORT", 40.78, -73.88
        )
        self._mock_stations([near, far])
        respx.get(DATA_URL).mock(
            return_value=Response(
                200,
                json=_data_payload(
                    [
                        _record("TMAX", -10),
                        _record("SNOW", 25, station="GHCND:USW00014732"),
                    ]
                ),
            )
        )
        result = self._lookup().get_weather("10001", date(2024, 1, 15))
        assert result.station_id == "USW00094728"
        assert result.snow == 25

    @respx.mock
    def test_unknown_zip_returns_bare_result(self):
        result = self._lookup().get_weather("99999", date(2024, 1, 15))
        assert result.zipcode == "99999"
        assert result.station_id is None
        assert result.tmax is None

    @respx.mock
    def test_no_stations_near_zip(self):
        self._mock_stations([])
        result = self._lookup().get_weather("10001", date(2024, 1, 15))
        assert result.station_id is None
        assert result.tmax is None

    @respx.mock
    def test_range_uses_one_data_call(self):
        near = self._station_entry(STATION, "NY CITY CENTRAL PARK", 40.78, -73.97)
        stations_route = self._mock_stations([near])
        days = ["2024-01-15T00:00:00", "2024-01-16T00:00:00", "2024-01-17T00:00:00"]
        data_route = respx.get(DATA_URL).mock(
            return_value=Response(
                200,
                json=_data_payload(
                    [_record("TMAX", 30 + i, day=d) for i, d in enumerate(days)]
                ),
            )
        )
        results = self._lookup().get_weather_range(
            "10001", date(2024, 1, 15), date(2024, 1, 17)
        )
        assert stations_route.call_count == 1
        assert data_route.call_count == 1
        assert [r.tmax for r in results] == [3.0, 3.1, 3.2]
        assert [r.date.day for r in results] == [15, 16, 17]

    @respx.mock
    def test_range_chunks_at_year_boundary(self):
        near = self._station_entry(STATION, "NY CITY CENTRAL PARK", 40.78, -73.97)
        self._mock_stations([near])
        data_route = respx.get(DATA_URL).mock(
            return_value=Response(200, json=_data_payload([]))
        )
        self._lookup().get_weather_range("10001", date(2023, 12, 30), date(2024, 1, 2))
        assert data_route.call_count == 2
        first = data_route.calls[0].request.url.params
        second = data_route.calls[1].request.url.params
        assert first["enddate"] == "2023-12-31"
        assert second["startdate"] == "2024-01-01"


class TestWeatherOnline:
    """End-to-end through the Weather facade."""

    @respx.mock
    def test_get_online(self, monkeypatch):
        monkeypatch.setenv("NCDC_TOKEN", "test-token")
        set_config(Config())
        respx.get(f"{CDO_BASE_URL}/stations").mock(
            return_value=Response(
                200,
                json={
                    "metadata": {"resultset": {"offset": 1, "count": 1, "limit": 1000}},
                    "results": [
                        {
                            "id": STATION,
                            "name": "NY CITY CENTRAL PARK",
                            "latitude": 40.78,
                            "longitude": -73.97,
                        }
                    ],
                },
            )
        )
        respx.get(DATA_URL).mock(
            return_value=Response(200, json=_data_payload([_record("TMAX", 44)]))
        )
        weather = Weather(online=True)
        weather._online_lookup = OnlineLookup(
            client=_client(),
            zip_coordinates_loader=lambda: {"10001": (40.7484, -73.9967)},
        )
        result = weather.get("10001", "2024-01-15")
        assert result.tmax == 4.4
        assert result.station_name == "NY CITY CENTRAL PARK"

    def test_online_without_token_raises(self):
        with pytest.raises(ValueError, match="cdo-web/token"):
            Weather(online=True)

    def test_process_csv_online_raises(self, monkeypatch, tmp_path):
        monkeypatch.setenv("NCDC_TOKEN", "test-token")
        set_config(Config())
        weather = Weather(online=True)
        with pytest.raises(ValueError, match="local database"):
            weather.process_csv(tmp_path / "in.csv", tmp_path / "out.csv")
