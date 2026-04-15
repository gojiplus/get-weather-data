"""Shared test fixtures."""

import pytest

from get_weather_data.core.database import Database
from get_weather_data.core.distance import Station


@pytest.fixture
def sample_stations() -> list[Station]:
    """Sample station data for testing."""
    return [
        Station(
            id="USC00011084",
            name="BREWTON",
            state="AL",
            lat=31.0581,
            lon=-87.0547,
            elevation=47.2,
            type="GHCND",
        ),
        Station(
            id="USW00013894",
            name="MOBILE",
            state="AL",
            lat=30.6833,
            lon=-88.2500,
            elevation=67.0,
            type="USAF-WBAN",
        ),
        Station(
            id="USC00016988",
            name="SELMA",
            state="AL",
            lat=32.4069,
            lon=-87.0214,
            elevation=42.7,
            type="GHCND",
        ),
    ]


@pytest.fixture
def temp_db(tmp_path) -> Database:
    """Create a temporary database."""
    db = Database(tmp_path / "test.sqlite")
    db.init_schema()
    return db


@pytest.fixture
def sample_zip_data() -> list[dict[str, str | float]]:
    """Sample ZIP code data for testing."""
    return [
        {
            "zipcode": "10001",
            "city": "New York",
            "state": "NY",
            "lat": 40.7484,
            "lon": -73.9967,
        },
        {
            "zipcode": "90210",
            "city": "Beverly Hills",
            "state": "CA",
            "lat": 34.0901,
            "lon": -118.4065,
        },
    ]
