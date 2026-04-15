"""Shared test fixtures."""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_stations():
    """Sample station data for testing."""
    return [
        (1, "USC00011084", "BREWTON", 31.0581, -87.0547, 47.2, "GHCND"),
        (2, "USW00013894", "MOBILE", 30.6833, -88.2500, 67.0, "USAF-WBAN"),
        (3, "USC00016988", "SELMA", 32.4069, -87.0214, 42.7, "GHCND"),
    ]


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database path."""
    return tmp_path / "test.sqlite"


@pytest.fixture
def sample_zip_data():
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
