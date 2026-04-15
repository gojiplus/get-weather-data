"""Tests for database operations."""


from get_weather_data.core.database import Database
from get_weather_data.core.distance import Station


class TestDatabase:
    """Tests for Database class."""

    def test_init_schema(self, tmp_path):
        """Test database schema initialization."""
        db = Database(tmp_path / "test.sqlite")
        db.init_schema()

        assert db.exists()
        assert db.count_stations() == 0
        assert db.count_zipcodes() == 0

    def test_insert_station(self, temp_db):
        """Test inserting a station."""
        station = Station(
            id="USC00011084",
            name="BREWTON",
            state="AL",
            lat=31.0581,
            lon=-87.0547,
            elevation=47.2,
            type="GHCND",
        )
        temp_db.insert_station(station)

        assert temp_db.count_stations() == 1
        assert temp_db.count_stations("GHCND") == 1

    def test_insert_stations_bulk(self, temp_db, sample_stations):
        """Test bulk station insert."""
        temp_db.insert_stations_bulk(sample_stations)

        assert temp_db.count_stations() == 3
        assert temp_db.count_stations("GHCND") == 2
        assert temp_db.count_stations("USAF-WBAN") == 1

    def test_get_stations(self, temp_db, sample_stations):
        """Test getting stations."""
        temp_db.insert_stations_bulk(sample_stations)

        all_stations = temp_db.get_stations()
        assert len(all_stations) == 3

        ghcnd_stations = temp_db.get_stations(station_type="GHCND")
        assert len(ghcnd_stations) == 2

    def test_insert_zipcode(self, temp_db):
        """Test inserting a ZIP code."""
        temp_db.insert_zipcode(
            zipcode="10001",
            city="New York",
            state="NY",
            lat=40.7484,
            lon=-73.9967,
        )

        assert temp_db.count_zipcodes() == 1

    def test_get_zipcode(self, temp_db):
        """Test getting a ZIP code."""
        temp_db.insert_zipcode(
            zipcode="10001",
            city="New York",
            state="NY",
            lat=40.7484,
            lon=-73.9967,
        )

        result = temp_db.get_zipcode("10001")
        assert result is not None
        lat, lon = result
        assert abs(lat - 40.7484) < 0.0001
        assert abs(lon - (-73.9967)) < 0.0001

    def test_get_zipcode_not_found(self, temp_db):
        """Test getting a non-existent ZIP code."""
        result = temp_db.get_zipcode("99999")
        assert result is None

    def test_closest_stations(self, temp_db, sample_stations):
        """Test closest stations caching."""
        temp_db.insert_stations_bulk(sample_stations)
        temp_db.insert_zipcode(
            zipcode="36420",
            city="Brewton",
            state="AL",
            lat=31.0581,
            lon=-87.0547,
        )

        temp_db.set_closest_stations(
            "36420",
            [("USC00011084", 100), ("USW00013894", 50000)],
        )

        closest = temp_db.get_closest_stations("36420")
        assert len(closest) == 2
        assert closest[0] == ("USC00011084", 100)
        assert closest[1] == ("USW00013894", 50000)
