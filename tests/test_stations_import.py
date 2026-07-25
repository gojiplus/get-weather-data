"""Tests for the station/ZIP import wrappers (download stubbed)."""

import pytest

from get_weather_data.core.config import Config, set_config
from get_weather_data.core.database import Database
from get_weather_data.stations import ghcnd, isd, zipcodes


@pytest.fixture
def db(tmp_path) -> Database:
    set_config(Config(ncdc_token=None, data_dir=tmp_path, cache_dir=tmp_path))
    d = Database(tmp_path / "s.sqlite")
    d.init_schema()
    return d


def _ghcnd_line(sid, lat, lon, elev, state, name):
    line = list(" " * 85)
    line[0:11] = sid.ljust(11)
    line[12:20] = f"{lat:>8.4f}"
    line[21:30] = f"{lon:>9.4f}"
    line[31:37] = f"{elev:>6.1f}"
    line[38:40] = state.ljust(2)
    line[41 : 41 + len(name)] = name
    return "".join(line).rstrip() + "\n"


def test_import_ghcnd(db, tmp_path, monkeypatch):
    fixture = tmp_path / "ghcnd.txt"
    fixture.write_text(
        _ghcnd_line("USW00094728", 40.78, -73.97, 42.7, "NY", "CENTRAL PARK")
        + _ghcnd_line("USC00011084", 31.06, -87.05, 47.2, "AL", "BREWTON")
    )
    monkeypatch.setattr(ghcnd, "download_ghcnd_stations", lambda *a, **k: fixture)
    count = ghcnd.import_ghcnd_stations(db)
    assert count == 2
    assert db.count_stations("GHCND") == 2


def test_import_isd(db, tmp_path, monkeypatch):
    fixture = tmp_path / "isd.csv"
    fixture.write_text(
        "USAF,WBAN,STATION NAME,CTRY,ST,ICAO,LAT,LON,ELEV(M),BEGIN,END\n"
        "725030,14732,LAGUARDIA,US,NY,KLGA,40.779,-73.880,3.4,1973,2026\n"
    )
    monkeypatch.setattr(isd, "download_isd_stations", lambda *a, **k: fixture)
    count = isd.import_isd_stations(db)
    assert count == 1
    assert db.count_stations("USAF-WBAN") == 1


def test_import_zipcodes(db, tmp_path, monkeypatch):
    cols = [
        "US",
        "10001",
        "New York",
        "New York",
        "NY",
        "New York",
        "061",
        "",
        "",
        "40.75",
        "-73.99",
        "4",
    ]
    fixture = tmp_path / "US.txt"
    fixture.write_text("\t".join(cols) + "\n")
    monkeypatch.setattr(zipcodes, "download_zipcodes", lambda *a, **k: fixture)
    count = zipcodes.import_zipcodes(db)
    assert count == 1
    assert db.count_zipcodes() == 1
    assert db.get_zipcode("10001") == (40.75, -73.99)
