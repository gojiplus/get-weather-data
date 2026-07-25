"""Tests for station-list and ZIP parsers (fixture text, no network)."""

from get_weather_data.stations import ghcnd, isd, zipcodes


def _ghcnd_line(sid, lat, lon, elev, state, name):
    # Fixed-width: id[0:11] lat[12:20] lon[21:30] elev[31:37] state[38:40] name[41:71]
    line = list(" " * 85)
    line[0:11] = sid.ljust(11)
    line[12:20] = f"{lat:>8.4f}"
    line[21:30] = f"{lon:>9.4f}"
    line[31:37] = f"{elev:>6.1f}"
    line[38:40] = state.ljust(2)
    line[41 : 41 + len(name)] = name
    return "".join(line).rstrip() + "\n"


class TestParseGhcnd:
    def test_country_filter_and_columns(self, tmp_path):
        path = tmp_path / "ghcnd-stations.txt"
        path.write_text(
            _ghcnd_line("USW00094728", 40.7789, -73.9692, 42.7, "NY", "NY CENTRAL PARK")
            + _ghcnd_line("CA001108447", 49.0, -122.0, 5.0, "BC", "VANCOUVER")
            + _ghcnd_line("MX000076680", 19.4, -99.1, 2309.0, "", "MEXICO CITY")
            + _ghcnd_line("ASN00008290", -31.0, 121.0, 400.0, "", "PERTH")  # dropped
        )
        stations = ghcnd.parse_ghcnd_stations(path)
        ids = {s.id for s in stations}
        assert ids == {"USW00094728", "CA001108447", "MX000076680"}
        nyc = next(s for s in stations if s.id == "USW00094728")
        assert nyc.lat == 40.7789
        assert nyc.lon == -73.9692
        assert nyc.state == "NY"
        assert nyc.name == "NY CENTRAL PARK"
        assert nyc.type == "GHCND"

    def test_malformed_line_skipped(self, tmp_path):
        path = tmp_path / "s.txt"
        good = _ghcnd_line("USW00094728", 40.0, -73.0, 10.0, "NY", "OK")
        bad = "US_BAD_ID  not-a-number here too    XX  BROKEN\n"
        path.write_text(good + bad)
        stations = ghcnd.parse_ghcnd_stations(path)
        assert [s.id for s in stations] == ["USW00094728"]


class TestParseIsd:
    HEADER = "USAF,WBAN,STATION NAME,CTRY,ST,ICAO,LAT,LON,ELEV(M),BEGIN,END\n"

    def test_country_filter_id_and_elevation(self, tmp_path):
        path = tmp_path / "isd-history.csv"
        path.write_text(
            self.HEADER
            + "725030,14732,LAGUARDIA,US,NY,KLGA,40.779,-73.880,3.4,1973,2026\n"
            + "710930,99999,VANCOUVER INTL,CA,BC,CYVR,49.195,-123.182,4.3,1955,2026\n"
            + "037720,99999,LONDON,GB,,EGLL,51.478,-0.461,25.3,1948,2026\n"  # dropped
            + "999999,99999,NO COORDS,US,,,,,,,\n"  # blank lat/lon -> skipped
        )
        stations = isd.parse_isd_stations(path)
        ids = {s.id for s in stations}
        assert ids == {"725030-14732", "710930-99999"}
        lga = next(s for s in stations if s.id == "725030-14732")
        assert lga.type == "USAF-WBAN"
        assert lga.elevation == 3.4

    def test_blank_elevation_is_none(self, tmp_path):
        path = tmp_path / "isd.csv"
        path.write_text(
            self.HEADER + "725030,14732,X,US,NY,KLGA,40.0,-73.0,,1973,2026\n"
        )
        stations = isd.parse_isd_stations(path)
        assert stations[0].elevation is None


class TestParseZipcodes:
    def _line(self, zc, city, state, county, lat, lon):
        # 12 tab-separated columns (GeoNames US.txt); lat=col 9, lon=col 10
        cols = [
            "US",
            zc,
            city,
            "New York",
            state,
            county,
            "061",
            "",
            "",
            str(lat),
            str(lon),
            "4",
        ]
        return "\t".join(cols) + "\n"

    def test_parses_fields(self, tmp_path):
        path = tmp_path / "US.txt"
        path.write_text(
            self._line("10001", "New York", "NY", "New York", 40.7484, -73.9967)
            + self._line("90210", "Beverly Hills", "CA", "Los Angeles", 34.09, -118.4)
            + "US\t99999\tShort\n"  # too few columns -> skipped
        )
        rows = zipcodes.parse_zipcodes(path)
        by_zip = {r["zipcode"]: r for r in rows}
        assert set(by_zip) == {"10001", "90210"}
        assert by_zip["10001"]["lat"] == 40.7484
        assert by_zip["10001"]["state"] == "NY"

    def test_zip_centroids(self, tmp_path, monkeypatch):
        path = tmp_path / "US.txt"
        path.write_text(self._line("10001", "NYC", "NY", "New York", 40.75, -73.99))
        monkeypatch.setattr(zipcodes, "download_zipcodes", lambda *a, **k: path)
        centroids = zipcodes.zip_centroids()
        assert centroids == {"10001": (40.75, -73.99)}
