"""Weather station to ZIP lookup."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.geocoders import Nominatim


def slices(s: str, *args: int) -> Any:
    """Yield slices of a string."""
    position = 0
    for length in args:
        yield s[position : position + length]
        position += length


def get_zip_from_coords(lat: str, lng: str) -> list[str]:
    """Get postal codes near the given coordinates using geopy."""
    try:
        geolocator = Nominatim(user_agent="get-weather-data")
        location = geolocator.reverse(f"{lat}, {lng}", exactly_one=True)

        if location and location.raw.get("address"):
            postal_code = location.raw["address"].get("postcode", "")
            if postal_code:
                return [postal_code]
        return []
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logging.warning(f"Geocoding failed for ({lat}, {lng}): {e}")
        return []


def load_save_csvfile(infilename: str, outfilename: str, source: str = "ghcnd") -> None:
    """Load station data and save with ZIP codes."""
    reader = open(infilename, "r", encoding="utf-8")
    total_rows = 0
    try:
        writer = open(outfilename, "r+", encoding="utf-8")
        for _ in writer:
            total_rows += 1
    except IOError:
        writer = open(outfilename, "w", encoding="utf-8")

    if source == "ghcnd":
        prog = re.compile(".{11}(.{9})(.{10})")
    elif source == "coop":
        prog = re.compile(".{191}(.{16})(.{16})")
    else:
        prog = re.compile(".{11}(.{9})(.{10})")

    j = -1
    for row in reader:
        j += 1
        if j < total_rows:
            continue

        match = prog.match(row)
        if match is None:
            continue
        lat, lng = match.group(1, 2)

        lat = lat.strip()
        lng = lng.strip()

        result = get_zip_from_coords(lat, lng)

        out = row.rstrip("\n")
        for postal_code in result:
            out = out + f"{postal_code:>10}"

        if result:
            out += f"{0.0:>15}"
        out += "\n"
        out = f"{source:<20}" + out
        print(out)
        writer.write(out)
        time.sleep(1)

    reader.close()
    writer.close()


if __name__ == "__main__":
    load_save_csvfile("coop-stations.txt", "coop-stations-out.txt", source="coop")
