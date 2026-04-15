"""Build closest stations index for ZIP codes."""

import logging

from get_weather_data.core.config import get_config
from get_weather_data.core.database import Database
from get_weather_data.core.distance import find_closest

logger = logging.getLogger("get_weather_data")


def build_closest_index(
    db: Database | None = None,
    ghcn_count: int | None = None,
    usaf_count: int | None = None,
) -> int:
    """Build index of closest stations for each ZIP code.

    Args:
        db: Database instance. Uses default if None.
        ghcn_count: Number of GHCND stations per ZIP. Uses config default if None.
        usaf_count: Number of USAF stations per ZIP. Uses config default if None.

    Returns:
        Number of ZIP codes processed.
    """
    if db is None:
        db = Database()

    config = get_config()
    if ghcn_count is None:
        ghcn_count = config.ghcn_station_count
    if usaf_count is None:
        usaf_count = config.usaf_station_count

    # Load all stations
    ghcn_stations = db.get_stations(station_type="GHCND")
    usaf_stations = db.get_stations(station_type="USAF-WBAN")

    logger.info(
        f"Loaded {len(ghcn_stations)} GHCND, {len(usaf_stations)} USAF stations"
    )

    # Get all ZIP codes
    zipcodes = db.execute(
        "SELECT zipcode, lat, lon FROM zipcodes WHERE lat IS NOT NULL"
    )

    processed = 0
    total = len(zipcodes)

    for zipcode, lat, lon in zipcodes:
        if lat is None or lon is None:
            continue

        closest_stations = []

        # Find closest GHCND stations
        if ghcn_count > 0:
            ghcn_closest = find_closest(lat, lon, ghcn_stations, n=ghcn_count)
            for sd in ghcn_closest:
                closest_stations.append((sd.station.id, sd.distance_meters))

        # Find closest USAF stations
        if usaf_count > 0:
            usaf_closest = find_closest(lat, lon, usaf_stations, n=usaf_count)
            for sd in usaf_closest:
                closest_stations.append((sd.station.id, sd.distance_meters))

        # Store in database
        db.set_closest_stations(zipcode, closest_stations)

        processed += 1
        if processed % 5000 == 0:
            logger.info(f"Processed {processed}/{total} ZIP codes...")

    logger.info(f"Built closest index for {processed} ZIP codes")
    return processed
