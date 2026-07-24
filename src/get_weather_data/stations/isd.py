"""ISD (Integrated Surface Database) station data import."""

import csv
import logging
from pathlib import Path

from get_weather_data.core.cache import ensure_fresh_download
from get_weather_data.core.config import get_config
from get_weather_data.core.database import Database
from get_weather_data.core.distance import Station

logger = logging.getLogger("get_weather_data")

ISD_HISTORY_URL = "https://www.ncei.noaa.gov/pub/data/noaa/isd-history.csv"


def download_isd_stations(output_path: Path | None = None, force: bool = False) -> Path:
    """Download the ISD station history file (refreshes when stale).

    Args:
        output_path: Where to save the file. Uses cache dir if None.
        force: Re-download even when the cached copy is fresh.

    Returns:
        Path to downloaded file.

    Raises:
        RuntimeError: If the download fails after retries.
    """  # noqa: DOC502 - raised by ensure_fresh_download
    if output_path is None:
        output_path = get_config().stations_cache_dir / "isd-history.csv"
    return ensure_fresh_download(ISD_HISTORY_URL, output_path, force=force)


def parse_isd_stations(file_path: Path) -> list[Station]:
    """Parse ISD history CSV file.

    CSV columns:
    - USAF: 6-digit USAF station ID
    - WBAN: 5-digit WBAN station ID
    - STATION NAME
    - CTRY: Country code
    - ST: State abbreviation
    - ICAO: ICAO code
    - LAT: Latitude (decimal degrees)
    - LON: Longitude (decimal degrees)
    - ELEV(M): Elevation in meters

    Args:
        file_path: Path to isd-history.csv

    Returns:
        List of Station objects for US stations only.
    """
    stations = []

    with open(file_path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only include US stations
            if row.get("CTRY") != "US":
                continue

            try:
                usaf = row.get("USAF", "")
                wban = row.get("WBAN", "")
                station_id = f"{usaf}-{wban}"
                name = row.get("STATION NAME", "")
                state = row.get("ST", "")

                lat_str = row.get("LAT", "")
                lon_str = row.get("LON", "")
                elev_str = row.get("ELEV(M)", "")

                if not lat_str or not lon_str:
                    continue

                lat = float(lat_str)
                lon = float(lon_str)
                elevation = float(elev_str) if elev_str else None

                stations.append(
                    Station(
                        id=station_id,
                        name=name,
                        state=state,
                        lat=lat,
                        lon=lon,
                        elevation=elevation,
                        type="USAF-WBAN",
                    )
                )
            except (ValueError, KeyError) as e:
                logger.debug(f"Skipping malformed row: {e}")
                continue

    return stations


def import_isd_stations(db: Database | None = None, force: bool = False) -> int:
    """Download and import ISD stations to database.

    Args:
        db: Database instance. Uses default if None.
        force: Re-download source files even when fresh.

    Returns:
        Number of stations imported.
    """
    if db is None:
        db = Database()

    logger.info("Downloading ISD stations...")
    file_path = download_isd_stations(force=force)

    logger.info("Parsing ISD stations...")
    stations = parse_isd_stations(file_path)

    logger.info(f"Importing {len(stations)} ISD stations...")
    db.insert_stations_bulk(stations)

    return len(stations)
