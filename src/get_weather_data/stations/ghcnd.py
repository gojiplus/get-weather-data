"""GHCND station data import."""

import logging
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.database import Database
from get_weather_data.core.distance import Station
from get_weather_data.core.download import download

logger = logging.getLogger("get_weather_data")

GHCND_STATIONS_URL = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"


def download_ghcnd_stations(output_path: Path | None = None) -> Path:
    """Download GHCND stations list.

    Args:
        output_path: Where to save the file. Uses cache dir if None.

    Returns:
        Path to downloaded file.
    """
    if output_path is None:
        output_path = get_config().stations_cache_dir / "ghcnd-stations.txt"

    if not output_path.exists():
        download(GHCND_STATIONS_URL, output_path)

    return output_path


def parse_ghcnd_stations(file_path: Path) -> list[Station]:
    """Parse GHCND stations file.

    Format: Fixed-width text file
    - Columns 1-11: Station ID
    - Columns 13-20: Latitude
    - Columns 22-30: Longitude
    - Columns 32-37: Elevation
    - Columns 39-40: State
    - Columns 42-71: Station name

    Args:
        file_path: Path to ghcnd-stations.txt

    Returns:
        List of Station objects for US stations only.
    """
    stations = []

    with open(file_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            # Only include US stations
            if not line.startswith("US"):
                continue

            try:
                station_id = line[0:11].strip()
                lat = float(line[12:20].strip())
                lon = float(line[21:30].strip())
                elevation = float(line[31:37].strip())
                state = line[38:40].strip()
                name = line[41:71].strip()

                stations.append(
                    Station(
                        id=station_id,
                        name=name,
                        state=state,
                        lat=lat,
                        lon=lon,
                        elevation=elevation,
                        type="GHCND",
                    )
                )
            except (ValueError, IndexError) as e:
                logger.debug(f"Skipping malformed line: {e}")
                continue

    return stations


def import_ghcnd_stations(db: Database | None = None) -> int:
    """Download and import GHCND stations to database.

    Args:
        db: Database instance. Uses default if None.

    Returns:
        Number of stations imported.
    """
    if db is None:
        db = Database()

    logger.info("Downloading GHCND stations...")
    file_path = download_ghcnd_stations()

    logger.info("Parsing GHCND stations...")
    stations = parse_ghcnd_stations(file_path)

    logger.info(f"Importing {len(stations)} GHCND stations...")
    db.insert_stations_bulk(stations)

    return len(stations)
