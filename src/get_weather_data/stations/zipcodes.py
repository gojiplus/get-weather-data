"""ZIP code data import from GeoNames."""

import logging
import zipfile
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.database import Database
from get_weather_data.core.download import download

logger = logging.getLogger("get_weather_data")

GEONAMES_ZIP_URL = "https://download.geonames.org/export/zip/US.zip"


def download_zipcodes(output_dir: Path | None = None) -> Path:
    """Download and extract US ZIP code data from GeoNames.

    Args:
        output_dir: Directory to extract to. Uses cache dir if None.

    Returns:
        Path to extracted US.txt file.
    """
    if output_dir is None:
        output_dir = get_config().stations_cache_dir

    output_file = output_dir / "US.txt"

    if not output_file.exists():
        zip_path = output_dir / "US.zip"
        logger.info("Downloading ZIP code data from GeoNames...")
        download(GEONAMES_ZIP_URL, zip_path)

        logger.info("Extracting ZIP code data...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extract("US.txt", output_dir)

        # Clean up zip file
        zip_path.unlink()

    return output_file


def parse_zipcodes(file_path: Path) -> list[dict]:
    """Parse GeoNames US.txt file.

    Tab-separated format:
    - country code (US)
    - postal code
    - place name (city)
    - admin name1 (state name)
    - admin code1 (state abbreviation)
    - admin name2 (county)
    - admin code2 (county code)
    - admin name3
    - admin code3
    - latitude
    - longitude
    - accuracy

    Args:
        file_path: Path to US.txt file.

    Returns:
        List of ZIP code dictionaries.
    """
    zipcodes = []

    with open(file_path, encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 11:
                continue

            try:
                zipcodes.append(
                    {
                        "zipcode": parts[1],
                        "city": parts[2],
                        "state": parts[4],  # State abbreviation
                        "county": parts[5],
                        "lat": float(parts[9]),
                        "lon": float(parts[10]),
                    }
                )
            except (ValueError, IndexError) as e:
                logger.debug(f"Skipping malformed line: {e}")
                continue

    return zipcodes


def import_zipcodes(db: Database | None = None) -> int:
    """Download and import ZIP codes to database.

    Args:
        db: Database instance. Uses default if None.

    Returns:
        Number of ZIP codes imported.
    """
    if db is None:
        db = Database()

    file_path = download_zipcodes()

    logger.info("Parsing ZIP codes...")
    zipcodes = parse_zipcodes(file_path)

    logger.info(f"Importing {len(zipcodes)} ZIP codes...")

    with db.connection() as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO zipcodes (zipcode, city, state, lat, lon, county)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (z["zipcode"], z["city"], z["state"], z["lat"], z["lon"], z["county"])
                for z in zipcodes
            ],
        )
        conn.commit()

    return len(zipcodes)
