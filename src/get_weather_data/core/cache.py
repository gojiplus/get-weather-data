"""Cache freshness and disk management.

Station lists and ZIP data change over time, so they re-download after
``config.cache_max_age_days``. Historical weather data (years before
last year) is immutable and never expires; the current and previous
year are still accumulating observations and refresh on the same TTL.
"""

import logging
import shutil
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from get_weather_data.core.config import get_config
from get_weather_data.core.download import download_with_retry

logger = logging.getLogger("get_weather_data")

_SECONDS_PER_DAY = 86400


def is_fresh(path: Path, max_age_days: int) -> bool:
    """Whether a cached file exists and is younger than the TTL.

    Args:
        path: Cached file path.
        max_age_days: Maximum age in days.

    Returns:
        True if the file exists and is fresh.
    """
    if not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < max_age_days * _SECONDS_PER_DAY


def ensure_fresh_download(
    url: str,
    path: Path,
    max_age_days: int | None = None,
    force: bool = False,
) -> Path:
    """Download a file unless a fresh cached copy exists.

    Args:
        url: Source URL.
        path: Cache destination.
        max_age_days: TTL in days (default: config.cache_max_age_days).
        force: Re-download even when fresh.

    Returns:
        Path to the cached file.

    Raises:
        RuntimeError: If the download fails after retries.
    """
    if max_age_days is None:
        max_age_days = get_config().cache_max_age_days
    if not force and is_fresh(path, max_age_days):
        return path
    if path.exists():
        logger.info(f"Cached copy of {path.name} is stale; refreshing")
    if download_with_retry(url, path) is None:
        if path.exists():
            logger.warning(f"Refresh of {url} failed; using stale cached copy")
            return path
        raise RuntimeError(f"Failed to download {url}")
    return path


def year_is_immutable(year: int, today: date | None = None) -> bool:
    """Whether a data year can no longer change.

    NOAA keeps appending to the current year's files (and late reports
    trickle into the previous year); anything older is final.

    Args:
        year: Calendar year of the data.
        today: Override for testing.

    Returns:
        True when the year's data is final.
    """
    current = (today or date.today()).year
    return year < current - 1


@dataclass
class CacheEntry:
    """Disk usage of one cache area."""

    name: str
    path: Path
    files: int
    bytes: int


def _dir_usage(name: str, path: Path) -> CacheEntry:
    files = 0
    total = 0
    if path.exists():
        for item in path.rglob("*"):
            if item.is_file():
                files += 1
                total += item.stat().st_size
    return CacheEntry(name=name, path=path, files=files, bytes=total)


def cache_info() -> list[CacheEntry]:
    """Disk usage per cache area.

    Returns:
        One entry per cache area (ghcn, gsod, stations, database).
    """
    config = get_config()
    entries = [
        _dir_usage("ghcn", config.ghcn_cache_dir),
        _dir_usage("gsod", config.gsod_cache_dir),
        _dir_usage("stations", config.stations_cache_dir),
    ]
    db = config.database_path
    entries.append(
        CacheEntry(
            name="database",
            path=db,
            files=1 if db.exists() else 0,
            bytes=db.stat().st_size if db.exists() else 0,
        )
    )
    return entries


def clear_cache(
    ghcn: bool = False,
    gsod: bool = False,
    stations: bool = False,
    clear_all: bool = False,
) -> int:
    """Delete cached data files.

    Args:
        ghcn: Clear the yearly GHCN databases.
        gsod: Clear the per-station GSOD CSVs.
        stations: Clear station lists and ZIP data.
        clear_all: Clear everything above.

    Returns:
        Bytes freed.
    """
    config = get_config()
    targets: list[Path] = []
    if ghcn or clear_all:
        targets.append(config.ghcn_cache_dir)
    if gsod or clear_all:
        targets.append(config.gsod_cache_dir)
    if stations or clear_all:
        targets.append(config.stations_cache_dir)

    freed = 0
    for target in targets:
        if not target.exists():
            continue
        freed += _dir_usage(target.name, target).bytes
        shutil.rmtree(target)
        logger.info(f"Cleared cache: {target}")
    return freed
