"""Download utilities for get-weather-data."""

import io
import logging
import zipfile
from pathlib import Path

import httpx

logger = logging.getLogger("get_weather_data")


def download(url: str, output_path: Path | str, timeout: float = 120.0) -> Path:
    """Download a file from URL to local path.

    Args:
        url: URL to download from.
        output_path: Local path to save file.
        timeout: Request timeout in seconds.

    Returns:
        Path to downloaded file.

    Raises:
        httpx.HTTPStatusError: If download fails.
    """
    output_path = Path(output_path)
    logger.info(f"Downloading {url}...")

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        output_path.write_bytes(response.content)

    logger.debug(f"Downloaded to {output_path}")
    return output_path


def download_and_extract(
    url: str,
    output_dir: Path | str,
    timeout: float = 120.0,
) -> list[Path]:
    """Download a zip file and extract its contents.

    Args:
        url: URL to download zip from.
        output_dir: Directory to extract files to.
        timeout: Request timeout in seconds.

    Returns:
        List of extracted file paths.

    Raises:
        httpx.HTTPStatusError: If download fails.
        zipfile.BadZipFile: If file is not a valid zip.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Downloading {url}...")

    with httpx.Client(timeout=timeout, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            extracted = []
            for name in zf.namelist():
                if not name.startswith("__") and not name.endswith("/"):
                    zf.extract(name, output_dir)
                    extracted.append(output_dir / name)
                    logger.debug(f"Extracted {name}")

    return extracted


def download_with_retry(
    url: str,
    output_path: Path | str,
    max_retries: int = 3,
    timeout: float = 120.0,
) -> Path | None:
    """Download with automatic retry on failure.

    Args:
        url: URL to download from.
        output_path: Local path to save file.
        max_retries: Maximum number of retry attempts.
        timeout: Request timeout in seconds.

    Returns:
        Path to downloaded file, or None if all retries failed.
    """
    import time

    output_path = Path(output_path)

    for attempt in range(max_retries):
        try:
            return download(url, output_path, timeout)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.warning(f"File not found: {url}")
                return None
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                logger.warning(f"Download failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Download failed after {max_retries} attempts: {url}")
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10
                logger.warning(f"Error {e}, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error(f"Download failed: {e}")
                return None

    return None
