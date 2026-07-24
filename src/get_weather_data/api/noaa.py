"""NOAA Climate Data Online (CDO) Web Services v2 client.

Get a free token at https://www.ncdc.noaa.gov/cdo-web/token and set the
NCDC_TOKEN environment variable. Tokens are limited to 5 requests per
second and 10,000 requests per day.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import httpx

from get_weather_data.core.config import get_config

logger = logging.getLogger("get_weather_data")

CDO_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"
TOKEN_URL = "https://www.ncdc.noaa.gov/cdo-web/token"  # noqa: S105 - URL, not a secret

# CDO caps /data at 1000 records per request; larger result sets are
# fetched via metadata.resultset offset pagination.
PAGE_LIMIT = 1000

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class NOAAAPIError(Exception):
    """The CDO API returned an unrecoverable error or retries ran out."""


@dataclass
class StationInfo:
    """Metadata for a CDO station."""

    id: str
    name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    elevation: float | None = None


@dataclass
class NOAAClient:
    """Client for NOAA CDO Web Services v2.

    Args:
        token: CDO API token. Falls back to the NCDC_TOKEN environment
            variable (via config) when not given.
        base_url: API base URL.
        timeout: Per-request timeout in seconds.
        max_retries: Retries for rate-limit/server/transport errors.
        retry_delay: Base delay for exponential backoff, in seconds.
        min_request_interval: Client-side throttle between requests, in
            seconds (the API allows 5 requests per second).
    """

    token: str | None = None
    base_url: str = CDO_BASE_URL
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    min_request_interval: float = 0.25
    _last_request: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        """Fill the token from config and validate it.

        Raises:
            ValueError: If no token is configured.
        """
        if self.token is None:
            self.token = get_config().ncdc_token

        if not self.token:
            raise ValueError(
                f"NCDC token required for online mode. Get one at {TOKEN_URL} "
                "and set the NCDC_TOKEN environment variable."
            )

    def _throttle(self) -> None:
        """Space requests out to respect the 5 requests/second limit."""
        if self.min_request_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self._last_request = time.monotonic()

    def _request(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        ok_404: bool = False,
    ) -> dict[str, Any] | None:
        """GET an endpoint and return its parsed JSON body.

        Args:
            endpoint: Path under the base URL (e.g. "data").
            params: Query parameters.
            ok_404: If True, a 404 returns None instead of raising.

        Returns:
            Parsed JSON dict ({} for an empty body), or None on an
            allowed 404.

        Raises:
            NOAAAPIError: On client errors, or when retries run out on
                rate-limit/server/transport errors.
        """
        # CDO v2 has carried a deprecation notice since 2019 but remains
        # live. If it is ever retired, migrate to the NCEI Data Service
        # API: https://www.ncei.noaa.gov/access/services/data/v1
        url = f"{self.base_url}/{endpoint}"
        headers = {"token": self.token or ""}
        last_error = ""

        for attempt in range(self.max_retries + 1):
            self._throttle()
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    response = client.get(url, params=params, headers=headers)
            except httpx.TransportError as exc:
                last_error = f"transport error: {exc}"
                logger.warning("CDO request failed (%s), retrying", last_error)
                self._backoff(attempt, None)
                continue

            if response.status_code == 200:
                if not response.content:
                    return {}
                return response.json()
            if response.status_code == 404 and ok_404:
                return None
            if response.status_code in _RETRYABLE_STATUS:
                last_error = f"HTTP {response.status_code}"
                logger.warning("CDO request returned %s, retrying", last_error)
                self._backoff(attempt, response.headers.get("Retry-After"))
                continue
            if response.status_code == 401:
                raise NOAAAPIError(
                    f"CDO API rejected the token (HTTP 401). Check NCDC_TOKEN; "
                    f"tokens are issued at {TOKEN_URL}."
                )
            raise NOAAAPIError(
                f"CDO API error for {endpoint}: HTTP {response.status_code} "
                f"{response.text[:200]}"
            )

        raise NOAAAPIError(
            f"CDO API request for {endpoint} failed after "
            f"{self.max_retries + 1} attempts ({last_error})"
        )

    def _backoff(self, attempt: int, retry_after: str | None) -> None:
        """Sleep before the next retry attempt."""
        if retry_after is not None:
            try:
                time.sleep(float(retry_after))
                return
            except ValueError:
                pass
        time.sleep(self.retry_delay * 2**attempt)

    def _request_paginated(
        self, endpoint: str, params: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """GET all pages of a collection endpoint.

        Args:
            endpoint: Path under the base URL.
            params: Query parameters (limit/offset are managed here).

        Returns:
            Concatenated "results" entries across all pages.
        """
        results: list[dict[str, Any]] = []
        offset = 1
        while True:
            page = self._request(
                endpoint, {**params, "limit": PAGE_LIMIT, "offset": offset}
            )
            if not page:
                break
            results.extend(page.get("results", []))
            resultset = page.get("metadata", {}).get("resultset", {})
            count = int(resultset.get("count", len(results)))
            if len(results) >= count:
                break
            offset += PAGE_LIMIT
        return results

    def get_data(
        self,
        zipcode: str,
        start: date,
        end: date,
        datatypes: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch GHCND records for a ZIP code and date range.

        Values come back in raw GHCN units (tenths for temperatures and
        precipitation), matching the bulk-file backend.

        Args:
            zipcode: 5-digit US ZIP code.
            start: Start date (inclusive).
            end: End date (inclusive). CDO caps GHCND requests at one
                year; callers must chunk longer ranges.
            datatypes: Optional GHCND element codes to restrict to.

        Returns:
            List of record dicts with date/datatype/station/value keys.
        """
        params: dict[str, Any] = {
            "datasetid": "GHCND",
            "locationid": f"ZIP:{zipcode}",
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
        }
        if datatypes:
            # httpx encodes a list value as repeated datatypeid params
            params["datatypeid"] = datatypes
        return self._request_paginated("data", params)

    def get_stations(
        self,
        extent: tuple[float, float, float, float],
        start: date,
        end: date,
    ) -> list[StationInfo]:
        """Find GHCND stations within a bounding box, active in a period.

        Args:
            extent: Bounding box as (south, west, north, east) degrees.
            start: Period start; stations must have data covering it.
            end: Period end.

        Returns:
            List of StationInfo for matching stations.
        """
        south, west, north, east = extent
        payload = self._request_paginated(
            "stations",
            {
                "datasetid": "GHCND",
                "extent": f"{south},{west},{north},{east}",
                "startdate": start.isoformat(),
                "enddate": end.isoformat(),
            },
        )
        return [
            StationInfo(
                id=entry["id"],
                name=entry.get("name"),
                latitude=entry.get("latitude"),
                longitude=entry.get("longitude"),
                elevation=entry.get("elevation"),
            )
            for entry in payload
            if entry.get("id")
        ]

    def get_data_for_stations(
        self,
        station_ids: list[str],
        start: date,
        end: date,
    ) -> list[dict[str, Any]]:
        """Fetch GHCND records for specific stations and a date range.

        Args:
            station_ids: CDO station ids (e.g. "GHCND:USW00094728").
            start: Start date (inclusive).
            end: End date (inclusive, within one year of start).

        Returns:
            List of record dicts with date/datatype/station/value keys.
        """
        params: dict[str, Any] = {
            "datasetid": "GHCND",
            # httpx encodes a list value as repeated stationid params
            "stationid": station_ids,
            "startdate": start.isoformat(),
            "enddate": end.isoformat(),
        }
        return self._request_paginated("data", params)

    def get_station(self, station_id: str) -> StationInfo | None:
        """Fetch metadata for a station.

        Args:
            station_id: CDO station id (e.g. "GHCND:USW00094728").

        Returns:
            StationInfo, or None if the station is unknown.
        """
        payload = self._request(f"stations/{station_id}", ok_404=True)
        if not payload or "id" not in payload:
            return None
        return StationInfo(
            id=payload["id"],
            name=payload.get("name"),
            latitude=payload.get("latitude"),
            longitude=payload.get("longitude"),
            elevation=payload.get("elevation"),
        )
