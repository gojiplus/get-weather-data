"""NOAA Climate Data Online (CDO) API client.

Get token from https://www.ncdc.noaa.gov/cdo-web/token
Set NCDC_TOKEN environment variable with the token.
"""

import logging
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date

import httpx

from get_weather_data.core.config import get_config

logger = logging.getLogger("get_weather_data")

CDO_BASE_URL = "http://www.ncdc.noaa.gov/cdo-services/services"


@dataclass
class NOAAClient:
    """Client for NOAA Climate Data Online API."""

    token: str | None = None
    base_url: str = CDO_BASE_URL
    timeout: float = 500.0
    retry_delay: float = 5.0

    def __post_init__(self) -> None:
        if self.token is None:
            config = get_config()
            self.token = config.ncdc_token

        if not self.token:
            raise ValueError(
                "NCDC token required. Get one at https://www.ncdc.noaa.gov/cdo-web/token "
                "and set NCDC_TOKEN environment variable."
            )

    def _fetch_xml(self, uri: str) -> ET.Element:
        """Fetch XML from URI with retry logic."""
        while True:
            time.sleep(1)
            logger.debug(f"Fetching: {uri}")

            with httpx.Client(timeout=self.timeout) as client:
                response = client.get(uri)
                response.raise_for_status()
                root = ET.fromstring(response.content)

            if root.tag == "cdoError":
                name_elem = root.find("name")
                msg_elem = root.find("message")
                name = name_elem.text if name_elem is not None else "Unknown"
                msg = msg_elem.text if msg_elem is not None else "Unknown"
                logger.warning(f"CDO Error: {name} - {msg}")
                time.sleep(self.retry_delay)
            else:
                return root

    def get_ghcnd(
        self,
        zipcode: str,
        target_date: date,
    ) -> dict[str, str | float]:
        """Get GHCND data for a ZIP code from NOAA API.

        Args:
            zipcode: 5-digit US ZIP code.
            target_date: Date to get data for.

        Returns:
            Dict with station info and weather values.
        """
        year = f"{target_date.year:04d}"
        month = f"{target_date.month:02d}"
        day = f"{target_date.day:02d}"
        date_str = f"{year}-{month}-{day}T00:00:00.000"

        uri = (
            f"{self.base_url}/datasets/GHCND/locations/ZIP:{zipcode}/data"
            f"?year={year}&month={month}&day={day}&_type=xml&token={self.token}"
        )

        root = self._fetch_xml(uri)

        result: dict[str, str | float] = {}
        page_count_str = root.get("pageCount")

        if page_count_str is None:
            return result

        page_count = int(page_count_str)
        data = root.findall(f".//*[date='{date_str}']")

        for i in range(2, page_count + 1):
            paged_uri = f"{uri}&page={i}"
            paged_root = self._fetch_xml(paged_uri)
            data.extend(paged_root.findall(f".//*[date='{date_str}']"))

        station_id = self._get_most_common_station(data)
        if not station_id:
            return result

        result["station_id"] = station_id
        station_info = self._get_station_info(station_id, "GHCND")
        result.update(station_info)

        for elem in data:
            station_elem = elem.find("station")
            if station_elem is not None and station_elem.text == station_id:
                data_type_elem = elem.find("dataType")
                value_elem = elem.find("value")
                if data_type_elem is not None and value_elem is not None:
                    if data_type_elem.text and value_elem.text:
                        result[data_type_elem.text] = value_elem.text

        return result

    def get_precip_hly(
        self,
        zipcode: str,
        target_date: date,
    ) -> dict[str, str | float]:
        """Get hourly precipitation data from NOAA API.

        Args:
            zipcode: 5-digit US ZIP code.
            target_date: Date to get data for.

        Returns:
            Dict with station info and hourly precipitation values.
        """
        year = f"{target_date.year:04d}"
        month = f"{target_date.month:02d}"
        day = f"{target_date.day:02d}"
        date_prefix = f"{year}-{month}-{day}T"

        uri = (
            f"{self.base_url}/datasets/PRECIP_HLY/locations/ZIP:{zipcode}"
            f"/datatypes/HPCP/data?year={year}&month={month}&day={day}"
            f"&token={self.token}&_type=xml"
        )

        root = self._fetch_xml(uri)

        result: dict[str, str | float] = {}
        page_count_str = root.get("pageCount")

        if page_count_str is None:
            return result

        page_count = int(page_count_str)
        data = []

        for child in root:
            date_elem = child.find("date")
            if date_elem is not None and date_elem.text:
                if date_elem.text.startswith(date_prefix):
                    data.append(child)

        for i in range(2, page_count + 1):
            paged_uri = f"{uri}&page={i}"
            paged_root = self._fetch_xml(paged_uri)
            for child in paged_root:
                date_elem = child.find("date")
                if date_elem is not None and date_elem.text:
                    if date_elem.text.startswith(date_prefix):
                        data.append(child)

        station_id = self._get_most_common_station(data)
        if not station_id:
            return result

        result["station_id"] = station_id
        station_info = self._get_station_info(station_id, "PRECIP_HLY")
        result.update(station_info)

        for elem in data:
            station_elem = elem.find("station")
            if station_elem is not None and station_elem.text == station_id:
                date_elem = elem.find("date")
                value_elem = elem.find("value")
                if date_elem is not None and date_elem.text:
                    if value_elem is not None and value_elem.text:
                        hour = int(date_elem.text[11:13])
                        result[f"HPCP_{hour:02d}"] = value_elem.text

        return result

    def _get_most_common_station(self, data: list[ET.Element]) -> str:
        """Get the most common station ID from data elements."""
        stations: dict[str, int] = {}
        for elem in data:
            station_elem = elem.find("station")
            if station_elem is not None and station_elem.text:
                sid = station_elem.text
                stations[sid] = stations.get(sid, 0) + 1

        if not stations:
            return ""

        return max(stations.items(), key=lambda x: x[1])[0]

    def _get_station_info(self, station_id: str, dataset: str) -> dict[str, str]:
        """Get station information from NOAA API."""
        uri = (
            f"{self.base_url}/datasets/{dataset}/stations/{station_id}"
            f"?token={self.token}&_type=xml"
        )

        root = self._fetch_xml(uri)
        station = root.find(f".//*[id='{station_id}']")

        if station is None:
            return {}

        result: dict[str, str] = {}

        display_name = station.find("displayName")
        if display_name is not None and display_name.text:
            result["station_name"] = display_name.text

        lat = station.find("latitude")
        if lat is not None and lat.text:
            result["station_lat"] = lat.text

        lon = station.find("longitude")
        if lon is not None and lon.text:
            result["station_lon"] = lon.text

        cnty = station.find("*[type='CNTY']")
        if cnty is not None:
            cnty_id = cnty.find("id")
            if cnty_id is not None and cnty_id.text:
                result["fips_county"] = cnty_id.text.replace("FIPS:", "")

        st = station.find("*[type='ST']")
        if st is not None:
            st_id = st.find("id")
            if st_id is not None and st_id.text:
                result["fips_state"] = st_id.text.replace("FIPS:", "")

        return result


def get_ghcnd_by_zip(
    zipcode: str,
    target_date: date,
    token: str | None = None,
) -> dict[str, str | float]:
    """Convenience function to get GHCND data by ZIP code.

    Args:
        zipcode: 5-digit US ZIP code.
        target_date: Date to get data for.
        token: NCDC API token. Uses NCDC_TOKEN env var if None.

    Returns:
        Dict with weather data.
    """
    client = NOAAClient(token=token)
    return client.get_ghcnd(zipcode, target_date)
