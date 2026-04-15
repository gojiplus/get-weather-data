#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import gzip
import logging
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any

import httpx

try:
    import numpy as np
    from scipy.spatial import cKDTree

    KDTREE_AVAILABLE = True
except ImportError:
    KDTREE_AVAILABLE = False
    np = None  # type: ignore[assignment]
    cKDTree = None  # type: ignore[assignment, misc]

nauticalMilePerLat = 60.00721
nauticalMilePerLongitude = 60.10793
rad = math.pi / 180.0
metersPerNauticalMile = 1852

STATION_INFO_COLS = ["sid", "type", "name", "lat", "lon", "nth", "distance"]

USAF_WBAN_DATA = [
    ("TEMP", 24, 30),
    ("DEWP", 35, 41),
    ("SLP", 46, 52),
    ("STP", 57, 63),
    ("VISIB", 68, 73),
    ("AWND", 78, 83),
    ("MXSPD", 88, 93),
    ("GUST", 95, 100),
    ("TMAX", 102, 108),
    ("MAXF", 108, 109),
    ("TMIN", 110, 116),
    ("MINF", 116, 117),
    ("PRCP", 118, 123),
    ("PRCPF", 123, 124),
    ("SNWD", 125, 130),
    ("FRSHTT", 132, 138),
]


def download(url: str, local: str) -> None:
    """Download a file from URL to local path."""
    logging.info(f"Downloading '{url}'...")
    start = time.time()
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        Path(local).write_bytes(response.content)
    elapse = time.time() - start
    logging.info(f"Elapse time: {elapse:f} seconds")


def build_ghcn_database(args: Any, year: str, dbname: str) -> None:
    """Build GHCN database for a given year."""
    conn = None
    try:
        local = os.path.join(args.dbpath, f"{year:s}.csv.gz")
        if not os.path.exists(local):
            if not os.path.exists(args.dbpath):
                os.makedirs(args.dbpath)
            url = f"ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year:s}.csv.gz"
            download(url, local)
        conn = sqlite3.connect(dbname)
        c = conn.cursor()
        c.execute(f"""CREATE TABLE IF NOT EXISTS `ghcn_{year:s}` (
`id` VARCHAR(12) NOT NULL,
`date` VARCHAR(8) NOT NULL,
`element` VARCHAR(4) NULL,
`value` VARCHAR(6) NULL,
`m_flag` VARCHAR(1) NULL,
`q_flag` VARCHAR(1) NULL,
`s_flag` VARCHAR(1) NULL,
`obs_time` VARCHAR(4) NULL)""")

        c.execute(f"""create index if not exists idx_id_time
on ghcn_{year:s} (id, date)""")

        c.execute("PRAGMA journal_mode = OFF")
        c.execute("PRAGMA synchronous = OFF")
        c.execute("PRAGMA cache_size = 1000000")
        logging.info("Importing...")
        start = time.time()
        with gzip.open(local, "rt") as f:
            reader = csv.reader(f)
            c.executemany(
                f"""INSERT OR IGNORE INTO ghcn_{year:s}
(id, date, element, value, m_flag, q_flag, s_flag, obs_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                reader,
            )
        elapse = time.time() - start
        logging.info(f"Import time: {elapse:f} seconds")
    except Exception as e:
        logging.error(str(e))
        raise
    finally:
        if conn:
            logging.info("Commiting...")
            start = time.time()
            conn.commit()
            conn.close()
            elapse = time.time() - start
            logging.info(f"Commit time: {elapse:f} seconds")


class WeatherByZip:
    """Weather data lookup by ZIP code."""

    not_found_list: list[str] = []

    def get_stations(self) -> list[tuple[Any, ...]]:
        """Returns all stations from database."""
        conn = sqlite3.connect(self.args.zip2ws_db)
        c = conn.cursor()
        c.execute("select rowid, id, name, lat, lon, type from stations")
        stations = []
        for r in c:
            stations.append(r)
        conn.close()
        return stations

    def __init__(self, args: Any) -> None:
        self.args = args
        columns_file = args.columns
        try:
            with open(columns_file, "r", encoding="utf-8") as f:
                self.output_columns = [r.strip() for r in f.readlines() if r[0] != "#"]
        except FileNotFoundError:
            columns_path = str(files(__package__) / args.columns)
            with open(columns_path, "r", encoding="utf-8") as f:
                self.output_columns = [r.strip() for r in f.readlines() if r[0] != "#"]

        self.stations = self.get_stations()
        logging.info(f"Number of Stations = {len(self.stations):d}")

    def search_weather_data(
        self, dist: list[tuple[Any, ...]], data: dict[str, Any]
    ) -> dict[str, Any]:
        """Search for weather data at nearest stations."""
        year = f"{data['year']:04d}"
        month = f"{data['month']:02d}"
        day = f"{data['day']:02d}"
        logging.debug(f"Date: {year:s}/{month:s}/{day:s}")
        found = 0
        nth = 0
        raw: list[str] = []
        values: dict[str, Any] = {}
        current_year = ""
        orders = self.output_columns
        conn2 = None
        c2 = None
        for s in dist:
            if self.args.nth > 0 and nth >= self.args.nth:
                logging.info(f"Reach maximum n-th stations: {nth:d}")
                return values
            if self.args.distance > 0 and s[0] >= self.args.distance * 1000:
                logging.info(f"Reach maximum distance: {self.args.distance:d}")
                return values
            nth += 1
            values["nth"] = nth
            sid = s[1]
            stype = s[2]
            values["sid"] = sid
            values["type"] = stype
            values["name"] = s[3]
            values["lat"] = s[4]
            values["lon"] = s[5]
            values["distance"] = s[0]
            if stype == "GHCND":
                if not self.args.uses_sqlite:
                    datadir = "./data/ghcn-daily/all/"
                    if not os.path.exists(datadir):
                        os.makedirs(datadir)
                    datafile = datadir + f"{sid:s}.dly"
                    urlfile = (
                        "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/"
                        f"all/{sid:s}.dly"
                    )
                    if not self.download_data_file(urlfile, datafile):
                        continue
                    search = sid + year + month
                    match = False
                    with open(datafile, "r", encoding="utf-8") as df:
                        for line in df:
                            if line[:17] == search:
                                element = line[17:21]
                                offset = 21 + int(day) * 8 - 8
                                value = line[offset : offset + 5]
                                mflag = line[offset + 5 : offset + 6]
                                qflag = line[offset + 6 : offset + 7]
                                sflag = line[offset + 7 : offset + 8]
                                logging.debug(
                                    f"<{element:s}> <{value:s}> <{mflag:s}> "
                                    f"<{qflag:s}> <{sflag:s}>"
                                )
                                match = True
                                if value != "-9999" and (element in orders):
                                    if not values.get(element):
                                        values[element] = value
                                        found += 1
                                        raw.append(line)
                            elif match:
                                break
                else:
                    if current_year != year:
                        current_year = year
                        dbname = os.path.join(
                            self.args.dbpath, f"ghcn_{year:s}.sqlite3"
                        )
                        if not os.path.exists(dbname):
                            build_ghcn_database(self.args, year, dbname)
                        conn2 = sqlite3.connect(dbname)
                        c2 = conn2.cursor()
                        c2.execute("PRAGMA journal_mode = MEMORY")
                        c2.execute("PRAGMA synchronous = OFF")
                        c2.execute("PRAGMA temp_store = MEMORY")
                        c2.execute("PRAGMA cache_size = 500000")
                    if c2 is not None:
                        ghcn_values = self.get_ghcn_data(c2, sid, year, month, day)
                        for element in ghcn_values:
                            value = ghcn_values[element]
                            if value != "-9999" and (element in orders):
                                if not values.get(element):
                                    found += 1
                                    values[element] = value
            elif stype == "USAF-WBAN":
                datadir = f"./data/gsod/{year:s}/"
                if not os.path.exists(datadir):
                    os.makedirs(datadir)
                datafile = datadir + f"{sid:s}-{year:s}.op.gz"
                urlfile = (
                    f"ftp://ftp2.ncdc.noaa.gov/pub/data/gsod/"
                    f"{year:s}/{sid:s}-{year:s}.op.gz"
                )
                if not self.download_data_file(urlfile, datafile):
                    continue
                search_str = year + month + day
                with gzip.open(datafile, "rt") as df:
                    for line in df:
                        if line[14:22] == search_str:
                            match = False
                            for d in USAF_WBAN_DATA:
                                element = d[0]
                                value_str = line[d[1] : d[2]]
                                if element in ["TMAX", "TMIN"]:
                                    if value_str != "9999.9":
                                        value_num = self.f2c(float(value_str)) * 10
                                    else:
                                        value_num = value_str  # type: ignore[assignment]
                                elif element in ["AWND"]:
                                    if value_str != "999.9":
                                        value_num = self.kn2ms(float(value_str)) * 10
                                    else:
                                        value_num = value_str  # type: ignore[assignment]
                                else:
                                    value_num = value_str  # type: ignore[assignment]
                                if not values.get(element) and (element in orders):
                                    found += 1
                                    values[element] = value_num
                                    match = True
                            if match:
                                raw.append(line)
                            break
            if found >= len(orders):
                break
        logging.debug(values)
        return values

    def search(self, search: dict[str, Any]) -> list[dict[str, Any]]:
        """Search for weather data for a ZIP code over a date range."""
        zipcode = "0" * (5 - len(search["zip"])) + search["zip"]

        logging.info(f"Search for: '{zipcode:s}'")

        from_date = datetime(
            year=search["from.year"],
            month=search["from.month"],
            day=search["from.day"],
        )
        to_date = datetime(
            year=search["to.year"], month=search["to.month"], day=search["to.day"]
        )
        results: list[dict[str, Any]] = []
        conn = sqlite3.connect(self.args.zip2ws_db)
        c = conn.cursor()
        c.execute(
            "select zipcode, lat, lon, gm_lat, gm_lon, rowid from zip"
            " where zipcode = ?",
            (zipcode,),
        )
        r = c.fetchone()
        dist = self.sort_stations(zipcode, r, self.stations)
        while from_date <= to_date:
            data: dict[str, Any] = {}
            data["uniqid"] = search["uniqid"]
            data["zip"] = zipcode
            data["year"] = from_date.year
            data["month"] = from_date.month
            data["day"] = from_date.day
            if dist and len(dist) > 0:
                wdata = self.search_weather_data(dist, data)
                data.update(wdata)
            results.append(data)
            from_date += timedelta(days=1)
        return results

    def download_data_file(self, url: str, file: str) -> bool:
        """Download weather data file from server if it doesn't exist locally."""
        if url in self.not_found_list:
            logging.warning(f"This URL no data on server {url:s}")
            return False
        if not os.path.exists(file):
            retry = 0
            while True:
                try:
                    logging.info(f"Downloading '{url:s}'")
                    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                        response = client.get(url)
                        response.raise_for_status()
                        Path(file).write_bytes(response.content)
                    return True
                except httpx.HTTPStatusError as e:
                    status_code = e.response.status_code
                    logging.warning(f"HTTP error code = {status_code}")
                    if status_code == 404 or status_code == 550:
                        self.not_found_list.append(url)
                        return False
                    logging.warning(f"HTTP error = {e!s}")
                    if retry < 5:
                        retry += 1
                        logging.info(f"Retry #{retry:d}: waiting...({retry * 10:d}s)")
                        time.sleep(retry * 10)
                    else:
                        logging.warning(f"Cannot download data from URL = {url:s}")
                        return False
                except Exception as e:
                    m = re.match(r".*\s(\d\d\d)\s.*", str(e))
                    if m:
                        logging.warning(f"Error code = {m.group(1):s}")
                        if m.group(1) == "550":
                            self.not_found_list.append(url)
                            return False
                    logging.warning(f"Unknown error = {e!s}")
                    if retry < 5:
                        retry += 1
                        logging.info(f"Retry #{retry:d}: waiting...({retry * 10:d}s)")
                        time.sleep(retry * 10)
                    else:
                        logging.warning(f"Cannot download data from URL = {url:s}")
                        return False
        return True

    def metersGeoDistance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Returns calculate distance between two lat lons in meters."""
        yDistance = (lat2 - lat1) * nauticalMilePerLat
        xDistance = (
            (math.cos(lat1 * rad) + math.cos(lat2 * rad))
            * (lon2 - lon1)
            * (nauticalMilePerLongitude / 2)
        )

        distance = math.sqrt(yDistance**2 + xDistance**2)

        return distance * metersPerNauticalMile

    def f2c(self, f: float) -> float:
        """Convert Fahrenheit to Celsius."""
        return (f - 32) * 5.0 / 9.0

    def kn2ms(self, kn: float) -> float:
        """Convert Knots to m/s."""
        return 0.51444 * kn

    def sort_stations(
        self,
        zipcode: str,
        r: tuple[Any, ...] | None,
        stations: list[tuple[Any, ...]],
    ) -> list[tuple[int, str, str, str, float, float]] | None:
        """Returns sorted stations list by distance."""
        if r is None:
            logging.warning(f"zipcode = {zipcode:s} not found")
            return None
        lat1 = r[3]
        if lat1 is None:
            lat1 = r[1]
        lon1 = r[4]
        if lon1 is None:
            lon1 = r[2]
        if lat1 == "" or lon1 == "":
            logging.warning(f"not lat/lon for zipcode = {zipcode:s}")
            return None
        lat1 = float(lat1)
        lon1 = float(lon1)

        if KDTREE_AVAILABLE and len(stations) > 100:
            valid_stations = [
                (s[0], s[1], s[5], s[2], s[3], s[4]) for s in stations if s[3] and s[4]
            ]
            if valid_stations:
                coords = np.array([(s[4], s[5]) for s in valid_stations])
                tree = cKDTree(coords)
                distances, indices = tree.query([lat1, lon1], k=len(coords))
                if isinstance(distances, float):
                    distances = [distances]
                    indices = [indices]
                return [
                    (
                        int(d * 111000),
                        valid_stations[i][1],
                        valid_stations[i][2],
                        valid_stations[i][3],
                        valid_stations[i][4],
                        valid_stations[i][5],
                    )
                    for d, i in zip(distances, indices)
                ]

        dist: list[tuple[int, str, str, str, float, float]] = []
        for s in stations:
            id = s[1]
            name = s[2]
            stype = s[5]
            if s[3] is None or s[4] is None:
                continue
            lat2 = float(s[3])
            lon2 = float(s[4])
            try:
                distance = self.metersGeoDistance(lat1, lon1, lat2, lon2)
            except Exception:
                distance = float(sys.maxsize)
            dist.append((int(distance), id, stype, name, lat2, lon2))
        return sorted(dist)

    def get_ghcn_data(
        self,
        cursor: sqlite3.Cursor,
        sid: str,
        year: str,
        month: str,
        day: str,
    ) -> dict[str, str]:
        """Get GHCN data from database."""
        cursor.execute(
            f"select * from ghcn_{year:s} where id = ? and date = ?",
            (sid, f"{int(year):02d}{int(month):02d}{int(day):02d}"),
        )
        values: dict[str, str] = {}
        for r in cursor:
            values[r[2]] = r[3]
        return values
