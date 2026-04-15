#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import io
import logging
import math
import os
import signal
import sqlite3
import sys
import zipfile
from dataclasses import dataclass
from importlib.resources import files
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import click
import httpx
from geopy.exc import GeocoderServiceError, GeocoderTimedOut
from geopy.geocoders import Nominatim
from rich.console import Console

try:
    import numpy as np
    from scipy.spatial import cKDTree

    KDTREE_AVAILABLE = True
except ImportError:
    KDTREE_AVAILABLE = False
    np = None  # type: ignore[assignment]
    cKDTree = None  # type: ignore[assignment, misc]

FREE_ZIPCODE_DOWNLOAD_URL = "https://download.geonames.org/export/zip/US.zip"
GHCND_STATIONS_LIST_URL = (
    "http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
)
ISD_STATIONS_LIST_URL = "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"


def _get_data_path(filename: str) -> str:
    """Get path to a data file within the package."""
    return str(files(__package__) / "data" / filename)


def _get_inventory_path(filename: str) -> str:
    """Get path to an inventory file within the package."""
    return str(files(__package__) / "inventories" / filename)


US_ZIP_LIST = _get_data_path("free-zipcode-database-primary.csv")
GHCND_STATIONS_LIST = _get_data_path("ghcnd-stations.txt")
ASOS_STATIONS_LIST = _get_inventory_path("asos-stations.txt")
COOP_STATIONS_LIST = _get_inventory_path("coop-act.txt")
ISD_STATIONS_LIST = _get_data_path("isd-history.csv")

SQLITE_DB_NAME = _get_data_path("zip2ws.sqlite")
CSV_OUTPUT_FILE = _get_data_path("zip-stations.csv")
NO_GHCN = 3
NO_COOP = 0
NO_USAF = 2
LOGFILE = "zip2ws.log"

console = Console()


@dataclass
class Options:
    """Configuration options for zip2ws operations."""

    database: str = SQLITE_DB_NAME
    ghcn: int = NO_GHCN
    coop: int = NO_COOP
    usaf: int = NO_USAF
    distance: int = 0
    outfile: str = CSV_OUTPUT_FILE
    use_zlatlon: bool = False


def setup_logging(verbose: bool = False, log_file: str = LOGFILE) -> None:
    """Set up logging with console and file handlers."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=3),
        ],
    )


nauticalMilePerLat = 60.00721
nauticalMilePerLongitude = 60.10793
rad = math.pi / 180.0
metersPerNauticalMile = 1852


def metersGeoDistance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Returns calculate distance between two lat lons in meters."""
    yDistance = (lat2 - lat1) * nauticalMilePerLat
    xDistance = (
        (math.cos(lat1 * rad) + math.cos(lat2 * rad))
        * (lon2 - lon1)
        * (nauticalMilePerLongitude / 2)
    )

    distance = math.sqrt(yDistance**2 + xDistance**2)

    return distance * metersPerNauticalMile


def getLatLonByZip(zip_code: str) -> tuple[float, float] | None:
    """Returns Lat/Lon by Zip using geopy Nominatim."""
    try:
        geolocator = Nominatim(user_agent="get-weather-data")
        location = geolocator.geocode(f"{zip_code}, USA")
        if location:
            return (location.latitude, location.longitude)
        return None
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logging.warning(f"Geocoding failed for {zip_code}: {e}")
        return None


def importZip(options: Options) -> None:
    """Create and import Zip code to database table."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    try:
        c.execute(
            "CREATE TABLE zip (zipcode varchar(6) unique, city varchar(32), "
            "state varchar(4), lat real, lon real, gm_lat real, gm_lon real, "
            "diff real, zipcodetype varchar(10), locationtype varchar(10), "
            "location varchar(64), decommisioned varchar(5), "
            "taxreturnsfiled integer, estimatedpopulation integer, "
            "totalwages integer)"
        )
    except sqlite3.OperationalError:
        logging.warning("Table zip already created")

    zip_file_path = Path(US_ZIP_LIST)
    if zip_file_path.suffix == ".csv":
        with open(US_ZIP_LIST, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                try:
                    c.execute(
                        "INSERT OR IGNORE INTO zip (zipcode, city, state, lat, lon, "
                        "zipcodetype, locationtype, location, decommisioned, "
                        "taxreturnsfiled, estimatedpopulation, totalwages) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            row[0],
                            row[2],
                            row[3],
                            row[5],
                            row[6],
                            row[1],
                            row[4],
                            row[7],
                            row[8],
                            row[9] if len(row) > 9 else "",
                            row[10] if len(row) > 10 else "",
                            row[11] if len(row) > 11 else "",
                        ),
                    )
                except Exception:
                    logging.warning(f"Cannot insert row ==> {row!s}")
    else:
        logging.warning(f"Unknown zip file format: {US_ZIP_LIST}")
    conn.commit()
    conn.close()


def createStationsTable(c: sqlite3.Cursor) -> None:
    """Create table 'stations'."""
    try:
        c.execute("""CREATE TABLE stations
                     (id varchar(32) unique, name varchar(64), state varchar(4),
                      lat real, lon real, elev real, type varchar(16))""")
    except sqlite3.OperationalError:
        logging.warning("Table stations already created")


def importGHCND(options: Options) -> None:
    """Import GHCND stations list for database table."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(GHCND_STATIONS_LIST, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line[0:2].upper() != "US":
                continue
            state = line[38:40].strip()
            name = line[41:71].strip()
            line = " ".join(line.split())
            row = line.split(" ")
            try:
                c.execute(
                    "INSERT OR IGNORE INTO stations "
                    "(id, name, state, lat, lon, elev, type) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'GHCND')",
                    (row[0], name, state, float(row[1]), float(row[2]), float(row[3])),
                )
            except Exception:
                logging.warning(f"Cannot insert row ==> {row!s}")
    conn.commit()
    conn.close()


def importASOS(options: Options) -> None:
    """Import ASOS stations list for database table."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(ASOS_STATIONS_LIST, encoding="utf-8", errors="replace") as f:
        for line in f:
            country = line[89:109].strip()
            if country != "UNITED STATES":
                continue
            id = line[0:8].strip()
            name = line[27:57].strip()
            state = line[110:112].strip()
            lat = line[144:153].strip()
            lon = line[154:164].strip()
            elev = line[165:171].strip()
            try:
                c.execute(
                    "INSERT OR IGNORE INTO stations "
                    "(id, name, state, lat, lon, elev, type) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'ASOS')",
                    (id, name, state, float(lat), float(lon), float(elev)),
                )
            except Exception:
                logging.warning(f"Cannot insert row ==> {line!s}")
    conn.commit()
    conn.close()


def importCOOP(options: Options) -> None:
    """Import COOP stations list for database table."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(COOP_STATIONS_LIST, encoding="utf-8", errors="replace") as f:
        for line in f:
            country = line[38:58].strip()
            if country != "UNITED STATES":
                continue
            id = line[0:9].strip().replace(" ", "")
            state = line[59:61]
            name = line[99:130].strip()
            lat_str = line[131:139].strip()
            a = lat_str.split(" ")
            lat = float(a[0])
            if lat > 0:
                lat += (float(a[1]) + float(a[2]) / 60) / 60
            else:
                lat -= (float(a[1]) + float(a[2]) / 60) / 60
            lon_str = line[140:150].strip()
            a = lon_str.split(" ")
            lon = float(a[0])
            if lon > 0:
                lon += (float(a[1]) + float(a[2]) / 60) / 60
            else:
                lon -= (float(a[1]) + float(a[2]) / 60) / 60
            elev = line[150:156].strip()
            try:
                c.execute(
                    "INSERT OR IGNORE INTO stations "
                    "(id, name, state, lat, lon, elev, type) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'COOP')",
                    (id, name, state, float(lat), float(lon), float(elev)),
                )
            except Exception:
                logging.warning(f"Cannot insert row ==> {(name, lat, lon, elev)!s}")
    conn.commit()
    conn.close()


def importISD(options: Options) -> None:
    """Import ISD stations list for database table."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(ISD_STATIONS_LIST, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        next(reader)
        for r in reader:
            country = r[3]
            if country != "US":
                continue
            id = r[0] + "-" + r[1]
            state = r[5]
            name = r[2]
            try:
                lat: float | None = float(r[7]) / 1000.0
            except (ValueError, IndexError):
                lat = None
            try:
                lon: float | None = float(r[8]) / 1000.0
            except (ValueError, IndexError):
                lon = None
            try:
                elev: float | None = float(r[9]) / 10.0
            except (ValueError, IndexError):
                elev = None
            try:
                c.execute(
                    "INSERT OR IGNORE INTO stations "
                    "(id, name, state, lat, lon, elev, type) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'USAF-WBAN')",
                    (id, name, state, lat, lon, elev),
                )
            except Exception:
                logging.warning(f"Cannot insert row ==> {(name, lat, lon, elev)!s}")
    conn.commit()
    conn.close()


def getStations(options: Options, type: str) -> list[tuple[Any, ...]]:
    """Query stations by specific type ('GHCND', 'ASOS', 'COOP', 'USAF-WBAN')."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    if type == "ALL":
        c.execute("select rowid, id, name, lat, lon from stations")
    else:
        c.execute(
            "select rowid, id, name, lat, lon from stations where type = ?", (type,)
        )
    stations = []
    for r in c:
        stations.append(r)
    conn.close()
    return stations


def updateLatLonByGeocoding(options: Options) -> None:
    """Update lat/lon using Geocoding API."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    total = 0
    c.execute(
        "select rowid, zipcode, lat, lon, gm_lat, gm_lon from zip "
        "where gm_lat is null or gm_lon is null order by rowid"
    )
    zip_list: list[tuple[Any, ...]] = []
    for r in c:
        zip_list.append(r)
        total += 1
    n = 0
    for r in zip_list:
        n = n + 1
        logging.info(f"<{n:d}/{total:d}>")
        zid = r[0]
        zip_code = r[1]
        lat = r[2]
        lon = r[3]
        gc = getLatLonByZip(zip_code)
        logging.info(f"Geocoding API ('{zip_code!s}') ==> {gc!s}")
        if gc is None:
            logging.warning(f"No Lat/Lon data for zip: {zip_code!s}")
            continue
        gm_lat = gc[0]
        gm_lon = gc[1]
        if lat == "" or lon == "":
            c.execute(
                "update zip set gm_lat = ?, gm_lon = ? where rowid = ?",
                (gm_lat, gm_lon, zid),
            )
        else:
            try:
                distance = metersGeoDistance(float(lat), float(lon), gm_lat, gm_lon)
            except Exception:
                distance = float(sys.maxsize)
            c.execute(
                "update zip set gm_lat = ?, gm_lon = ?, diff = ? where rowid = ?",
                (gm_lat, gm_lon, distance, zid),
            )
        conn.commit()
    conn.commit()
    conn.close()


def sortedStationsDistance(
    lat: float, lon: float, stations: list[tuple[Any, ...]]
) -> list[tuple[int, Any]]:
    """Returns stations list sorted by distance from specific lat/lon."""
    if KDTREE_AVAILABLE and len(stations) > 100:
        valid_stations = [(s[0], s[3], s[4]) for s in stations if s[3] and s[4]]
        if valid_stations:
            coords = np.array([(s[1], s[2]) for s in valid_stations])
            tree = cKDTree(coords)
            distances, indices = tree.query([lat, lon], k=len(coords))
            if isinstance(distances, float):
                distances = [distances]
                indices = [indices]
            return [
                (int(d * 111000), valid_stations[i][0])
                for d, i in zip(distances, indices)
            ]

    dist: list[tuple[int, Any]] = []
    for s in stations:
        sid = s[0]
        if s[3] is None or s[4] is None:
            continue
        lat2 = float(s[3])
        lon2 = float(s[4])
        try:
            distance = metersGeoDistance(lat, lon, lat2, lon2)
        except Exception:
            distance = float(sys.maxsize)
        dist.append((int(distance), sid))
    return sorted(dist)


def updateClosestStations(options: Options) -> None:
    """Find closest weather station and update to table 'closest'."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createClosestTable(c)

    c.execute("select max(zid) from closest")
    r = c.fetchone()
    if r[0] is None:
        last_zid = 0
    else:
        last_zid = r[0]
    c.execute(
        "select rowid, zipcode, lat, lon, gm_lat, gm_lon from zip "
        "where rowid > ? and ((lat <> '' and lon <> '') or "
        "(gm_lat is not null and gm_lon is not null)) order by rowid",
        (last_zid,),
    )
    total = 0
    zip_list: list[tuple[Any, ...]] = []
    for r in c:
        zip_list.append(r)
        total += 1
    n = 0
    for r in zip_list:
        n = n + 1
        logging.info(f"<{n:d}/{total:d}>")
        zid = r[0]
        lat = r[2]
        lon = r[3]
        if options.use_zlatlon:
            gm_lat = None
            gm_lon = None
        else:
            gm_lat = r[4]
            gm_lon = r[5]
        if gm_lat is not None and gm_lon is not None:
            lat1 = gm_lat
            lon1 = gm_lon
        elif lat != "" and lon != "":
            lat1 = float(lat)
            lon1 = float(lon)
        else:
            continue
        if options.distance != 0:
            stations = getStations(options, "ALL")
            dist = sortedStationsDistance(lat1, lon1, stations)
            for d in dist:
                if d[0] <= options.distance * 1000:
                    c.execute(
                        "INSERT OR IGNORE INTO closest (zid, sid, distance) "
                        "VALUES (?, ?, ?)",
                        (zid, d[1], d[0]),
                    )
                else:
                    break
        else:
            for a, b in [
                ("GHCND", options.ghcn),
                ("USAF-WBAN", options.usaf),
                ("COOP", options.coop),
            ]:
                stations = getStations(options, a)
                dist = sortedStationsDistance(lat1, lon1, stations)
                for d in dist[:b]:
                    c.execute(
                        "INSERT OR IGNORE INTO closest (zid, sid, distance) "
                        "VALUES (?, ?, ?)",
                        (zid, d[1], d[0]),
                    )
        conn.commit()
    conn.commit()
    conn.close()


def createClosestTable(c: sqlite3.Cursor) -> None:
    """Create closest table."""
    try:
        c.execute("""CREATE TABLE closest (zid INT, sid INT, distance FLOAT,
               UNIQUE(zid, sid) ON CONFLICT REPLACE)""")
    except sqlite3.OperationalError:
        logging.warning("Table closest already created")


def dropClosestTable(options: Options) -> None:
    """Drop the closest table."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    try:
        c.execute("drop table closest")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()
    logging.info("Drop 'closest' table completed")


def clearGoogleLatLon(options: Options) -> None:
    """Clear Google Maps lat/lon data."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    try:
        c.execute("update zip set gm_lat = null, gm_lon = null")
    except Exception:
        pass
    conn.commit()
    conn.close()
    logging.info("Clear Google Maps Lat/Lon completed")


def exportClosestStations(options: Options) -> None:
    """Export closest stations for each zip code to CSV file."""
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    c2 = conn.cursor()

    try:
        c.execute("select max(n) from (select count(*) n from closest group by zid)")
        r = c.fetchone()
        max_station = r[0]
    except Exception:
        logging.warning(
            "No closest station in database, please run the script with -c to update"
        )
        conn.close()
        return

    try:
        csvfile = open(options.outfile, "w", newline="", encoding="utf-8")
        csvwriter = csv.writer(
            csvfile,
            dialect="excel",
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
        )
    except Exception:
        logging.error("Cannot create output file")
        sys.exit(-1)

    headers = [
        "zip",
        "lat",
        "lon",
        "gm_lat",
        "gm_lon",
        "diff",
        "city",
        "state",
        "zipcodetype",
        "locationtype",
        "location",
        "decommisioned",
        "taxreturnsfiled",
        "estimatedpopulation",
        "totalwages",
    ]
    for i in range(max_station):
        headers.append(f"st{i + 1:d}_id")
        headers.append(f"st{i + 1:d}_name")
        headers.append(f"st{i + 1:d}_dist")
    csvwriter.writerow(headers)

    c.execute(
        "select rowid, zipcode, lat, lon, gm_lat, gm_lon, diff, city, state, "
        "zipcodetype, locationtype, location, decommisioned, taxreturnsfiled, "
        "estimatedpopulation, totalwages from zip order by rowid"
    )
    for r in c:
        a: list[Any] = []
        logging.info(f"Export zip: {r[1]!s}")
        a += list(r[1:])
        c2.execute(
            "select id, name, distance from closest c join stations s "
            "on c.sid = s.rowid where c.zid = ? order by c.distance",
            (r[0],),
        )
        i = 0
        for f in c2:
            a += list(f)
            i += 1
            if i >= max_station:
                break
        csvwriter.writerow(a)
    conn.close()
    csvfile.close()


def download(url: str, local: str) -> None:
    """Download a file from URL to local path."""
    logging.info(f"Downloading '{url}'...")
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()
        Path(local).write_bytes(response.content)


def download_and_convert_zipcode_data(url: str, output_csv: str) -> None:
    """Download geonames zip file and convert to CSV format."""
    logging.info(f"Downloading zip code data from '{url}'...")
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        response = client.get(url)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            us_txt = None
            for name in zf.namelist():
                if name.endswith(".txt") and not name.startswith("readme"):
                    us_txt = name
                    break

            if not us_txt:
                raise ValueError("US.txt not found in zip file")

            with zf.open(us_txt) as infile:
                content = infile.read().decode("utf-8")

                with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow(
                        [
                            "Zipcode",
                            "ZipCodeType",
                            "City",
                            "State",
                            "LocationType",
                            "Lat",
                            "Long",
                            "Location",
                            "Decommissioned",
                            "TaxReturnsFiled",
                            "EstimatedPopulation",
                            "TotalWages",
                        ]
                    )

                    for line in content.splitlines():
                        parts = line.strip().split("\t")
                        if len(parts) >= 11:
                            zipcode = parts[1]
                            city = parts[2]
                            state = parts[4]
                            lat = parts[9]
                            lon = parts[10]
                            county = parts[5] if len(parts) > 5 else ""

                            writer.writerow(
                                [
                                    zipcode,
                                    "STANDARD",
                                    city,
                                    state,
                                    "",
                                    lat,
                                    lon,
                                    county,
                                    "FALSE",
                                    "",
                                    "",
                                    "",
                                ]
                            )

    logging.info(f"Converted zip code data saved to '{output_csv}'")


def signal_handler(sig: int, frame: Any) -> None:  # noqa: ARG001
    """Handle Ctrl+C signal."""
    del sig, frame
    logging.info("You pressed Ctrl+C!")
    os._exit(1)


@click.group()
@click.option(
    "-D",
    "--database",
    default=SQLITE_DB_NAME,
    help=f"Database name (default: {SQLITE_DB_NAME})",
)
@click.option("-v", "--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def cli(ctx: click.Context, database: str, verbose: bool) -> None:
    """ZIP to Weather Station mapper."""
    ctx.ensure_object(dict)
    ctx.obj["database"] = database
    setup_logging(verbose)
    signal.signal(signal.SIGINT, signal_handler)


@cli.command("import")
@click.pass_context
def import_data(ctx: click.Context) -> None:
    """Import ZIP and station data."""
    database = ctx.obj["database"]
    console.print("[bold]Importing data...[/bold]")

    if not os.path.exists(US_ZIP_LIST):
        download_and_convert_zipcode_data(FREE_ZIPCODE_DOWNLOAD_URL, US_ZIP_LIST)

    if not os.path.exists(GHCND_STATIONS_LIST):
        download(GHCND_STATIONS_LIST_URL, GHCND_STATIONS_LIST)

    if not os.path.exists(ISD_STATIONS_LIST):
        download(ISD_STATIONS_LIST_URL, ISD_STATIONS_LIST)

    options = Options(database=database)
    importZip(options)
    importGHCND(options)
    importISD(options)
    console.print("[green]Import completed.[/green]")


@cli.command()
@click.option(
    "--ghcn", default=NO_GHCN, type=int, help="Number of closest GHCN stations"
)
@click.option(
    "--usaf", default=NO_USAF, type=int, help="Number of closest USAF stations"
)
@click.option(
    "--coop", default=NO_COOP, type=int, help="Number of closest COOP stations"
)
@click.option(
    "-d", "--distance", default=0, type=int, help="Maximum distance (km) from ZIP"
)
@click.option("--use-zlatlon", is_flag=True, help="Use ZIP lat/lon instead of geocoded")
@click.pass_context
def closest(
    ctx: click.Context,
    ghcn: int,
    usaf: int,
    coop: int,
    distance: int,
    use_zlatlon: bool,
) -> None:
    """Calculate closest stations for each ZIP."""
    options = Options(
        database=ctx.obj["database"],
        ghcn=ghcn,
        usaf=usaf,
        coop=coop,
        distance=distance,
        use_zlatlon=use_zlatlon,
    )
    console.print("[bold]Calculating closest stations...[/bold]")
    updateClosestStations(options)
    console.print("[green]Closest stations updated.[/green]")


@cli.command()
@click.option("-o", "--outfile", default=CSV_OUTPUT_FILE, help="Output CSV file path")
@click.pass_context
def export(ctx: click.Context, outfile: str) -> None:
    """Export closest stations to CSV."""
    options = Options(database=ctx.obj["database"], outfile=outfile)
    console.print(f"[bold]Exporting to {outfile}...[/bold]")
    exportClosestStations(options)
    console.print("[green]Export completed.[/green]")


@cli.command()
@click.pass_context
def geocode(ctx: click.Context) -> None:
    """Update lat/lon using Geocoding API."""
    options = Options(database=ctx.obj["database"])
    console.print("[bold]Geocoding ZIP codes...[/bold]")
    updateLatLonByGeocoding(options)
    console.print("[green]Geocoding completed.[/green]")


@cli.command("drop-closest")
@click.pass_context
def drop_closest(ctx: click.Context) -> None:
    """Drop the closest table."""
    options = Options(database=ctx.obj["database"])
    dropClosestTable(options)


@cli.command("clear-glatlon")
@click.pass_context
def clear_glatlon(ctx: click.Context) -> None:
    """Clear geocoded lat/lon data."""
    options = Options(database=ctx.obj["database"])
    clearGoogleLatLon(options)


def main(argv: list[str] | None = None) -> None:
    """Main entry point."""
    if argv is None:
        cli()
    else:
        cli(argv[1:])


if __name__ == "__main__":
    main()
