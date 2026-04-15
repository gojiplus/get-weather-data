"""
Get weather data from weather station nearest to a zip code using NOAA web service

Get token from http://www.ncdc.noaa.gov/cdo-web/token
Set NCDC_TOKEN environment variable with the token you get

"""

from __future__ import annotations

import csv
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from logging.handlers import RotatingFileHandler

import click
import httpx
from rich.console import Console

CSV_OUTPUT_FILE = "output.csv"
NCDC_TOKEN = os.environ.get("NCDC_TOKEN")
LOGFILE = "noaaweb.log"

console = Console()


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


def get_station_id(data: list[ET.Element]) -> str:
    """Get the most common station ID from data elements."""
    stations: dict[str, int] = {}
    for d in data:
        station_elem = d.find("station")
        if station_elem is not None and station_elem.text:
            id = station_elem.text
            if id in stations:
                stations[id] += 1
            else:
                stations[id] = 1

    max_count = 0
    station_id = ""
    for k, v in stations.items():
        if max_count < v:
            max_count = v
            station_id = k
    logging.info("stations: " + str(stations) + " ---> use station: " + station_id)
    return station_id


def get_content(uri: str, string_log: str | None = None) -> ET.Element:
    """Fetch XML content from URI with retry logic."""
    while True:
        time.sleep(1)
        if string_log is not None:
            logging.info(string_log + uri)
        with httpx.Client(timeout=500.0) as client:
            response = client.get(uri)
            response.raise_for_status()
            resource = response.content
        root = ET.fromstring(resource)
        if root.tag == "cdoError":
            time.sleep(5)
            name_elem = root.find("name")
            msg_elem = root.find("message")
            name_text = name_elem.text if name_elem is not None else "Unknown"
            msg_text = msg_elem.text if msg_elem is not None else "Unknown"
            logging.warning(f"name: {name_text}, message: {msg_text}")
        else:
            break
    return root


def get_GHCND(zipcode: str, year: str, month: str, day: str, token: str) -> list[str]:
    """Get GHCND weather data for a given location and date."""
    orders = [
        "AWND",
        "DAPR",
        "FRGT",
        "FRTH",
        "GAHT",
        "MDPR",
        "PGTM",
        "PRCP",
        "SNOW",
        "SNWD",
        "THIC",
        "TMAX",
        "TMIN",
        "TOBS",
        "WDFG",
        "WESD",
        "WESF",
        "WSFG",
        "WT01",
        "WT03",
        "WT04",
        "WT05",
        "WT06",
        "WT07",
        "WT08",
        "WT09",
        "WT11",
        "WT14",
        "WT16",
        "WT18",
    ]
    values: dict[str, str] = {}
    result: list[str] = ["" for _ in range(8)]

    uri = (
        f"http://www.ncdc.noaa.gov/cdo-services/services/datasets/GHCND/"
        f"locations/ZIP:{zipcode}/data?year={year}&month={month}&day={day}"
        f"&_type=xml&token={token}"
    )
    root = get_content(uri, "GHCND uri: ")

    if root.get("pageCount") is not None:
        page_count = int(root.get("pageCount", "1"))
        data = root.findall(f".//*[date='{year}-{month}-{day}T00:00:00.000']")
        for i in range(2, page_count + 1):
            uri2 = uri + "&page=" + str(i)
            root = get_content(uri2, f"GHCND uri ({i}): ")
            data.extend(root.findall(f".//*[date='{year}-{month}-{day}T00:00:00.000']"))

        ghcnd_id = get_station_id(data)
        if ghcnd_id != "":
            for d in data:
                station_elem = d.find("station")
                if station_elem is not None and ghcnd_id == station_elem.text:
                    data_type_elem = d.find("dataType")
                    value_elem = d.find("value")
                    if data_type_elem is not None and value_elem is not None:
                        if data_type_elem.text and value_elem.text:
                            values[data_type_elem.text] = value_elem.text

            logging.info("values: " + str(values))
            result = get_station_information(ghcnd_id, "GHCND", token)
        else:
            logging.warning("No station GHCND")

    for o in orders:
        result.append(values.get(o, ""))

    return result


def get_station_information(station_id: str, dataset: str, token: str) -> list[str]:
    """Get station information from NOAA API."""
    uri = (
        f"http://www.ncdc.noaa.gov/cdo-services/services/datasets/{dataset}/"
        f"stations/{station_id}?token={token}&_type=xml"
    )
    root = get_content(uri)

    station = root.find(f".//*[id='{station_id}']")
    if station is None:
        return [""] * 8

    display_name_elem = station.find("displayName")
    display_name = display_name_elem.text if display_name_elem is not None else ""

    lat_elem = station.find("latitude")
    lat = lat_elem.text if lat_elem is not None else ""

    long_elem = station.find("longitude")
    long = long_elem.text if long_elem is not None else ""

    cnty = station.find("*[type='CNTY']")
    if cnty is not None:
        cnty_id_elem = cnty.find("id")
        cnty_str = (
            cnty_id_elem.text.replace("FIPS:", "")
            if cnty_id_elem is not None and cnty_id_elem.text
            else ""
        )
    else:
        cnty_str = ""

    st = station.find("*[type='ST']")
    if st is not None:
        st_id_elem = st.find("id")
        st_str = (
            st_id_elem.text.replace("FIPS:", "")
            if st_id_elem is not None and st_id_elem.text
            else ""
        )
    else:
        st_str = ""

    zip_elem = station.find("*[type='ZIP']")
    if zip_elem is not None:
        zip_id_elem = zip_elem.find("id")
        zip_id = (
            zip_id_elem.text.replace("ZIP:", "")
            if zip_id_elem is not None and zip_id_elem.text
            else ""
        )
        zip_display_elem = zip_elem.find("displayName")
        zip_display_name = zip_display_elem.text if zip_display_elem is not None else ""
    else:
        zip_id = ""
        zip_display_name = ""

    return [
        station_id,
        display_name or "",
        lat or "",
        long or "",
        cnty_str,
        st_str,
        zip_id,
        zip_display_name or "",
    ]


def get_PRECIP_HLY(
    zipcode: str, year: str, month: str, day: str, token: str
) -> list[str]:
    """Get hourly precipitation data for a given location and date."""
    values: dict[int, str] = {}
    uri = (
        f"http://www.ncdc.noaa.gov/cdo-services/services/datasets/PRECIP_HLY/"
        f"locations/ZIP:{zipcode}/datatypes/HPCP/data?year={year}&month={month}"
        f"&day={day}&token={token}&_type=xml"
    )
    root = get_content(uri, "PRECIP_HLY uri: ")

    result: list[str] = ["" for _ in range(8)]
    if root.get("pageCount") is not None:
        page_count = int(root.get("pageCount", "1"))
        data: list[ET.Element] = []
        for child in root:
            date_elem = child.find("date")
            if date_elem is not None and date_elem.text:
                if date_elem.text.find(f"{year}-{month}-{day}T") != -1:
                    data.append(child)
        for i in range(2, page_count + 1):
            uri2 = uri + "&page=" + str(i)
            root = get_content(uri2, f"PRECIP_HLY uri ({i:d}): ")

            for child in root:
                date_elem = child.find("date")
                if date_elem is not None and date_elem.text:
                    if date_elem.text.find(f"{year}-{month}-{day}T") != -1:
                        data.append(child)

        coop_id = get_station_id(data)
        if coop_id != "":
            for d in data:
                station_elem = d.find("station")
                if station_elem is not None and coop_id == station_elem.text:
                    date_elem = d.find("date")
                    value_elem = d.find("value")
                    if date_elem is not None and date_elem.text:
                        if value_elem is not None and value_elem.text:
                            logging.info(f"{date_elem.text} {value_elem.text}")
                            values[int(date_elem.text[11:13])] = value_elem.text

            logging.info("values: " + str(values))
            result = get_station_information(coop_id, "PRECIP_HLY", token)
        else:
            logging.warning("PRECIP_HLY No station")

    for o in range(0, 24):
        result.append(values.get(o, ""))

    return result


def load_save_csvfile(infilename: str, outfilename: str) -> None:
    """Load input CSV and save weather data to output CSV."""
    token = NCDC_TOKEN
    total_rows = 0
    start_line = (
        '"","uniqid","zip","year","month","day","GHCND id","Display Name",'
        '"Lat","Long","FIPS (CNTY)","FIPS (ST)","ZIP","ZIP Display Name",'
        '"AWND - Average daily wind speed (tenths of meters per second)",'
        '"DAPR - Number of days included in the multiday precipitation total (MDPR)",'
        '"FRGT - Top of frozen ground layer (cm)",'
        '"FRTH - Thickness of frozen ground layer (cm)",'
        '"GAHT - Difference between river and gauge height (cm)",'
        '"MDPR - Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)",'
        '"PGTM - Peak gust time (hours and minutes, i.e., HHMM)",'
        '"PRCP - Precipitation (tenths of mm)","SNOW - Snowfall (mm)",'
        '"SNWD - Snow depth (mm)","THIC - Thickness of ice on water (tenths of mm)",'
        '"TMAX - Maximum temperature (tenths of degrees C)",'
        '"TMIN - Minimum temperature (tenths of degrees C)",'
        '"TOBS - Temperature at the time of observation (tenths of degrees C)",'
        '"WDFG - Direction of peak wind gust (degrees)",'
        '"WESD - Water equivalent of snow on the ground (tenths of mm)",'
        '"WESF - Water equivalent of snowfall (tenths of mm)",'
        '"WSFG - Peak guest wind speed (tenths of meters per second)",'
        '"WT01 - Fog, ice fog, or freezing fog (may include heavy fog)",'
        '"WT03 - Thunder","WT04 - Ice pellets, sleet, snow pellets, or small hail",'
        '"WT05 - Hail (may include small hail)",'
        '"WT06 - Glaze or rime",'
        '"WT07 - Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction",'
        '"WT08 - Smoke or haze",'
        '"WT09 - Blowing or drifting snow","WT11 - High or damaging winds",'
        '"WT14 - Drizzle","WT16 - Rain (may include freezing rain, drizzle, and freezing drizzle)",'
        '"WT18 - Snow, snow pellets, snow grains, or ice crystals",'
        '"COOP id","Display Name","Lat","Long","FIPS (CNTY)","FIPS (ST)",'
        '"ZIP","ZIP Display Name","00:00 - HPCP (Precipitation (100th of an inch))",'
        '"01:00 - HPCP",'
        '"02:00 - HPCP","03:00 - HPCP","04:00 - HPCP","05:00 - HPCP",'
        '"06:00 - HPCP","07:00 - HPCP","08:00 - HPCP","09:00 - HPCP",'
        '"10:00 - HPCP","11:00 - HPCP",'
        '"12:00 - HPCP","13:00 - HPCP","14:00 - HPCP","15:00 - HPCP",'
        '"16:00 - HPCP","17:00 - HPCP","18:00 - HPCP",'
        '"19:00 - HPCP","20:00 - HPCP","21:00 - HPCP","22:00 - HPCP",'
        '"23:00 - HPCP"\n'
    )
    try:
        with open(outfilename, "r", encoding="utf-8") as output:
            for _ in output:
                total_rows += 1
    except IOError:
        pass

    if total_rows == 0:
        with open(outfilename, "w", encoding="utf-8") as output:
            output.write(start_line)
        total_rows = 1

    with open(outfilename, "a", newline="", encoding="utf-8") as output:
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)

        with open(infilename, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            j = -1
            for row in reader:
                j += 1
                if j < total_rows:
                    continue

                zipcode = row[2]
                year = row[3]
                month = row[4]
                day = row[5]

                logging.info("row: " + str(row))

                row1 = row.copy()
                try:
                    row.extend(get_GHCND(zipcode, year, month, day, token or ""))
                    row.extend(get_PRECIP_HLY(zipcode, year, month, day, token or ""))
                except httpx.HTTPStatusError as e:
                    logging.error(str(e))
                    logging.warning("Wait 5 minutes to continue")
                    row = row1
                    time.sleep(5 * 60)
                    try:
                        row.extend(get_GHCND(zipcode, year, month, day, token or ""))
                        row.extend(
                            get_PRECIP_HLY(zipcode, year, month, day, token or "")
                        )
                    except httpx.HTTPStatusError as e1:
                        logging.error(str(e1))
                        logging.error("Exit!!! Wait few hours to continue again")
                        break
                logging.info("result: " + str(row))
                writer.writerow(row)
                time.sleep(2)


@click.command()
@click.argument("inputfile", required=False)
@click.option(
    "-o",
    "--outfile",
    default=CSV_OUTPUT_FILE,
    help=f"CSV Output file name (default: {CSV_OUTPUT_FILE})",
)
@click.option("-v", "--verbose", is_flag=True, help="Verbose output")
def cli(inputfile: str | None, outfile: str, verbose: bool) -> None:
    """Get weather data from NOAA web service."""
    setup_logging(verbose)
    if not inputfile:
        console.print("[red]Please specify input file[/red]")
        sys.exit(-1)
    else:
        load_save_csvfile(inputfile, outfile)


def main(argv: list[str] | None = None) -> None:
    """Main entry point."""
    if argv is None:
        cli()
    else:
        cli(argv[1:])


if __name__ == "__main__":
    main()
