#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import urllib
import re
import time
import math
import sqlite3
import sys
import gzip
import logging
import csv

from datetime import datetime, timedelta
from pkg_resources import resource_filename

# Python 2 and 3
try:
    from urllib.request import urlopen
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import urlopen, HTTPError


"""Constants
"""
nauticalMilePerLat = 60.00721
nauticalMilePerLongitude = 60.10793
rad = math.pi / 180.0
metersPerNauticalMile = 1852

STATION_INFO_COLS = ["sid", "type", "name", "lat", "lon", "nth", "distance"]

USAF_WBAN_DATA = [('TEMP', 24, 30), ('DEWP', 35, 41), ('SLP', 46, 52),
                  ('STP', 57, 63), ('VISIB', 68, 73), ('AWND', 78, 83),
                  ('MXSPD', 88, 93), ('GUST', 95, 100), ('TMAX', 102, 108),
                  ('MAXF', 108, 109), ('TMIN', 110, 116), ('MINF', 116, 117),
                  ('PRCP', 118, 123), ('PRCPF', 123, 124), ('SNWD', 125, 130),
                  ('FRSHTT', 132, 138)]


def download(url, local):
    print("Downloading '{:s}'...".format(url))
    start = time.time()
    response = urlopen(url)
    content = response.read()
    with open(local, 'wb') as f:
        f.write(content)
    elapse = time.time() - start
    print("Elapse time: {:f} seconds".format(elapse))


def build_ghcn_database(args, year, dbname):
    conn = None
    try:
        local = os.path.join(args.dbpath, "{:s}.csv.gz".format(year))
        if not os.path.exists(local):
            if not os.path.exists(args.dbpath):
                os.makedirs(args.dbpath)
            url = "ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{:s}.csv.gz".format(year)
            download(url, local)
        conn = sqlite3.connect(dbname)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS `ghcn_{:s}` (
`id` VARCHAR(12) NOT NULL,
`date` VARCHAR(8) NOT NULL,
`element` VARCHAR(4) NULL,
`value` VARCHAR(6) NULL,
`m_flag` VARCHAR(1) NULL,
`q_flag` VARCHAR(1) NULL,
`s_flag` VARCHAR(1) NULL,
`obs_time` VARCHAR(4) NULL)""".format(year))

        c.execute("""create index if not exists idx_id_time
on ghcn_{:s} (id, date)""".format(year))

        c.execute("PRAGMA journal_mode = OFF")
        c.execute("PRAGMA synchronous = OFF")
        # Fastest way if no memory limit
        #c.execute("PRAGMA temp_store = MEMORY")
        # Taking about 1GB memory (N * 1024)
        c.execute("PRAGMA cache_size = 1000000")
        print("Importing...")
        start = time.time()
        with gzip.open(local, 'rb') as f:
            reader = csv.reader(f)
            c.executemany("""INSERT OR IGNORE INTO ghcn_{:s}
(id, date, element, value, m_flag, q_flag, s_flag, obs_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".format(year), reader)
        elapse = time.time() - start
        print("Import time: {:f} seconds".format(elapse))
    except Exception as e:
        logging.error(str(e))
        raise
    finally:
        if conn:
            print("Commiting...")
            start = time.time()
            conn.commit()
            conn.close()
            elapse = time.time() - start
            print("Commit time: {:f} seconds".format(elapse))


class WeatherByZip(object):

    not_found_list = []

    def get_stations(self):
        """Returns all stations from database
        """
        conn = sqlite3.connect(self.args.zip2ws_db)
        c = conn.cursor()
        c.execute("select rowid, id, name, lat, lon, type from stations")
        stations = []
        for r in c:
            stations.append(r)
        conn.close()
        return stations

    def __init__(self, args):
        self.args = args
        try:
            with open(args.columns, 'rb') as f:
                self.output_columns = [r.strip() for r in f.readlines()
                                       if r[0] != '#']
        except:
            args.columns = resource_filename(__name__, args.columns)
            with open(args.columns, 'rb') as f:
                self.output_columns = [r.strip() for r in f.readlines()
                                       if r[0] != '#']

        self.stations = self.get_stations()
        logging.info('Number of Stations = {:d}'.format(len(self.stations)))

    def search_weather_data(self, dist, data):
        year = "{:04d}" .format(data['year'])
        month = "{:02d}".format(data['month'])
        day = "{:02d}".format(data['day'])
        logging.debug("Date: {:s}/{:s}/{:s}".format(year, month, day))
        found = 0
        nth = 0
        raw = []
        values = {}
        current_year = ""
        orders = self.output_columns
        for s in dist:
            if self.args.nth > 0 and nth >= self.args.nth:
                logging.info("Reach maximum n-th stations: {:d}".format(nth))
                return values
            if self.args.distance > 0 and s[0] >= self.args.distance * 1000:
                logging.info("Reach maximum distance: {:d}"
                             .format(self.args.distance))
                return values
            nth += 1
            values['nth'] = nth
            sid = s[1]
            stype = s[2]
            values['sid'] = sid
            values['type'] = stype
            values['name'] = s[3]
            values['lat'] = s[4]
            values['lon'] = s[5]
            values['distance'] = s[0]
            if stype == 'GHCND':
                if not self.args.uses_sqlite:
                    datadir = './data/ghcn-daily/all/'
                    if not os.path.exists(datadir):
                        os.makedirs(datadir)
                    datafile = datadir + '{:s}.dly'.format(sid)
                    urlfile = ('ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/'
                               'all/{:s}.dly'.format(sid))
                    if not self.download_data_file(urlfile, datafile):
                        continue
                    search = sid + year + month
                    match = False
                    with open(datafile, 'rb') as df:
                        for l in df:
                            if (l[:17] == search):
                                element = l[17:21]
                                offset = 21 + int(day) * 8 - 8
                                value = l[offset:offset + 5]
                                mflag = l[offset + 5:offset + 6]
                                qflag = l[offset + 6:offset + 7]
                                sflag = l[offset + 7:offset + 8]
                                logging.debug("<{:s}> <{:s}> <{:s}> <{:s}>"
                                              " <{:s}>".format(element, value,
                                                               mflag, qflag,
                                                               sflag))
                                match = True
                                if value != '-9999' and (element in orders):
                                    if not values.get(element):
                                        values[element] = value
                                        found += 1
                                        raw.append(l)
                            elif match:
                                break
                else:
                    if current_year != year:
                        current_year = year
                        dbname = os.path.join(self.args.dbpath,
                                              "ghcn_{:s}.sqlite3".format(year))
                        if not os.path.exists(dbname):
                            build_ghcn_database(self.args, year, dbname)
                        conn2 = sqlite3.connect(dbname)
                        c2 = conn2.cursor()
                        # FIXME: try to optimize with these options
                        c2.execute("PRAGMA journal_mode = MEMORY")
                        c2.execute("PRAGMA synchronous = OFF")
                        c2.execute("PRAGMA temp_store = MEMORY")
                        c2.execute("PRAGMA cache_size = 500000")
                    ghcn_values = self.get_ghcn_data(c2, sid, year, month, day)
                    for element in ghcn_values:
                        value = ghcn_values[element]
                        if value != '-9999' and (element in orders):
                            if not values.get(element):
                                found += 1
                                values[element] = value
            elif stype == 'USAF-WBAN':
                datadir = './data/gsod/{:s}/'.format(year)
                if not os.path.exists(datadir):
                    os.makedirs(datadir)
                datafile = datadir + '{:s}-{:s}.op.gz'.format(sid, year)
                urlfile = ('ftp://ftp2.ncdc.noaa.gov/pub/data/gsod/'
                           '{:s}/{:s}-{:s}.op.gz'.format(year, sid, year))
                if not self.download_data_file(urlfile, datafile):
                    continue
                search = year + month + day
                with gzip.open(datafile, 'rb') as df:
                    for l in df:
                        if l[14:22] == search:
                            match = False
                            for d in USAF_WBAN_DATA:
                                element = d[0]
                                value = l[d[1]:d[2]]
                                if element in ['TMAX', 'TMIN']:
                                    if value != '9999.9':
                                        value = self.f2c(float(value)) * 10
                                elif element in ['AWND']:
                                    if value != '999.9':
                                        value = self.kn2ms(float(value)) * 10
                                if (not values.get(element) and
                                   (element in orders)):
                                    found += 1
                                    values[element] = value
                                    match = True
                            if match:
                                raw.append(l)
                            break
            if found >= len(orders):
                break
        logging.debug(values)
        # For debug only
        # values['RAW'] = ''.join(raw)
        return values

    def search(self, search):
        zipcode = '0' * (5 - len(search['zip'])) + search['zip']

        logging.info("Search for: '{:s}'".format(zipcode))

        from_date = datetime(year=search['from.year'],
                             month=search['from.month'],
                             day=search['from.day'])
        to_date = datetime(year=search['to.year'],
                           month=search['to.month'],
                           day=search['to.day'])
        columns = (['uniqid', 'zip', 'year', 'month', 'day'] +
                   STATION_INFO_COLS + self.output_columns)
        results = []
        conn = sqlite3.connect(self.args.zip2ws_db)
        c = conn.cursor()
        c.execute('select zipcode, lat, lon, gm_lat, gm_lon, rowid from zip'
                  ' where zipcode = ?', (zipcode,))
        r = c.fetchone()
        dist = self.sort_stations(zipcode, r, self.stations)
        while from_date <= to_date:
            data = {}
            data['uniqid'] = search['uniqid']
            data['zip'] = zipcode
            data['year'] = from_date.year
            data['month'] = from_date.month
            data['day'] = from_date.day
            if dist and len(dist) > 0:
                wdata = self.search_weather_data(dist, data)
                data.update(wdata)
            results.append(data)
            from_date += timedelta(days=1)
        return results

    def download_data_file(self, url, file):
        """Download weather data file from server if it not exist in local directory
        """
        if url in self.not_found_list:
            logging.warn("This URL no data on server {:s}".format(url))
            return False
        if not os.path.exists(file):
            retry = 0
            while True:
                try:
                    logging.info("Downloading '{:s}'".format(url))
                    urllib.urlretrieve(url, file)
                    return True
                except Exception as e:
                    m = re.match(".*\s(\d\d\d)\s.*", str(e))
                    if m:
                        logging.warn("FTP server error code = {:s}"
                                     .format(m.group(1)))
                        if m.group(1) == '550':
                            self.not_found_list.append(url)
                            return False
                    logging.warn("Unknown FTP server error = {:s}"
                                 .format(str(e)))
                    if retry < 5:
                        retry += 1
                        logging.info("Retry #{:d}: waiting...({:d}s)"
                                     .format(retry, retry * 10))
                        time.sleep(retry * 10)
                    else:
                        logging.warn("Cannot download data from URL = {:s}"
                                     .format(url))
                        return False
        return True

    def metersGeoDistance(self, lat1, lon1, lat2, lon2):
        """Returns calculate distance between two lat lons in meters
        """
        yDistance = (lat2 - lat1) * nauticalMilePerLat
        xDistance = ((math.cos(lat1 * rad) + math.cos(lat2 * rad)) *
                     (lon2 - lon1) * (nauticalMilePerLongitude / 2))

        distance = math.sqrt(yDistance**2 + xDistance**2)

        return distance * metersPerNauticalMile

    def f2c(self, f):
        """Convert Fahrenheit to Celsius
        """
        return (f - 32) * 5.0/9.0

    def kn2ms(self, kn):
        """Convert Knots to m/s
        """
        return 0.51444 * kn

    def sort_stations(self, zipcode, r, stations):
        """Returns sorted stations list by distance
        """
        if r is None:
            logging.warn("zipcode = {:s} not found".format(zipcode))
            return None
        lat1 = r[3]
        if lat1 is None:
            lat1 = r[1]
        lon1 = r[4]
        if lon1 is None:
            lon1 = r[2]
        if lat1 == '' or lon1 == '':
            logging.warn("not lat/lon for zipcode = {:s}".format(zipcode))
            return None
        lat1 = float(lat1)
        lon1 = float(lon1)
        dist = []
        for s in stations:
            # sid = s[0]
            id = s[1]
            name = s[2]
            stype = s[5]
            if s[3] is None or s[4] is None:
                continue
            lat2 = float(s[3])
            lon2 = float(s[4])
            try:
                distance = self.metersGeoDistance(lat1, lon1, lat2, lon2)
            except:
                distance = sys.maxint
            dist.append((int(distance), id, stype, name, lat2, lon2))
        return sorted(dist)

    def get_ghcn_data(self, cursor, sid, year, month, day):
        cursor.execute("select * from ghcn_{:s} where id = ? and date = ?"
                       .format(year), (sid, '{:02d}{:02d}{:02d}'
                                       .format(int(year), int(month),
                                               int(day))))
        values = {}
        for r in cursor:
            values[r[2]] = r[3]
        return values
