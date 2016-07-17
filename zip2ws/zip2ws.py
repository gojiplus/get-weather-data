#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import signal
import logging
import optparse
import math
import sqlite3
import csv
import pygeocoder
from pygeocoder import Geocoder

# Python 2 and 3
try:
    from urllib.request import urlopen
    from urllib.error import HTTPError
except ImportError:
    from urllib2 import urlopen, HTTPError

from pkg_resources import resource_filename

"""Script default configuration
"""
FREE_ZIPCODE_DOWNLOAD_URL = "http://federalgovernmentzipcodes.us/free-zipcode-database-Primary.csv"
GHCND_STATIONS_LIST_URL   = "http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
ISD_STATIONS_LIST_URL     = "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"

US_ZIP_LIST         =   resource_filename(__name__, "data/free-zipcode-database-primary.csv")
GHCND_STATIONS_LIST =   resource_filename(__name__, "data/ghcnd-stations.txt")
ASOS_STATIONS_LIST  =   resource_filename(__name__, "inventories/asos-stations.txt")
COOP_STATIONS_LIST  =   resource_filename(__name__, "inventories/coop-act.txt")
ISD_STATIONS_LIST   =   resource_filename(__name__, "data/isd-history.csv")

SQLITE_DB_NAME      =   resource_filename(__name__, "data/zip2ws.sqlite")
CSV_OUTPUT_FILE     =   resource_filename(__name__, "data/zip-stations.csv")
NO_GHCN             =   3
NO_COOP             =   0
NO_USAF             =   2
LOGFILE             = 'zip2ws.log'


class Logger(object):
    """Standard output wrapper class
    """
    def __init__(self, filename=LOGFILE):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)


"""Constants
"""
nauticalMilePerLat = 60.00721
nauticalMilePerLongitude = 60.10793
rad = math.pi / 180.0
metersPerNauticalMile = 1852


def metersGeoDistance(lat1, lon1, lat2, lon2):
    """Returns calculate distance between two lat lons in meters
    """
    yDistance = (lat2 - lat1) * nauticalMilePerLat
    xDistance = (math.cos(lat1 * rad) + math.cos(lat2 * rad)) * (lon2 - lon1) * (nauticalMilePerLongitude / 2)

    distance = math.sqrt(yDistance**2 + xDistance**2)

    return distance * metersPerNauticalMile


def getLatLonByZip(zip):
    """Returns Lat/Lon by Zip (Using Google Maps Geocoding API)
       Usage Limit:
       https://developers.google.com/maps/documentation/geocoding/#Limits
    """
    try:
        results = Geocoder.geocode(zip)
        return results[0].coordinates
    except Exception as e:
        print(e.status)
        if e.status != pygeocoder.GeocoderError.G_GEO_ZERO_RESULTS:
            # Raise except if OVER_USAGE_LIMIT
            raise
        return None


def importZip(options):
    """Create and import Zip code to database table
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE zip (zipcode varchar(6) unique, city varchar(32), state varchar(4), lat real, lon real, gm_lat real, gm_lon real, diff real, zipcodetype varchar(10), locationtype varchar(10), location varchar(64), decommisioned varchar(5), taxreturnsfiled integer, estimatedpopulation integer, totalwages integer)')
    except:
        raise
        print("WARNING: Table zip already created")

    with open(US_ZIP_LIST, 'rb') as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            try:
                c.execute("INSERT OR IGNORE INTO zip (zipcode, city, state, lat, lon, zipcodetype, locationtype, location, decommisioned, taxreturnsfiled, estimatedpopulation, totalwages) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[0], row[2], row[3], row[5], row[6], row[1], row[4], row[7], row[8], row[9], row[10], row[11]))
            except:
                raise
                print("WARNING: Cannot insert row ==> {0!s}".format(str(row)))
    conn.commit()
    conn.close()


def createStationsTable(c):
    """Create table "stations"
    """
    try:
        c.execute('''CREATE TABLE stations
                     (id varchar(32) unique, name varchar(64), state varchar(4), lat real, lon real, elev real, type varchar(16))''')
    except:
        print("WARNING: Table stations already created")


def importGHCND(options):
    """Import GHCND stations list for database table
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(GHCND_STATIONS_LIST) as f:
        for l in f:
            if l[0:2].upper() != 'US': continue
            state = l[38:40].strip()
            name = l[41:71].strip()
            l = ' '.join(l.split())
            row = l.split(' ')
            try:
                c.execute("INSERT OR IGNORE INTO stations (id, name, state, lat, lon, elev, type) VALUES (?, ?, ?, ?, ?, ?, 'GHCND')", (row[0], name.decode('utf-8'), state, float(row[1]), float(row[2]), float(row[3])))
            except:
                print("WARNING: Cannot insert row ==> {0!s}".format(str(row)))
    conn.commit()
    conn.close()


def importASOS(options):
    """Import ASOS stations list for database table
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(ASOS_STATIONS_LIST) as f:
        for l in f:
            country = l[89:109].strip()
            if country != "UNITED STATES": continue
            id = l[0:8].strip()
            name = l[27:57].strip()
            state = l[110:112].strip()
            lat = l[144:153].strip()
            lon = l[154:164].strip()
            elev = l[165:171].strip()
            try:
                c.execute("INSERT OR IGNORE INTO stations (id, name, state, lat, lon, elev, type) VALUES (?, ?, ?, ?, ?, ?, 'ASOS')", (id, name, state, float(lat), float(lon), float(elev)))
            except:
                print("WARNING: Cannot insert row ==> {0!s}".format(str(l)))
    conn.commit()
    conn.close()


def importCOOP(options):
    """Import COOP stations list for database table
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(COOP_STATIONS_LIST) as f:
        for l in f:
            country = l[38:58].strip()
            if country != "UNITED STATES": continue
            id = l[0:9].strip().replace(' ', '')
            state = l[59:61]
            name = l[99:130].strip()
            lat = l[131:139].strip()
            a = lat.split(' ')
            lat = float(a[0])
            if lat > 0:
                lat += (float(a[1]) + float(a[2])/60)/60
            else:
                lat -= (float(a[1]) + float(a[2])/60)/60
            lon = l[140:150].strip()
            a = lon.split(' ')
            lon = float(a[0])
            if lon > 0:
                lon += (float(a[1]) + float(a[2])/60)/60
            else:
                lon -= (float(a[1]) + float(a[2])/60)/60
            elev = l[150:156].strip()
            #print name, lat, lon, elev
            try:
                c.execute(u"INSERT OR IGNORE INTO stations (id, name, state, lat, lon, elev, type) VALUES (?, ?, ?, ?, ?, ?, 'COOP')", (id, name, state, float(lat), float(lon), float(elev)))
            except:
                print("WARNING: Cannot insert row ==> {0!s}".format(str((name, lat, lon, elev))))
    conn.commit()
    conn.close()


def importISD(options):
    """Import ISD stations list for database table

    Integrated Surface Database Station History, June 2013

    USAF = Air Force Datsav3 station number
    WBAN = NCDC WBAN number
    CTRY = WMO historical country ID, followed by FIPS country ID
    ST = State for US stations
    CALL = ICAO call sign
    LAT = Latitude in thousandths of decimal degrees
    LON = Longitude in thousandths of decimal degrees
    ELEV = Elevation in tenths of meters
    BEGIN = Beginning Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.
    END = Ending Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.

    Notes:
    - Missing station name, etc indicate the metadata are not currently available.
    - The term "bogus" indicates that the station name, etc are not available.
    - For a small % of the station entries in this list, climatic data are not 
      available. These issues will be addressed. To determine data availability 
      for each location, see the 'ish-inventory.txt' or 'ish-inventory.csv' file. 
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createStationsTable(c)

    with open(ISD_STATIONS_LIST) as f:
        reader = csv.reader(f)
        reader.next()
        for r in reader:
            country = r[3]
            if country != "US": continue
            id = r[0] + '-' + r[1]
            state = r[5]
            name = r[2]
            try:
                lat = float(r[7])/1000.0
            except:
                lat = None
            try:
                lon = float(r[8])/1000.0
            except:
                lon = None
            try:
                elev = float(r[9])/10.0
            except:
                elev = None
            #print name, lat, lon, elev
            try:
                c.execute(u"INSERT OR IGNORE INTO stations (id, name, state, lat, lon, elev, type) VALUES (?, ?, ?, ?, ?, ?, 'USAF-WBAN')", (id, name, state, lat, lon, elev))
            except:
                print("WARNING: Cannot insert row ==> {0!s}".format(str((name, lat, lon, elev))))
    conn.commit()
    conn.close()


def getStations(options, type):
    """Query stations by specific type ('GHCND', 'ASOS', 'COOP', 'USAF-WBAN')
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    if type == "ALL":
        c.execute("select rowid, id, name, lat, lon from stations")
    else:
        c.execute("select rowid, id, name, lat, lon from stations where type = ?",(type,))
    stations = []
    for r in c:
        stations.append(r)
    conn.close()
    return stations


def updateLatLonByGeocoding(options):
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    total = 0
    c.execute("select rowid, zipcode, lat, lon, gm_lat, gm_lon from zip where gm_lat is null or gm_lon is null order by rowid")
    zip = []
    for r in c:
        zip.append(r)
        total += 1
    n = 0
    for r in zip:
        n = n + 1
        print("<{0:d}/{1:d}>".format(n, total))
        zid = r[0]
        zip = r[1]
        lat = r[2]
        lon = r[3]
        gc = getLatLonByZip(zip)
        print("Geocoding API ('{0!s}') ==> {1!s}".format(zip, str(gc)))
        if gc is None:
            print("WARNING: No Lat/Lon data for zip: {0!s} (Google)".format((zip)))
            continue
        gm_lat = gc[0]
        gm_lon = gc[1]
        if lat == '' or lon == '':
            c.execute("update zip set gm_lat = ?, gm_lon = ? where rowid = ?", (gm_lat, gm_lon, zid))
        else:
            try:
                distance = metersGeoDistance(float(lat), float(lon), gm_lat, gm_lon)
            except:
                distanc = sys.maxint
            c.execute("update zip set gm_lat = ?, gm_lon = ?, diff = ? where rowid = ?", (gm_lat, gm_lon, distance, zid))
        conn.commit()
    conn.commit()
    conn.close()


def sortedStationsDistance(lat, lon, stations):
    """Returns stations list sorted by distance from specific lat/log
    """
    dist = []
    for s in stations:
        sid = s[0]
        id = s[1]
        name = s[2]
        if s[3] is None or s[4] is None: continue
        #print s[3], s[4]
        lat2 = float(s[3])
        lon2 = float(s[4])
        try:
            distance = metersGeoDistance(lat, lon, lat2, lon2)
        except:
            distance = sys.maxint
        dist.append((int(distance), sid))
    return sorted(dist)


def updateClosestStations(options):
    """Find closest weather station and update to table 'closest'
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()

    createClosestTable(c)

    c.execute("select max(zid) from closest")
    r = c.fetchone()
    if r[0] is None:
        last_zid = 0
    else:
        last_zid = r[0]
    c.execute("select rowid, zipcode, lat, lon, gm_lat, gm_lon from zip where rowid > ? and ((lat <> '' and lon <> '') or (gm_lat is not null and gm_lon is not null)) order by rowid", (last_zid, ))
    total = 0
    zip = []
    for r in c:
        zip.append(r)
        total += 1
    n = 0
    for r in zip:
        n = n + 1
        #print r
        print("<{0:d}/{1:d}>".format(n, total)) 
        zid = r[0]
        zip = r[1]
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
        elif lat != '' and lon != '':
            lat1 = float(lat)
            lon1 = float(lon)
        else:
            continue
        if options.distance != 0:
            stations = getStations(options, "ALL")
            dist = sortedStationsDistance(lat1, lon1, stations)
            for d in dist:
                if d[0] <= options.distance * 1000:
                    #print d
                    c.execute("INSERT OR IGNORE INTO closest (zid, sid, distance) VALUES (?, ?, ?)", (zid, d[1], d[0]))
                else:
                    break
        else: 
            for (a, b) in [('GHCND', options.ghcn), ('USAF-WBAN', options.usaf), ('COOP', options.coop)]:
                stations = getStations(options, a)
                dist = sortedStationsDistance(lat1, lon1, stations)
                for d in dist[:b]:
                    c.execute("INSERT OR IGNORE INTO closest (zid, sid, distance) VALUES (?, ?, ?)", (zid, d[1], d[0]))
        conn.commit()
    conn.commit()
    conn.close()


def createClosestTable(c):
    """Create closest table
    """
    try:
        c.execute('''CREATE TABLE closest (zid INT, sid INT, distance FLOAT, UNIQUE(zid, sid) ON CONFLICT REPLACE)''')
    except:
        print("WARNING: Table closest already created")


def dropClosestTable(options):
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    try:
        c.execute("drop table closest")
    except:
        pass
    conn.commit()
    conn.close()
    print("Drop 'closest' table completed")


def clearGoogleLatLon(options):
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    try:
        c.execute("update zip set gm_lat = null, gm_lon = null")
    except:
        pass
    conn.commit()
    conn.close()
    print("Clear Google Maps Lat/Lon completed")


def exportClosestStations(options):
    """Export closest stations for each zip code to CSV file
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    c2 = conn.cursor()

    try:
        c.execute("select max(n) from (select count(*) n from closest group by zid)")
        r = c.fetchone()
        max_station = r[0]
    except:
        print("WARNING: No closest station in database, please run the script with -c to update")
        conn.close()
        return

    """Create output file
    """
    try:
        csvfile = open(options.outfile, 'wb')
        csvwriter = csv.writer(csvfile, dialect='excel', delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
    except:
        print("ERROR: Cannot create output file")
        sys.exit(-1)


    # Prepare header
    headers = ['zip', 'lat', 'lon', 'gm_lat', 'gm_lon', 'diff', 'city', 'state', 'zipcodetype', 'locationtype', 'location', 'decommisioned', 'taxreturnsfiled', 'estimatedpopulation', 'totalwages']
    for i in range(max_station):
        headers.append('st{0:d}_id'.format((i + 1)))
        headers.append('st{0:d}_name'.format((i + 1)))
        headers.append('st{0:d}_dist'.format((i + 1)))
    # Write header
    csvwriter.writerow(headers)

    c.execute("select rowid, zipcode, lat, lon, gm_lat, gm_lon, diff, city, state, zipcodetype, locationtype, location, decommisioned, taxreturnsfiled, estimatedpopulation, totalwages from zip order by rowid")
    for r in c:
        a = []
        print("Export zip: {0!s}".format(r[1]))
        a += r[1:]
        c2.execute("select id, name, distance from closest c join stations s on c.sid = s.rowid where c.zid = ? order by c.distance", (r[0],))
        i = 0
        for f in c2:
            a += f
            i += 1
            if i >= max_station:
                break
        csvwriter.writerow(a)
    conn.close()
    csvfile.close()


def parse_command_line(argv):
    """Command line options parser for the script
    """
    usage = "usage: %prog [options]"

    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-D", "--database", action="store", 
                      dest="database", default=SQLITE_DB_NAME,
                      help="Database name (default: {0!s})".format((SQLITE_DB_NAME)))        
    parser.add_option("-i", "--import", action="store_true", 
                      dest="importdb", default=False,
                      help="Create and import database")                             
    parser.add_option("-g", "--geocode", action="store_true", 
                      dest="geocode", default=False,
                      help="Query and update Lat/Lon by Google Maps Geocoding API")    
    parser.add_option("-c", "--closest", action="store_true", 
                      dest="closest", default=False,
                      help="Calculate and update closest table")    
    parser.add_option("--ghcn", action="store", 
                      type="int", dest="ghcn", default=NO_GHCN,
                      help="Number of closest stations for GHCN (default: {0:d})".format((NO_GHCN)))
    parser.add_option("--coop", action="store", 
                      type="int", dest="coop", default=NO_COOP,
                      help="Number of closest stations for COOP (default: {0:d})".format((NO_COOP)))
    parser.add_option("--usaf", action="store", 
                      type="int", dest="usaf", default=NO_USAF,
                      help="Number of closest stations for USAF (default: {0:d})".format((NO_USAF)))
    parser.add_option("-d", "--distance", action="store", 
                      type="int", dest="distance", default=0,
                      help="Maximum distance of stations from Zip location (Default: 0)")
    parser.add_option("-e", "--export", action="store_true", 
                      dest="export", default=False,
                      help="Export closest stations for each Zip to CSV file")    
    parser.add_option("-o", "--outfile", action="store", 
                      dest="outfile", default=CSV_OUTPUT_FILE,
                      help="CSV Output file name (default: {0!s})".format((CSV_OUTPUT_FILE)))
    parser.add_option("--drop-closest", action="store_true", 
                      dest="drop_closest", default=False,
                      help="Drop closet table")
    parser.add_option("--clear-glatlon", action="store_true", 
                      dest="clear_glatlon", default=False,
                      help="Clear Google Maps Geocoding API Lat/Lon")
    parser.add_option("--use-zlatlon", action="store_true", 
                      dest="use_zlatlon", default=False,
                      help="Use Zip Lat/Lon instead of Google Geocoding Lat/Lon")        
    return parser.parse_args(argv)


def download(url, local):
    print("Downloading '{:s}'...".format(url))
    response = urlopen(url)
    content = response.read()
    with open(local, 'wb') as f:
        f.write(content)


def main(argv=sys.argv):
    sys.stdout = Logger()
    signal.signal(signal.SIGINT, signal_handler)

    (options, args) = parse_command_line(argv)

    if not os.path.exists(US_ZIP_LIST):
        download(FREE_ZIPCODE_DOWNLOAD_URL, US_ZIP_LIST)

    if not os.path.exists(GHCND_STATIONS_LIST):
        download(GHCND_STATIONS_LIST_URL, GHCND_STATIONS_LIST)

    if not os.path.exists(ISD_STATIONS_LIST):
        download(ISD_STATIONS_LIST_URL, ISD_STATIONS_LIST)

    if not os.path.exists(options.database) or options.importdb:
        importZip(options)
        importGHCND(options)
        # FIXME: ASOS and COOP no longer available
        #importASOS(options)
        #importCOOP(options)
        importISD(options)
    if options.drop_closest:
        dropClosestTable(options)
    if options.clear_glatlon:
        clearGoogleLatLon(options)
    if options.geocode:
        updateLatLonByGeocoding(options)
    if options.closest:
        updateClosestStations(options)
    if options.export:
        exportClosestStations(options)


def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    os._exit(1)

if __name__ == "__main__":
    main()
