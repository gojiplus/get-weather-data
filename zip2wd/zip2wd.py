import csv
import urllib
import urllib2
import sqlite3
import os
import calendar
import gzip
import time
import sys
import math
import re
import optparse
import signal

from datetime import date, timedelta

SQLITE_DB_NAME      = 'zip2ws.sqlite'
ZIP_STATIONS_FILE   = 'zip-stations.csv'
CSV_OUTPUT_FILE     = 'output.csv'
COLUMN_NAMES_FILE   = 'column-names.txt'
NTH_CLOSEST_STATION = 5
LOGFILE             = 'zip2wd.log'

class Logger(object):
    """Standard output wrapper class
    """
    def __init__(self, filename=LOGFILE):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        
def parse_command_line(argv):
    """Command line options parser for the script
    """
    usage = "usage: %prog [options] <input file>"
            
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-c", "--closest", action="store", 
                  type="int", dest="closest",
                  help="Search within n-th closest station")    
    parser.add_option("-d", "--distance", action="store", 
                  type="int", dest="distance",
                  help="Search within distance (KM)")    
    parser.add_option("-D", "--database", action="store", 
                      dest="database", default=SQLITE_DB_NAME,
                      help="Database name (default: %s)" % (SQLITE_DB_NAME))        
    parser.add_option("-o", "--outfile", action="store", 
                      dest="outfile", default=CSV_OUTPUT_FILE,
                      help="CSV Output file name (default: %s)" % (CSV_OUTPUT_FILE))    
    parser.add_option("-z", "--zip2ws", action="store_true", 
                  dest="zip2ws", default=False,
                  help="Search by closest table of zip2ws")    
    parser.add_option("--columns", action="store", 
                      dest="columns", default=COLUMN_NAMES_FILE,
                      help="Column names file (default: %s)" % (COLUMN_NAMES_FILE))            
    return parser.parse_args(argv)

not_found_list = []
def download_data_file(url, file):
    """Download weather data file from server if it not exist in local directory
    """
    if url in not_found_list:
        print("WARNING: This URL no data on server %s" % (url))
        return False
    if not os.path.exists(file):
        retry = 0
        while True:
            try:
                print url, file
                urllib.urlretrieve(url, file)
                return True
            except Exception as e:
                m = re.match(".*\s(\d\d\d)\s.*", str(e))
                if m:
                    print("WARNING: FTP server error code = %s" % (m.group(1))) 
                    if m.group(1) == '550':
                        not_found_list.append(url)
                        return False
                print("WARNING: Unknown FTP server error = %s" % str(e)) 
                if retry < 5:
                    retry += 1
                    print("Retry #%d: waiting...(%ds)" % (retry, retry * 10))
                    time.sleep(retry * 10)
                else:
                    print("WARNING: Cannot download data from URL = %s" % url)
                    return False
    return True

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

    distance = math.sqrt( yDistance**2 + xDistance**2 )

    return distance * metersPerNauticalMile
    
def f2c(f):
    """Convert Fahrenheit to Celsius
    """
    return (f - 32) * 5.0/9.0

def kn2ms(kn):
    """Convert Knots to m/s
    """
    return 0.51444 * kn
    
def getStations(options):
    """Returns all stations from database
    """
    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    c.execute("select rowid, id, name, lat, lon, type from stations")
    stations = []
    for r in c:
        stations.append(r)
    conn.close()
    return stations

def sortStations(zipcode, r, stations):
    """Returns sorted stations list by distance
    """
    if r is None:
        print("WARNING: zipcode = %s not found" % (zipcode))
        return None
    lat1 = r[3]
    if lat1 is None:
        lat1 = r[1]
    lon1 = r[4]
    if lon1 is None:
        lon1 = r[2]
    if lat1 == '' or lon1 == '':
        print("WARNING: not lat/lon for zipcode = %s" % (zipcode))
        return None
    lat1 = float(lat1)
    lon1 = float(lon1)
    dist = []
    for s in stations:
        sid = s[0]
        id = s[1]
        name = s[2]
        type = s[5]
        if s[3] is None or s[4] is None: continue
        lat2 = float(s[3])
        lon2 = float(s[4])
        try:
            distance = metersGeoDistance(lat1, lon1, lat2, lon2)
        except:
            distance = sys.maxint
        dist.append((int(distance), id, type, name, lat2, lon2))
    return sorted(dist)

def writeResultRow(writer, row):
    """Write result row to file
    """
    print("result: %s\n" % str(row))
    writer.writerow(row)
    
def main(options, args):
    """Main program
    """
    MONTH_ABBR = dict((k,v.lower()) for k,v in enumerate(calendar.month_abbr))
    USAF_WBAN_DATA = [('TEMP', 24, 30), ('DEWP', 35, 41), ('SLP', 46, 52), ('STP', 57, 63), ('VISIB', 68, 73), ('AWND', 78, 83), ('MXSPD', 88, 93), ('GUST', 95, 100), ('TMAX', 102, 108), ('MAXF', 108, 109), ('TMIN', 110, 116), ('MINF', 116, 117), ('PRCP', 118, 123), ('PRCPF', 123, 124), ('SNWD', 125, 130), ('FRSHTT', 132, 138)]

    COMMON_ORDERS = ["sid", "type", "name", "lat", "lon", "nth", "distance"]

    conn = sqlite3.connect(options.database)
    c = conn.cursor()
    stations = getStations(options)
    
    BASIC_HEADERS = '"uniqid","zip","year","month","day"'
    EXTENDED_HEADERS = '"uniqid","zip","from.year","from.month","from.day","to.year","to.month","to.day"'

    out_extended = None
    total_rows = 0             
    try:
        with open(options.outfile, 'rb') as of:
            reader = csv.reader(of)
            out_extended = False
            for r in reader:
                if total_rows == 0 and r[2] == 'from.year':
                    out_extended = True
                total_rows += 1
    except IOError:
        pass

    options.extended = False
    options.maxdays = 0
    with open(options.inputfile, 'r') as csvfile:
        reader = csv.reader(csvfile)
        r = reader.next()
        if len(r) > 5:
            options.extended = True
            for r in reader:
                from_date = date(year=int(r[2]), month=int(r[3]), day=int(r[4]))
                to_date = date(year=int(r[5]), month=int(r[6]), day=int(r[7]))
                diff = to_date - from_date
                if diff.days > options.maxdays:
                    options.maxdays = diff.days

    options.maxdays += 1
    
<<<<<<< HEAD
    if out_extended is not None and out_extended != options.extended:
=======
    if out_extended not in [None, options.extended]:
>>>>>>> master
        print("ERROR: Input/Output files in different format")
        sys.exit(-1)
    
    if options.extended:
        headers = EXTENDED_HEADERS
    else:
        headers = BASIC_HEADERS
    
    orders = COMMON_ORDERS
    with open(options.columns, 'rb') as f:
        extended_orders = [r.strip() for r in f.readlines() if r[0] != '#']
        orders.extend(extended_orders)
        if options.extended:
            columns = ['"%s.%d"' % (r, d) for d in range(1, options.maxdays + 1) for r in orders]
        else:
            columns = ['"%s"' % (r) for d in range(1, options.maxdays + 1) for r in orders]        
        headers += ',' + ','.join(columns) + '\n'
    
    if total_rows == 0:
        with open(options.outfile, 'wb') as of:
            of.write(headers)
        total_rows = 1        
    output = open(options.outfile, 'ab')
    writer = csv.writer(output, quoting=csv.QUOTE_ALL)
    
    with open(options.inputfile, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        j = -1
        for row in reader:
            j += 1
            if j < total_rows:
                continue

            zipcode = '0'*(5 - len(row[1])) + row[1]
            
            year = row[2]
            month = row[3]
            day = row[4]
            from_date = date(year=int(year), month=int(month), day=int(day))

            print "row: " + str(row)

            if options.extended:
                to_date = date(year=int(row[5]), month=int(row[6]), day=int(row[7]))
            else:
                to_date = from_date
                
            c.execute('select zipcode, lat, lon, gm_lat, gm_lon, rowid from zip where zipcode = ?', (zipcode,))
            r = c.fetchone()
            if options.zip2ws:
                dist = []
                if r is not None:
                    c.execute("select distance, id, s.type, s.name, s.lat, s.lon from closest c join stations s on c.sid = s.rowid where c.zid = ? order by c.distance", (r[5],))
                    for r in c:
                        dist.append((int(r[0]), r[1], r[2], r[3], r[4], r[5]))
            else:
                dist = sortStations(zipcode, r, stations)
            if dist is None or len(dist) == 0:
                writeResultRow(writer, row)
                continue
            while True:
                year = str(from_date.year)
                month = str(from_date.month)
                day = str(from_date.day)
                found = False
                values = {}
                nth = 0
                for s in dist:
                    nth += 1
                    if options.closest is not None and nth > options.closest:
                        print("WARNING: N-th station > %d" % options.closest)
                        break
                    if options.distance is not None and (s[0] > options.distance * 1000):
                        print("WARNING: Distance > %d km" % options.distance)
                        break
                    values['nth'] = nth
                    sid = s[1]
                    type = s[2]
                    values['sid'] = sid
                    values['type'] = type
                    values['name'] = s[3]
                    values['lat'] = s[4]
                    values['lon'] = s[5]
                    values['distance'] = s[0]
                    raw = []
                    if type == 'GHCND':
                        datadir = './data/ghcn-daily/all/'
                        if not os.path.exists(datadir):
                            os.makedirs(datadir)
                        datafile = datadir + '%s.dly' % (sid)
                        urlfile = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/all/%s.dly' % (sid)
                        if not download_data_file(urlfile, datafile):
                            continue
                        search = sid + year + month
                        print("Search for: " + search)
                        match = False
                        with open(datafile, 'rb') as df:
                            for l in df:
                                if (l[:17] == search):
                                    element = l[17:21]
                                    offset = 21 + int(day)*8 - 8
                                    value = l[offset:offset + 5]
                                    mflag = l[offset+5:offset + 6]
                                    qflag = l[offset+6:offset + 7]
                                    sflag = l[offset+7:offset + 8]
                                    #print "<%s> <%s> <%s> <%s> <%s>" % (element, value, mflag, qflag, sflag)
                                    values[element] = value
                                    match = True
                                    found = True
                                    raw.append(l)
                                elif match:
                                    break
                    elif type == 'COOP':
                        datadir = './data/coop/3200/%s/' % (year)
                        if not os.path.exists(datadir):
                            os.makedirs(datadir)
                        datafile = datadir + '3200%s%s' % (MONTH_ABBR[int(month)], year)
                        urlfile = 'ftp://ftp3.ncdc.noaa.gov/pub/data/3200/%s/3200%s%s' % (year, MONTH_ABBR[int(month)], year)
                        if not download_data_file(urlfile, datafile):
                            continue
                        search = year + month
                        print("Search for: " + search)
                        match = False
                        with open(datafile, 'rb') as df:
                            for l in df:
                                if l[3:9] == sid[:6] and l[17:23] == search:
                                    element = l[11:15]
                                    offset = 30
                                    while offset < len(l) - 1 and int(l[offset:offset+2]) != int(day):
                                        offset = offset + 12
                                    if offset >= len(l) - 1:
                                        print("WARNING: data not found for day = %s" % (day))
                                    else:
                                        offset += 4
                                        value = l[offset:offset + 6]
                                        qflag1 = l[offset+6:offset + 7]
                                        qflag2 = l[offset+7:offset + 8]
                                        if element in ['TOBS', 'TMAX', 'TMIN']:
                                            value = f2c(float(value)) * 10
                                        #print "<%s> <%s> <%s> <%s>" % (element, value, qflag1, qflag2)
                                        values[element] = value
                                    match = True
                                    found = True
                                    raw.append(l)
                                elif match:
                                    break
                    elif type == 'USAF-WBAN':
                        datadir = './data/gsod/%s/' % (year)
                        if not os.path.exists(datadir):
                            os.makedirs(datadir)
                        datafile = datadir + '%s-%s.op.gz' % (sid, year)
                        urlfile = 'ftp://ftp2.ncdc.noaa.gov/pub/data/gsod/%s/%s-%s.op.gz' % (year, sid, year)
                        if not download_data_file(urlfile, datafile):
                            continue
                        search = year + month + day
                        print("Search for: " + search)
                        with gzip.open(datafile, 'rb') as df:
                            for l in df:
                                if l[14:22] == search:
                                    for d in USAF_WBAN_DATA:
                                        element = d[0]
                                        value = l[d[1]:d[2]]
                                        if element in ['TMAX', 'TMIN']:
                                            if value != '9999.9':
                                                value = f2c(float(value))*10
                                        elif element in ['AWND']:
                                            if value != '999.9':
                                                value = kn2ms(float(value))*10
                                        #print "<%s> <%s>" % (element, value)
                                        values[element] = value
                                    found = True
                                    raw.append(l)
                                    break
                    if found:
                        break
                #print values
                values['RAW'] = ''.join(raw)
                for o in orders:
                    row.append(values.get(o, "NA"))
                from_date = from_date + timedelta(days=1)
                if from_date > to_date:
                    break
            writeResultRow(writer, row)
    output.close()
    conn.close()
    
def signal_handler(signal, frame):
    print 'You pressed Ctrl+C!'
    os._exit(1)
    
if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')
    sys.stdout = Logger()
    signal.signal(signal.SIGINT, signal_handler)
    
    print("{0} r4 (2013/07/24)\n".format(sys.argv[0]))
    (options, args) = parse_command_line(sys.argv)
    if len(args) < 2:
        print("Please specific input file")
    else:
        options.inputfile = args[1]
        if options.closest is None and options.distance is None:
            options.zip2ws = True
        main(options, args)
        