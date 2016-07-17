'''
Get weather data from weather station nearest to a zip code using NOAA web service
    
Get token from http://www.ncdc.noaa.gov/cdo-web/token
Replace token_here with the token you get

'''
import sys
import optparse
import csv
try:
    # For Python 3.0 and later
    from urllib.request import urlopen, HTTPError
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen, HTTPError

import time
import xml.etree.ElementTree as ET
import os

CSV_OUTPUT_FILE     = 'output.csv'
NCDC_TOKEN          = os.environ.get('NCDC_TOKEN', None)


def get_station_id(data):
    stations = {}
    for d in data:
        id = d.find('station').text
        if id in stations:
            stations[id] += 1
        else:
            stations[id] = 1

    max = 0
    station_id = ""
    for k, v in stations.items():
        if max < v:
            max = v
            station_id = k
    print("stations: " + str(stations) + " ---> use station: " + station_id)
    return station_id


def get_content(uri, string_log=None):
    root = None
    while (True):
        time.sleep(1)
        if string_log is not None:
            print(string_log + uri)
        resource = urlopen(uri, timeout=500).read()
        root = ET.fromstring(resource)
        if root.tag == 'cdoError':
            time.sleep(5)
            print("name: " + root.find('name') + ", message: " + root.find('message'))
        else:
            break
    return root


def get_GHCND(zipcode, year, month, day, token):
    orders = ["AWND", "DAPR", "FRGT", "FRTH", "GAHT", "MDPR", "PGTM", "PRCP", "SNOW", "SNWD", "THIC", "TMAX", "TMIN",
              "TOBS", "WDFG", "WESD", "WESF", "WSFG", "WT01", "WT03", "WT04", "WT05", "WT06", "WT07", "WT08", "WT09",
              "WT11", "WT14", "WT16", "WT18"]
    values = {}
    result = ["" for i in range(8)]

    uri = 'http://www.ncdc.noaa.gov/cdo-services/services/datasets/GHCND/locations/ZIP:{0!s}/data?year={1!s}&month={2!s}&day={3!s}&_type=xml&token={4!s}'.format(
        zipcode, year, month, day, token)
    root = get_content(uri, "GHCND uri: ")

    if root.get('pageCount') is not None:
        page_count = int(root.get('pageCount'))
        data = root.findall(".//*[date='{0!s}-{1!s}-{2!s}T00:00:00.000']".format(year, month, day))
        for i in range(2, page_count + 1):
            uri2 = uri + '&page=' + str(i)
            root = get_content(uri2, "GHCND uri ({0!s}): ".format(i))
            data.extend(root.findall(".//*[date='{0!s}-{1!s}-{2!s}T00:00:00.000']".format(year, month, day)))

        ghcnd_id = get_station_id(data)
        if ghcnd_id != "":
            for d in data:
                if ghcnd_id == d.find('station').text:
                    values[d.find('dataType').text] = d.find('value').text

            print("values: " + str(values))
            result = get_station_information(ghcnd_id, 'GHCND', token)
        else:
            print("No station GHCND")

    for o in orders:
        result.append(values.get(o, ""))

    return result


def get_station_information(station_id, dataset, token):
    uri = "http://www.ncdc.noaa.gov/cdo-services/services/datasets/{0!s}/stations/{1!s}?token={2!s}&_type=xml".format(
        dataset, station_id, token)
    # print 'station uri: ' + uri
    root = get_content(uri)

    station = root.find(".//*[id='{0!s}']".format(station_id))
    dispplay_name = station.find('displayName').text
    lat = station.find('latitude').text
    long = station.find('longitude').text
    cnty = station.find("*[type='CNTY']")
    if cnty is not None:
        cnty = cnty.find('id').text.replace("FIPS:", "")
    else:
        cnty = ""
    st = station.find("*[type='ST']")
    if st is not None:
        st = st.find('id').text.replace("FIPS:", "")
    else:
        st = ""

    zip = station.find("*[type='ZIP']")
    if zip is not None:
        zip_id = zip.find('id').text.replace("ZIP:", "")
        zip_display_name = zip.find('displayName').text
    else:
        zip_id = ""
        zip_display_name = ""

    return [station_id, dispplay_name, lat, long, cnty, st, zip_id, zip_display_name]


def get_PRECIP_HLY(zipcode, year, month, day, token):
    values = {}
    uri = "http://www.ncdc.noaa.gov/cdo-services/services/datasets/PRECIP_HLY/locations/ZIP:{0!s}/datatypes/HPCP/data?year={1!s}&month={2!s}&day={3!s}&token={4!s}&_type=xml".format(
        zipcode, year, month, day, token)
    root = get_content(uri, "PRECIP_HLY uri: ")

    result = ["" for i in range(8)]
    if root.get('pageCount') is not None:
        page_count = int(root.get('pageCount'))
        data = []
        for child in root:
            date = child.find('date').text
            if date.find('{0!s}-{1!s}-{2!s}T'.format(year, month, day)) != -1:
                data.append(child)
        for i in range(2, page_count + 1):
            uri2 = uri + '&page=' + str(i)
            root = get_content(uri2, "PRECIP_HLY uri ({0:d}): ".format(i))

            for child in root:
                date = child.find('date').text
                if date.find('{0!s}-{1!s}-{2!s}T'.format(year, month, day)) != -1:
                    data.append(child)

        coop_id = get_station_id(data)
        if coop_id != "":
            for d in data:
                if coop_id == d.find('station').text:
                    print(d.find('date').text, d.find('value').text)
                    values[int(d.find('date').text[11:13])] = d.find('value').text

            print("values: " + str(values))
            result = get_station_information(coop_id, 'PRECIP_HLY', token)
        else:
            print("PRECIP_HLY No station")

    for o in range(0, 24):
        result.append(values.get(o, ""))

    return result


def load_save_csvfile(infilename, outfilename):
    token = NCDC_TOKEN
    total_rows = 0
    #no, uniquid, zip, year, month, day, (columns for station information for DAILY), 30 columns for DAILY, (columns for station information for HOURLY), 24 columns for HOURLY
    start_line = '"","uniqid","zip","year","month","day","GHCND id","Display Name","Lat","Long","FIPS (CNTY)","FIPS (ST)","ZIP","ZIP Display Name",' + \
                 '"AWND - Average daily wind speed (tenths of meters per second)","DAPR - Number of days included in the multiday precipitation total (MDPR)",' + \
                 '"FRGT - Top of frozen ground layer (cm)","FRTH - Thickness of frozen ground layer (cm)","GAHT - Difference between river and gauge height (cm)",' + \
                 '"MDPR - Multiday precipitation total (tenths of mm; use with DAPR and DWPR, if available)","PGTM - Peak gust time (hours and minutes, i.e., HHMM)",' + \
                 '"PRCP - Precipitation (tenths of mm)","SNOW - Snowfall (mm)","SNWD - Snow depth (mm)","THIC - Thickness of ice on water (tenths of mm)",' + \
                 '"TMAX - Maximum temperature (tenths of degrees C)","TMIN - Minimum temperature (tenths of degrees C)",' + \
                 '"TOBS - Temperature at the time of observation (tenths of degrees C)","WDFG - Direction of peak wind gust (degrees)",' + \
                 '"WESD - Water equivalent of snow on the ground (tenths of mm)","WESF - Water equivalent of snowfall (tenths of mm)",' + \
                 '"WSFG - Peak guest wind speed (tenths of meters per second)","WT01 - Fog, ice fog, or freezing fog (may include heavy fog)",' + \
                 '"WT03 - Thunder","WT04 - Ice pellets, sleet, snow pellets, or small hail","WT05 - Hail (may include small hail)",' + \
                 '"WT06 - Glaze or rime","WT07 - Dust, volcanic ash, blowing dust, blowing sand, or blowing obstruction","WT08 - Smoke or haze",' + \
                 '"WT09 - Blowing or drifting snow","WT11 - High or damaging winds","WT14 - Drizzle","WT16 - Rain (may include freezing rain, drizzle, and freezing drizzle)",' + \
                 '"WT18 - Snow, snow pellets, snow grains, or ice crystals",' + \
                 '"COOP id","Display Name","Lat","Long","FIPS (CNTY)","FIPS (ST)","ZIP","ZIP Display Name","00:00 - HPCP (Precipitation (100th of an inch))","01:00 - HPCP",' + \
                 '"02:00 - HPCP","03:00 - HPCP","04:00 - HPCP","05:00 - HPCP","06:00 - HPCP","07:00 - HPCP","08:00 - HPCP","09:00 - HPCP","10:00 - HPCP","11:00 - HPCP",' + \
                 '"12:00 - HPCP","13:00 - HPCP","14:00 - HPCP","15:00 - HPCP","16:00 - HPCP","17:00 - HPCP","18:00 - HPCP",' + \
                 '"19:00 - HPCP","20:00 - HPCP","21:00 - HPCP","22:00 - HPCP","23:00 - HPCP"\n'
    try:
        output = open(outfilename, 'r', 1)
        for row in output:
            total_rows += 1
        output.close();
        
    except IOError:
        None

    if total_rows == 0:
        output = open(outfilename, 'w', 1)
        output.write(start_line)
        output.close()
        total_rows = 1
    if sys.version_info >= (3, 0, 0):
        output = open(outfilename, 'a', 1, newline='')
    else:
        output = open(outfilename, 'ab', 1)

    writer = csv.writer(output, quoting=csv.QUOTE_ALL)

    with open(infilename, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        j = -1
        for row in reader:
          
            j += 1
            if j < total_rows:
                continue

            zipcode = row[2]
            year = row[3]
            month = row[4]
            day = row[5]

            print("row: " + str(row))
           
            row1 = row
            try:
                row.extend(get_GHCND(zipcode, year, month, day, token))
                row.extend(get_PRECIP_HLY(zipcode, year, month, day, token))
            except HTTPError as e:
                print(e.reason)
                print('Wait 5 minutes to continue')
                row = row1
                time.sleep(5 * 60)
                try:
                    row.extend(get_GHCND(zipcode, year, month, day, token))
                    row.extend(get_PRECIP_HLY(zipcode, year, month, day, token))
                except HTTPError as e1:
                    print(e1.reason)
                    print('Exit!!! Wait few hours to continue again')
                    break
            print("result: " + str(row))
            print("")
            writer.writerow(row)
            time.sleep(2)

            # if j % 500 == 0:
            #     break

    output.close()

def parse_command_line(argv):
    """Command line options parser for the script
    """
    usage = "usage: %prog [options] <input file>"
            
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-o", "--outfile", action="store", 
                      dest="outfile", default=CSV_OUTPUT_FILE,
                      help="CSV Output file name (default: {0!s})".format((CSV_OUTPUT_FILE)))
    return parser.parse_args(argv)


def main(argv=sys.argv):
    (options, args) = parse_command_line(argv)
    if len(args) < 2:
        print("Please specific input file")
        sys.exit(-1)
    else:
        options.inputfile = args[1]
        load_save_csvfile(options.inputfile, options.outfile)

if __name__ == "__main__":
    main()

#load_save_csvfile(samplein.csv', 'sampleout1.csv')
# <cdoError>
# <script/>
# <name>Temporarily Unavailable</name>
# <message>
# Usage frequency exeeded. 1 web service query/second.
# </message>
# </cdoError>
