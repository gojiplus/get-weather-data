##--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~--++
##                                                        
##        Get the (5) nearest zip codes for GHCND or COOP weather station files (lat./long.)
##        Last Edited: 7.10.13               
##        Author: Gaurav Sood                             
##		 
##        COOP Station List from: http://www.ncdc.noaa.gov/homr/reports/platforms;jsessionid=794891457456A71118A5D0D81CA80100
##		  Depends on geonames: https://github.com/ashchristopher/python-geonames
##        Geonames depends on urllib/2
##        In geonames.py amend timeout settings for urllib: resource = urllib2.urlopen(uri, timeout=500).readlines()
##--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~--++

import re
import geonames
import time

def slices(s, *args):
    position = 0
    for length in args:
        yield s[position:position + length]
        position += length

def load_save_csvfile(infilename, outfilename, source='ghcnd'):
    reader = open(infilename, 'r')
    total_rows = 0
    try:
        writer = open(outfilename, 'r+', 1)
        for row in writer:
            total_rows += 1
    except IOError:
        writer = open(outfilename, 'w', 1)
            
    if source == 'ghcnd':
        prog = re.compile(".{11}(.{9})(.{10})")
    elif source == 'coop':
        prog = re.compile(".{191}(.{16})(.{16})")
        
    j = -1
    for row in reader:
        j += 1
        if j < total_rows:
            continue        
        
        match = prog.match(row)
        lat, lng = match.group(1, 2)
        
        lat = lat.strip()
        lng = lng.strip()
        result=geonames.findNearbyPostalCodes(lat=lat, lng=lng, country='US', maxRows=5)
        i = 0
        index = 0
        indexes = []
        if result:
            try:
                min_distance = float(result[0]['distance'])
            except:
                print 'Get limitation'
                break
            
            for r in result:
                distance = float(r['distance'])
                if min_distance > distance:
                    index = i
                    min_distance = distance
                    indexes = [index]
                elif min_distance == distance:
                    indexes.append(i)
                i += 1
            
        out = row.rstrip('\n')
        merge = {result[index]['postalCode'] for index in indexes}
        for m in merge:
            out = out + '{:>10}'.format(m)
            
        if len(indexes) > 0:
            out += '{:>15}'.format(min_distance)
        out += '\n'
        out = '{:<20}'.format(source) + out
        print out
        writer.write(out)
        time.sleep(1)
            
    reader.close()
    writer.close()

#load_save_csvfile('ghcnd-stations.txt', 'ghcnd-stations-out.txt', source='ghcnd')
load_save_csvfile('coop-stations.txt', 'coop-stations-out.txt', source='coop')