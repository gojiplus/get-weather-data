Get Weather Data
~~~~~~~~~~~~~~~~
.. image:: https://travis-ci.org/soodoku/get-weather-data.svg?branch=master
    :target: https://travis-ci.org/soodoku/get-weather-data
.. image:: https://img.shields.io/pypi/v/get-weather-data.svg?maxAge=2592000
    :target: https://pypi.python.org/pypi/get-weather-data
.. image:: https://img.shields.io/pypi/dd/get-weather-data.svg?maxAge=2592000
    :target: https://pypi.python.org/pypi/get-weather-data

Scripts for finding out the weather in a particular zip code on a
particular day (or a set of dates). You can also find weather stations
near a zip code, and vice-versa.

Background
^^^^^^^^^^

From `Bad Weather: Getting weather data by zip and
date <http://gbytes.gsood.com/2013/06/27/bad-weather-getting-weather-data-by-zip-and-date/>`__:

Some brief ground clearing before we begin. Weather data come from
weather stations, which can belong to any of the five or more
'networks,' each of which collect somewhat different data, sometimes
label the same data differently, and have different reporting protocols.
The only geographic information that typically comes with weather
stations is their latitude and longitude. By “weather”, we may mean
temperature, rain, wind, snow, etc. and we may want data on these for
every second, minute, hour, day, month etc. It is good to keep in mind
that not all weather stations report data for all units of time, and
there can be a fair bit of missing data. Getting data at coarse time
units like day, month, etc. typically involves making some decisions
about what particular statistic is the most useful. So for instance, you
may want, minimum and maximum (for daily temperature), or totals (for
rainfall and snow). With that primer, let’s begin.

We begin with what not to do. Do not use the `NOAA web
service <http://www.ncdc.noaa.gov/cdo-web/webservices>`__. The API
provides a straightforward way to get 'weather' data for a particular
zip for a particular month. Except, the requests often return nothing.
It isn't clear why. The documentation doesn't say whether the search for
the closest weather station is limited to X kilometers because without
that, one should have data for all zip codes and all dates. Nor does the
API bother to return how far the weather station is from which it got
the data, though one can get that post hoc using `Google Geocoding
API <https://developers.google.com/maps/documentation/geocoding/>`__.
However, given the possibility that the backend for the API would
improve over time, here's an usage example :-

Usage
-----

::

    Usage: noaaweb-script.py [options] <input file>
    
    Options:
    -h, --help            show this help message and exit
    -o OUTFILE, --outfile=OUTFILE
                        CSV Output file name (default: output.csv)


Example
-------

NCDC Web service requires token to access, you'll get it from https://www.ncdc.noaa.gov/cdo-web/token.

You must set environment variable NCDC_TOKEN with the valid NCDC token, On Windows

::

    set NCDC_TOKEN=<your NCDC token>

or On Linux

::

    export NCDC_TOKEN=<your NCDC token>

::

    noaaweb intput.csv


Sample input file
=================

::

    no,uniqid,zip,year,month,day
    2000,1,7853,1999,12,15
    2000,2,70503,1999,12,15
    2000,3,38118,1999,12,26
    2000,4,32548,1999,12,17
    2000,5,7863,1999,12,17
    2000,6,10705,1999,12,15
    2000,7,80931,1999,12,19
    2000,8,3878,1999,12,17
    2000,9,17222,1999,12,16
    2000,10,7831,1999,12,20
    ...

On to what can be done. The 'web service' that you can use is `Farmer's
Almanac's <http://www.almanac.com/weather>`__. Sleuthing using scripts
that we discuss later reveal that The Almanac reports data from the
NWS-USAF-NAVY stations (`ftp link to the data
file <ftp://ftp.ncdc.noaa.gov/pub/data/inventories/WBAN.TXT.Z>`__. And
it appears to have data for most times though no information is provided
on the weather station from which it got the data and the distance to
the zip code.

If you intend to look for data from `GHCND <http://www.ncdc.noaa.gov/oa/climate/ghcn-daily/>`_ and `ISD <https://www.ncdc.noaa.gov/isd/data-access/>`_, there are two
kinds of crosswalks that you can create – one that goes from zip codes
to weather stations, and one that goes from weather stations to zip
codes. I assume that we don’t have access to shape files (for census zip
codes), and that postal zip codes encompass a geographic region. To
create a weather station to zip code crosswalk, web service such as
Geonames or Google Geocoding API can be used. If the station lat,./long.
is in the zip code, the distance comes up as zero. Otherwise the
distance is calculated as distance from the “centroid” of the zip code. For creating a zip code to weather station
crosswalk, we get centroids of each zip using a web service such as
Google (or use already provided centroids from free zip databases). And
then find the 'nearest' weather stations by calculating distances to
each of the weather stations. For a given set of zip codes, you can get
a list of closest weather stations (you can choose to get n closest
stations, or say all weather stations within x kilometers radius, and/or
choose to get stations from particular network(s)) using the following usage example :-

Usage
-----

::

    Usage: zip2ws-script.py [options]
    
    Options:
      -h, --help            show this help message and exit
      -D DATABASE, --database=DATABASE
                            Database name (default: zip2ws.sqlite)
      -i, --import          Create and import database
      -g, --geocode         Query and update Lat/Lon by Google Maps Geocoding API
      -c, --closest         Calculate and update closest table
      --ghcn=GHCN           Number of closest stations for GHCN (default: 3)
      --coop=COOP           Number of closest stations for COOP (default: 0)
      --usaf=USAF           Number of closest stations for USAF (default: 2)
      -d DISTANCE, --distance=DISTANCE
                            Maximum distance of stations from Zip location
                            (Default: 0)
      -e, --export          Export closest stations for each Zip to CSV file
      -o OUTFILE, --outfile=OUTFILE
                            CSV Output file name (default: F:\sandbox\gsood\get-
                            weather-data\get-weather-data\venv\lib\site-
                            packages\zip2ws\data\zip-stations.csv)
      --drop-closest        Drop closet table
      --clear-glatlon       Clear Google Maps Geocoding API Lat/Lon
      --use-zlatlon         Use Zip Lat/Lon instead of Google Geocoding Lat/Lon
    
Example
-------

- **Import zip codes and weather stations to database**
    ::
    
        zip2ws -i

- **Build closest weather station table for each zip code**
    ::
    
        zip2ws -c
    
    *It takes several minutes to process all zip codes*
    
- **Export closest stations for each zip codes to file**
    ::
    
        zip2ws -e -o output.csv
    

The output lists for each zip code weather
stations arranged by proximity. The task of getting weather data from
the closest station is simple thereon – get data (on a particular set of
columns of your choice) from the closest weather station from which the
data are available. You can do that for a particular zip code and date
(and date range) combination using the following usage example :-

To getting weather data there are two commands, one is the manager command (`zipwd-manager`) it will create a server process to dispatch job (list of zip codes and date range) to the workers process that will be create by another command (`zipwd-worker`) All workers will looking for weather data from thiers local database and put back the results to the manager process. We can have multiple workers run on same or difference machine also.

Usage
-----

- **Manager**
    ::
    
        usage: zip2wd-manager-script.py [-h] [--config CONFIG] [-o OUTFILE] [-v]
                                        inputs [inputs ...]
        
        Weather search by ZIP (Manager)
        
        positional arguments:
          inputs                CSV input file(s) name
        
        optional arguments:
          -h, --help            show this help message and exit
          --config CONFIG       Default configuration file (default: zip2wd.cfg)
          -o OUTFILE, --out OUTFILE
                                Search results in CSV (default: output.csv)
          -v, --verbose         Verbose message

- **Worker**
    ::
    
        usage: zip2wd-worker-script.py [-h] [--config CONFIG] [-v]
        
        Weather search by ZIP (Worker)
        
        optional arguments:
          -h, --help       show this help message and exit
          --config CONFIG  Default configuration file (default: zip2wd.cfg)
          -v, --verbose    Verbose message    

Configuration file
------------------
There are script settings in the configuration (`zip2wd.cfg`)

::

    [manager]
    ip = 127.0.0.1
    port = 9999
    authkey = 1234
    batch_size = 10

    [worker]
    uses_sqlite = yes
    processes = 4
    nth = 0
    distance = 30

    [output]
    columns = column-names.txt

    [db]
    zip2ws = zip2ws.sqlite
    path = ./data/

-  ``ip`` and ``port`` - IP address and port of manager process that the
   worker will be connect to.
-  ``authkey`` - A shared password which is used to authenticate between
   manager and worker processes.
-  ``batch_size`` - A number of zipcodes that manager process dispatch
   to worker process each time.

-  ``uses_sqlite`` - Uses weather data from imported SQLite3 database if
   ``yes`` (recommend for speed) or download weather data for individual
   weather station on demand if ``no``
-  ``processes`` - A number of process will be forked on the worker
   process.
-  ``nth`` - Search within n-th closest station [set to ``0`` for
   unlimited]
-  ``distance`` - Search within distance (KM) [set to ``0`` for
   unlimited]

-  ``column`` - A column file that contains list of weather data column
   to be output

-  ``zip2ws`` - SQLite3 database of zip codes and weather stations
-  ``path`` - Path relative to database files

Example
-------

- **Manager**
    ::
    
        zip2wd-manager input.csv
    
- **Worker**
    ::
    
        zip2wd-worker

Authors
^^^^^^^

Suriyan Laohaprapanon and Gaurav Sood

License
^^^^^^^

Scripts are released under the `MIT License <LICENSE>`__.
