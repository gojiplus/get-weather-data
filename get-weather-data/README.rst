Get Weather Data
~~~~~~~~~~~~~~~~

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
improve over time, here's a `script for getting the daily weather data,
and hourly precipitation data <noaaweb/>`__.

On to what can be done. The 'web service' that you can use is `Farmer's
Almanac's <http://www.almanac.com/weather>`__. Sleuthing using scripts
that we discuss later reveal that The Almanac reports data from the
NWS-USAF-NAVY stations (`ftp link to the data
file <ftp://ftp.ncdc.noaa.gov/pub/data/inventories/WBAN.TXT.Z>`__. And
it appears to have data for most times though no information is provided
on the weather station from which it got the data and the distance to
the zip code.

If you intend to look for data from GHCND, COOP or ASOS, there are two
kinds of crosswalks that you can create – one that goes from zip codes
to weather stations, and one that goes from weather stations to zip
codes. I assume that we don’t have access to shape files (for census zip
codes), and that postal zip codes encompass a geographic region. To
create a weather station to zip code crosswalk, web service such as
Geonames or Google Geocoding API can be used. If the station lat,./long.
is in the zip code, the distance comes up as zero. Otherwise the
distance is calculated as distance from the “centroid” of the zip code
(see `geonames script that finds 5 nearest zips for each weather
station <ws2zip/>`__). For creating a zip code to weather station
crosswalk, we get centroids of each zip using a web service such as
Google (or use already provided centroids from free zip databases). And
then find the 'nearest' weather stations by calculating distances to
each of the weather stations. For a given set of zip codes, you can get
a list of closest weather stations (you can choose to get n closest
stations, or say all weather stations within x kilometers radius, and/or
choose to get stations from particular network(s)) using the following
`script <zip2ws/>`__. The output lists for each zip code weather
stations arranged by proximity. The task of getting weather data from
the closest station is simple thereon – get data (on a particular set of
columns of your choice) from the closest weather station from which the
data are available. You can do that for a particular zip code and date
(and date range) combination using the following `script <zip2wd/>`__.

List of Scripts
^^^^^^^^^^^^^^^

1. Find nearest zip codes given a list of weather stations (COOP and
   GHCND) via geonames: `Data and scripts <ws2zip/>`__

2. Find nearest weather stations given a list of zip codes: `Data and
   scripts <zip2ws/>`__

3. Get data from nearest weather station given a list of zip codes and
   date or range of dates: `Script <zip2wd/>`__

4. Multi-threaded version of 3. Get data from nearest weather station
   given a list of zip codes and date or range of dates:
   `Script <zip2wd_mp/>`__

5. Get data from nearest weather station given a list of zip codes and
   date using the NOAA webservice: `Script <noaaweb/>`__

Authors
^^^^^^^

Suriyan Laohaprapanon and Gaurav Sood

License
^^^^^^^

Scripts are released under the `MIT License <LICENSE>`__.
