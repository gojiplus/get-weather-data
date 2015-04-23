### Get Weather Data from Weather Station Nearest to a Zip Code using the NOAA Web Service

Get data from nearest weather station given a list of zip codes and date (see [sample input file](https://github.com/soodoku/Weather-Data/blob/master/noaaweb/samplein.csv)) using the [NOAA webservice](https://www.ncdc.noaa.gov/cdo-web/webservices). The script appends all the weather data from NOAA along with the GHCND id, name, lat. and longitude of the weather station from which the data are being gotten to the input file (see [sample output file](https://github.com/soodoku/Weather-Data/blob/master/noaaweb/sampleout.csv)).

The script needs an API token from NOAA. You can get a token from the [NCDC site](http://www.ncdc.noaa.gov/cdo-web/token).

#### Usage
<pre><code>noaaweb.py samplein.csv sampleout.csv</code></pre>

#### License
The script is under the [MIT License](https://github.com/soodoku/Weather-Data/blob/master/License.md).

