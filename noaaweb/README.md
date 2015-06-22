### Get Weather Data from Weather Station Nearest to a Zip Code using the NOAA Web Service

Get data from nearest weather station given a list of zip codes and date (see [sample input file](https://github.com/soodoku/Weather-Data/blob/master/noaaweb/samplein.csv) for the format in which data are expected) using the [NOAA webservice](https://www.ncdc.noaa.gov/cdo-web/webservices). The script appends all the weather data from NOAA along with the GHCND id, name, lat. and longitude of the weather station from which the data are being gotten to the input file (see [sample output file](https://github.com/soodoku/Weather-Data/blob/master/noaaweb/sampleout.csv)).

The script needs an API token from NOAA. You can get a token from the [NCDC site](http://www.ncdc.noaa.gov/cdo-web/token).

#### Usage

* Before running the file, open `noaaweb.py` in a text editor and replace `NCDC_TOKEN` with your NCDC token.
* The default output file name is `output.csv`. To specify a custom output file name, pass `-o outfilename_of_choice`.
* The script keeps track of the rows that have been processed. (It does so by taking row number from the output file as the start.) Thus, if halted in between, it will start with the last processed row. 

#### Example
<pre><code>python noaaweb.py samplein.csv -o sampleout.csv</code></pre>

#### License
The script is under the [MIT License](https://github.com/soodoku/Weather-Data/blob/master/License.md).

