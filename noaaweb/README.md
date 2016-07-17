### Get Data from Weather Station Nearest to a Zip Code using the NOAA Web Service

Get data from nearest weather station given a list of zip codes and date (see [sample input file](samplein.csv) for the format in which data are expected) using the [NOAA webservice](https://www.ncdc.noaa.gov/cdo-web/webservices). The script appends all the weather data from NOAA along with the GHCND id, name, lat. and longitude of the weather station from which the data are being gotten to the input file (see [sample output file](sampleout.csv)).

#### Using NOAA-Web
* To get started, clone this subfolder from the repository: 
```
git clone https://github.com/mfbx9da4/git-sub-dir.git
cd git-sub-dir
python get_git_sub_dir.py soodoku/get-weather-data/noaaweb
cd noaaweb
```
* The script needs an API token from NOAA. You can get a token from the [NCDC site](http://www.ncdc.noaa.gov/cdo-web/token).
* Before running the file, open `noaaweb.py` in a text editor and replace `NCDC_TOKEN` with your NCDC token.
* The default output file name is `output.csv`. To specify a custom output file name, pass `-o outfilename_of_choice`.
* The script keeps track of the rows that have been processed. (It does so by taking row number from the output file as the start.) Thus, if halted in between, it will start with the last processed row. 

**Example**  
`python noaaweb.py samplein.csv -o sampleout.csv`

#### Note 
Requests to NOAA API "often return nothing. It isn't clear why. The documentation doesn't say whether the search for the closest weather station is limited to X kilometers because without that, one should have data for all zip codes and all dates. Nor does the API bother to return how far the weather station is from which it got the data." (From [Bad Weather: Getting weather data by zip and date](http://gbytes.gsood.com/2013/06/27/bad-weather-getting-weather-data-by-zip-and-date/)). See for instance [sample output file](sampleout.csv) produced using the NOAA API. 

#### License
The script is under the [MIT License](https://opensource.org/licenses/MIT).