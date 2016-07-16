### zip2wd_mp: Get Weather Data For a List of Zip Codes For a Range of Dates (Multi-processing version)

Given a zip code and a date or a range of dates, it gets weather data (you get to specify which data) from the closest weather station from which the data are available. If given a range of dates, it fetches all the specified columns for each of the days in the intervening p period.

How it does it:

This script is based of the script that calculates nearest weather station based on variety of metrics. 

You can use a variety of options to choose the kinds of weather stations from which you want data. For instance, you can get 
data only from USAF stations. 

The script features on demand data downloads. So it pings the local directory and sees if weather data for a particular day and time are present and if they are not, then it tries to download it from the NOAA website. On occassion the script may run into bandwidth bottlenecks and you may want to run the script again to download all the data that is needed.

#### Prerequisites:

1. [zip2ws.sqlite](zip2ws.sqlite) is based off finding the nearest weather station project.  
  
2. Input File Types:  
	a. Basic: The input file format should be CSV and should contain 6 columns with following columns names:   
  `uniqid, zip, year, month, day`  
	See [sample-input-basic.csv](sample-input-basic.csv) for sample input file.

	b. Extended: The input file format contain 9 columns with the following columns names:  
	`uniqid, zip, from.year, from.month, from.day, to.year, to.month, to.day  
	See [sample-input-extend.csv](sample-input-extend.csv) for sample input file.

3. Column Name File: This file contain list of weather data columns chosen for output file.  
	The column names begining with character '#' will not be appear in the output file.    
	(see [column-names.txt](column-names.txt) for sample file)

	For what these column names stand for, see [column-names-info.txt](column-names-info.txt)

4. GHCN Weather Data in SQLite3 database: These files create by a script [import-db.sh](data/import-db.sh) for each year.

	e.g. for year 2000

	```
	cd data
	./import-db.sh 2000
	```

	The script will download daily weather data (GHCN-Daily) from NOAA server for year 2000 and import to SQLite3 database file (e.g. `ghcn_2000.sqlite3`)


#### Configuration file

There are script settings in the configuration. [zip2wd.cfg](zip2wd.cfg)

```
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
```

* `ip` and `port` - IP address and port of manager process that the worker will be connect to.
* `authkey` - A shared password which is used to authenticate between manager and worker processes.
* `batch_size` - A number of zipcodes that manager process dispatch to worker process each time.

* `uses_sqlite` - Uses weather data from imported SQLite3 database if `yes` (recommend for speed) or download weather data for individual weather station on demand if `no`
* `processes` - A number of process will be forked on the worker process.
* `nth` - Search within n-th closest station [set to `0` for unlimited]
* `distance` - Search within distance (KM) [set to `0` for unlimited]

* `column` - A column file that contains list of weather data column to be output

* `zip2ws` - SQLite3 database of zip codes and weather stations
* `path` - Path relative to database files

#### Usage

##### Manager process
```
usage: manager.py [-h] [--config CONFIG] [-o OUTFILE] [-v] inputs [inputs ...]

Weather search by ZIP (Manager)

positional arguments:
  inputs                CSV input file(s) name

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG       Default configuration file (default: zip2wd.cfg)
  -o OUTFILE, --out OUTFILE
                        Search results in CSV (default: output.csv)
  -v, --verbose         Verbose message
```

##### Worker process
```
usage: worker.py [-h] [--config CONFIG] [-v]

Weather search by ZIP (Worker)

optional arguments:
  -h, --help       show this help message and exit
  --config CONFIG  Default configuration file (default: zip2wd.cfg)
  -v, --verbose    Verbose message
```

#### Example:

1. Run manager process search weather data for the input file [sample-input-extend.csv](sample-input-extend.csv)

	```
	python manager.py sample-input-extend.csv
	```

	The default output file is `output.csv`

2. Run worker process

	```
	python worker.py
	```

	The manager will dispatch job (list of zip codes and date range) to the connected workers. The worker process also forks a number of process (specify by `processes` in the configuration file) to search the weather data for each zip code and put back the results to the manager process.

	We can have multiple workers run on same or difference machine.

#### Output
For each day you get weather columns that you mention in column-names  
See column-names-info for details  
SID = Station ID  
type= Type of Station  
Name = Name of Area  
Lat  = Latitude  
Long = Longitude  
Nth  = N on the list of closest weather stations  
Distance = Distance from zip code centroid to weather station lat/long in meters
