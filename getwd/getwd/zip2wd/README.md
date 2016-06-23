### zip2wd: Get Weather Data For a List of Zip Codes For a Range of Dates

Given a zip code and a date or a range of dates, it gets weather data (you get to specify which data) from the closest weather station from which the data are available. If given a range of dates, it fetches all the specified columns for each of the days in the intervening p period.

How it does it:

This script is based of the script that calculates nearest weather station based on variety of metrics. 

You can use a variety of options to choose the kinds of weather stations from which you want data. For instance, you can get 
data only from USAF stations. 

The script features on demand data downloads. So it pings the local directory and sees if weather data for a particular day and time are present and if they are not, then it tries to download it from the NOAA website. On occassion the script may run into bandwidth bottlenecks and you may want to run the script again to download all the data that is needed.

#### Prerequisites:

1. [zip2ws.sqlite](zip2ws.sqlite) is based off finding the nearest weather station project.  
   This sqlite database can be updated using this script as well. 
  
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

#### Usage
```
zip2wd.py [options] <input file>

Options:
  -h, --help            show this help message and exit
  -c CLOSEST, --closest=CLOSEST
                        Search within n-th closest station
  -d DISTANCE, --distance=DISTANCE
                        Search within distance (KM)
  -D DATABASE, --database=DATABASE
                        Database name (default: zip2ws.sqlite)
  -o OUTFILE, --outfile=OUTFILE
                        CSV Output file name (default: output.csv)
  -z, --zip2ws          Search by closest table of zip2ws
  --columns=COLUMNS     Column names file (default: column-names.txt)

```

#### Example:

1. Search weather data from 5 closest stations  
    ```python zip2wd.py -c 5 naes00r.csv```

2. Search weather data from closest stations within 30km  
    ```python zip2wd.py -d 30 naes00r.csv```

3. Search weather data using pre-calculated zip stations list from closest table  
    ```python zip2wd.py -z naes00r.csv```

4. **Note:**  Pre-calculated zip-stations list can be updated by zip2ws script for example   
    ```python zip2ws.py -c --ghcn=0 --coop=0 --usaf=10```
   
   The above command calculates and updates the closest table with 10 closest USAF-WBAN station type    
   
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
