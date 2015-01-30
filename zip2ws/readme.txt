~~~~~~~~~~~~~~~~~~~~~~
Readme for the data
~~~~~~~~~~~~~~~~~~~~~~

The following fields from:  free-zipcode-database-primary.csv
   (http://federalgovernmentzipcodes.us/free-zipcode-database-Primary.csv)

zip, lat, long, city, state, zipcodetype, locationtype, location, decommisioned, taxreturns,
estimatedpopulation, totalwages

gm_lat/gm_long: lat./long. of centroids of zip codes via Google API. 
diff: distance in meters between Google API estimated centroid of zip code and lat/long that comes with the database.

list of stations: ordered from closest to furthest
stX_id: station id
stX_name: name of station
stX_distance: distance to zip centroid

----------------------------

Readme for the script
~~~~~~~~~~~~~~~~~~~~~~~~~~~

What it does: 
1. Finds (certain kinds of) weather stations "nearest" (within a certain distance, or X number of closest) to each zip code centroid
2. Finds centroids of zip codes using Google API 

Weather stations come in lots of varieties. We limit ourselves to weather stations of the 
following four kinds -
1) ghcnd-stations.txt ==> GHCND stations list
   (http://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt)
   
2) asos-stations.txt  ==> ASOS stations list
   (http://www.ncdc.noaa.gov/homr/file/asos-stations.txt)
   
3) coop-act.txt       ==> COOP stations list (Active only)
   (ftp://ftp.ncdc.noaa.gov/pub/data/inventories/COOP-ACT.TXT)
   
4) ish-history.csv    ==> USAF-WBAN stations list
   (http://www1.ncdc.noaa.gov/pub/data/ish/ish-history.csv)


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Running the script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To run the script, you will need to install two Python libraries - 

pygeocoder (https://bitbucket.org/xster/pygeocoder/wiki/Home)
   (To install, you can simply use: $ pip install pygeocoder)
   
requests (http://docs.python-requests.org/en/latest/)
   (To install, you can simply user: $ pip install requests)

Don't forget the inventories directory  that contains the station files and 
zip csv that is imported. The inventories folder should be in the same folder 
as the script.

----------------------------

Usage: zip2ws_r3.py [options]

Options:
  -h, --help            show this help message and exit
  -D DATABASE, --database=DATABASE
                        Database name (default: zip2ws.sqlite)
  -i, --import          Create and import database
  -g, --geocode         Query and update Lat/Lon by Google Maps Geocoding API
  -c, --closest         Calculate and update closest table
  --ghcn=GHCN           Number of closest stations for GHCN (default: 3)
  --coop=COOP           Number of closest stations for COOP (default: 1)
  --usaf=USAF           Number of closest stations for USAF (default: 1)
  -d DISTANCE, --distance=DISTANCE
                        Maximum distance of stations from Zip location
                        (Default: 0)
  -e, --export          Export closest stations for each Zip to CSV file
  -o OUTFILE, --outfile=OUTFILE
                        CSV Output file name (default: zip-stations.csv)
  --drop-closest        Drop closet table
  --clear-glatlon       Clear Google Maps Geocoding API Lat/Lon
  --use-zlatlon         Use Zip Lat/Lon instead of Google Geocoding Lat/Lon


Start using the script by creating and importing the database.
Do so by running -

	python zip2ws_r3.py -i
	
Next task is to update the closest weather stations table. This you can do by executing...
	python zip2ws_r3.py -c

This task uses the Google lat/long. If you want them to use other lat/long, 
    python zip2ws_r3.py -c --use-zlatlon

NOTE: If you interrupt the script inbetween and restart it again, the script will start processing from
where it left off. 

If you want to find a set number of closest stations, specify the type and number of 
weather stations. For instance, to find 5 GHCND statons, 3 COOP stations, and 2 USAF stations, 
run...

    python zip2ws_r3.py -c --ghcn=5 --coop=3 --usaf=2 

To find all weather stations within 30KM and organized by closest to furthest,

    python zip2ws_r3.py -c -d 30
    
To export results to a CSV file, "closest.csv", run..

    python zip2ws_r3.py -e -o closest.csv


To find out centroids of zip codes using Google Maps Geocoding API, use
	python zip2ws_r3.py -g

Keep in mind that Google Maps Geocoding API usage limit is 2,500 Query/Day/IP Address.
So you can quickly run into the limit. The script will raise the exception "OVER_QUERY_LIMIT"
if the limit is breached. But do not fear. You can run the script multiple times to code a 
greater number of zip codes. If you are unahppy with the results, use the option: --clear-glatlon 
to clear exist data.