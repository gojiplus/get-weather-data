# Quick Start

## Installation

```bash
pip install get-weather-data
```

## Online mode (no setup)

If you have a free NOAA token (<https://www.ncdc.noaa.gov/cdo-web/token>,
set it as `NCDC_TOKEN`), you can query the CDO API directly — no
station-database build, just a small cached ZIP-coordinates file:

```python
from get_weather_data import Weather

weather = Weather(online=True)
result = weather.get("10001", "2024-01-15")
```

Online mode is rate-limited (5 requests/second, 10,000/day); batch CSV
processing requires the local database below.

## Setup

For batch work or offline use, run setup once to download station data:

```python
from get_weather_data import Weather

weather = Weather()
weather.setup()  # Downloads ~60MB, takes a few minutes
```

This creates a local SQLite database with:

- ~93K GHCN weather stations (US, Canada, Mexico)
- ~9K USAF/WBAN airport stations
- ~41K US ZIP code coordinates

## Get Weather for a Single Location

Query by ZIP code or by coordinates — values come back as real metric
floats (°C, mm, m/s), or imperial with `Weather(units="imperial")`:

```python
from get_weather_data import Weather

weather = Weather()

result = weather.get("10001", "2024-01-15")
# result = weather.get((40.7484, -73.9967), "2024-01-15")  # same thing

print(f"Station: {result.station_name}")
print(f"Distance: {result.station_distance_meters:,} m")
print(f"Max temp: {result.tmax} °C")
print(f"Min temp: {result.tmin} °C")
```

A field is `None` when no nearby station reported it; a genuine zero
(0 °C, 0 mm) is `0.0`.

## Get Weather for a Date Range

```python
from datetime import date
from get_weather_data import Weather

weather = Weather()

results = weather.get_range(
    "90210",
    start_date=date(2024, 7, 1),
    end_date=date(2024, 7, 7),
)

for r in results:
    tmax = f"{r.tmax:.0f}°C" if r.tmax is not None else "N/A"
    print(f"{r.date}: {tmax}")
```

## Process a CSV File

If you have a CSV with ZIP codes (or coordinates) and dates:

```python
from get_weather_data import Weather

weather = Weather()

# ZIP-based (zip, year, month, day columns)
weather.process_csv("input.csv", "output.csv")

# Coordinate-based
weather.process_csv(
    "points.csv",
    "output.csv",
    lat_column="lat",
    lon_column="lon",
    date_column="date",
)
```

The output CSV gains the weather columns (in your chosen units) plus a
`weather_error` column for rows that could not be resolved — a bad row
never aborts the job, and output is written incrementally.

## Weather Variables

| Variable | Description | Metric | Imperial |
|----------|-------------|--------|----------|
| `tmax` | Maximum temperature | °C | °F |
| `tmin` | Minimum temperature | °C | °F |
| `tavg` | Average temperature | °C | °F |
| `tobs` | Temperature at observation time | °C | °F |
| `prcp` | Precipitation | mm | in |
| `snow` | Snowfall (GHCN stations only) | mm | in |
| `snwd` | Snow depth | mm | in |
| `awnd` | Average wind speed | m/s | mph |
