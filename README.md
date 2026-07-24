# Get Weather Data

[![PyPI Version](https://img.shields.io/pypi/v/get-weather-data.svg)](https://pypi.python.org/pypi/get-weather-data)
[![CI](https://github.com/gojiplus/get-weather-data/actions/workflows/ci.yml/badge.svg)](https://github.com/gojiplus/get-weather-data/actions/workflows/ci.yml)
[![Downloads](https://pepy.tech/badge/get-weather-data)](https://pepy.tech/project/get-weather-data)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docs](https://github.com/gojiplus/get-weather-data/actions/workflows/docs.yml/badge.svg)](https://gojiplus.github.io/get-weather-data/)

Historical daily weather for any US ZIP code or latitude/longitude.
NOAA station data (GHCN Daily and GSOD), automatic nearest-station
selection, consistent metric or imperial output.

## Installation

```bash
pip install get-weather-data
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv pip install get-weather-data
```

## Quick Start

### Python API

```python
from get_weather_data import Weather

# Initialize and set up database (downloads ~60MB first time)
weather = Weather()
weather.setup()

# By ZIP code...
result = weather.get("10001", "2024-01-15")
print(f"Max temp: {result.tmax} °C")  # e.g. -1.6
print(f"Precip:  {result.prcp} mm")  # 0.0 means zero, None means no data
print(f"Station: {result.station_name} ({result.station_distance_meters} m away)")

# ...or by coordinates
result = weather.get((40.7484, -73.9967), "2024-01-15")

# Imperial units if you want them
weather_f = Weather(units="imperial")
result = weather_f.get("10001", "2024-01-15")
print(f"Max temp: {result.tmax} °F")
```

### Command Line

```bash
# Set up database (first time only)
get-weather setup

# By ZIP or by coordinates; metric by default
get-weather get 10001 2024-01-15
get-weather get "40.75,-73.99" 2024-01-15 --units imperial

# Process a CSV file
get-weather process input.csv output.csv
```

### Online Mode (no setup)

Skip the station-database build by querying NOAA's Climate Data Online
API directly (only a small ZIP-coordinates file, a few MB, is cached on
first use). Get a free token at
<https://www.ncdc.noaa.gov/cdo-web/token> and set `NCDC_TOKEN`:

```python
from get_weather_data import Weather

weather = Weather(online=True)  # requires NCDC_TOKEN
result = weather.get("10001", "2024-01-15")
```

```bash
NCDC_TOKEN=your-token get-weather get 10001 2024-01-15 --online
```

Notes on online mode:

- Tokens are limited to 5 requests/second and 10,000 requests/day, so
  `process_csv` (batch jobs) requires the local database.
- Same result contract as the local path: nearest reporting station
  first, same units, real station distances. Online covers GHCN
  stations only (no GSOD fallback).

## Features

- **ZIP or lat/lon everywhere**: `get()`, `get_range()`, CSV batch, and
  the CLI all take either a ZIP code or coordinates
- **Consistent units**: real metric values (°C, mm, m/s) across the
  API, CLI, and CSV output — or `units="imperial"` (°F, in, mph)
- **Automatic station selection**: nearest station first, farther
  stations fill in missing variables
- **Two data sources**: GHCN Daily (~93K US/CA/MX stations) and GSOD
  (~9K airport stations)
- **Robust batch processing**: streams CSVs in chunks, one bad row gets
  a `weather_error` note instead of killing the job
- **Cache management**: TTL-based refresh of station lists,
  `get-weather cache info` / `cache clear`

## Usage Examples

### Get Weather for a Date Range

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
    if r.tmax is not None:
        print(f"{r.date}: High {r.tmax:.0f}°C")
```

### Process a CSV File

```python
from get_weather_data import Weather

weather = Weather()

# ZIP-based input (zip, year, month, day columns)
weather.process_csv("locations.csv", "with_weather.csv")

# Coordinate-based input
weather.process_csv(
    "points.csv",
    "with_weather.csv",
    lat_column="lat",
    lon_column="lon",
    date_column="date",
)
```

Output rows carry the weather columns below (already in your chosen
units), plus `weather_error` explaining any row that could not be
resolved. More examples in the [examples/](examples/) directory.

## Weather Variables

All values are floats in the units below (or their imperial
equivalents); `None`/empty means the station network had no reading —
a genuine zero is reported as `0.0`.

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

## Data Sources

This package uses data from NOAA's National Centers for Environmental
Information:

- **GHCN Daily**: Global Historical Climatology Network daily
  summaries (~93K stations across the US, Canada, and Mexico —
  border ZIPs get the truly nearest station)
- **GSOD**: Global Summary of the Day from USAF/WBAN airport stations
  (~9K); GSOD reports no snowfall, so `snow` comes from GHCN stations
- **GeoNames**: ZIP code to coordinates mapping

## Database Setup and Disk Use

`setup()` downloads the station lists and ZIP coordinates (~60MB) and
builds a nearest-stations index; it takes a few minutes, once. Station
lists refresh automatically when older than 30 days.

Weather data itself is fetched lazily per year: each GHCN year you
touch builds a local SQLite file (roughly 1–3 GB for recent years);
GSOD adds one small CSV per station-year. Historical years never
re-download; the current and previous year refresh monthly. Inspect or
reclaim space anytime:

```bash
get-weather cache info
get-weather cache clear --ghcn        # or --gsod / --stations / --all
```

```python
weather = Weather()
weather.setup()

info = weather.info()
print(f"GHCN stations: {info['ghcn_stations']:,}")
print(f"USAF stations: {info['usaf_stations']:,}")
print(f"ZIP codes: {info['zipcodes']:,}")
```

## Configuration

Custom database location:

```python
weather = Weather(database_path="/path/to/my.db")
```

Or via CLI:

```bash
get-weather --database /path/to/my.db setup
```

## Upgrading from v3

v4 is a breaking release:

- **Values are now real metric floats** (°C, mm, m/s) everywhere —
  previously the Python API returned raw GHCN tenths. Divide-by-10
  code should be removed.
- **Run `get-weather setup --force`** (or `weather.setup(force=True)`)
  after upgrading: v4 fixes a nearest-station ranking bug, so indexes
  built by v3 contain wrong distances.
- `WeatherResult` gained `tobs`, `latitude`, `longitude`, and `units`;
  it is now importable from the package root.

## License

MIT License. See [LICENSE](LICENSE) for details.

## Authors

- Suriyan Laohaprapanon
- Gaurav Sood
