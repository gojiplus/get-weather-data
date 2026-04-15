# Get Weather Data

[![PyPI Version](https://img.shields.io/pypi/v/get-weather-data.svg)](https://pypi.python.org/pypi/get-weather-data)
[![Downloads](https://pepy.tech/badge/get-weather-data)](https://pepy.tech/project/get-weather-data)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://soodoku.github.io/get-weather-data/)

Get historical weather data for US ZIP codes. Uses NOAA weather station data (GHCN Daily and GSOD) with automatic station selection based on proximity.

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

# Initialize and set up database (downloads ~50MB first time)
weather = Weather()
weather.setup()

# Get weather for a ZIP code and date
result = weather.get("10001", "2024-01-15")

print(f"Max temp: {result.tmax / 10:.1f} °C")
print(f"Min temp: {result.tmin / 10:.1f} °C")
print(f"Station: {result.station_name}")
```

### Command Line

```bash
# Set up database (first time only)
get-weather setup

# Get weather for a location and date
get-weather get 10001 2024-01-15

# Process a CSV file
get-weather process input.csv output.csv
```

## Features

- **Simple API**: One class, three methods: `setup()`, `get()`, `process_csv()`
- **Automatic station selection**: Finds nearest weather stations for each ZIP code
- **Two data sources**: GHCN Daily (~100K US stations) and GSOD (~3K stations)
- **Batch processing**: Process CSV files with ZIP codes and dates
- **Local database**: SQLite database for fast repeated queries
- **CLI tool**: Command-line interface for quick lookups

## Usage Examples

### Get Weather for a Date Range

```python
from datetime import date
from get_weather_data import Weather

weather = Weather()

results = weather.get_range(
    zipcode="90210",
    start_date=date(2024, 7, 1),
    end_date=date(2024, 7, 7),
)

for r in results:
    print(f"{r.date}: High {r.tmax/10:.0f}°C, Low {r.tmin/10:.0f}°C")
```

### Process a CSV File

```python
from get_weather_data import Weather

weather = Weather()

# Input CSV should have zip, year, month, day columns
weather.process_csv(
    input_path="locations.csv",
    output_path="locations_with_weather.csv",
    zipcode_column="zip",
    year_column="year",
    month_column="month",
    day_column="day",
)
```

More examples in the [examples/](examples/) directory.

## Data Sources

This package uses data from NOAA's National Centers for Environmental Information:

- **GHCN Daily**: Global Historical Climatology Network daily summaries
- **GSOD**: Global Summary of the Day from USAF/WBAN stations
- **GeoNames**: ZIP code to coordinates mapping

## Weather Variables

| Variable | Description | Unit |
|----------|-------------|------|
| `tmax` | Maximum temperature | tenths of °C |
| `tmin` | Minimum temperature | tenths of °C |
| `tavg` | Average temperature | tenths of °C |
| `prcp` | Precipitation | tenths of mm |
| `snow` | Snowfall | mm |
| `snwd` | Snow depth | mm |
| `awnd` | Average wind speed | tenths of m/s |

## Database Setup

The first time you run `setup()`, the package downloads:

- GHCN station list (~20K US stations)
- ISD station list (~3K US stations)
- US ZIP code coordinates from GeoNames

Then it builds an index mapping each ZIP code to nearby weather stations. This takes a few minutes but only needs to be done once.

```python
weather = Weather()
weather.setup()  # Downloads data, builds index

# Check what was imported
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

## License

MIT License. See [LICENSE](LICENSE) for details.

## Authors

- Suriyan Laohaprapanon
- Gaurav Sood
