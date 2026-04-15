# Quick Start

## Installation

```bash
pip install get-weather-data
```

## Setup

The first time you use the package, run setup to download station data:

```python
from get_weather_data import Weather

weather = Weather()
weather.setup()  # Downloads ~50MB, takes a few minutes
```

This creates a local SQLite database with:
- ~20K GHCN weather stations
- ~3K USAF/WBAN stations
- ~40K US ZIP code coordinates

## Get Weather for a Single Location

```python
from get_weather_data import Weather

weather = Weather()

result = weather.get("10001", "2024-01-15")

print(f"Station: {result.station_name}")
print(f"Distance: {result.station_distance_meters:,} m")
print(f"Max temp: {result.tmax / 10:.1f} °C")
print(f"Min temp: {result.tmin / 10:.1f} °C")
```

## Get Weather for a Date Range

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
    tmax = f"{r.tmax / 10:.0f}°C" if r.tmax else "N/A"
    print(f"{r.date}: {tmax}")
```

## Process a CSV File

If you have a CSV with ZIP codes and dates:

```python
from get_weather_data import Weather

weather = Weather()

weather.process_csv(
    input_path="input.csv",
    output_path="output.csv",
    zipcode_column="zip",
    year_column="year",
    month_column="month",
    day_column="day",
)
```

The output CSV will have additional columns for weather data.

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

Values are in tenths (divide by 10 to get standard units).
