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

## No-code app

Prefer clicking to coding? A [Streamlit app](app/) gives the same data
through a web UI — enter a ZIP or lat/lon, pick dates, get a table,
chart, and CSV download. Run it with
`streamlit run app/streamlit_app.py` or deploy it free on Streamlit
Community Cloud.

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
- **pandas-ready**: `Weather.get_frame(...)` returns a tidy DataFrame
  (`pip install get-weather-data[pandas]`)
- **13 variables**: temperatures, precipitation, snow, wind + gust,
  dew point, pressures, and visibility
- **Quality-controlled**: values that failed NOAA's QC are dropped by
  default; `include_flags=True` surfaces the per-field QC flag
- **Present weather**: `include_weather_types=True` reports the day's
  phenomena (fog, thunder, hail, freezing rain, ...) from GHCN `WT**`
  codes and GSOD's `FRSHTT` indicator
- **Explainable gaps**: `explain=True` tells you *why* a value is
  missing (`stations_considered` + a per-field `missing` reason) — a
  blank cell becomes diagnosable, not silent
- **Automatic station selection**: nearest station first, farther
  stations fill in missing variables; `coverage(...)` reports how well
  a point is served
- **Interpolation**: `Weather(interpolate=True)` inverse-distance-weights
  the nearest stations instead of taking the single closest one
- **Universal CONUS coverage**: `source="grid"` (or `"auto"`) pulls
  NOAA's authoritative 5-km nClimGrid daily grid, so *any* Lower-48
  lat/long returns real temperature + precipitation — no station gaps
  (`pip install get-weather-data[grid]`)
- **Three data sources**: GHCN Daily (~93K US/CA/MX stations), GSOD
  (~9K airport stations), and the nClimGrid gridded product
- **Robust batch processing**: streams CSVs in chunks, one bad row gets
  a `weather_error` note instead of killing the job
- **Parquet + SQL**: stream batch output to columnar Parquet and query
  it (or a glob of many files) with `query_weather(...)` via DuckDB
  (`pip install get-weather-data[parquet]`)
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

### Get a pandas DataFrame

```python
from get_weather_data import Weather

weather = Weather()  # needs: pip install get-weather-data[pandas]

df = weather.get_frame("90210", "2024-07-01", "2024-07-07")
# one row per day; columns = metadata + tmax/tmin/prcp/... in your units
df[["date", "tmax", "prcp"]].head()
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

### Parquet output and SQL queries

For the analytical "millions of rows" workflow, write results as
**Parquet** (typed, compressed columns — far smaller than CSV and
directly queryable) and run **SQL** over them with DuckDB. Needs the
`parquet` extra (`pip install get-weather-data[parquet]`):

```python
from get_weather_data import Weather, query_weather

# Streaming Parquet output (inferred from the .parquet suffix; no pandas
# needed, memory stays bounded over millions of rows):
Weather().process_csv("locations.csv", "weather.parquet", date_column="date")

# Query one file or a glob of many with SQL:
rows = query_weather(
    "SELECT station_id, avg(tmax) AS mean_high "
    "FROM t WHERE prcp > 0 GROUP BY station_id ORDER BY mean_high DESC",
    tables={"t": "weather.parquet"},  # or "runs/*.parquet"
)
```

`get-weather process in.csv out.parquet` (or `--format parquet`) does
the same on the CLI. The daily and hourly DataFrames also write Parquet
directly: `get_frame(...).to_parquet(path)` /
`get_hourly_frame(...).to_parquet(path)`.

### Interpolate between stations

For a point *between* stations, `interpolate=True` blends the nearest
stations by inverse-distance weighting rather than returning the single
closest station's raw value:

```python
weather = Weather(interpolate=True)
result = weather.get((40.75, -73.99), "2024-01-15")
# result.station_type == "interpolated"
```

### Check coverage before trusting the numbers

```python
cov = weather.coverage("59718", "2024-01-01", "2024-12-31")
print(cov.station_name, cov.station_distance_meters, "m away")
print(f"tmax present on {cov.fraction('tmax'):.0%} of days")
```

### Hourly data

For sub-daily detail, `get_hourly(...)` returns one `HourlyResult` per
hour from NOAA's ISD-Lite (nearest airport/USAF-WBAN station).
**Timestamps are UTC** and the local station database is required
(`setup()`); there is no online equivalent.

```python
weather = Weather(units="imperial")

hours = weather.get_hourly("11371", "2023-07-16")  # LaGuardia, one UTC day
for h in hours[:3]:
    print(h.observed_at, h.temp, "F", h.wind_speed, "mph", h.wind_direction, "deg")

# pandas: one row per hour (needs the [pandas] extra)
df = weather.get_hourly_frame("11371", "2023-07-16", "2023-07-17")
```

CLI: `get-weather hourly 11371 2023-07-16 [--end 2023-07-17] [--units imperial]`.
Fields: `temp`, `dewpoint`, `sea_level_pressure`, `wind_direction`
(degrees), `wind_speed`, `sky_condition`, `precip_1h`, `precip_6h`.

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
| `wind_gust` | Peak wind gust | m/s | mph |
| `dewpoint` | Average dew point | °C | °F |
| `sea_level_pressure` | Sea-level pressure | hPa | inHg |
| `station_pressure` | Station pressure | hPa | inHg |
| `visibility` | Visibility (GSOD stations only) | km | mi |

With `include_flags=True`, `result.flags` maps each field to its GHCN
quality-control flag (blank = passed all checks; GHCN stations only).

### Present-weather types

The numeric variables above say how much rain fell but not whether it
was *thunder*, *freezing rain*, or *fog*. With
`include_weather_types=True`, `result.weather_types` is a set of the
day's phenomena, drawn from GHCN `WT**` occurrence codes (fog, thunder,
hail, glaze, blowing snow, ...) and, for GSOD airport stations, the
`FRSHTT` indicator (fog / rain / snow / hail / thunder / tornado):

```python
w = Weather(online=True, include_weather_types=True)
r = w.get("11430", "2023-07-16")  # JFK on a stormy afternoon
print(sorted(r.weather_types))  # ['fog', 'heavy_fog', 'thunder']
```

`get-weather get <loc> <date> --weather-types` shows them on the CLI,
and `process --weather-types` adds a comma-joined `weather_types`
column to the batch CSV.

### Why is a value missing?

A blank `tmax` could mean many things: no station nearby, a station
that measures only precipitation, or a gap on that date. With
`explain=True`, the result carries `stations_considered` and a
`missing` map giving a reason for each requested field that came back
empty:

```python
w = Weather(online=True, explain=True)
r = w.get("59221", "2023-01-15")  # remote NE Montana
print(r.stations_considered)  # e.g. 20
print(r.missing.get("tmax"))
# 'none of the 20 CDO stations near this point reported tmax on 2023-01-15'
```

`get --explain` prints the reasons on the CLI, `process --explain` adds
`stations_considered` and `missing` columns to the batch CSV, and
`get_frame(...)` includes them when present. (For a data consumer,
this turns a silent `NaN` into a diagnosable gap.)

## Which backend?

| `source=` | Data | Coverage | Needs | Notes |
|-----------|------|----------|-------|-------|
| `"station"` (default) | Raw GHCN/GSOD station observations | Wherever a station is near | `setup()` (local DB) | All 13 variables |
| `"grid"` | nClimGrid 5-km daily grid | **Any point in the Lower 48** | `[grid]` extra | Temp + precip only; CONUS only |
| `"auto"` | Station where available, grid to fill gaps | Best of both in CONUS | both | Prefers real observations |

```python
from get_weather_data import Weather  # pip install get-weather-data[grid]

# Any Lower-48 coordinate, guaranteed — no local database needed
weather = Weather(source="grid")
result = weather.get((44.06, -121.31), "2024-01-15")  # remote Oregon
print(result.tmax, result.station_type)  # -> value, "gridded"
```

nClimGrid caveats: contiguous US only (no Alaska/Hawaii/PR), maximum,
minimum, and average temperature plus precipitation only, and a ~2-3
day latency for the most recent days. Its daily values follow NOAA's
"24-hour period ending in the early morning of the specified day"
convention, which can differ slightly from a single station's
calendar-day value.

## Data Sources

This package uses data from NOAA's National Centers for Environmental
Information:

- **GHCN Daily**: Global Historical Climatology Network daily
  summaries (~93K stations across the US, Canada, and Mexico —
  border ZIPs get the truly nearest station)
- **GSOD**: Global Summary of the Day from USAF/WBAN airport stations
  (~9K); GSOD reports no snowfall, so `snow` comes from GHCN stations
- **nClimGrid-Daily**: NOAA's authoritative 5-km gridded daily
  temperature and precipitation for the contiguous US, 1951-present,
  interpolated from GHCN-Daily (used by `source="grid"`/`"auto"`)
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
