# Command Line Interface

The `get-weather` command provides a CLI for common operations.

## Setup

Download station data and build the database:

```bash
get-weather setup
```

Options:
- `--force`: Rebuild even if database exists
- `--no-ghcn`: Skip GHCN stations
- `--no-usaf`: Skip USAF stations
- `--no-zipcodes`: Skip ZIP codes
- `--no-index`: Skip building closest stations index

## Get Weather

Get weather for a single ZIP code and date:

```bash
get-weather get 10001 2024-01-15
```

Output shows temperature, precipitation, and station info in a table.

## Process CSV

Add weather data to a CSV file:

```bash
get-weather process input.csv output.csv
```

Options:
- `--zip-column`: Column name for ZIP codes (default: "zip")
- `--date-column`: Column with date in YYYY-MM-DD format
- `--year-column`: Column for year (default: "year")
- `--month-column`: Column for month (default: "month")
- `--day-column`: Column for day (default: "day")

Example with date column:

```bash
get-weather process data.csv output.csv --zip-column zipcode --date-column event_date
```

## Info

Show database statistics:

```bash
get-weather info
```

## Global Options

These options work with all commands:

- `-d, --database PATH`: Use custom database path
- `-v, --verbose`: Enable verbose output
- `--version`: Show version
- `--help`: Show help
