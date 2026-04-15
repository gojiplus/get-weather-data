#!/bin/bash
# CLI usage examples for get-weather-data
#
# Install: pip install get-weather-data
# Or:      uv pip install get-weather-data

# 1. Set up the database (first time only, downloads ~50MB)
get-weather setup

# 2. Show database statistics
get-weather info

# 3. Get weather for a single location and date
get-weather get 10001 2024-01-15

# 4. Process a CSV file
# Input CSV must have a ZIP code column and date columns (either "date" or "year/month/day")
get-weather process input.csv output.csv --zip-column zip --year-column year --month-column month --day-column day

# 5. Use a custom database location
get-weather --database /path/to/my.db setup
get-weather --database /path/to/my.db get 10001 2024-01-15

# 6. Verbose output
get-weather -v get 10001 2024-01-15
