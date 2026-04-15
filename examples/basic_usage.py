#!/usr/bin/env python3
"""Basic usage example: Get weather data for a ZIP code and date.

This example shows how to:
1. Initialize the Weather class
2. Set up the database (download stations and ZIP codes)
3. Query weather data for a specific location and date
"""

from datetime import date

from get_weather_data import Weather

# Initialize
weather = Weather(verbose=True)

# Set up the database (downloads ~50MB of station data, takes a few minutes first time)
# After first run, this is instant
weather.setup()

# Get weather for NYC on January 15, 2024
result = weather.get("10001", date(2024, 1, 15))

print(f"Weather for ZIP {result.zipcode} on {result.date}")
print(f"  Station: {result.station_name} ({result.station_id})")
print(f"  Distance: {result.station_distance_meters:,} meters")
print()

if result.tmax is not None:
    print(f"  Max temp: {result.tmax / 10:.1f} °C")
if result.tmin is not None:
    print(f"  Min temp: {result.tmin / 10:.1f} °C")
if result.prcp is not None:
    print(f"  Precipitation: {result.prcp / 10:.1f} mm")
if result.snow is not None:
    print(f"  Snowfall: {result.snow} mm")
