#!/usr/bin/env python3
"""Get weather data for a date range.

This example shows how to query weather over multiple days.
"""

from datetime import date

from get_weather_data import Weather

weather = Weather()

# Get weather for a week
results = weather.get_range(
    zipcode="90210",
    start_date=date(2024, 7, 1),
    end_date=date(2024, 7, 7),
)

print("Weather for Beverly Hills (90210), July 1-7, 2024")
print("-" * 50)

for r in results:
    tmax = f"{r.tmax / 10:.0f}°C" if r.tmax else "N/A"
    tmin = f"{r.tmin / 10:.0f}°C" if r.tmin else "N/A"
    print(f"{r.date}: High {tmax}, Low {tmin}")
