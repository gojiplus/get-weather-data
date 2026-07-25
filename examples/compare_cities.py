#!/usr/bin/env python3
"""Compare a week of daily high temperatures across a few US cities.

A small real-world analysis using the online backend (no local database
build) — you only need a free NOAA token:

    pip install "get-weather-data[pandas]"
    export NCDC_TOKEN=...   # https://www.ncdc.noaa.gov/cdo-web/token
    python examples/compare_cities.py
"""

import pandas as pd

from get_weather_data import Weather

CITIES = {
    "New York": "10001",
    "Chicago": "60601",
    "Phoenix": "85001",
    "Miami": "33101",
    "Denver": "80201",
}

START, END = "2024-07-08", "2024-07-14"

weather = Weather(online=True, units="imperial")

frames = []
for city, zipcode in CITIES.items():
    df = weather.get_frame(zipcode, START, END)
    df["city"] = city
    frames.append(df[["city", "date", "tmax"]])

data = pd.concat(frames, ignore_index=True)

# One column per city, one row per day, values = daily high (°F)
pivot = data.pivot(index="date", columns="city", values="tmax")
print(f"Daily high temperature (°F), {START} to {END}\n")
print(pivot.round(0).to_string())

hottest = data.loc[data["tmax"].idxmax()]
print(
    f"\nHottest reading: {hottest['city']} on "
    f"{hottest['date'].date()} at {hottest['tmax']:.0f} °F"
)
print("\nWeek average high by city (°F):")
print(pivot.mean().round(1).sort_values(ascending=False).to_string())
