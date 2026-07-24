"""Get weather data for US ZIP codes.

This package provides tools to:
- Download and build a database of weather stations
- Map ZIP codes to nearest weather stations
- Fetch historical weather data from GHCN and GSOD datasets
- Process CSV files with ZIP codes to add weather data

Basic usage:
    from get_weather_data import Weather

    weather = Weather()
    weather.setup()  # Download station data and build database

    # Get weather for a ZIP code and date
    result = weather.get("10001", "2024-01-15")
    print(f"Max temp: {result.tmax}")

    # Process a CSV file
    weather.process_csv("input.csv", "output.csv")
"""

from importlib.metadata import PackageNotFoundError, version

from get_weather_data.main import Weather
from get_weather_data.weather.location import LocationInput
from get_weather_data.weather.results import WeatherResult
from get_weather_data.weather.units import Units

try:
    __version__ = version("get-weather-data")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "LocationInput",
    "Units",
    "Weather",
    "WeatherResult",
    "__version__",
]
