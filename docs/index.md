# Get Weather Data

Get historical weather data for US ZIP codes using NOAA weather station data.

```{toctree}
:maxdepth: 2

quickstart
api
cli
```

## Installation

```bash
pip install get-weather-data
```

## Quick Example

```python
from get_weather_data import Weather

weather = Weather()
weather.setup()  # First time only

result = weather.get("10001", "2024-01-15")
print(f"Max temp: {result.tmax / 10:.1f} °C")
```

## Features

- Simple Python API with one class
- Command-line interface
- Automatic weather station selection
- Batch CSV processing
- GHCN Daily and GSOD data sources
