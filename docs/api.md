# API Reference

## Weather Class

The main interface for getting weather data.

```{eval-rst}
.. autoclass:: get_weather_data.Weather
   :members:
   :undoc-members:
```

## WeatherResult

Data returned from weather queries.

```{eval-rst}
.. autoclass:: get_weather_data.weather.lookup.WeatherResult
   :members:
   :undoc-members:
```

## OnlineLookup

Database-free lookup backed by the NOAA CDO Web Services v2 API
(used when `Weather(online=True)`; requires `NCDC_TOKEN`). Stations
are resolved from ZIP centroids (small cached GeoNames file), nearest
first, so results carry real station distances.

```{eval-rst}
.. autoclass:: get_weather_data.weather.online.OnlineLookup
   :members:
   :undoc-members:
```

## NOAAClient

Low-level CDO v2 API client.

```{eval-rst}
.. autoclass:: get_weather_data.api.NOAAClient
   :members:
   :undoc-members:
```

## Database

Low-level database operations.

```{eval-rst}
.. autoclass:: get_weather_data.core.database.Database
   :members:
   :undoc-members:
```

## Station

Weather station data structure.

```{eval-rst}
.. autoclass:: get_weather_data.core.distance.Station
   :members:
   :undoc-members:
```
