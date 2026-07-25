# Changelog

Notable changes to get-weather-data. Versions follow the git tags.

## 4.2.0

Added

- `Weather(interpolate=True)` inverse-distance-weights the nearest
  stations for a queried point instead of taking the single closest
  station, with an optional temperature lapse-rate correction (active
  once a query-point elevation is available).

## 4.1.0

Added

- `Weather.get_frame(...)` returns a tidy pandas DataFrame (optional
  extra: `pip install get-weather-data[pandas]`).
- Five new variables: peak wind gust, dew point, sea-level and station
  pressure, and visibility — GSOD fields that were previously computed
  and discarded, plus their GHCN codes.
- `include_flags=True` surfaces the GHCN quality-control flag per field
  on `WeatherResult.flags`.
- `Weather.coverage(...)` reports per-element data availability over a
  range and the station credited on the most days.

Changed

- GHCN values that failed a NOAA quality-control check are now dropped
  by default (`drop_flagged`), a correctness improvement.

## 4.0.0

- Real metric/imperial units everywhere (previously raw GHCN tenths);
  fixed a ~25x GSOD precipitation/snow-depth unit bug.
- Lat/lon accepted everywhere (`get`, `get_range`, CSV, CLI).
- Unit-sphere nearest-station ranking (v3 local indexes need
  `setup --force`).
- Thread-safe per-year GHCN builds, cache TTL + `cache` CLI, streaming
  batch with a `weather_error` column, and Canada/Mexico border
  stations.
- Optional CDO v2 online mode (`Weather(online=True)`).
