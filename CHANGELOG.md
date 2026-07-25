# Changelog

Notable changes to get-weather-data. Versions follow the git tags.

## 5.2.0

Added

- Present-weather phenomena: `Weather(include_weather_types=True)`
  populates `result.weather_types` (a set like `{"fog", "thunder"}`)
  from GHCN `WT**` occurrence codes and GSOD's `FRSHTT` indicator, on
  both the station and online backends.
- `--weather-types` flag on `get-weather get` (shown in the table) and
  `get-weather process` (adds a comma-joined `weather_types` CSV
  column); `get_frame(...)` adds the column when the data carries it.

Changed

- Library hygiene: attach a `NullHandler` to the package logger so
  importing get-weather-data never emits log output unless the host
  application configures logging.

## 5.1.0

Added

- Real end-to-end test suite (`pytest -m live`) validating output against
  known events (Buffalo 2022 blizzard, Phoenix July 2023 heat) and
  cross-checking the station and gridded backends.
- `examples/compare_cities.py` — compare several cities' temperatures.

Changed

- Raised the online backend's station search breadth so temperature is
  reached in dense community-observer (CoCoRaHS) metros.
- Test coverage raised to ~89% and now enforced in CI (floor 85).

Fixed

- Online mode could return `None` temperatures near cities blanketed by
  precipitation-only CoCoRaHS stations (e.g. Buffalo): the airport
  station carrying temperature was crowded out of the station budget.

## 5.0.0

Added

- Gridded backend: `Weather(source="grid")` serves NOAA nClimGrid-Daily,
  an authoritative 5-km daily grid, so any contiguous-US lat/long
  returns real temperature and precipitation with no station gaps and
  no local database. Sliced a cell at a time over OPeNDAP
  (`pip install get-weather-data[grid]`).
- `source="auto"` prefers raw station observations and fills days the
  station network cannot cover from the grid.
- A no-code [Streamlit app](app/) (gridded backend, no key/quota) for
  point-and-click access.

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
