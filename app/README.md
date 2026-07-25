# get-weather-data Streamlit app

A no-code web UI for [get-weather-data](https://pypi.org/project/get-weather-data/):
enter a US ZIP code or lat/lon, pick a date range, and get NOAA daily
weather as a table, chart, and CSV download.

It is backed by the nClimGrid gridded product, so it needs no API key,
no quota, and no local station database — any point in the contiguous
US works, and it fits a small free dyno.

## Run locally

```bash
pip install -r app/requirements.txt
streamlit run app/streamlit_app.py
```

## Deploy (Streamlit Community Cloud)

Point a new app at this repo with:

- **Main file path**: `app/streamlit_app.py`
- **Requirements**: `app/requirements.txt` (picked up automatically)

Nothing else to configure — no secrets required.
