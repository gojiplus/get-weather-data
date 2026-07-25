"""Public Streamlit app for get-weather-data.

Enter a US ZIP code or lat/lon, pick a date range, and get NOAA daily
weather as a table, chart, and CSV download. Backed by the nClimGrid
gridded product (no API key, no quota, any point in the Lower 48), so
it runs on a small free dyno without a local station database.

Run locally:  streamlit run app/streamlit_app.py
"""

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from get_weather_data import Weather
from get_weather_data.weather.gridded import in_conus
from get_weather_data.weather.location import parse_location
from get_weather_data.weather.units import ELEMENTS, unit_label

st.set_page_config(page_title="US Weather Data", page_icon="🌦️", layout="centered")

st.title("🌦️ US Weather Data")
st.caption(
    "Historical daily weather for any point in the contiguous US, from "
    "NOAA's authoritative nClimGrid record. No account needed."
)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch(location: str, start: str, end: str, units: str) -> pd.DataFrame:
    """Fetch a weather DataFrame from the gridded backend (cached)."""
    weather = Weather(source="grid", units=units)  # type: ignore[arg-type]
    return weather.get_frame(location, start, end)


with st.form("query"):
    location = st.text_input(
        "Location", value="10001", help='ZIP code (e.g. 10001) or "lat,lon"'
    )
    col1, col2 = st.columns(2)
    start = col1.date_input("Start date", value=date(2024, 1, 1))
    end = col2.date_input("End date", value=date(2024, 1, 31))
    units = st.radio("Units", ["metric", "imperial"], horizontal=True)
    submitted = st.form_submit_button("Get weather", type="primary")

if submitted:
    # Validate the location and CONUS coverage up front
    try:
        parsed = parse_location(location)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    if start > end:
        st.error("Start date must be on or before the end date.")
        st.stop()

    with st.spinner("Fetching from NOAA nClimGrid…"):
        try:
            frame = fetch(location, str(start), str(end), units)
        except Exception as exc:  # noqa: BLE001 - surfaced to the user
            st.error(f"Could not fetch data: {exc}")
            st.stop()

    if frame.empty or frame["tmax"].isna().all():
        lat = frame["latitude"].iloc[0] if len(frame) else None
        lon = frame["longitude"].iloc[0] if len(frame) else None
        if lat is not None and not in_conus(float(lat), float(lon)):
            st.warning(
                "That point is outside the contiguous US. nClimGrid covers "
                "the Lower 48 only (no Alaska, Hawaii, or Puerto Rico)."
            )
        else:
            st.warning("No data available for that location and date range.")
        st.stop()

    # Headline metrics from the first day with data
    st.subheader(f"{location} — {start} to {end}")
    grid_fields = ["tmax", "tmin", "tavg", "prcp"]
    cols = st.columns(len(grid_fields))
    for col, field in zip(cols, grid_fields, strict=True):
        code = next(c for c, spec in ELEMENTS.items() if spec.field == field)
        series = frame[field].dropna()
        if len(series):
            col.metric(
                ELEMENTS[code].description,
                f"{series.mean():.1f} {unit_label(code, units)}",  # type: ignore[arg-type]
                help="Range average",
            )

    # Temperature chart
    temp_cols = [c for c in ("tmax", "tmin", "tavg") if frame[c].notna().any()]
    if temp_cols:
        st.line_chart(frame.set_index("date")[temp_cols])

    # Table + download
    display = frame[["date", *grid_fields]].copy()
    st.dataframe(display, use_container_width=True, hide_index=True)
    st.download_button(
        "Download CSV",
        frame.to_csv(index=False).encode(),
        file_name=f"weather_{location}_{start}_{end}.csv",
        mime="text/csv",
    )

st.divider()
st.caption(
    "Data: NOAA nClimGrid-Daily (5-km, 1951–present). Built with "
    "[get-weather-data](https://pypi.org/project/get-weather-data/). "
    "Values follow nClimGrid's morning-ending day convention; recent "
    "days lag ~2–3 days."
)
