#!/usr/bin/env python3
"""Example: Batch process a CSV file with ZIP codes and dates.

This example demonstrates:
1. Creating a sample CSV with random ZIP codes and dates
2. Processing the CSV to add weather data
3. Displaying the results

Prerequisites:
    pip install get-weather-data
    get-weather setup  # Downloads station data (~2 min)
"""

import csv
import random
import tempfile
from datetime import date, timedelta
from pathlib import Path

from get_weather_data import Weather

# Sample US ZIP codes from various cities
ZIP_CODES = [
    "10001",  # NYC
    "90210",  # Beverly Hills
    "60601",  # Chicago
    "77001",  # Houston
    "85001",  # Phoenix
    "19101",  # Philadelphia
    "78201",  # San Antonio
    "92101",  # San Diego
    "75201",  # Dallas
    "95101",  # San Jose
    "32801",  # Orlando
    "80201",  # Denver
    "98101",  # Seattle
    "30301",  # Atlanta
]


def generate_sample_csv(path: Path, num_rows: int = 20) -> None:
    """Generate a sample CSV with random ZIP codes and dates."""
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)

    rows = []
    for i in range(num_rows):
        zip_code = random.choice(ZIP_CODES)
        days_offset = random.randint(0, (end_date - start_date).days)
        target_date = start_date + timedelta(days=days_offset)
        rows.append(
            {
                "id": i + 1,
                "zip": zip_code,
                "year": target_date.year,
                "month": target_date.month,
                "day": target_date.day,
            }
        )

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "zip", "year", "month", "day"])
        writer.writeheader()
        writer.writerows(rows)


def display_results(path: Path) -> None:
    """Display weather results in a readable format."""
    with open(path) as f:
        rows = list(csv.DictReader(f))

    print("\n" + "=" * 70)
    print("WEATHER DATA RESULTS")
    print("=" * 70 + "\n")

    for row in rows[:10]:  # Show first 10
        date_str = f"{row['year']}-{row['month'].zfill(2)}-{row['day'].zfill(2)}"
        print(f"ZIP {row['zip']} on {date_str}")

        if row["station_id"]:
            print(f"  Station: {row['station_name']} ({row['station_type']})")
            print(f"  Distance: {int(row['station_distance_meters']):,} m")

            if row["tmax"]:
                tmax_c = float(row["tmax"]) / 10
                print(f"  Max Temp: {tmax_c:.1f}°C ({tmax_c * 9/5 + 32:.1f}°F)")
            if row["tmin"]:
                tmin_c = float(row["tmin"]) / 10
                print(f"  Min Temp: {tmin_c:.1f}°C ({tmin_c * 9/5 + 32:.1f}°F)")
            if row["prcp"]:
                print(f"  Precipitation: {float(row['prcp']) / 10:.1f} mm")
        else:
            print("  No weather data available")
        print()

    # Summary
    has_data = sum(1 for r in rows if r["station_id"])
    print(f"Summary: {has_data}/{len(rows)} rows have weather data")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        input_csv = Path(tmpdir) / "input.csv"
        output_csv = Path(tmpdir) / "output.csv"

        # Generate sample data
        print("Generating sample CSV with 20 random ZIP codes and dates...")
        generate_sample_csv(input_csv, num_rows=20)

        # Process with weather data
        print("Fetching weather data (this may download data from NOAA)...")
        weather = Weather()
        count = weather.process_csv(
            input_path=input_csv,
            output_path=output_csv,
            zipcode_column="zip",
            year_column="year",
            month_column="month",
            day_column="day",
            parallel=True,
        )
        print(f"Processed {count} rows")

        # Display results
        display_results(output_csv)


if __name__ == "__main__":
    main()
