#!/usr/bin/env python3
"""Process a CSV file with ZIP codes and dates to add weather data.

This example shows how to batch process a CSV file.
"""

import csv
from pathlib import Path

from get_weather_data import Weather

# Create sample input CSV
sample_data = """id,zip,year,month,day,event
1,10001,2024,1,15,Meeting
2,90210,2024,7,4,Holiday
3,60601,2024,3,20,Conference
"""

# Write sample input
input_file = Path("/tmp/weather_input.csv")
input_file.write_text(sample_data)

# Process
weather = Weather()
output_file = Path("/tmp/weather_output.csv")

rows = weather.process_csv(
    input_path=input_file,
    output_path=output_file,
    zipcode_column="zip",
    year_column="year",
    month_column="month",
    day_column="day",
)

print(f"Processed {rows} rows")
print(f"Output written to: {output_file}")
print()

# Show results
print("Results:")
print("-" * 80)
with open(output_file) as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(
            f"ZIP {row['zip']}: {row['tmax']} tmax, {row['tmin']} tmin, station {row['station_id']}"
        )
