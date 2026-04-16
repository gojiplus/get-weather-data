"""Command-line interface for get-weather-data."""

import sys

import click
from rich.console import Console
from rich.table import Table

from get_weather_data import Weather, __version__

console = Console()


@click.group()
@click.version_option(version=__version__)
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
@click.option(
    "-d",
    "--database",
    type=click.Path(),
    help="Path to SQLite database",
)
@click.pass_context
def cli(ctx: click.Context, verbose: bool, database: str | None) -> None:
    """Get weather data for US ZIP codes.

    This tool downloads weather station data, builds a database mapping
    ZIP codes to nearby stations, and fetches historical weather data.
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["database"] = database


@cli.command()
@click.option("--force", is_flag=True, help="Force rebuild even if database exists")
@click.option("--no-ghcn", is_flag=True, help="Skip GHCN stations")
@click.option("--no-usaf", is_flag=True, help="Skip USAF/WBAN stations")
@click.option("--no-zipcodes", is_flag=True, help="Skip ZIP code import")
@click.option("--no-index", is_flag=True, help="Skip closest stations index")
@click.pass_context
def setup(
    ctx: click.Context,
    force: bool,
    no_ghcn: bool,
    no_usaf: bool,
    no_zipcodes: bool,
    no_index: bool,
) -> None:
    """Set up the database with station and ZIP code data.

    Downloads weather station lists and ZIP code data from NOAA and GeoNames,
    then builds an index of closest stations for each ZIP code.
    """
    weather = Weather(
        database_path=ctx.obj["database"],
        verbose=ctx.obj["verbose"],
    )

    console.print("[bold]Setting up weather database...[/bold]")

    weather.setup(
        force=force,
        ghcn_stations=not no_ghcn,
        usaf_stations=not no_usaf,
        zipcodes=not no_zipcodes,
        closest_index=not no_index,
    )

    stats = weather.info()
    console.print("\n[green]Setup complete![/green]")
    console.print(f"  GHCN stations: {stats['ghcn_stations']:,}")
    console.print(f"  USAF stations: {stats['usaf_stations']:,}")
    console.print(f"  ZIP codes: {stats['zipcodes']:,}")


@cli.command()
@click.argument("zipcode")
@click.argument("target_date")
@click.pass_context
def get(ctx: click.Context, zipcode: str, target_date: str) -> None:
    """Get weather data for a ZIP code and date.

    ZIPCODE: 5-digit US ZIP code (e.g., 10001)
    TARGET_DATE: Date in YYYY-MM-DD format (e.g., 2024-01-15)
    """
    weather = Weather(
        database_path=ctx.obj["database"],
        verbose=ctx.obj["verbose"],
    )

    try:
        result = weather.get(zipcode, target_date)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

    table = Table(title=f"Weather for {zipcode} on {target_date}")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Station ID", result.station_id or "N/A")
    table.add_row("Station Name", result.station_name or "N/A")
    table.add_row("Station Type", result.station_type or "N/A")
    table.add_row(
        "Distance",
        (
            f"{result.station_distance_meters:,} m"
            if result.station_distance_meters
            else "N/A"
        ),
    )
    table.add_row("Max Temp", f"{result.tmax / 10:.1f} °C" if result.tmax else "N/A")
    table.add_row("Min Temp", f"{result.tmin / 10:.1f} °C" if result.tmin else "N/A")
    table.add_row("Avg Temp", f"{result.tavg / 10:.1f} °C" if result.tavg else "N/A")
    table.add_row(
        "Precipitation", f"{result.prcp / 10:.1f} mm" if result.prcp else "N/A"
    )
    table.add_row("Snowfall", f"{result.snow} mm" if result.snow else "N/A")
    table.add_row("Snow Depth", f"{result.snwd} mm" if result.snwd else "N/A")
    table.add_row("Avg Wind", f"{result.awnd / 10:.1f} m/s" if result.awnd else "N/A")

    console.print(table)


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
@click.option("--zip-column", default="zip", help="ZIP code column name or index")
@click.option("--date-column", help="Date column (YYYY-MM-DD format)")
@click.option("--year-column", default="year", help="Year column name")
@click.option("--month-column", default="month", help="Month column name")
@click.option("--day-column", default="day", help="Day column name")
@click.option("--parallel/--no-parallel", default=True, help="Use parallel processing")
@click.option("--workers", type=int, help="Number of worker threads (default: auto)")
@click.pass_context
def process(
    ctx: click.Context,
    input_file: str,
    output_file: str,
    zip_column: str,
    date_column: str | None,
    year_column: str,
    month_column: str,
    day_column: str,
    parallel: bool,
    workers: int | None,
) -> None:
    """Process a CSV file and add weather data.

    INPUT_FILE: Path to input CSV file
    OUTPUT_FILE: Path to output CSV file

    The input CSV must have a ZIP code column and either a date column
    (YYYY-MM-DD format) or separate year/month/day columns.
    """
    weather = Weather(
        database_path=ctx.obj["database"],
        verbose=ctx.obj["verbose"],
    )

    mode = "parallel" if parallel else "sequential"
    console.print(f"[bold]Processing {input_file} ({mode})...[/bold]")

    count = weather.process_csv(
        input_path=input_file,
        output_path=output_file,
        zipcode_column=zip_column,
        date_column=date_column,
        year_column=year_column if not date_column else None,
        month_column=month_column if not date_column else None,
        day_column=day_column if not date_column else None,
        parallel=parallel,
        max_workers=workers,
    )

    console.print(f"[green]Processed {count:,} rows[/green]")
    console.print(f"Output written to: {output_file}")


@cli.command()
@click.pass_context
def info(ctx: click.Context) -> None:
    """Show database statistics."""
    weather = Weather(
        database_path=ctx.obj["database"],
        verbose=ctx.obj["verbose"],
    )

    stats = weather.info()

    table = Table(title="Database Statistics")
    table.add_column("Item", style="cyan")
    table.add_column("Count", style="green", justify="right")

    table.add_row("GHCN stations", f"{stats['ghcn_stations']:,}")
    table.add_row("USAF stations", f"{stats['usaf_stations']:,}")
    table.add_row("Total stations", f"{stats['total_stations']:,}")
    table.add_row("ZIP codes", f"{stats['zipcodes']:,}")

    console.print(table)


def main() -> None:
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
