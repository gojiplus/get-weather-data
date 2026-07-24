"""Command-line interface for get-weather-data."""

import sys

import click
from rich.console import Console
from rich.table import Table

from get_weather_data import Weather, __version__
from get_weather_data.weather.units import ELEMENTS, unit_label

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
@click.argument("location")
@click.argument("target_date")
@click.option(
    "--units",
    type=click.Choice(["metric", "imperial"]),
    default="metric",
    help="Unit system for the values shown",
)
@click.option(
    "--elements",
    help="Comma-separated element codes to fetch (e.g. TMAX,PRCP)",
)
@click.option(
    "--online",
    is_flag=True,
    help="Query the NOAA CDO API directly (no local database; requires NCDC_TOKEN)",
)
@click.pass_context
def get(
    ctx: click.Context,
    location: str,
    target_date: str,
    units: str,
    elements: str | None,
    online: bool,
) -> None:
    """Get weather data for a location and date.

    LOCATION: 5-digit US ZIP code (e.g., 10001) or "lat,lon"
    coordinates (e.g., "40.75,-73.99")

    TARGET_DATE: Date in YYYY-MM-DD format (e.g., 2024-01-15)
    """
    try:
        weather = Weather(
            database_path=ctx.obj["database"],
            verbose=ctx.obj["verbose"],
            online=online,
            units=units,  # type: ignore[arg-type]
        )
        element_list = elements.split(",") if elements else None
        result = weather.get(location, target_date, elements=element_list)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

    table = Table(title=f"Weather for {location} on {target_date}")
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("Station ID", result.station_id or "N/A")
    table.add_row("Station Name", result.station_name or "N/A")
    table.add_row("Station Type", result.station_type or "N/A")
    table.add_row(
        "Distance",
        (
            f"{result.station_distance_meters:,} m"
            if result.station_distance_meters is not None
            else "N/A"
        ),
    )
    for element, spec in ELEMENTS.items():
        value = getattr(result, spec.field)
        label = unit_label(element, result.units)
        table.add_row(
            spec.description,
            f"{value:.1f} {label}" if value is not None else "N/A",
        )

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

    try:
        stats = weather.info()
    except RuntimeError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)

    table = Table(title="Database Statistics")
    table.add_column("Item", style="cyan")
    table.add_column("Count", style="green", justify="right")

    table.add_row("GHCN stations", f"{stats['ghcn_stations']:,}")
    table.add_row("USAF stations", f"{stats['usaf_stations']:,}")
    table.add_row("Total stations", f"{stats['total_stations']:,}")
    table.add_row("ZIP codes", f"{stats['zipcodes']:,}")

    console.print(table)


@cli.group()
def cache() -> None:
    """Inspect or clear cached data files."""


@cache.command("info")
def cache_info_cmd() -> None:
    """Show disk usage of each cache area."""
    from get_weather_data.core.cache import cache_info

    table = Table(title="Cache Usage")
    table.add_column("Area", style="cyan")
    table.add_column("Path")
    table.add_column("Files", justify="right")
    table.add_column("Size", style="green", justify="right")

    for entry in cache_info():
        table.add_row(
            entry.name,
            str(entry.path),
            f"{entry.files:,}",
            f"{entry.bytes / 1e6:,.1f} MB",
        )
    console.print(table)


@cache.command("clear")
@click.option("--ghcn", is_flag=True, help="Clear yearly GHCN databases")
@click.option("--gsod", is_flag=True, help="Clear per-station GSOD files")
@click.option("--stations", is_flag=True, help="Clear station lists and ZIP data")
@click.option("--all", "clear_all", is_flag=True, help="Clear everything")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt")
def cache_clear_cmd(
    ghcn: bool, gsod: bool, stations: bool, clear_all: bool, yes: bool
) -> None:
    """Delete cached data files (they re-download on next use)."""
    from get_weather_data.core.cache import clear_cache

    if not (ghcn or gsod or stations or clear_all):
        console.print("Nothing selected; use --ghcn/--gsod/--stations/--all")
        sys.exit(1)
    if not yes and not click.confirm("Delete the selected caches?"):
        sys.exit(1)
    freed = clear_cache(ghcn=ghcn, gsod=gsod, stations=stations, clear_all=clear_all)
    console.print(f"[green]Freed {freed / 1e6:,.1f} MB[/green]")


def main() -> None:
    """Entry point for CLI."""
    cli()


if __name__ == "__main__":
    main()
