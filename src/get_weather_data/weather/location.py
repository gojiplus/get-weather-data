"""Location parsing: ZIP codes and lat/lon coordinates."""

from dataclasses import dataclass

# A ZIP code string ("10001"), a "lat,lon" string ("40.75,-73.99"),
# or a (lat, lon) tuple.
LocationInput = str | tuple[float, float]


@dataclass(frozen=True)
class ResolvedLocation:
    """A query point with its originating ZIP code, when known."""

    lat: float
    lon: float
    zipcode: str | None = None


def _validate_coords(lat: float, lon: float) -> tuple[float, float]:
    """Range-check a coordinate pair.

    Args:
        lat: Latitude in degrees.
        lon: Longitude in degrees.

    Returns:
        The validated (lat, lon) pair.

    Raises:
        ValueError: If either value is out of range.
    """
    if not (-90.0 <= lat <= 90.0):
        raise ValueError(f"Latitude {lat} out of range [-90, 90]")
    if not (-180.0 <= lon <= 180.0):
        raise ValueError(f"Longitude {lon} out of range [-180, 180]")
    return (lat, lon)


def parse_location(raw: LocationInput) -> str | tuple[float, float]:
    """Parse a location input into a ZIP string or coordinate pair.

    Args:
        raw: ZIP code string, "lat,lon" string, or (lat, lon) tuple.

    Returns:
        A zero-padded 5-digit ZIP string, or a validated (lat, lon)
        tuple.

    Raises:
        ValueError: If the input cannot be parsed or is out of range.
    """
    if isinstance(raw, tuple):
        lat, lon = raw
        return _validate_coords(float(lat), float(lon))

    text = raw.strip()
    if "," in text:
        parts = text.split(",")
        if len(parts) != 2:
            raise ValueError(f"Cannot parse coordinates from {raw!r}")
        try:
            lat, lon = float(parts[0]), float(parts[1])
        except ValueError:
            raise ValueError(f"Cannot parse coordinates from {raw!r}") from None
        return _validate_coords(lat, lon)

    if not text.isdigit() or len(text) > 5:
        raise ValueError(
            f"Cannot parse location {raw!r}: expected a 5-digit ZIP code, "
            '"lat,lon" string, or (lat, lon) tuple'
        )
    return text.zfill(5)
