"""Spatial interpolation of station observations.

Inverse-distance weighting (IDW) blends the nearest stations that report
an element, so a point *between* stations gets a weighted estimate
rather than the single nearest station's raw value. For temperatures an
optional lapse-rate correction adjusts each station to the query point's
elevation before weighting.
"""

from dataclasses import dataclass

# Standard environmental lapse rate: temperature falls ~6.5 °C per km of
# elevation gain.
LAPSE_RATE_C_PER_M = 0.0065

# Below this separation two points are treated as coincident.
_COINCIDENT_METERS = 1.0


@dataclass
class Sample:
    """One station's contribution to an interpolated value.

    Attributes:
        distance_meters: Distance from the query point.
        value: The station's metric value for the element.
        elevation_meters: Station elevation, if known (for lapse).
    """

    distance_meters: float
    value: float
    elevation_meters: float | None = None


def idw(
    samples: list[Sample],
    power: float = 2.0,
    target_elevation_meters: float | None = None,
    is_temperature: bool = False,
) -> float | None:
    """Inverse-distance-weighted estimate from station samples.

    Args:
        samples: Station contributions for one element.
        power: IDW exponent (higher = more local; 2 is typical).
        target_elevation_meters: Query-point elevation; when given with
            is_temperature, each sample is lapse-adjusted to it.
        is_temperature: Whether to apply the lapse-rate correction.

    Returns:
        The weighted value, or None if there are no samples.
    """
    if not samples:
        return None

    adjusted: list[tuple[float, float]] = []
    for sample in samples:
        value = sample.value
        if (
            is_temperature
            and target_elevation_meters is not None
            and sample.elevation_meters is not None
        ):
            # Move the reading from the station's elevation to the query
            # elevation: cooler as you go up.
            drop = target_elevation_meters - sample.elevation_meters
            value -= drop * LAPSE_RATE_C_PER_M
        adjusted.append((sample.distance_meters, value))

    # A coincident station wins outright (avoids divide-by-zero).
    for distance, value in adjusted:
        if distance <= _COINCIDENT_METERS:
            return value

    weight_sum = 0.0
    weighted = 0.0
    for distance, value in adjusted:
        weight = 1.0 / (distance**power)
        weight_sum += weight
        weighted += weight * value
    return weighted / weight_sum
