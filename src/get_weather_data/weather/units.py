"""Weather element registry and unit conversions.

GHCN raw scales differ by element: temperatures, precipitation, and
wind come in tenths; snowfall and snow depth in whole millimeters.
Everything internal converts to metric (°C, mm, m/s) exactly once and
imperial (°F, in, mph) is derived from metric on request.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

Units = Literal["metric", "imperial"]

IN_TO_MM = 25.4
MS_TO_MPH = 2.2369362920544
KNOTS_TO_MS = 0.514444


def _c_to_f(celsius: float) -> float:
    return celsius * 9.0 / 5.0 + 32.0


def f_to_c(fahrenheit: float) -> float:
    """Convert Fahrenheit to Celsius."""
    return (fahrenheit - 32.0) * 5.0 / 9.0


def _mm_to_in(mm: float) -> float:
    return mm / IN_TO_MM


def _ms_to_mph(ms: float) -> float:
    return ms * MS_TO_MPH


@dataclass(frozen=True)
class ElementSpec:
    """How one GHCN element maps to physical units."""

    field: str
    description: str
    raw_divisor: float
    metric_label: str
    imperial_label: str
    to_imperial: Callable[[float], float]


ELEMENTS: dict[str, ElementSpec] = {
    "TMAX": ElementSpec("tmax", "Maximum temperature", 10, "°C", "°F", _c_to_f),
    "TMIN": ElementSpec("tmin", "Minimum temperature", 10, "°C", "°F", _c_to_f),
    "TAVG": ElementSpec("tavg", "Average temperature", 10, "°C", "°F", _c_to_f),
    "TOBS": ElementSpec(
        "tobs", "Temperature at observation time", 10, "°C", "°F", _c_to_f
    ),
    "PRCP": ElementSpec("prcp", "Precipitation", 10, "mm", "in", _mm_to_in),
    "SNOW": ElementSpec("snow", "Snowfall", 1, "mm", "in", _mm_to_in),
    "SNWD": ElementSpec("snwd", "Snow depth", 1, "mm", "in", _mm_to_in),
    "AWND": ElementSpec(
        "awnd", "Average daily wind speed", 10, "m/s", "mph", _ms_to_mph
    ),
}


def ghcn_raw_to_metric(element: str, raw: float) -> float:
    """Convert a raw GHCN value to metric units.

    Args:
        element: GHCN element code (e.g. "TMAX").
        raw: Raw value as stored by GHCN.

    Returns:
        Value in metric units (°C, mm, or m/s).
    """
    return raw / ELEMENTS[element].raw_divisor


def convert(element: str, metric_value: float, units: Units) -> float:
    """Convert a metric value to the requested unit system.

    Args:
        element: GHCN element code.
        metric_value: Value in metric units.
        units: Target unit system.

    Returns:
        Converted value (unchanged for metric).
    """
    if units == "imperial":
        return ELEMENTS[element].to_imperial(metric_value)
    return metric_value


def unit_label(element: str, units: Units) -> str:
    """Display label for an element's unit in the given system.

    Args:
        element: GHCN element code.
        units: Unit system.

    Returns:
        Short unit label (e.g. "°C" or "in").
    """
    spec = ELEMENTS[element]
    return spec.imperial_label if units == "imperial" else spec.metric_label


def normalize_elements(elements: list[str] | None) -> list[str]:
    """Validate and upper-case a user-supplied element list.

    Args:
        elements: Element codes in any case, or None for all.

    Returns:
        Upper-cased element codes.

    Raises:
        ValueError: If an element code is unknown.
    """
    if elements is None:
        return list(ELEMENTS)
    normalized = [e.upper() for e in elements]
    unknown = [e for e in normalized if e not in ELEMENTS]
    if unknown:
        raise ValueError(
            f"Unknown weather element(s): {', '.join(unknown)}. "
            f"Valid elements: {', '.join(ELEMENTS)}"
        )
    return normalized
