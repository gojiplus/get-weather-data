"""Weather-type (present-weather) phenomena.

GHCN daily files carry ``WT**`` occurrence codes (value ``1`` = the
phenomenon occurred that day); GSOD carries a 6-character ``FRSHTT``
indicator. Both are occurrence flags without units, so they live outside
the numeric element registry. This module maps both to a common set of
lowercase phenomenon names.
"""

# GHCN WT** code -> phenomenon name (GHCN-Daily readme).
WT_CODES: dict[str, str] = {
    "WT01": "fog",
    "WT02": "heavy_fog",
    "WT03": "thunder",
    "WT04": "sleet",
    "WT05": "hail",
    "WT06": "glaze",
    "WT07": "blowing_dust",
    "WT08": "smoke_haze",
    "WT09": "blowing_snow",
    "WT10": "tornado",
    "WT11": "high_winds",
    "WT12": "blowing_spray",
    "WT13": "mist",
    "WT14": "drizzle",
    "WT15": "freezing_drizzle",
    "WT16": "rain",
    "WT17": "freezing_rain",
    "WT18": "snow",
    "WT19": "unknown_precip",
    "WT21": "ground_fog",
    "WT22": "ice_fog",
}

# GSOD FRSHTT indicator: one character per phenomenon, in order.
FRSHTT_ORDER = ("fog", "rain", "snow", "hail", "thunder", "tornado")


def ghcn_weather_type(code: str) -> str | None:
    """Phenomenon name for a GHCN element code, or None if not a WT code.

    Args:
        code: GHCN element code (e.g. "WT03").

    Returns:
        The phenomenon name, or None.
    """
    return WT_CODES.get(code)


def format_weather_types(types: set[str] | None) -> str:
    """Render a weather-type set as a stable, comma-joined string.

    Args:
        types: Phenomenon names, or None when not collected.

    Returns:
        Sorted comma-joined names (e.g. "fog,thunder"), or "" when the
        set is empty or None.
    """
    if not types:
        return ""
    return ",".join(sorted(types))


def parse_frshtt(indicator: str) -> set[str]:
    """Parse a GSOD FRSHTT indicator into phenomenon names.

    Args:
        indicator: The 6-character FRSHTT string (e.g. "010001").

    Returns:
        Set of phenomena flagged as present.
    """
    phenomena = set()
    for name, char in zip(FRSHTT_ORDER, indicator.strip(), strict=False):
        if char == "1":
            phenomena.add(name)
    return phenomena
