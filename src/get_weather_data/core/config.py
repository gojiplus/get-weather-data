"""Configuration management for get-weather-data."""

import os
from dataclasses import dataclass, field
from pathlib import Path

# XDG Base Directory paths
_XDG_DATA_HOME = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
_XDG_CACHE_HOME = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
_XDG_CONFIG_HOME = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))

APP_NAME = "get-weather-data"


@dataclass
class Config:
    """Configuration for get-weather-data."""

    # Paths
    data_dir: Path = field(default_factory=lambda: _XDG_DATA_HOME / APP_NAME)
    cache_dir: Path = field(default_factory=lambda: _XDG_CACHE_HOME / APP_NAME)
    config_dir: Path = field(default_factory=lambda: _XDG_CONFIG_HOME / APP_NAME)

    # Database
    database_path: Path | None = None

    # Station settings
    ghcn_station_count: int = 3
    usaf_station_count: int = 2
    coop_station_count: int = 0

    # API settings
    ncdc_token: str | None = None

    # Cache settings
    cache_max_age_days: int = 30

    def __post_init__(self) -> None:
        """Set up derived paths and load environment variables."""
        if self.database_path is None:
            self.database_path = self.data_dir / "weather.db"

        if self.ncdc_token is None:
            self.ncdc_token = os.environ.get("NCDC_TOKEN")

        # Ensure directories exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def ghcn_cache_dir(self) -> Path:
        """Cache directory for GHCN data."""
        path = self.cache_dir / "ghcn"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def gsod_cache_dir(self) -> Path:
        """Cache directory for GSOD data."""
        path = self.cache_dir / "gsod"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def stations_cache_dir(self) -> Path:
        """Cache directory for station data files."""
        path = self.cache_dir / "stations"
        path.mkdir(parents=True, exist_ok=True)
        return path


# Global config instance
_config: Config | None = None


def get_config() -> Config:
    """Get or create the global configuration."""
    global _config
    if _config is None:
        _config = Config()
    return _config


def set_config(config: Config) -> None:
    """Set the global configuration."""
    global _config
    _config = config
