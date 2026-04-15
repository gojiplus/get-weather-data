"""Logging setup for get-weather-data."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    verbose: bool = False,
    log_file: Path | str | None = None,
    name: str = "get_weather_data",
) -> logging.Logger:
    """Set up logging with console and optional file handlers.

    Args:
        verbose: If True, set level to DEBUG, otherwise INFO.
        log_file: Optional path for log file output.
        name: Logger name.

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(name)

    # Clear existing handlers
    logger.handlers.clear()

    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter("%(message)s")
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler (if requested)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10_000_000,
            backupCount=3,
        )
        file_handler.setLevel(level)
        file_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "get_weather_data") -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)
