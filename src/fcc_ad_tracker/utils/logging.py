"""Structured logging setup for the FCC Ad Tracker.

Uses Python's standard logging with Rich handler for beautiful console output
and a file handler for persistent logs.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


_configured = False

console = Console(stderr=True)


def setup_logging(
    verbose: bool = False,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """Configure logging for the application.

    Args:
        verbose: If True, set level to DEBUG. Otherwise INFO.
        log_file: Optional path to a log file for persistent logging.

    Returns:
        The root logger for the fcc_ad_tracker package.
    """
    global _configured
    if _configured:
        return logging.getLogger("fcc_ad_tracker")

    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("fcc_ad_tracker")
    logger.setLevel(level)

    # Rich console handler
    rich_handler = RichHandler(
        console=console,
        show_time=True,
        show_path=verbose,
        markup=True,
        rich_tracebacks=True,
        tracebacks_show_locals=verbose,
    )
    rich_handler.setLevel(level)
    rich_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(rich_handler)

    # File handler (if requested)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)

    # Suppress noisy third-party loggers
    for noisy in ("httpx", "httpcore", "playwright", "anthropic"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _configured = True
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a child logger under the fcc_ad_tracker namespace."""
    return logging.getLogger(f"fcc_ad_tracker.{name}")
