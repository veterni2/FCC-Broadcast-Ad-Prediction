"""Political file crawler orchestrator.

Coordinates the OPIF API client and browser to discover new
political ad documents, deduplicates against the SQLite registry,
and manages incremental crawl state.
"""

from __future__ import annotations

from typing import Optional

from ..core.db import DatabaseManager
from ..core.models import Station
from ..utils.logging import get_logger

log = get_logger("crawler")


async def crawl_stations(
    db: DatabaseManager,
    stations: list[Station],
    year: int,
    incremental: bool = False,
) -> dict[str, int]:
    """Crawl FCC OPIF for political ad documents across all stations.

    Args:
        db: Database manager instance.
        stations: List of stations to crawl.
        year: Campaign year to crawl.
        incremental: If True, only crawl stations not checked recently.

    Returns:
        Dict with crawl statistics: new_docs, skipped_existing, errors.
    """
    # TODO: Implement in Phase 2
    log.warning("Crawler not yet implemented")
    return {"new_docs": 0, "skipped_existing": 0, "errors": 0}
