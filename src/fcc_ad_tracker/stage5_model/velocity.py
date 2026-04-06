"""Weekly filing velocity time series.

Counts new documents and summed dollar amounts per ISO week,
grouped by operator. Useful as a leading indicator of political
ad revenue ahead of quarterly earnings releases.
"""

from __future__ import annotations

from typing import Optional

from ..core.db import DatabaseManager
from ..utils.logging import get_logger

log = get_logger("velocity")


def compute_filing_velocity(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
) -> list[dict]:
    """Compute weekly filing velocity by operator.

    Args:
        db: Database manager instance.
        operator: Operator filter.
        year: Campaign year filter.

    Returns:
        List of weekly records: {iso_week, operator, doc_count, total_dollars}.
    """
    # TODO: Implement in Phase 5
    log.warning("Filing velocity not yet implemented")
    return []
