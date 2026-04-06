"""Coverage metrics calculator.

Coverage metrics MUST be shown alongside any revenue figure.
The user must always see data quality before the number.

Metrics calculated:
- Total documents attempted vs. successfully extracted
- Dollar coverage by operator and DMA
- Failed extraction list (manual review queue)
- Stations with zero filings (possible data gap)
"""

from __future__ import annotations

from typing import Optional

from ..core.db import DatabaseManager
from ..core.models import CoverageMetrics
from ..utils.logging import get_logger

log = get_logger("coverage")


def compute_coverage(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
) -> list[CoverageMetrics]:
    """Compute coverage metrics by operator.

    Args:
        db: Database manager instance.
        operator: Operator filter.
        year: Campaign year filter.

    Returns:
        List of CoverageMetrics, one per operator (and optionally per DMA).
    """
    # TODO: Implement in Phase 5
    log.warning("Coverage metrics not yet implemented")
    return []
