"""Revenue aggregation by operator, DMA, office type, and quarter.

Revenue attribution rules:
1. Revenue attributed by flight_date (primary), invoice_period (fallback)
2. Documents with revenue_date_unknown=True are EXCLUDED
3. INVOICE dollars (realized) and CONTRACT dollars (pipeline) are SEPARATE columns
4. No extrapolation, smoothing, or imputation — ever
"""

from __future__ import annotations

from typing import Optional

from ..core.db import DatabaseManager
from ..utils.logging import get_logger

log = get_logger("aggregator")


def aggregate_revenue(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
) -> dict:
    """Aggregate extracted revenue data for the financial model.

    Args:
        db: Database manager instance.
        operator: Operator filter.
        year: Campaign year filter.

    Returns:
        Dict with aggregated revenue tables:
        - by_operator_quarter: operator x quarter revenue matrix
        - by_dma: DMA-level revenue with race metadata
        - by_office_type: revenue split by office type
    """
    # TODO: Implement in Phase 5
    log.warning("Revenue aggregation not yet implemented")
    return {
        "by_operator_quarter": [],
        "by_dma": [],
        "by_office_type": [],
    }
