"""Cross-cycle comparison (2022 vs 2024 vs 2026).

Joins data across election cycles for the same DMAs and office types.
Normalizes by week-of-cycle (weeks before election day), not calendar date,
to handle the ramp-up pattern correctly.
"""

from __future__ import annotations

from typing import Optional

from ..core.db import DatabaseManager
from ..utils.logging import get_logger

log = get_logger("cycle_compare")


def compare_cycles(
    db: DatabaseManager,
    operator: Optional[str] = None,
    cycles: Optional[list[int]] = None,
) -> list[dict]:
    """Compare political ad revenue across election cycles.

    Args:
        db: Database manager instance.
        operator: Operator filter.
        cycles: List of election years to compare. Defaults to [2022, 2024, 2026].

    Returns:
        List of comparison records by DMA/office_type/cycle.
    """
    # TODO: Implement in Phase 5
    log.warning("Cycle comparison not yet implemented")
    return []
