"""Excel workbook generation using XlsxWriter.

Produces a 5-tab workbook:
1. Operator Summary — quarterly revenue by operator (gross/net), doc counts, coverage
2. DMA Detail — DMA-level breakout with race metadata
3. Weekly Velocity — filing velocity time series with embedded chart
4. Cycle Comparison — 2022 vs 2024 vs 2026 with growth rates
5. Raw Data — full extraction-level detail for audit trail

EVERY tab includes a coverage disclaimer header.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from ..config.settings import get_settings
from ..utils.logging import get_logger

log = get_logger("excel_writer")


def generate_workbook(
    operator_summary: list[dict],
    dma_detail: list[dict],
    velocity_data: list[dict],
    cycle_comparison: list[dict],
    raw_data: list[dict],
    coverage_stats: dict,
    output_path: Optional[Path] = None,
    operators: Optional[list[str]] = None,
    year: Optional[int] = None,
) -> Path:
    """Generate the political ad revenue model Excel workbook.

    Args:
        operator_summary: Operator-level quarterly aggregation.
        dma_detail: DMA-level detail with race overlay.
        velocity_data: Weekly filing velocity time series.
        cycle_comparison: Cross-cycle comparison data.
        raw_data: Full extraction records for audit trail.
        coverage_stats: Coverage metrics for disclaimers.
        output_path: Output file path. Auto-generated if None.
        operators: Operator names for filename.
        year: Campaign year for filename.

    Returns:
        Path to the generated workbook.
    """
    if output_path is None:
        settings = get_settings()
        ops_str = "_".join(operators or ["all"])
        year_str = str(year or "all")
        timestamp = datetime.now().strftime("%Y%m%d")
        output_path = (
            settings.output.output_dir
            / f"political_ad_model_{ops_str}_{year_str}_{timestamp}.xlsx"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # TODO: Implement in Phase 5
    log.warning(f"Excel workbook generation not yet implemented. Would write to: {output_path}")

    return output_path
