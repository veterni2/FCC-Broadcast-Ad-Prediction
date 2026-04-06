"""Excel workbook generation using XlsxWriter.

Produces a 5-tab workbook:
1. Operator Summary — quarterly revenue by operator (gross/net), doc counts, coverage
2. DMA Detail — DMA-level breakout with race metadata
3. Weekly Velocity — filing velocity time series with embedded chart
4. Cycle Comparison — 2022 vs 2024 vs 2026 with growth rates
5. Raw Data — full extraction-level detail for audit trail

EVERY tab includes a coverage disclaimer header.

Data integrity guarantees:
- INVOICE and CONTRACT dollars are NEVER mixed in the same revenue line.
- NULL amounts are never imputed or shown as 0 without a disclaimer.
- Coverage rate is shown on every tab before any dollar figures.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import xlsxwriter

from ..config.settings import get_settings
from ..core.models import CoverageMetrics
from ..utils.logging import get_logger

log = get_logger("excel_writer")

# Fields excluded from the Raw Data tab (contain large blobs or internal keys)
_RAW_EXCLUDE_FIELDS = {"raw_text"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_millions(value: Optional[float]) -> Optional[float]:
    """Convert a dollar value to millions. Returns None for None/zero."""
    if value is None:
        return None
    return value / 1_000_000


def _coverage_pct(coverage_stats: list[CoverageMetrics]) -> float:
    """Compute aggregate coverage rate across all CoverageMetrics objects."""
    total_attempted = sum(c.total_documents_attempted for c in coverage_stats)
    total_extracted = sum(c.total_documents_extracted for c in coverage_stats)
    if total_attempted == 0:
        return 0.0
    return total_extracted / total_attempted


def _operator_coverage_map(coverage_stats: list[CoverageMetrics]) -> dict[str, float]:
    """Return {operator_name: coverage_rate} from coverage stats."""
    return {
        c.operator_name: c.coverage_rate
        for c in coverage_stats
        if c.operator_name is not None
    }


def _write_disclaimer(ws, row: int, disclaimer_text: str, warning_fmt, merge_end_col: int = 8) -> None:
    """Write the coverage disclaimer into a merged row."""
    ws.merge_range(row, 0, row, merge_end_col, disclaimer_text, warning_fmt)


def _write_headers(ws, row: int, headers: list[str], header_fmt) -> None:
    """Write a header row."""
    for col, label in enumerate(headers):
        ws.write(row, col, label, header_fmt)


def _safe_write(ws, row: int, col: int, value, number_fmt=None, string_fmt=None) -> None:
    """Write a cell, choosing number vs string format based on value type.

    None/missing values are written as '--' (string) to make the gap explicit.
    """
    if value is None:
        ws.write_string(row, col, "--", string_fmt)
    elif isinstance(value, (int, float)):
        if number_fmt is not None:
            ws.write_number(row, col, value, number_fmt)
        else:
            ws.write_number(row, col, value)
    else:
        if string_fmt is not None:
            ws.write_string(row, col, str(value), string_fmt)
        else:
            ws.write_string(row, col, str(value))


# ---------------------------------------------------------------------------
# Tab writers
# ---------------------------------------------------------------------------


def _write_operator_summary(
    workbook,
    operator_summary: list[dict],
    coverage_stats: list[CoverageMetrics],
    formats: dict,
    title_str: str,
) -> None:
    ws = workbook.add_worksheet("Operator Summary")
    ws.set_column(0, 0, 28)  # Operator
    ws.set_column(1, 1, 12)  # Quarter
    ws.set_column(2, 7, 16)  # Dollar/doc columns
    ws.set_column(8, 8, 12)  # Coverage %

    # Row 0: Title
    ws.merge_range(0, 0, 0, 8, title_str, formats["title"])

    # Row 1: Coverage disclaimer
    agg_rate = _coverage_pct(coverage_stats)
    disclaimer = (
        f"\u26a0 COVERAGE: {agg_rate:.1%} of discovered documents successfully extracted. "
        "Gaps shown as gaps \u2014 never imputed."
    )
    _write_disclaimer(ws, 1, disclaimer, formats["warning"], merge_end_col=8)

    # Row 2: blank
    # Row 3: column headers
    headers = [
        "Operator",
        "Quarter",
        "Invoice Gross ($M)",
        "Invoice Net ($M)",
        "Contract Gross ($M)",
        "Contract Net ($M)",
        "Invoice Docs",
        "Contract Docs",
        "Coverage %",
    ]
    _write_headers(ws, 3, headers, formats["header"])

    op_cov = _operator_coverage_map(coverage_stats)

    for i, row_data in enumerate(operator_summary):
        r = 4 + i
        op_name = row_data.get("operator_name") or ""
        quarter = row_data.get("quarter") or ""

        inv_gross = _to_millions(row_data.get("invoice_gross"))
        inv_net = _to_millions(row_data.get("invoice_net"))
        con_gross = _to_millions(row_data.get("contract_gross"))
        con_net = _to_millions(row_data.get("contract_net"))
        inv_docs = row_data.get("invoice_doc_count")
        con_docs = row_data.get("contract_doc_count")
        cov = op_cov.get(op_name)

        ws.write_string(r, 0, op_name)
        ws.write_string(r, 1, quarter)
        _safe_write(ws, r, 2, inv_gross, number_fmt=formats["money"])
        _safe_write(ws, r, 3, inv_net, number_fmt=formats["money"])
        _safe_write(ws, r, 4, con_gross, number_fmt=formats["money"])
        _safe_write(ws, r, 5, con_net, number_fmt=formats["money"])
        _safe_write(ws, r, 6, inv_docs)
        _safe_write(ws, r, 7, con_docs)
        if cov is not None:
            ws.write_number(r, 8, cov, formats["pct"])
        else:
            ws.write_string(r, 8, "--")


def _write_dma_detail(
    workbook,
    dma_detail: list[dict],
    coverage_stats: list[CoverageMetrics],
    formats: dict,
) -> None:
    ws = workbook.add_worksheet("DMA Detail")
    ws.set_column(0, 0, 10)   # DMA Rank
    ws.set_column(1, 1, 30)   # DMA Name
    ws.set_column(2, 2, 28)   # Operator
    ws.set_column(3, 3, 12)   # Quarter
    ws.set_column(4, 6, 16)   # Dollar/doc columns

    # Row 0: title
    agg_rate = _coverage_pct(coverage_stats)
    ws.merge_range(0, 0, 0, 6, "FCC Political Ad Revenue Model \u2014 DMA Detail", formats["title"])

    # Row 1: disclaimer
    disclaimer = (
        f"\u26a0 COVERAGE: {agg_rate:.1%} of discovered documents successfully extracted. "
        "Gaps shown as gaps \u2014 never imputed."
    )
    _write_disclaimer(ws, 1, disclaimer, formats["warning"], merge_end_col=6)

    # Row 3: headers
    headers = [
        "DMA Rank",
        "DMA Name",
        "Operator",
        "Quarter",
        "Invoice Gross ($M)",
        "Invoice Net ($M)",
        "Invoice Docs",
    ]
    _write_headers(ws, 3, headers, formats["header"])

    for i, row_data in enumerate(dma_detail):
        r = 4 + i
        dma_rank = row_data.get("dma_rank")
        dma_name = row_data.get("dma_name") or ""
        op_name = row_data.get("operator_name") or ""
        quarter = row_data.get("quarter") or ""

        inv_gross = _to_millions(row_data.get("invoice_gross"))
        inv_net = _to_millions(row_data.get("invoice_net"))
        inv_docs = row_data.get("invoice_doc_count")

        _safe_write(ws, r, 0, dma_rank)
        ws.write_string(r, 1, dma_name)
        ws.write_string(r, 2, op_name)
        ws.write_string(r, 3, quarter)
        _safe_write(ws, r, 4, inv_gross, number_fmt=formats["money"])
        _safe_write(ws, r, 5, inv_net, number_fmt=formats["money"])
        _safe_write(ws, r, 6, inv_docs)


def _write_weekly_velocity(
    workbook,
    velocity_data: list[dict],
    coverage_stats: list[CoverageMetrics],
    formats: dict,
) -> None:
    ws = workbook.add_worksheet("Weekly Velocity")
    ws.set_column(0, 0, 14)   # ISO Week
    ws.set_column(1, 1, 28)   # Operator
    ws.set_column(2, 4, 16)   # Numeric columns

    # Row 0: title
    agg_rate = _coverage_pct(coverage_stats)
    ws.merge_range(0, 0, 0, 5, "FCC Political Ad Revenue Model \u2014 Weekly Filing Velocity", formats["title"])

    # Row 1: disclaimer
    disclaimer = (
        f"\u26a0 COVERAGE: {agg_rate:.1%} of discovered documents successfully extracted. "
        "Gaps shown as gaps \u2014 never imputed."
    )
    _write_disclaimer(ws, 1, disclaimer, formats["warning"], merge_end_col=5)

    # Row 3: headers
    headers = [
        "ISO Week",
        "Operator",
        "New Docs",
        "Cumulative Docs",
        "Invoice Gross ($M)",
    ]
    _write_headers(ws, 3, headers, formats["header"])

    first_data_row = 4
    for i, row_data in enumerate(velocity_data):
        r = first_data_row + i
        iso_week = row_data.get("iso_week") or ""
        op_name = row_data.get("operator_name") or ""
        doc_count = row_data.get("doc_count")
        cumulative = row_data.get("cumulative_docs")
        inv_gross = _to_millions(row_data.get("invoice_gross"))

        ws.write_string(r, 0, iso_week)
        ws.write_string(r, 1, op_name)
        _safe_write(ws, r, 2, doc_count)
        _safe_write(ws, r, 3, cumulative)
        _safe_write(ws, r, 4, inv_gross, number_fmt=formats["money"])

    last_data_row = first_data_row + len(velocity_data) - 1

    # Add a line chart only when there is data to plot
    if velocity_data:
        chart = workbook.add_chart({"type": "line"})
        chart.add_series({
            "name": "Cumulative Docs",
            "categories": ["Weekly Velocity", first_data_row, 0, last_data_row, 0],
            "values": ["Weekly Velocity", first_data_row, 3, last_data_row, 3],
        })
        chart.set_title({"name": "Filing Velocity \u2014 Cumulative Documents"})
        chart.set_x_axis({"name": "ISO Week"})
        chart.set_y_axis({"name": "Cumulative Documents"})
        ws.insert_chart("G4", chart)


def _write_cycle_comparison(
    workbook,
    cycle_comparison: list[dict],
    coverage_stats: list[CoverageMetrics],
    formats: dict,
) -> None:
    ws = workbook.add_worksheet("Cycle Comparison")
    ws.set_column(0, 0, 16)   # Week of Cycle
    ws.set_column(1, 1, 28)   # Operator
    ws.set_column(2, 4, 16)   # Gross columns
    ws.set_column(5, 6, 14)   # Growth columns

    # Row 0: title
    agg_rate = _coverage_pct(coverage_stats)
    ws.merge_range(
        0, 0, 0, 6,
        "FCC Political Ad Revenue Model \u2014 Cycle Comparison (2022 / 2024 / 2026)",
        formats["title"],
    )

    # Row 1: disclaimer
    disclaimer = (
        f"\u26a0 COVERAGE: {agg_rate:.1%} of discovered documents successfully extracted. "
        "Gaps shown as gaps \u2014 never imputed."
    )
    _write_disclaimer(ws, 1, disclaimer, formats["warning"], merge_end_col=6)

    # Row 3: headers
    headers = [
        "Week of Cycle",
        "Operator",
        "2022 Gross ($M)",
        "2024 Gross ($M)",
        "2026 Gross ($M)",
        "2024 vs 2022",
        "2026 vs 2024",
    ]
    _write_headers(ws, 3, headers, formats["header"])

    for i, row_data in enumerate(cycle_comparison):
        r = 4 + i
        woc = row_data.get("week_of_cycle")
        op_name = row_data.get("operator_name") or ""

        gross_2022 = _to_millions(row_data.get("2022_gross"))
        gross_2024 = _to_millions(row_data.get("2024_gross"))
        gross_2026 = _to_millions(row_data.get("2026_gross"))
        growth_2024_vs_2022 = row_data.get("yoy_growth_2024_vs_2022")
        growth_2026_vs_2024 = row_data.get("yoy_growth_2026_vs_2024")

        _safe_write(ws, r, 0, woc)
        ws.write_string(r, 1, op_name)
        _safe_write(ws, r, 2, gross_2022, number_fmt=formats["money"])
        _safe_write(ws, r, 3, gross_2024, number_fmt=formats["money"])
        _safe_write(ws, r, 4, gross_2026, number_fmt=formats["money"])
        _safe_write(ws, r, 5, growth_2024_vs_2022, number_fmt=formats["pct"])
        _safe_write(ws, r, 6, growth_2026_vs_2024, number_fmt=formats["pct"])


def _write_raw_data(
    workbook,
    raw_data: list[dict],
    coverage_stats: list[CoverageMetrics],
    formats: dict,
) -> None:
    ws = workbook.add_worksheet("Raw Data")

    # Row 0: title
    agg_rate = _coverage_pct(coverage_stats)
    # Determine column count — need at least 1 for merge
    if raw_data:
        col_keys = [k for k in raw_data[0].keys() if k not in _RAW_EXCLUDE_FIELDS]
    else:
        col_keys = []
    # Require at least 1 extra column so merge_range spans more than one cell.
    merge_end = max(len(col_keys) - 1, 1)

    ws.merge_range(
        0, 0, 0, merge_end,
        "FCC Political Ad Revenue Model \u2014 Raw Data (Audit Trail)",
        formats["title"],
    )

    # Row 1: disclaimer
    disclaimer = (
        f"\u26a0 COVERAGE: {agg_rate:.1%} of discovered documents successfully extracted. "
        "Gaps shown as gaps \u2014 never imputed. "
        "This tab is for audit only. Do not use for investment decisions without validating coverage."
    )
    _write_disclaimer(ws, 1, disclaimer, formats["warning"], merge_end_col=merge_end)

    if not col_keys:
        ws.write_string(3, 0, "No raw data available.", formats["warning"])
        return

    # Row 3: headers
    _write_headers(ws, 3, col_keys, formats["header"])

    for i, row_data in enumerate(raw_data):
        r = 4 + i
        for col, key in enumerate(col_keys):
            val = row_data.get(key)
            if val is None:
                ws.write_string(r, col, "--")
            elif isinstance(val, (int, float)):
                ws.write_number(r, col, val)
            else:
                ws.write_string(r, col, str(val))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_workbook(
    operator_summary: list[dict],
    dma_detail: list[dict],
    velocity_data: list[dict],
    cycle_comparison: list[dict],
    raw_data: list[dict],
    coverage_stats: list[CoverageMetrics],
    output_path: Optional[Path] = None,
    operators: Optional[list[str]] = None,
    year: Optional[int] = None,
) -> Path:
    """Generate the political ad revenue model Excel workbook.

    Produces a 5-tab workbook (Operator Summary, DMA Detail, Weekly Velocity,
    Cycle Comparison, Raw Data).  Every tab begins with a coverage disclaimer
    row.  INVOICE and CONTRACT dollars are never mixed in the same column.
    NULL amounts are shown as '--' and never imputed.

    Args:
        operator_summary: Operator-level quarterly aggregation from
            aggregator.aggregate_revenue()["by_operator_quarter"].
            Fields: operator_name, quarter, invoice_gross, invoice_net,
                    contract_gross, contract_net, invoice_doc_count,
                    contract_doc_count.
        dma_detail: DMA-level detail from
            aggregator.aggregate_revenue()["by_dma"].
            Fields: dma_rank, dma_name, operator_name, quarter,
                    invoice_gross, invoice_net, invoice_doc_count.
        velocity_data: Weekly filing velocity from
            velocity.compute_filing_velocity().
            Fields: iso_week, operator_name, doc_count, cumulative_docs,
                    invoice_gross.
        cycle_comparison: Cross-cycle comparison from
            cycle_compare.compare_cycles().
            Fields: week_of_cycle, operator_name, 2022_gross, 2024_gross,
                    2026_gross, yoy_growth_2024_vs_2022,
                    yoy_growth_2026_vs_2024.
        raw_data: Full extraction records from db.get_extractions_for_model().
        coverage_stats: CoverageMetrics objects for disclaimer headers.
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

    # Build title string for tab 1
    ops_label = ", ".join(operators) if operators else "All Operators"
    year_label = str(year) if year else "All Years"
    title_str = f"FCC Political Ad Revenue Model \u2014 {ops_label} {year_label}"

    log.info(f"Generating workbook: {output_path}")

    workbook = xlsxwriter.Workbook(str(output_path))

    # ------------------------------------------------------------------
    # Shared formats
    # ------------------------------------------------------------------
    formats: dict = {
        "title": workbook.add_format({"bold": True, "font_size": 14}),
        "header": workbook.add_format({
            "bold": True,
            "bg_color": "#1F4E79",
            "font_color": "white",
        }),
        # Millions format: $1.2 M
        "money": workbook.add_format({"num_format": '$#,##0.0,," M"'}),
        "pct": workbook.add_format({"num_format": "0.0%"}),
        "warning": workbook.add_format({
            "bold": True,
            "bg_color": "#FFC000",
            "font_color": "#000000",
            "text_wrap": True,
        }),
    }

    # ------------------------------------------------------------------
    # Write each tab
    # ------------------------------------------------------------------
    _write_operator_summary(workbook, operator_summary, coverage_stats, formats, title_str)
    _write_dma_detail(workbook, dma_detail, coverage_stats, formats)
    _write_weekly_velocity(workbook, velocity_data, coverage_stats, formats)
    _write_cycle_comparison(workbook, cycle_comparison, coverage_stats, formats)
    _write_raw_data(workbook, raw_data, coverage_stats, formats)

    workbook.close()

    log.info(f"Workbook written to {output_path}")
    return output_path
