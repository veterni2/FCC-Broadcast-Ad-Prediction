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

    Issues two queries against the database — one for document counts and
    extraction status, one for revenue sums — then merges the results.

    Args:
        db: Database manager instance.
        operator: Operator filter (partial match, case-insensitive).
        year: Campaign year filter.

    Returns:
        List of CoverageMetrics, one per operator, sorted by operator_name.
    """
    # -----------------------------------------------------------------------
    # Query 1: document counts by operator and extraction status
    # -----------------------------------------------------------------------
    count_query = """
        SELECT operator_name,
               COUNT(*) as total_docs,
               SUM(CASE WHEN extraction_status = 'success' THEN 1 ELSE 0 END) as extracted,
               SUM(CASE WHEN extraction_status = 'failed' THEN 1 ELSE 0 END) as failed
        FROM documents
        WHERE pdf_downloaded = 1
    """
    count_params: list = []

    if operator:
        count_query += " AND LOWER(operator_name) LIKE ?"
        count_params.append(f"%{operator.lower()}%")
    if year:
        count_query += " AND year = ?"
        count_params.append(year)

    count_query += " GROUP BY operator_name ORDER BY operator_name"

    # -----------------------------------------------------------------------
    # Query 2: revenue sums from extractions, joined to documents for filters
    # -----------------------------------------------------------------------
    revenue_query = """
        SELECT d.operator_name,
               SUM(CASE WHEN e.document_type = 'INVOICE'
                        THEN COALESCE(e.gross_amount, 0) ELSE 0 END) as invoice_gross,
               SUM(CASE WHEN e.document_type = 'CONTRACT'
                        THEN COALESCE(e.gross_amount, 0) ELSE 0 END) as contract_gross,
               SUM(COALESCE(e.gross_amount, 0)) as total_gross
        FROM extractions e
        JOIN documents d ON e.doc_uuid = d.doc_uuid
        WHERE e.revenue_date_unknown = 0
    """
    revenue_params: list = []

    if operator:
        revenue_query += " AND LOWER(d.operator_name) LIKE ?"
        revenue_params.append(f"%{operator.lower()}%")
    if year:
        revenue_query += " AND d.year = ?"
        revenue_params.append(year)

    revenue_query += " GROUP BY d.operator_name"

    with db.read() as conn:
        count_rows = conn.execute(count_query, count_params).fetchall()
        revenue_rows = conn.execute(revenue_query, revenue_params).fetchall()

    log.info(
        f"Coverage query returned {len(count_rows)} operator count rows "
        f"and {len(revenue_rows)} revenue rows"
    )

    # -----------------------------------------------------------------------
    # Build revenue lookup keyed by operator_name
    # -----------------------------------------------------------------------
    revenue_by_op: dict[str, dict] = {}
    for row in revenue_rows:
        op = row["operator_name"]
        revenue_by_op[op] = {
            "invoice_gross": row["invoice_gross"] or 0.0,
            "contract_gross": row["contract_gross"] or 0.0,
            "total_gross": row["total_gross"] or 0.0,
        }

    # -----------------------------------------------------------------------
    # Merge and construct CoverageMetrics
    # -----------------------------------------------------------------------
    metrics: list[CoverageMetrics] = []

    for row in count_rows:
        op = row["operator_name"]
        total_docs: int = row["total_docs"] or 0
        extracted: int = row["extracted"] or 0
        failed: int = row["failed"] or 0
        coverage_rate: float = extracted / total_docs if total_docs > 0 else 0.0

        rev = revenue_by_op.get(op, {
            "invoice_gross": 0.0,
            "contract_gross": 0.0,
            "total_gross": 0.0,
        })

        metrics.append(
            CoverageMetrics(
                operator_name=op,
                dma_name=None,
                total_documents_attempted=total_docs,
                total_documents_extracted=extracted,
                total_documents_failed=failed,
                coverage_rate=coverage_rate,
                total_dollars_extracted=rev["total_gross"],
                invoice_dollars=rev["invoice_gross"],
                contract_dollars=rev["contract_gross"],
            )
        )

    log.info(f"Coverage computation complete: {len(metrics)} operators")
    return metrics
