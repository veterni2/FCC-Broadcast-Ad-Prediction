"""Revenue aggregation by operator, DMA, office type, and quarter.

Revenue attribution rules:
1. Revenue attributed by flight_date (primary), invoice_period (fallback)
2. Documents with revenue_date_unknown=True are EXCLUDED
3. INVOICE dollars (realized) and CONTRACT dollars (pipeline) are SEPARATE columns
4. No extrapolation, smoothing, or imputation — ever
"""

from __future__ import annotations

from collections import defaultdict
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
        operator: Operator filter (partial match, case-insensitive).
        year: Campaign year filter.

    Returns:
        Dict with aggregated revenue tables:
        - by_operator_quarter: operator x quarter revenue matrix
        - by_dma: DMA-level revenue for INVOICE docs only
        - by_office_type: revenue split by office type for INVOICE docs only
    """
    records = db.get_extractions_for_model(operator=operator, year=year)
    log.info(f"Loaded {len(records)} extraction records for aggregation")

    # -----------------------------------------------------------------------
    # by_operator_quarter
    # Keyed by (operator_name, quarter, document_type)
    # -----------------------------------------------------------------------
    # Structure per key: {invoice_gross, invoice_net, invoice_count,
    #                     contract_gross, contract_net, contract_count}
    oq_map: dict[tuple, dict] = defaultdict(lambda: {
        "invoice_gross": 0.0,
        "invoice_net": 0.0,
        "invoice_doc_count": 0,
        "contract_gross": 0.0,
        "contract_net": 0.0,
        "contract_doc_count": 0,
    })

    # by_dma: keyed by (dma_name, dma_rank, operator_name, quarter), INVOICE only
    dma_map: dict[tuple, dict] = defaultdict(lambda: {
        "invoice_gross": 0.0,
        "invoice_net": 0.0,
        "invoice_doc_count": 0,
    })

    # by_office_type: keyed by (office_type, operator_name, quarter), INVOICE only
    ot_map: dict[tuple, dict] = defaultdict(lambda: {
        "invoice_gross": 0.0,
        "invoice_doc_count": 0,
    })

    for rec in records:
        operator_name: str = rec.get("operator_name") or "Unknown"
        quarter: str = rec.get("revenue_quarter") or "Unknown"
        doc_type: str = rec.get("document_type") or ""
        gross: Optional[float] = rec.get("gross_amount")
        net: Optional[float] = rec.get("net_amount")

        # --- by_operator_quarter ---
        oq_key = (operator_name, quarter)
        bucket = oq_map[oq_key]
        if doc_type == "INVOICE":
            if gross is not None:
                bucket["invoice_gross"] += gross
            if net is not None:
                bucket["invoice_net"] += net
            bucket["invoice_doc_count"] += 1
        elif doc_type == "CONTRACT":
            if gross is not None:
                bucket["contract_gross"] += gross
            if net is not None:
                bucket["contract_net"] += net
            bucket["contract_doc_count"] += 1

        # --- by_dma (INVOICE only) ---
        if doc_type == "INVOICE":
            dma_name: str = rec.get("dma_name") or "Unknown"
            dma_rank: Optional[int] = rec.get("dma_rank")
            dma_key = (dma_name, dma_rank, operator_name, quarter)
            dma_bucket = dma_map[dma_key]
            if gross is not None:
                dma_bucket["invoice_gross"] += gross
            if net is not None:
                dma_bucket["invoice_net"] += net
            dma_bucket["invoice_doc_count"] += 1

            # --- by_office_type (INVOICE only) ---
            office_type: str = rec.get("folder_office_type") or "unknown"
            ot_key = (office_type, operator_name, quarter)
            ot_bucket = ot_map[ot_key]
            if gross is not None:
                ot_bucket["invoice_gross"] += gross
            ot_bucket["invoice_doc_count"] += 1

    # -----------------------------------------------------------------------
    # Build output lists
    # -----------------------------------------------------------------------

    by_operator_quarter = sorted(
        [
            {
                "operator_name": op,
                "quarter": qtr,
                "invoice_gross": v["invoice_gross"],
                "invoice_net": v["invoice_net"],
                "contract_gross": v["contract_gross"],
                "contract_net": v["contract_net"],
                "invoice_doc_count": v["invoice_doc_count"],
                "contract_doc_count": v["contract_doc_count"],
            }
            for (op, qtr), v in oq_map.items()
        ],
        key=lambda x: (x["operator_name"], x["quarter"]),
    )

    by_dma = sorted(
        [
            {
                "dma_rank": dma_rank,
                "dma_name": dma_name,
                "operator_name": op,
                "quarter": qtr,
                "invoice_gross": v["invoice_gross"],
                "invoice_net": v["invoice_net"],
                "invoice_doc_count": v["invoice_doc_count"],
            }
            for (dma_name, dma_rank, op, qtr), v in dma_map.items()
        ],
        key=lambda x: (x["dma_rank"] if x["dma_rank"] is not None else 99999, x["quarter"]),
    )

    by_office_type = sorted(
        [
            {
                "office_type": office_type,
                "operator_name": op,
                "quarter": qtr,
                "invoice_gross": v["invoice_gross"],
                "invoice_doc_count": v["invoice_doc_count"],
            }
            for (office_type, op, qtr), v in ot_map.items()
        ],
        key=lambda x: (x["operator_name"], x["quarter"]),
    )

    log.info(
        f"Aggregation complete: {len(by_operator_quarter)} operator-quarter rows, "
        f"{len(by_dma)} DMA rows, {len(by_office_type)} office-type rows"
    )

    return {
        "by_operator_quarter": by_operator_quarter,
        "by_dma": by_dma,
        "by_office_type": by_office_type,
    }
