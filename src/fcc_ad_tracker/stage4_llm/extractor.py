"""LLM extraction orchestrator — batch processes documents through Claude.

Queries SQLite for documents with text extracted but not yet LLM-processed,
sends each through Claude for structured extraction, validates results,
and writes to the extractions table.

Enforces the per-run cost budget and commits partial results if exceeded.
"""

from __future__ import annotations

from typing import Optional

from ..config.settings import get_settings
from ..core.db import DatabaseManager
from ..core.exceptions import CostBudgetExceeded
from ..utils.dates import attribute_revenue_quarter
from ..utils.logging import get_logger
from .client import LLMClient
from .schemas import ExtractionConfidence

log = get_logger("extractor")


async def run_llm_extraction(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
    limit: Optional[int] = None,
) -> dict[str, int | float]:
    """Run LLM extraction on all unprocessed documents.

    Args:
        db: Database manager instance.
        operator: Operator name filter.
        year: Campaign year filter.
        limit: Maximum documents to process.

    Returns:
        Dict with stats: processed, success, failed, total_cost_usd.

    Raises:
        CostBudgetExceeded: If the cost budget is exceeded mid-run.
            Partial results are committed before raising.
    """
    settings = get_settings().llm
    budget = settings.cost_budget_per_run

    docs = db.get_unprocessed_docs(operator=operator, year=year, limit=limit)
    if not docs:
        log.info("No unprocessed documents found")
        return {"processed": 0, "success": 0, "failed": 0, "total_cost_usd": 0.0}

    log.info(f"Processing {len(docs)} documents through Claude extraction")

    client = LLMClient()
    processed = 0
    success = 0
    failed = 0

    run_id = db.start_run(
        stage="llm_extraction",
        operators=operator,
        year=year,
    )

    try:
        for doc in docs:
            # Check budget
            if client.total_cost >= budget:
                log.warning(
                    f"Cost budget exceeded: ${client.total_cost:.2f} >= ${budget:.2f}"
                )
                raise CostBudgetExceeded(client.total_cost, budget)

            doc_uuid = doc["doc_uuid"]
            raw_text = doc.get("raw_text", "")

            if not raw_text or not raw_text.strip():
                log.warning(f"Empty text for {doc_uuid}, marking as failed")
                db.mark_llm_processed(doc_uuid, "failed")
                failed += 1
                processed += 1
                continue

            try:
                result, usage = client.extract(raw_text)

                # Determine revenue quarter using the three-tier hierarchy
                quarter, date_source, date_unknown = attribute_revenue_quarter(
                    flight_start=result.flight_start,
                    flight_end=result.flight_end,
                    invoice_period_start=result.invoice_period_start,
                    invoice_period_end=result.invoice_period_end,
                    context_year=doc.get("year"),
                )

                # Determine extraction status
                if result.extraction_confidence == ExtractionConfidence.FAILED:
                    status = "failed"
                    failed += 1
                elif result.gross_or_net_flag == "neither":
                    status = "partial"
                    success += 1  # Still counts as processed
                else:
                    status = "success"
                    success += 1

                # Write extraction to database
                extraction_record = {
                    "doc_uuid": doc_uuid,
                    "document_type": result.document_type.value,
                    "advertiser_name": result.advertiser_name,
                    "office_type_extracted": result.office_type,
                    "gross_amount": result.gross_amount,
                    "net_amount": result.net_amount,
                    "agency_commission": result.agency_commission,
                    "gross_or_net_flag": result.gross_or_net_flag,
                    "class_of_time": result.class_of_time,
                    "num_spots": result.total_spots,
                    "lowest_unit_rate": result.lowest_unit_rate,
                    "actual_rate": None,  # Calculated from line items if needed
                    "flight_start": result.flight_start,
                    "flight_end": result.flight_end,
                    "invoice_date": result.invoice_date,
                    "invoice_period_start": result.invoice_period_start,
                    "invoice_period_end": result.invoice_period_end,
                    "station_callsign": result.station_callsign,
                    "dma_extracted": None,
                    "revenue_quarter": quarter,
                    "revenue_date_source": date_source,
                    "revenue_date_unknown": 1 if date_unknown else 0,
                    "extraction_confidence": result.extraction_confidence.value,
                    "confidence_notes": result.confidence_notes,
                    "input_tokens": usage["input_tokens"],
                    "output_tokens": usage["output_tokens"],
                    "estimated_cost_usd": usage["estimated_cost_usd"],
                }

                db.insert_extraction(extraction_record)
                db.mark_llm_processed(doc_uuid, status)

            except Exception as e:
                log.error(f"Extraction failed for {doc_uuid}: {e}")
                db.mark_llm_processed(doc_uuid, "failed")
                failed += 1

            processed += 1

    finally:
        # Always complete the run log, even on error
        db.complete_run(
            run_id=run_id,
            docs_processed=processed,
            docs_failed=failed,
            total_cost_usd=client.total_cost,
        )
        log.info(
            f"Extraction complete: {processed} processed, "
            f"{success} success, {failed} failed, "
            f"${client.total_cost:.4f} total cost"
        )

    return {
        "processed": processed,
        "success": success,
        "failed": failed,
        "total_cost_usd": client.total_cost,
    }
