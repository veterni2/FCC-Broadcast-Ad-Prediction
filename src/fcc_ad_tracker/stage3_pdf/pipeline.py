"""PDF text extraction pipeline orchestrator.

Coordinates the extraction flow:
1. Query SQLite for unextracted documents
2. Try PyMuPDF direct text extraction
3. If char count below threshold, run OCR pipeline
4. Store extracted text in database
5. Update document status

The pipeline respects the three-tier extraction hierarchy:
PyMuPDF (fast, clean PDFs) -> PaddleOCR (degraded scans) -> pytesseract (fallback).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..core.db import DatabaseManager
from ..utils.logging import get_logger

log = get_logger("pdf_pipeline")


async def run_text_extraction(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
    limit: Optional[int] = None,
    force: bool = False,
) -> dict[str, int]:
    """Run text extraction on all unextracted documents.

    Args:
        db: Database manager instance.
        operator: Operator name filter.
        year: Campaign year filter.
        limit: Maximum documents to process.
        force: Re-extract already processed documents.

    Returns:
        Dict with stats: extracted, ocr_needed, failed.
    """
    # TODO: Implement in Phase 3
    log.warning("Text extraction pipeline not yet implemented")
    return {"extracted": 0, "ocr_needed": 0, "failed": 0}
