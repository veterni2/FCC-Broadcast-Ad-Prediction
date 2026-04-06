"""PDF downloader — fetches political file PDFs from FCC OPIF.

Downloads PDFs using folder_id + file_manager_id from the FCC download API.
Saves to data/pdfs/{callsign}/{doc_uuid}.pdf with deduplication (skip if
file exists and size matches).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..config.settings import get_settings
from ..core.db import DatabaseManager
from ..utils.logging import get_logger

log = get_logger("downloader")


async def download_documents(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
    limit: Optional[int] = None,
) -> dict[str, int]:
    """Download PDFs for all undownloaded documents.

    Args:
        db: Database manager instance.
        operator: Operator name filter.
        year: Campaign year filter.
        limit: Maximum number of PDFs to download.

    Returns:
        Dict with download statistics: downloaded, skipped, failed.
    """
    # TODO: Implement in Phase 3
    log.warning("PDF downloader not yet implemented")
    return {"downloaded": 0, "skipped": 0, "failed": 0}
