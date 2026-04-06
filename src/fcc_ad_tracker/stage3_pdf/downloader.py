"""PDF downloader — fetches political file PDFs from FCC OPIF.

Downloads PDFs using folder_uuid + file_uuid from the confirmed download URL:
    GET https://publicfiles.fcc.gov/api/manager/download/{folder_uuid}/{file_uuid}.pdf

Saves to data/pdfs/{callsign}/{doc_uuid}.pdf with deduplication
(skips if file already exists and size roughly matches).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import httpx

from ..config.settings import get_settings
from ..core.db import DatabaseManager
from ..utils.logging import get_logger
from ..utils.rate_limiter import RateLimiter

log = get_logger("downloader")

_USER_AGENT = "FCC-Ad-Tracker/0.1.0 (political-file-research)"
_MIN_VALID_SIZE = 1000  # bytes — anything smaller is likely an error page


async def download_documents(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
    limit: Optional[int] = None,
) -> dict[str, int]:
    """Download PDFs for all undownloaded documents.

    Fetches each document from the FCC OPIF using the confirmed URL pattern:
        GET {base_url}/api/manager/download/{folder_uuid}/{doc_uuid}.pdf

    Files are streamed to disk to avoid loading large PDFs into memory.
    Already-downloaded files are skipped (deduplication by existence + size).

    Args:
        db: Database manager instance.
        operator: Operator name filter (e.g. "gray", "nexstar").
        year: Campaign year filter.
        limit: Maximum number of PDFs to download in this run.

    Returns:
        Dict with counters: ``downloaded``, ``skipped``, ``failed``.
    """
    settings = get_settings()
    docs = db.get_undownloaded_docs(operator=operator, year=year, limit=limit)

    if not docs:
        log.info("No undownloaded documents found — nothing to do.")
        return {"downloaded": 0, "skipped": 0, "failed": 0}

    log.info(f"Starting download run: {len(docs)} document(s) to process.")

    rate_limiter = RateLimiter(requests_per_second=settings.fcc.rate_limit_rps)

    counters: dict[str, int] = {"downloaded": 0, "skipped": 0, "failed": 0}

    headers = {"User-Agent": _USER_AGENT}
    timeout = httpx.Timeout(60.0, connect=15.0)

    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    ) as client:
        for doc in docs:
            doc_uuid: str = doc["doc_uuid"]
            folder_uuid: str | None = doc.get("folder_uuid")
            callsign: str = doc.get("callsign") or "UNKNOWN"

            # Guard: folder_uuid is required to build the download URL.
            if not folder_uuid:
                log.warning(
                    f"Skipping {doc_uuid} ({callsign}): folder_uuid is missing or empty."
                )
                counters["failed"] += 1
                continue

            url = (
                f"{settings.fcc.base_url}/api/manager/download"
                f"/{folder_uuid}/{doc_uuid}.pdf"
            )
            save_path = settings.pdf.storage_dir / callsign / f"{doc_uuid}.pdf"

            # Deduplication: if the file is already on disk and non-trivially sized,
            # trust it and move on — no HTTP request needed.
            if save_path.exists() and save_path.stat().st_size > _MIN_VALID_SIZE:
                log.debug(f"Already on disk, marking skipped: {save_path}")
                db.mark_downloaded(doc_uuid, str(save_path))
                counters["skipped"] += 1
                continue

            # Ensure target directory exists before streaming.
            save_path.parent.mkdir(parents=True, exist_ok=True)

            await rate_limiter.acquire()

            try:
                log.debug(f"Downloading {doc_uuid} from {url}")
                async with client.stream("GET", url) as response:
                    response.raise_for_status()

                    with save_path.open("wb") as fh:
                        async for chunk in response.aiter_bytes(chunk_size=65_536):
                            fh.write(chunk)

                final_size = save_path.stat().st_size
                if final_size < _MIN_VALID_SIZE:
                    # The server returned something, but it looks too small to be a
                    # real PDF — treat as a failure so we retry next run.
                    log.warning(
                        f"Downloaded file suspiciously small ({final_size} bytes) "
                        f"for {doc_uuid} — marking failed."
                    )
                    save_path.unlink(missing_ok=True)
                    counters["failed"] += 1
                    continue

                db.mark_downloaded(doc_uuid, str(save_path))
                log.info(
                    f"Downloaded {doc_uuid} ({callsign}) → {save_path} "
                    f"({final_size:,} bytes)"
                )
                counters["downloaded"] += 1

            except httpx.HTTPStatusError as exc:
                log.error(
                    f"HTTP {exc.response.status_code} downloading {doc_uuid} "
                    f"({callsign}): {url}"
                )
                save_path.unlink(missing_ok=True)
                counters["failed"] += 1

            except Exception as exc:  # noqa: BLE001
                log.error(
                    f"Unexpected error downloading {doc_uuid} ({callsign}): {exc!r}"
                )
                save_path.unlink(missing_ok=True)
                counters["failed"] += 1

    log.info(
        f"Download run complete — "
        f"downloaded={counters['downloaded']}, "
        f"skipped={counters['skipped']}, "
        f"failed={counters['failed']}"
    )
    return counters
