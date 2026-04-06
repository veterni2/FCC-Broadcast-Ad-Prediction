"""Stage 3 PDF pipeline orchestrator.

Coordinates text extraction across all downloaded-but-not-extracted documents.
Tries PyMuPDF first; falls back to OCR for scanned docs.

Extraction hierarchy:
    1. PyMuPDF   — fast, lossless for digitally-created PDFs
    2. PaddleOCR — preferred OCR engine for degraded/scanned pages
    3. Tesseract — fallback if PaddleOCR is not installed or crashes
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..config.settings import get_settings
from ..core.db import DatabaseManager
from ..utils.logging import get_logger
from .text_extract import extract_text_from_pdf

log = get_logger("pdf_pipeline")


async def run_pdf_pipeline(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
    limit: Optional[int] = None,
) -> dict[str, int]:
    """Run text extraction on all downloaded-but-not-yet-extracted documents.

    For each document the pipeline:
      1. Attempts PyMuPDF direct extraction (fast, no GPU).
      2. If the extracted text is sparse (< min_text_chars_per_page * page_count),
         or PyMuPDF signals OCR is needed, runs PaddleOCR.
      3. If PaddleOCR is unavailable or raises, falls back to Tesseract.
      4. Persists the result (including failures) to the database so the
         document is not retried unnecessarily on the next run.

    Args:
        db: Database manager instance.
        operator: Operator name filter (e.g. "gray", "nexstar").
        year: Campaign year filter.
        limit: Maximum documents to process in this run.

    Returns:
        Dict with counters:
            ``success``  — total documents successfully extracted (pymupdf + ocr),
            ``ocr_used`` — subset of successes that required OCR,
            ``failed``   — documents where all extraction methods failed,
            ``total``    — total documents attempted.
    """
    settings = get_settings()
    docs = db.get_unextracted_docs(operator=operator, year=year, limit=limit)

    if not docs:
        log.info("No unextracted documents found — nothing to do.")
        return {"success": 0, "ocr_used": 0, "failed": 0, "total": 0}

    log.info(f"Starting extraction run: {len(docs)} document(s) to process.")

    counters: dict[str, int] = {"success": 0, "ocr_used": 0, "failed": 0, "total": 0}

    for doc in docs:
        doc_uuid: str = doc["doc_uuid"]
        pdf_path_str: str | None = doc.get("pdf_path")
        counters["total"] += 1

        if not pdf_path_str:
            log.error(f"No pdf_path recorded for {doc_uuid} — skipping.")
            db.mark_text_extracted(doc_uuid, "", "failed", 0, 0)
            counters["failed"] += 1
            continue

        pdf_path = Path(pdf_path_str)
        if not pdf_path.exists():
            log.error(
                f"PDF file not found on disk for {doc_uuid}: {pdf_path} — skipping."
            )
            db.mark_text_extracted(doc_uuid, "", "failed", 0, 0)
            counters["failed"] += 1
            continue

        # --- Phase 1: PyMuPDF extraction ---
        try:
            text, page_count, char_count, needs_ocr = extract_text_from_pdf(pdf_path)
        except Exception as exc:  # noqa: BLE001
            log.error(f"PyMuPDF extraction raised for {doc_uuid}: {exc!r}")
            # Treat as if OCR is needed; page_count unknown so default to 0.
            text, page_count, char_count, needs_ocr = "", 0, 0, True

        min_chars = settings.pdf.min_text_chars_per_page * max(page_count, 1)
        ocr_required = needs_ocr or char_count < min_chars

        # --- Phase 2: OCR (if required) ---
        if ocr_required:
            ocr_text: str = ""
            confidence: float | None = None
            method: str = "failed"
            ocr_success = False

            # Attempt PaddleOCR first (preferred engine from settings).
            try:
                from .ocr import ocr_pdf_pages  # noqa: PLC0415

                preferred_engine = settings.pdf.ocr_engine
                log.debug(f"Running {preferred_engine} OCR on {doc_uuid}")
                ocr_text, confidence = ocr_pdf_pages(pdf_path, engine=preferred_engine)
                method = preferred_engine
                ocr_success = True

            except ImportError:
                log.warning(
                    f"OCR engine '{settings.pdf.ocr_engine}' not installed for "
                    f"{doc_uuid}; trying tesseract fallback."
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    f"OCR engine '{settings.pdf.ocr_engine}' failed for "
                    f"{doc_uuid}: {exc!r}; trying tesseract fallback."
                )

            # Tesseract fallback (only reached if preferred engine failed).
            if not ocr_success:
                try:
                    from .ocr import ocr_pdf_pages  # noqa: PLC0415

                    log.debug(f"Running tesseract OCR on {doc_uuid}")
                    ocr_text, confidence = ocr_pdf_pages(pdf_path, engine="tesseract")
                    method = "tesseract"
                    ocr_success = True

                except ImportError:
                    log.error(
                        f"Tesseract OCR not installed; cannot extract {doc_uuid}."
                    )
                except Exception as exc:  # noqa: BLE001
                    log.error(
                        f"Tesseract OCR also failed for {doc_uuid}: {exc!r}"
                    )

            if not ocr_success:
                # All extraction methods exhausted — record a failure marker so
                # this document is not retried on the next run.
                log.error(f"All extraction methods failed for {doc_uuid}.")
                db.mark_text_extracted(doc_uuid, "", "failed", 0, page_count)
                counters["failed"] += 1
                continue

            # OCR succeeded.
            text = ocr_text
            char_count = len(text)
            db.mark_text_extracted(
                doc_uuid,
                text,
                method,
                char_count,
                page_count,
                ocr_confidence=confidence,
            )
            log.info(
                f"OCR extracted {doc_uuid} via {method}: "
                f"{char_count:,} chars, {page_count} pages, "
                f"confidence={confidence:.3f if confidence is not None else 'N/A'}"
            )
            counters["success"] += 1
            counters["ocr_used"] += 1

        else:
            # --- Phase 1 was sufficient ---
            method = "pymupdf"
            db.mark_text_extracted(
                doc_uuid,
                text,
                method,
                char_count,
                page_count,
                ocr_confidence=None,
            )
            log.info(
                f"PyMuPDF extracted {doc_uuid}: {char_count:,} chars, {page_count} pages"
            )
            counters["success"] += 1

    log.info(
        f"Extraction run complete — "
        f"success={counters['success']}, "
        f"ocr_used={counters['ocr_used']}, "
        f"failed={counters['failed']}, "
        f"total={counters['total']}"
    )
    return counters
