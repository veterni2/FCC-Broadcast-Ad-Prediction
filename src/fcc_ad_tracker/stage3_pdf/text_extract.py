"""Direct text extraction from PDFs using PyMuPDF.

First-pass extraction for machine-readable PDFs. If the extracted
text has fewer characters per page than the threshold, the document
is flagged for OCR processing.

Uses PyMuPDF (fitz) which is ~60x faster than pdfminer and has
no external dependencies.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from ..config.settings import get_settings
from ..utils.logging import get_logger

log = get_logger("text_extract")


def extract_text_from_pdf(
    pdf_path: Path,
    min_chars_per_page: Optional[int] = None,
) -> tuple[str, int, int, bool]:
    """Extract text directly from a PDF using PyMuPDF.

    Args:
        pdf_path: Path to the PDF file.
        min_chars_per_page: Minimum characters per page to consider
            extraction successful. Defaults to settings value.

    Returns:
        Tuple of (text, page_count, char_count, needs_ocr).
        needs_ocr is True if char_count / page_count < min_chars_per_page.
    """
    if min_chars_per_page is None:
        min_chars_per_page = get_settings().pdf.min_text_chars_per_page

    try:
        import fitz  # PyMuPDF

        doc = fitz.open(str(pdf_path))
        pages_text: list[str] = []

        for page in doc:
            text = page.get_text("text")
            pages_text.append(text)

        doc.close()

        full_text = "\n\n".join(pages_text)
        page_count = len(pages_text)
        char_count = len(full_text.strip())

        # Determine if OCR is needed
        avg_chars = char_count / max(page_count, 1)
        needs_ocr = avg_chars < min_chars_per_page

        if needs_ocr:
            log.debug(
                f"Low text density ({avg_chars:.0f} chars/page) in {pdf_path.name} "
                f"— flagging for OCR"
            )
        else:
            log.debug(
                f"Extracted {char_count} chars from {page_count} pages in {pdf_path.name}"
            )

        return full_text, page_count, char_count, needs_ocr

    except Exception as e:
        log.error(f"PyMuPDF extraction failed for {pdf_path}: {e}")
        return "", 0, 0, True
