"""OCR engine for scanned/faxed political ad documents.

Primary engine: PaddleOCR (best accuracy on degraded docs, table detection).
Fallback: pytesseract (simpler install, less accurate on faxed documents).

The OCR engine is selected via the FCC_PDF_OCR_ENGINE environment variable.
If the primary engine is unavailable (import error), falls back automatically.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from PIL import Image

from ..config.settings import get_settings
from ..utils.logging import get_logger

log = get_logger("ocr")


def ocr_image(
    image: Image.Image,
    engine: Optional[str] = None,
) -> tuple[str, float]:
    """Run OCR on a preprocessed image.

    Args:
        image: PIL Image to OCR.
        engine: OCR engine to use ('paddleocr' or 'tesseract').
            Defaults to settings value.

    Returns:
        Tuple of (extracted_text, confidence_score).
        Confidence is 0.0-1.0, where 1.0 = high confidence.
    """
    if engine is None:
        engine = get_settings().pdf.ocr_engine

    if engine == "paddleocr":
        try:
            return _ocr_paddleocr(image)
        except ImportError:
            log.warning("PaddleOCR not installed, falling back to tesseract")
            return _ocr_tesseract(image)
    else:
        try:
            return _ocr_tesseract(image)
        except ImportError:
            log.warning("pytesseract not installed, trying PaddleOCR")
            return _ocr_paddleocr(image)


def ocr_pdf_pages(
    pdf_path: Path,
    engine: Optional[str] = None,
    dpi: Optional[int] = None,
) -> tuple[str, int, float]:
    """OCR all pages of a PDF by converting to images first.

    Uses PyMuPDF's get_pixmap() for PDF-to-image conversion (3x faster
    than pdf2image, no Poppler dependency).

    Args:
        pdf_path: Path to the PDF file.
        engine: OCR engine to use ('paddleocr' or 'tesseract').
            Defaults to settings value.
        dpi: Resolution for rendering. Defaults to settings value (300).

    Returns:
        Tuple of (full_text, page_count, avg_confidence).
    """
    if dpi is None:
        dpi = get_settings().pdf.dpi

    try:
        import fitz  # PyMuPDF
    except ImportError:
        log.error("PyMuPDF not installed — cannot render PDF to images")
        return "", 0, 0.0

    from .preprocess import preprocess_image

    doc = fitz.open(str(pdf_path))
    pages_text: list[str] = []
    confidences: list[float] = []

    zoom = dpi / 72.0  # 72 DPI is the default PDF resolution
    matrix = fitz.Matrix(zoom, zoom)

    for page_num, page in enumerate(doc):
        log.debug(f"OCR processing page {page_num + 1}/{len(doc)}")

        # Render page to image
        pixmap = page.get_pixmap(matrix=matrix)
        img = Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)

        # Preprocess for OCR
        img = preprocess_image(img)

        # Run OCR
        text, confidence = ocr_image(img, engine=engine)
        pages_text.append(text)
        confidences.append(confidence)

    doc.close()

    full_text = "\n\n".join(pages_text)
    page_count = len(pages_text)
    avg_confidence = sum(confidences) / max(len(confidences), 1)

    log.info(
        f"OCR complete: {page_count} pages, "
        f"{len(full_text)} chars, "
        f"avg confidence {avg_confidence:.2f}"
    )

    return full_text, page_count, avg_confidence


def _ocr_paddleocr(image: Image.Image) -> tuple[str, float]:
    """Run PaddleOCR on an image."""
    import numpy as np
    from paddleocr import PaddleOCR

    # Initialize PaddleOCR (lazy singleton would be better for production)
    ocr = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)

    # Convert PIL Image to numpy array
    img_array = np.array(image.convert("RGB"))

    result = ocr.ocr(img_array, cls=True)

    if not result or not result[0]:
        return "", 0.0

    lines: list[str] = []
    confidences: list[float] = []

    for line in result[0]:
        text = line[1][0]
        conf = line[1][1]
        lines.append(text)
        confidences.append(conf)

    full_text = "\n".join(lines)
    avg_confidence = sum(confidences) / max(len(confidences), 1)

    return full_text, avg_confidence


def _ocr_tesseract(image: Image.Image) -> tuple[str, float]:
    """Run pytesseract OCR on an image."""
    import pytesseract

    # Get text with confidence data
    data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)

    lines: list[str] = []
    confidences: list[float] = []
    current_line: list[str] = []
    current_line_num = -1

    for i, text in enumerate(data["text"]):
        if not text.strip():
            continue

        conf = int(data["conf"][i])
        if conf < 0:
            continue

        line_num = data["line_num"][i]
        if line_num != current_line_num:
            if current_line:
                lines.append(" ".join(current_line))
            current_line = []
            current_line_num = line_num

        current_line.append(text)
        confidences.append(conf / 100.0)

    if current_line:
        lines.append(" ".join(current_line))

    full_text = "\n".join(lines)
    avg_confidence = sum(confidences) / max(len(confidences), 1)

    return full_text, avg_confidence
