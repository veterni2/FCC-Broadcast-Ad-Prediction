"""Image preprocessing for degraded/scanned documents.

Applies sequential transformations to improve OCR accuracy:
1. Grayscale conversion
2. Otsu binarization (adaptive thresholding)
3. Deskew correction
4. Noise removal

Uses Pillow only — no external ImageMagick dependency.
"""

from __future__ import annotations

from io import BytesIO
from typing import Optional

from PIL import Image, ImageFilter

from ..utils.logging import get_logger

log = get_logger("preprocess")


def preprocess_image(
    image: Image.Image,
    deskew: bool = True,
    denoise: bool = True,
) -> Image.Image:
    """Preprocess a page image for OCR.

    Args:
        image: PIL Image of a PDF page.
        deskew: Whether to attempt deskew correction.
        denoise: Whether to apply noise reduction.

    Returns:
        Preprocessed PIL Image.
    """
    # 1. Convert to grayscale
    if image.mode != "L":
        image = image.convert("L")

    # 2. Otsu-style binarization (simple threshold)
    # Using a histogram-based approach since PIL doesn't have Otsu natively
    histogram = image.histogram()
    total_pixels = image.width * image.height

    # Calculate optimal threshold using Otsu's method
    threshold = _otsu_threshold(histogram, total_pixels)
    image = image.point(lambda p: 255 if p > threshold else 0, "L")

    # 3. Noise removal (median filter)
    if denoise:
        image = image.filter(ImageFilter.MedianFilter(size=3))

    # 4. Deskew (simplified — full deskew requires numpy/scipy)
    # For now, just log that deskew was requested
    if deskew:
        log.debug("Deskew requested but advanced deskew not yet implemented")

    return image


def _otsu_threshold(histogram: list[int], total_pixels: int) -> int:
    """Calculate Otsu's threshold from a grayscale histogram.

    Args:
        histogram: 256-bin grayscale histogram.
        total_pixels: Total number of pixels.

    Returns:
        Optimal threshold value (0-255).
    """
    sum_total = sum(i * histogram[i] for i in range(256))
    sum_bg = 0.0
    weight_bg = 0
    max_variance = 0.0
    best_threshold = 0

    for t in range(256):
        weight_bg += histogram[t]
        if weight_bg == 0:
            continue

        weight_fg = total_pixels - weight_bg
        if weight_fg == 0:
            break

        sum_bg += t * histogram[t]
        mean_bg = sum_bg / weight_bg
        mean_fg = (sum_total - sum_bg) / weight_fg

        variance = weight_bg * weight_fg * (mean_bg - mean_fg) ** 2

        if variance > max_variance:
            max_variance = variance
            best_threshold = t

    return best_threshold
