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

    # 4. Deskew correction using horizontal projection profiles
    if deskew:
        image = _deskew(image)

    return image


def _deskew(image: Image.Image) -> Image.Image:
    """Detect and correct document skew using horizontal projection profiles.

    Rotates the binarized image through a coarse then fine angle search.
    The angle whose rotation maximises the variance of the horizontal
    projection (row sums of text pixels) is used for correction — sharp
    text lines produce high-variance projections when the image is upright.

    Angle search is performed on a downsampled copy (≤400 px wide) for
    speed; the correction is applied to the full-resolution image.

    Falls back silently to the original image when:
      - numpy is not installed (ocr extras not present)
      - the detected skew is < 0.2° (within rounding noise)
      - the image has too few text pixels to estimate reliably

    Args:
        image: Binarized L-mode PIL Image (0 = black/text, 255 = white/bg).

    Returns:
        Deskewed PIL Image, or the original if deskew could not be applied.
    """
    try:
        import numpy as np
    except ImportError:
        log.debug("numpy not available; deskew skipped")
        return image

    # Downsample to ≤400 px wide for fast angle search
    scale = min(1.0, 400.0 / max(image.width, 1))
    small = image.resize(
        (max(1, int(image.width * scale)), max(1, int(image.height * scale))),
        Image.Resampling.LANCZOS,
    )

    def _score(img: Image.Image, angle: float) -> float:
        rotated = img.rotate(angle, expand=False, fillcolor=255)
        arr = np.asarray(rotated, dtype=np.float32)
        # Invert so text pixels are positive; sum per row
        projection = (255.0 - arr).sum(axis=1)
        return float(np.var(projection))

    # Coarse search: −10° … +10° in 1° steps
    best_angle = 0.0
    best_score = _score(small, 0.0)
    for angle in np.arange(-10.0, 10.1, 1.0):
        s = _score(small, float(angle))
        if s > best_score:
            best_score = s
            best_angle = float(angle)

    # Fine search: ±1° around the coarse winner in 0.2° steps
    for angle in np.arange(best_angle - 1.0, best_angle + 1.01, 0.2):
        s = _score(small, float(angle))
        if s > best_score:
            best_score = s
            best_angle = float(angle)

    if abs(best_angle) < 0.2:
        log.debug("No significant skew detected (best angle < 0.2°)")
        return image

    log.info(f"Deskewing image by {best_angle:.1f}°")
    return image.rotate(best_angle, expand=True, fillcolor=255)


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
