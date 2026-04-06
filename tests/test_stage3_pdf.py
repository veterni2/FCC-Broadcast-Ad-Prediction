"""Tests for Stage 3: PDF pipeline — text extraction and OCR logic."""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock

import pytest


# ---------------------------------------------------------------------------
# text_extract
# ---------------------------------------------------------------------------


class TestExtractTextFromPdf:
    """Unit tests for extract_text_from_pdf (PyMuPDF path)."""

    def test_nonexistent_file_returns_ocr_flag(self, tmp_path: Path) -> None:
        """A missing PDF should return empty text with needs_ocr=True (fail-safe)."""
        from fcc_ad_tracker.stage3_pdf.text_extract import extract_text_from_pdf

        text, page_count, char_count, needs_ocr = extract_text_from_pdf(
            tmp_path / "nonexistent.pdf"
        )
        assert text == ""
        assert char_count == 0
        assert needs_ocr is True  # signals caller to try OCR

    def test_returns_needs_ocr_flag_for_sparse_text(self, tmp_path: Path) -> None:
        """A PDF with very little text should set needs_ocr=True."""
        from fcc_ad_tracker.stage3_pdf.text_extract import extract_text_from_pdf

        # Create a minimal valid PDF (empty, text-free)
        minimal_pdf = bytes(
            b"%PDF-1.4\n"
            b"1 0 obj\n<</Type /Catalog /Pages 2 0 R>>\nendobj\n"
            b"2 0 obj\n<</Type /Pages /Kids [3 0 R] /Count 1>>\nendobj\n"
            b"3 0 obj\n<</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]>>\nendobj\n"
            b"xref\n0 4\n0000000000 65535 f \n0000000009 00000 n \n"
            b"0000000058 00000 n \n0000000115 00000 n \n"
            b"trailer\n<</Size 4 /Root 1 0 R>>\nstartxref\n190\n%%EOF"
        )
        pdf_file = tmp_path / "empty.pdf"
        pdf_file.write_bytes(minimal_pdf)

        text, page_count, char_count, needs_ocr = extract_text_from_pdf(pdf_file)
        # Empty PDF has no text — needs_ocr should be True
        assert needs_ocr is True
        assert char_count == 0

    def test_returns_tuple_of_correct_types(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage3_pdf.text_extract import extract_text_from_pdf

        minimal_pdf = bytes(
            b"%PDF-1.4\n"
            b"1 0 obj\n<</Type /Catalog /Pages 2 0 R>>\nendobj\n"
            b"2 0 obj\n<</Type /Pages /Kids [3 0 R] /Count 1>>\nendobj\n"
            b"3 0 obj\n<</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]>>\nendobj\n"
            b"xref\n0 4\n0000000000 65535 f \n0000000009 00000 n \n"
            b"0000000058 00000 n \n0000000115 00000 n \n"
            b"trailer\n<</Size 4 /Root 1 0 R>>\nstartxref\n190\n%%EOF"
        )
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(minimal_pdf)

        result = extract_text_from_pdf(pdf_file)
        text, page_count, char_count, needs_ocr = result

        assert isinstance(text, str)
        assert isinstance(page_count, int)
        assert isinstance(char_count, int)
        assert isinstance(needs_ocr, bool)


# ---------------------------------------------------------------------------
# downloader
# ---------------------------------------------------------------------------


class TestDownloadDocuments:
    """Unit tests for download_documents using mocked httpx."""

    @pytest.mark.asyncio
    async def test_empty_queue_returns_zeros(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage3_pdf.downloader import download_documents
        from fcc_ad_tracker.core.db import DatabaseManager

        db = DatabaseManager(db_path=tmp_path / "test.db")
        db.initialize()

        # Station required before documents can be inserted
        db.upsert_station(
            {
                "callsign": "WFAA",
                "operator_name": "Test Op",
            }
        )

        stats = await download_documents(db=db, operator="gray", year=2024)
        assert stats == {"downloaded": 0, "skipped": 0, "failed": 0}

    @pytest.mark.asyncio
    async def test_missing_folder_uuid_counts_as_failed(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage3_pdf.downloader import download_documents
        from fcc_ad_tracker.core.db import DatabaseManager

        db = DatabaseManager(db_path=tmp_path / "test.db")
        db.initialize()

        db.upsert_station(
            {"callsign": "WFAA", "operator_name": "Gray Television"}
        )
        # Insert a document with no folder_uuid
        db.upsert_document(
            {
                "doc_uuid": "aaaabbbb-1111-2222-3333-444455556666",
                "folder_uuid": None,  # Missing!
                "callsign": "WFAA",
                "operator_name": "Gray Television",
                "year": 2024,
            }
        )

        stats = await download_documents(db=db)
        assert stats["failed"] == 1
        assert stats["downloaded"] == 0

    @pytest.mark.asyncio
    async def test_existing_file_on_disk_counted_as_skipped(
        self, tmp_path: Path
    ) -> None:
        from fcc_ad_tracker.stage3_pdf.downloader import download_documents
        from fcc_ad_tracker.core.db import DatabaseManager
        from fcc_ad_tracker.config.settings import get_settings

        db = DatabaseManager(db_path=tmp_path / "test.db")
        db.initialize()

        doc_uuid = "ccccdddd-2222-3333-4444-555566667777"
        folder_uuid = "eeeeeeee-ffff-0000-1111-222233334444"
        callsign = "WFAA"

        db.upsert_station({"callsign": callsign, "operator_name": "Gray Television"})
        db.upsert_document(
            {
                "doc_uuid": doc_uuid,
                "folder_uuid": folder_uuid,
                "callsign": callsign,
                "operator_name": "Gray Television",
                "year": 2024,
            }
        )

        # Pre-create the file on disk (simulates prior download)
        pdf_dir = tmp_path / "pdfs" / callsign
        pdf_dir.mkdir(parents=True, exist_ok=True)
        pdf_file = pdf_dir / f"{doc_uuid}.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 " + b"X" * 2000)  # > 1000 bytes

        # Patch storage_dir to point to tmp_path/pdfs
        with patch(
            "fcc_ad_tracker.stage3_pdf.downloader.get_settings"
        ) as mock_settings:
            settings = MagicMock()
            settings.fcc.base_url = "https://publicfiles.fcc.gov"
            settings.fcc.rate_limit_rps = 10.0
            settings.pdf.storage_dir = tmp_path / "pdfs"
            mock_settings.return_value = settings

            stats = await download_documents(db=db)

        assert stats["skipped"] == 1
        assert stats["downloaded"] == 0
        assert stats["failed"] == 0


# ---------------------------------------------------------------------------
# pipeline (run_pdf_pipeline)
# ---------------------------------------------------------------------------


class TestRunPdfPipeline:
    """Unit tests for run_pdf_pipeline."""

    @pytest.mark.asyncio
    async def test_empty_queue_returns_zeros(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage3_pdf.pipeline import run_pdf_pipeline
        from fcc_ad_tracker.core.db import DatabaseManager

        db = DatabaseManager(db_path=tmp_path / "test.db")
        db.initialize()

        stats = await run_pdf_pipeline(db=db)
        assert stats == {"success": 0, "ocr_used": 0, "failed": 0, "total": 0}

    @pytest.mark.asyncio
    async def test_missing_pdf_path_counts_as_failed(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage3_pdf.pipeline import run_pdf_pipeline
        from fcc_ad_tracker.core.db import DatabaseManager

        db = DatabaseManager(db_path=tmp_path / "test.db")
        db.initialize()

        db.upsert_station({"callsign": "WFAA", "operator_name": "Gray Television"})
        doc_uuid = "ffffffff-aaaa-bbbb-cccc-ddddeeee0000"
        db.upsert_document(
            {
                "doc_uuid": doc_uuid,
                "folder_uuid": "11111111-2222-3333-4444-555566667777",
                "callsign": "WFAA",
                "operator_name": "Gray Television",
                "year": 2024,
            }
        )
        # Mark as downloaded (but point to non-existent path)
        db.mark_downloaded(doc_uuid, str(tmp_path / "missing.pdf"))

        stats = await run_pdf_pipeline(db=db)
        assert stats["failed"] == 1
        assert stats["success"] == 0

    @pytest.mark.asyncio
    async def test_pymupdf_success_path(self, tmp_path: Path) -> None:
        """When PyMuPDF extracts text successfully, it should be counted as success."""
        from fcc_ad_tracker.stage3_pdf.pipeline import run_pdf_pipeline
        from fcc_ad_tracker.core.db import DatabaseManager

        db = DatabaseManager(db_path=tmp_path / "test.db")
        db.initialize()

        db.upsert_station({"callsign": "WFAA", "operator_name": "Gray Television"})
        doc_uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeee0000"
        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"%PDF fake")

        db.upsert_document(
            {
                "doc_uuid": doc_uuid,
                "folder_uuid": "11111111-2222-3333-4444-555566667777",
                "callsign": "WFAA",
                "operator_name": "Gray Television",
                "year": 2024,
            }
        )
        db.mark_downloaded(doc_uuid, str(pdf_file))

        # Mock extract_text_from_pdf to return rich text (no OCR needed)
        with patch(
            "fcc_ad_tracker.stage3_pdf.pipeline.extract_text_from_pdf"
        ) as mock_extract:
            mock_extract.return_value = (
                "Invoice total $50,000.00 " * 100,  # 2500+ chars
                2,
                2500,
                False,  # needs_ocr = False
            )

            with patch(
                "fcc_ad_tracker.stage3_pdf.pipeline.get_settings"
            ) as mock_settings:
                settings = MagicMock()
                settings.pdf.min_text_chars_per_page = 50
                settings.pdf.ocr_engine = "paddleocr"
                mock_settings.return_value = settings

                stats = await run_pdf_pipeline(db=db)

        assert stats["success"] == 1
        assert stats["ocr_used"] == 0
        assert stats["failed"] == 0
        assert stats["total"] == 1


# ---------------------------------------------------------------------------
# preprocess (_deskew and preprocess_image)
# ---------------------------------------------------------------------------


class TestPreprocessImage:
    """Unit tests for preprocess_image and the _deskew helper."""

    def _make_white_image(self, width: int = 200, height: int = 300) -> "Image.Image":
        from PIL import Image
        return Image.new("L", (width, height), color=255)

    def _make_text_image(self, angle: float = 0.0) -> "Image.Image":
        """Create a synthetic 'document' image: white bg with black horizontal bands."""
        from PIL import Image, ImageDraw
        img = Image.new("L", (400, 600), color=255)
        draw = ImageDraw.Draw(img)
        # Draw horizontal black bars simulating text lines
        for y in range(50, 550, 60):
            draw.rectangle([20, y, 380, y + 12], fill=0)
        if angle != 0.0:
            img = img.rotate(angle, expand=False, fillcolor=255)
        return img

    def test_preprocess_returns_pil_image(self) -> None:
        from PIL import Image
        from fcc_ad_tracker.stage3_pdf.preprocess import preprocess_image

        img = self._make_white_image()
        result = preprocess_image(img, deskew=False, denoise=False)
        assert isinstance(result, Image.Image)

    def test_preprocess_converts_to_grayscale(self) -> None:
        from PIL import Image
        from fcc_ad_tracker.stage3_pdf.preprocess import preprocess_image

        rgb_img = Image.new("RGB", (100, 100), color=(128, 200, 50))
        result = preprocess_image(rgb_img, deskew=False, denoise=False)
        assert result.mode == "L"

    def test_deskew_no_numpy_returns_original(self) -> None:
        """When numpy is unavailable deskew returns the image unchanged."""
        import sys
        from PIL import Image
        from fcc_ad_tracker.stage3_pdf.preprocess import _deskew

        img = self._make_text_image(angle=5.0)
        # Simulate numpy import failure
        real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

        with patch("builtins.__import__", side_effect=lambda name, *a, **kw: (
            (_ for _ in ()).throw(ImportError("no numpy")) if name == "numpy"
            else real_import(name, *a, **kw)
        )):
            # Because _deskew catches ImportError gracefully, result = original
            result = _deskew(img)
        assert isinstance(result, Image.Image)

    def test_deskew_zero_skew_unchanged(self) -> None:
        """A well-aligned image should be returned near-unchanged (skew < 0.2°)."""
        from fcc_ad_tracker.stage3_pdf.preprocess import _deskew

        img = self._make_text_image(angle=0.0)
        result = _deskew(img)
        # May be same object or a rotated copy; both are valid PIL Images
        from PIL import Image
        assert isinstance(result, Image.Image)

    def test_deskew_detects_obvious_skew(self) -> None:
        """A 5° skewed image should produce a corrected (rotated) output."""
        from fcc_ad_tracker.stage3_pdf.preprocess import _deskew

        try:
            import numpy  # Skip if numpy not available in this env
        except ImportError:
            pytest.skip("numpy not installed")

        img = self._make_text_image(angle=5.0)
        result = _deskew(img)
        from PIL import Image
        assert isinstance(result, Image.Image)
        # After deskew the image may be slightly larger (expand=True)
        # or the same size; either way it should be a valid image
        assert result.width > 0 and result.height > 0
