"""End-to-end integration tests.

Tests the full pipeline from extracted text through LLM extraction to
Excel workbook generation, using mocked external dependencies.
No live network calls, no real Anthropic API, no Playwright.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path):
    from fcc_ad_tracker.core.db import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "test.db")
    db.initialize()
    return db


def _seed_station(db, callsign="WFAA", operator="Gray Television",
                  dma_rank=5, dma_name="Dallas-Fort Worth") -> None:
    db.upsert_station({
        "callsign": callsign,
        "operator_name": operator,
        "dma_rank": dma_rank,
        "dma_name": dma_name,
    })


def _insert_extraction(db, doc_uuid: str, quarter: str,
                       gross: float, net: float, doc_type: str = "INVOICE") -> None:
    db.insert_extraction({
        "doc_uuid": doc_uuid,
        "document_type": doc_type,
        "advertiser_name": "Test Advertiser",
        "office_type_extracted": "us-senate",
        "gross_amount": gross,
        "net_amount": net,
        "agency_commission": None,
        "gross_or_net_flag": "both",
        "class_of_time": "preemptable",
        "num_spots": 10,
        "lowest_unit_rate": None,
        "actual_rate": None,
        "flight_start": "09/01/2024",
        "flight_end": "09/30/2024",
        "invoice_date": None,
        "invoice_period_start": None,
        "invoice_period_end": None,
        "station_callsign": "WFAA",
        "dma_extracted": "Dallas-Fort Worth",
        "revenue_quarter": quarter,
        "revenue_date_source": "flight",
        "revenue_date_unknown": 0,
        "extraction_confidence": "high",
        "confidence_notes": [],
        "input_tokens": 500,
        "output_tokens": 100,
        "estimated_cost_usd": 0.003,
    })


# ---------------------------------------------------------------------------
# State machine progression
# ---------------------------------------------------------------------------


class TestPipelineStateProgression:
    """Verify a document flows correctly through all pipeline states."""

    def test_document_queued_for_extraction_after_download(
        self, tmp_path: Path
    ) -> None:
        """After mark_downloaded the doc should appear in the unextracted queue."""
        db = _make_db(tmp_path)
        _seed_station(db)
        doc_uuid = "aaaabbbb-1111-2222-3333-000000000001"

        db.upsert_document({
            "doc_uuid": doc_uuid,
            "folder_uuid": "ffff0000-aaaa-bbbb-cccc-111122220001",
            "callsign": "WFAA",
            "operator_name": "Gray Television",
            "year": 2024,
            "document_type": "INVOICE",
        })

        # Not yet downloaded — should not appear in unextracted queue
        assert db.get_unextracted_docs() == []

        db.mark_downloaded(doc_uuid, str(tmp_path / "test.pdf"))
        unextracted = db.get_unextracted_docs()
        assert len(unextracted) == 1
        assert unextracted[0]["doc_uuid"] == doc_uuid

    def test_document_queued_for_llm_after_text_extraction(
        self, tmp_path: Path
    ) -> None:
        """After mark_text_extracted the doc should appear in the LLM queue."""
        db = _make_db(tmp_path)
        _seed_station(db)
        doc_uuid = "aaaabbbb-1111-2222-3333-000000000002"

        db.upsert_document({
            "doc_uuid": doc_uuid,
            "folder_uuid": "ffff0000-aaaa-bbbb-cccc-111122220002",
            "callsign": "WFAA",
            "operator_name": "Gray Television",
            "year": 2024,
            "document_type": "INVOICE",
        })
        db.mark_downloaded(doc_uuid, str(tmp_path / "test.pdf"))

        # Downloaded but not extracted — LLM queue should be empty
        assert db.get_unprocessed_docs() == []

        db.mark_text_extracted(doc_uuid, "Invoice $50,000", "pymupdf", 16, 1)
        unprocessed = db.get_unprocessed_docs()
        assert len(unprocessed) == 1
        assert unprocessed[0]["doc_uuid"] == doc_uuid
        assert "raw_text" in unprocessed[0]

    def test_document_removed_from_queues_after_llm(self, tmp_path: Path) -> None:
        """After mark_llm_processed the doc should leave both queues."""
        db = _make_db(tmp_path)
        _seed_station(db)
        doc_uuid = "aaaabbbb-1111-2222-3333-000000000003"

        db.upsert_document({
            "doc_uuid": doc_uuid,
            "folder_uuid": "ffff0000-aaaa-bbbb-cccc-111122220003",
            "callsign": "WFAA",
            "operator_name": "Gray Television",
            "year": 2024,
        })
        db.mark_downloaded(doc_uuid, str(tmp_path / "test.pdf"))
        db.mark_text_extracted(doc_uuid, "Invoice $50,000", "pymupdf", 16, 1)
        db.mark_llm_processed(doc_uuid, "success")

        assert db.get_unextracted_docs() == []
        assert db.get_unprocessed_docs() == []

    def test_path_derived_document_type_persists(self, tmp_path: Path) -> None:
        """document_type inserted from URL path should survive a read-back."""
        db = _make_db(tmp_path)
        _seed_station(db)
        doc_uuid = "aaaabbbb-1111-2222-3333-000000000004"

        db.upsert_document({
            "doc_uuid": doc_uuid,
            "folder_uuid": "ffff0000-aaaa-bbbb-cccc-111122220004",
            "callsign": "WFAA",
            "operator_name": "Gray Television",
            "year": 2024,
            "document_type": "CONTRACT",
        })
        db.mark_downloaded(doc_uuid, str(tmp_path / "test.pdf"))
        db.mark_text_extracted(doc_uuid, "Contract $30,000", "pymupdf", 17, 1)

        unprocessed = db.get_unprocessed_docs()
        assert len(unprocessed) == 1
        assert unprocessed[0]["document_type"] == "CONTRACT"


# ---------------------------------------------------------------------------
# Stage 4 (mocked Anthropic) → Stage 5 aggregation
# ---------------------------------------------------------------------------


class TestStage4To5Integration:
    """LLM extraction with mocked Anthropic → financial model."""

    @pytest.mark.asyncio
    async def test_mock_extraction_flows_into_aggregator(
        self, tmp_path: Path
    ) -> None:
        """Mocked Claude response is persisted and correctly aggregated."""
        db = _make_db(tmp_path)
        _seed_station(db)
        doc_uuid = "ccccdddd-1111-2222-3333-000000000001"

        db.upsert_document({
            "doc_uuid": doc_uuid,
            "folder_uuid": "ffff0000-bbbb-cccc-dddd-aabb00000001",
            "callsign": "WFAA",
            "operator_name": "Gray Television",
            "dma_name": "Dallas-Fort Worth",
            "dma_rank": 5,
            "year": 2024,
            "document_type": "INVOICE",
            "create_ts": "2024-09-15T12:00:00Z",
        })
        db.mark_downloaded(doc_uuid, "/fake/path.pdf")
        db.mark_text_extracted(
            doc_uuid,
            "INVOICE  Station: WFAA  Gross: $125,000  Net: $106,250  "
            "Flight: 09/01/2024 - 09/30/2024",
            "pymupdf",
            80,
            1,
        )

        # Build mock Anthropic response
        tool_input = {
            "document_type": "INVOICE",
            "advertiser_name": "Cruz for Senate",
            "gross_amount": 125_000.0,
            "net_amount": 106_250.0,
            "agency_commission": 18_750.0,
            "gross_or_net_flag": "both",
            "flight_start": "09/01/2024",
            "flight_end": "09/30/2024",
            "invoice_date": None,
            "invoice_period_start": None,
            "invoice_period_end": None,
            "extraction_confidence": "high",
            "confidence_notes": [],
            "line_items": [],
        }
        tool_use_block = MagicMock()
        tool_use_block.type = "tool_use"
        tool_use_block.input = tool_input

        mock_response = MagicMock()
        mock_response.content = [tool_use_block]
        mock_response.usage.input_tokens = 600
        mock_response.usage.output_tokens = 150

        mock_ant_client = MagicMock()
        mock_ant_client.messages.create.return_value = mock_response

        llm_settings = MagicMock()
        llm_settings.llm.api_key.get_secret_value.return_value = "sk-test"
        llm_settings.llm.model = "claude-test"
        llm_settings.llm.max_tokens = 1024
        llm_settings.llm.temperature = 0.0
        llm_settings.llm.cost_budget_per_run = 10.0

        with patch("fcc_ad_tracker.stage4_llm.client.get_settings",
                   return_value=llm_settings), \
             patch("fcc_ad_tracker.stage4_llm.extractor.get_settings",
                   return_value=llm_settings), \
             patch("fcc_ad_tracker.stage4_llm.client.anthropic.Anthropic",
                   return_value=mock_ant_client):

            from fcc_ad_tracker.stage4_llm.extractor import run_llm_extraction
            stats = await run_llm_extraction(db=db)

        assert stats["success"] == 1
        assert stats["failed"] == 0

        # The Claude call should have received the INVOICE hint in user content
        call_kwargs = mock_ant_client.messages.create.call_args.kwargs
        user_msg = call_kwargs["messages"][0]["content"]
        assert "document_type=INVOICE" in user_msg

        # Stage 5: aggregate
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        model = aggregate_revenue(db=db)
        oq = model["by_operator_quarter"]
        assert len(oq) == 1
        assert oq[0]["operator_name"] == "Gray Television"
        assert oq[0]["invoice_gross"] == pytest.approx(125_000.0)
        assert oq[0]["invoice_net"] == pytest.approx(106_250.0)
        assert oq[0]["invoice_doc_count"] == 1

    def test_three_quarter_model_with_excel_output(self, tmp_path: Path) -> None:
        """Seed 3 quarters of extractions → verify aggregation + Excel file."""
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        from fcc_ad_tracker.stage5_model.velocity import compute_filing_velocity
        from fcc_ad_tracker.stage5_model.coverage import compute_coverage
        from fcc_ad_tracker.stage5_model.cycle_compare import compare_cycles
        from fcc_ad_tracker.stage5_model.excel_writer import generate_workbook

        db = _make_db(tmp_path)
        _seed_station(db)

        quarters = [
            ("2024-Q2", 500_000.0, 425_000.0),
            ("2024-Q3", 1_000_000.0, 850_000.0),
            ("2024-Q4", 1_500_000.0, 1_275_000.0),
        ]

        for idx, (quarter, gross, net) in enumerate(quarters):
            doc_uuid = f"eeeeeeee-1111-2222-3333-{idx:012d}"
            month = 4 + idx * 3  # 4, 7, 10

            db.upsert_document({
                "doc_uuid": doc_uuid,
                "folder_uuid": f"ffffffff-0000-1111-2222-{idx:012d}",
                "callsign": "WFAA",
                "operator_name": "Gray Television",
                "dma_name": "Dallas-Fort Worth",
                "dma_rank": 5,
                "year": 2024,
                "document_type": "INVOICE",
                "create_ts": f"2024-{month:02d}-15T10:00:00Z",
            })
            db.mark_downloaded(doc_uuid, f"/tmp/{doc_uuid}.pdf")
            db.mark_text_extracted(doc_uuid, "text", "pymupdf", 4, 1)
            db.mark_llm_processed(doc_uuid, "success")
            _insert_extraction(db, doc_uuid, quarter, gross, net)

        model_data = aggregate_revenue(db=db)
        velocity_data = compute_filing_velocity(db=db)
        coverage_data = compute_coverage(db=db)
        cycle_data = compare_cycles(db=db, cycles=[2024])

        output = tmp_path / "integration_test.xlsx"
        result = generate_workbook(
            operator_summary=model_data["by_operator_quarter"],
            dma_detail=model_data["by_dma"],
            velocity_data=velocity_data,
            cycle_comparison=cycle_data,
            raw_data=[],
            coverage_stats=coverage_data,
            output_path=output,
            operators=["gray"],
            year=2024,
        )

        assert result.exists()
        assert result.stat().st_size > 5_000

        # Verify 3 quarters produced
        oq = model_data["by_operator_quarter"]
        assert len(oq) == 3
        quarter_set = {r["quarter"] for r in oq}
        assert quarter_set == {"2024-Q2", "2024-Q3", "2024-Q4"}

        # Verify totals
        total_gross = sum(r["invoice_gross"] for r in oq)
        assert total_gross == pytest.approx(500_000 + 1_000_000 + 1_500_000)

        # Coverage: 3 docs, all successful
        assert len(coverage_data) == 1
        m = coverage_data[0]
        assert m.total_documents_extracted == 3
        assert m.coverage_rate == pytest.approx(1.0)
        assert m.invoice_dollars == pytest.approx(3_000_000.0)
