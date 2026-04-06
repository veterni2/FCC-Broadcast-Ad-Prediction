"""Tests for Stage 5: Financial model — aggregation, velocity, coverage, Excel."""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_db(tmp_path: Path):
    """Create and initialise a fresh DatabaseManager."""
    from fcc_ad_tracker.core.db import DatabaseManager
    db = DatabaseManager(db_path=tmp_path / "test.db")
    db.initialize()
    return db


def _seed_station(db, callsign="WFAA", operator="Gray Television", dma_rank=5, dma_name="Dallas"):
    db.upsert_station({
        "callsign": callsign,
        "operator_name": operator,
        "dma_rank": dma_rank,
        "dma_name": dma_name,
    })


def _seed_extraction(
    db,
    doc_uuid: str,
    callsign: str = "WFAA",
    operator: str = "Gray Television",
    dma_name: str = "Dallas",
    dma_rank: int = 5,
    year: int = 2024,
    document_type: str = "INVOICE",
    gross_amount: float | None = 100_000.0,
    net_amount: float | None = 85_000.0,
    revenue_quarter: str = "2024-Q3",
    folder_office_type: str = "us-senate",
) -> None:
    """Seed a document + extraction record into the DB."""
    db.upsert_document({
        "doc_uuid": doc_uuid,
        "folder_uuid": "ffffffff-0000-1111-2222-333344445555",
        "callsign": callsign,
        "operator_name": operator,
        "dma_name": dma_name,
        "dma_rank": dma_rank,
        "year": year,
        "race_level": "federal",
        "office_type": folder_office_type,
        "document_type": document_type,
        "create_ts": f"{year}-09-15T10:00:00Z",  # needed by velocity computation
    })
    db.mark_downloaded(doc_uuid, f"/tmp/{doc_uuid}.pdf")
    db.mark_text_extracted(doc_uuid, "Invoice total", "pymupdf", 100, 1)
    db.mark_llm_processed(doc_uuid, "success")

    db.insert_extraction({
        "doc_uuid": doc_uuid,
        "document_type": document_type,
        "advertiser_name": "Test Campaign",
        "office_type_extracted": folder_office_type,
        "gross_amount": gross_amount,
        "net_amount": net_amount,
        "agency_commission": None,
        "gross_or_net_flag": "both" if (gross_amount and net_amount) else "gross_only",
        "class_of_time": "preemptable",
        "num_spots": 10,
        "lowest_unit_rate": None,
        "actual_rate": None,
        "flight_start": "09/01/2024",
        "flight_end": "09/30/2024",
        "invoice_date": None,
        "invoice_period_start": None,
        "invoice_period_end": None,
        "station_callsign": callsign,
        "dma_extracted": dma_name,
        "revenue_quarter": revenue_quarter,
        "revenue_date_source": "flight",
        "revenue_date_unknown": 0,
        "extraction_confidence": "high",
        "confidence_notes": [],
        "input_tokens": 1000,
        "output_tokens": 200,
        "estimated_cost_usd": 0.006,
    })


# ---------------------------------------------------------------------------
# aggregator
# ---------------------------------------------------------------------------


class TestAggregateRevenue:
    def test_empty_db_returns_empty_lists(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        db = _make_db(tmp_path)
        result = aggregate_revenue(db=db)
        assert result["by_operator_quarter"] == []
        assert result["by_dma"] == []
        assert result["by_office_type"] == []

    def test_single_invoice_aggregation(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(
            db, "aaaaaaaa-1111-2222-3333-444455556666",
            gross_amount=500_000.0, net_amount=425_000.0,
            revenue_quarter="2024-Q3", document_type="INVOICE",
        )
        result = aggregate_revenue(db=db)
        oq = result["by_operator_quarter"]
        assert len(oq) == 1
        assert oq[0]["operator_name"] == "Gray Television"
        assert oq[0]["quarter"] == "2024-Q3"
        assert oq[0]["invoice_gross"] == pytest.approx(500_000.0)
        assert oq[0]["invoice_net"] == pytest.approx(425_000.0)
        assert oq[0]["invoice_doc_count"] == 1
        assert oq[0]["contract_gross"] == pytest.approx(0.0)

    def test_invoice_and_contract_are_separate(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000001",
                         document_type="INVOICE", gross_amount=300_000.0)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000002",
                         document_type="CONTRACT", gross_amount=150_000.0)
        result = aggregate_revenue(db=db)
        oq = result["by_operator_quarter"]
        assert len(oq) == 1
        assert oq[0]["invoice_gross"] == pytest.approx(300_000.0)
        assert oq[0]["contract_gross"] == pytest.approx(150_000.0)

    def test_multiple_quarters_separate_rows(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000011",
                         revenue_quarter="2024-Q3", gross_amount=100_000.0)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000012",
                         revenue_quarter="2024-Q4", gross_amount=200_000.0)
        result = aggregate_revenue(db=db)
        oq = result["by_operator_quarter"]
        assert len(oq) == 2
        quarters = {r["quarter"] for r in oq}
        assert quarters == {"2024-Q3", "2024-Q4"}

    def test_none_amount_excluded_from_sum(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000021",
                         gross_amount=100_000.0)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000022",
                         gross_amount=None)
        result = aggregate_revenue(db=db)
        oq = result["by_operator_quarter"]
        assert oq[0]["invoice_gross"] == pytest.approx(100_000.0)
        assert oq[0]["invoice_doc_count"] == 2  # Both docs counted

    def test_dma_detail_invoice_only(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.aggregator import aggregate_revenue
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000031",
                         document_type="INVOICE", gross_amount=50_000.0)
        _seed_extraction(db, "aaaaaaaa-1111-2222-3333-000000000032",
                         document_type="CONTRACT", gross_amount=20_000.0)
        result = aggregate_revenue(db=db)
        for row in result["by_dma"]:
            assert "invoice_gross" in row
            assert "contract_gross" not in row


# ---------------------------------------------------------------------------
# velocity
# ---------------------------------------------------------------------------


class TestComputeFilingVelocity:
    def test_empty_db_returns_empty(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.velocity import compute_filing_velocity
        db = _make_db(tmp_path)
        result = compute_filing_velocity(db=db)
        assert result == []

    def test_velocity_has_required_fields(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.velocity import compute_filing_velocity
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "bbbbbbbb-1111-2222-3333-000000000001", gross_amount=50_000.0)

        result = compute_filing_velocity(db=db)
        assert len(result) >= 1
        row = result[0]
        assert "iso_week" in row
        assert "operator_name" in row
        assert "doc_count" in row
        assert "cumulative_docs" in row
        assert "invoice_gross" in row

    def test_cumulative_docs_monotonically_increasing(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.velocity import compute_filing_velocity
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "cccccccc-1111-2222-3333-000000000001")
        _seed_extraction(db, "cccccccc-1111-2222-3333-000000000002")

        result = compute_filing_velocity(db=db)
        gray_rows = [r for r in result if "Gray" in r["operator_name"]]
        for i in range(1, len(gray_rows)):
            assert gray_rows[i]["cumulative_docs"] >= gray_rows[i - 1]["cumulative_docs"]


# ---------------------------------------------------------------------------
# coverage
# ---------------------------------------------------------------------------


class TestComputeCoverage:
    def test_empty_db_returns_empty(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.coverage import compute_coverage
        db = _make_db(tmp_path)
        result = compute_coverage(db=db)
        assert result == []

    def test_coverage_rate_calculation(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.coverage import compute_coverage
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "dddddddd-1111-2222-3333-000000000001", gross_amount=100_000.0)

        # Seed a failed doc
        db.upsert_document({
            "doc_uuid": "dddddddd-1111-2222-3333-000000000002",
            "folder_uuid": "ffffffff-0000-1111-2222-999999999999",
            "callsign": "WFAA",
            "operator_name": "Gray Television",
            "year": 2024,
        })
        db.mark_downloaded("dddddddd-1111-2222-3333-000000000002",
                           "/tmp/dddddddd-1111-2222-3333-000000000002.pdf")
        db.mark_text_extracted("dddddddd-1111-2222-3333-000000000002",
                               "bad text", "pymupdf", 8, 1)
        db.mark_llm_processed("dddddddd-1111-2222-3333-000000000002", "failed")

        result = compute_coverage(db=db)
        assert len(result) == 1
        m = result[0]
        assert m.operator_name == "Gray Television"
        assert m.total_documents_attempted == 2
        assert m.total_documents_extracted == 1
        assert m.coverage_rate == pytest.approx(0.5)

    def test_invoice_dollars_separate_from_contract(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.coverage import compute_coverage
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "eeeeeeee-1111-2222-3333-000000000001",
                         document_type="INVOICE", gross_amount=300_000.0)
        _seed_extraction(db, "eeeeeeee-1111-2222-3333-000000000002",
                         document_type="CONTRACT", gross_amount=100_000.0)

        result = compute_coverage(db=db)
        assert len(result) == 1
        m = result[0]
        assert m.invoice_dollars == pytest.approx(300_000.0)
        assert m.contract_dollars == pytest.approx(100_000.0)
        assert m.total_dollars_extracted == pytest.approx(400_000.0)


# ---------------------------------------------------------------------------
# cycle_compare
# ---------------------------------------------------------------------------


class TestCompareCycles:
    def test_empty_db_returns_empty(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.cycle_compare import compare_cycles
        db = _make_db(tmp_path)
        result = compare_cycles(db=db)
        assert result == []

    def test_single_cycle_no_prior_growth(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.cycle_compare import compare_cycles
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "ffffffff-1111-2222-3333-000000000001",
                         year=2024, revenue_quarter="2024-Q3", gross_amount=500_000.0)

        result = compare_cycles(db=db, cycles=[2022, 2024, 2026])
        assert len(result) >= 1
        row = result[0]
        assert row.get("2024_gross") == pytest.approx(500_000.0)
        # No 2022 data — growth rate should be None
        assert row.get("yoy_growth_2024_vs_2022") is None

    def test_result_has_required_fields(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.cycle_compare import compare_cycles
        db = _make_db(tmp_path)
        _seed_station(db)
        _seed_extraction(db, "00000000-aaaa-bbbb-cccc-000000000001",
                         year=2024, revenue_quarter="2024-Q3", gross_amount=300_000.0)

        result = compare_cycles(db=db, cycles=[2022, 2024, 2026])
        assert len(result) >= 1
        row = result[0]
        assert "week_of_cycle" in row
        assert "operator_name" in row
        assert "2024_gross" in row


# ---------------------------------------------------------------------------
# excel_writer
# ---------------------------------------------------------------------------


class TestGenerateWorkbook:
    def test_empty_data_creates_file(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.excel_writer import generate_workbook

        output = tmp_path / "test_output.xlsx"
        result = generate_workbook(
            operator_summary=[],
            dma_detail=[],
            velocity_data=[],
            cycle_comparison=[],
            raw_data=[],
            coverage_stats=[],
            output_path=output,
            operators=["gray"],
            year=2024,
        )
        assert result == output
        assert output.exists()
        assert output.stat().st_size > 0

    def test_with_data_creates_larger_file(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.excel_writer import generate_workbook
        from fcc_ad_tracker.core.models import CoverageMetrics

        output = tmp_path / "test_data.xlsx"
        operator_summary = [
            {
                "operator_name": "Gray Television",
                "quarter": "2024-Q3",
                "invoice_gross": 12_500_000.0,
                "invoice_net": 10_625_000.0,
                "contract_gross": 3_200_000.0,
                "contract_net": 2_720_000.0,
                "invoice_doc_count": 47,
                "contract_doc_count": 12,
            }
        ]
        coverage_stats = [
            CoverageMetrics(
                operator_name="Gray Television",
                total_documents_attempted=100,
                total_documents_extracted=85,
                total_documents_failed=15,
                coverage_rate=0.85,
                invoice_dollars=12_500_000.0,
                contract_dollars=3_200_000.0,
                total_dollars_extracted=15_700_000.0,
            )
        ]

        result = generate_workbook(
            operator_summary=operator_summary,
            dma_detail=[],
            velocity_data=[],
            cycle_comparison=[],
            raw_data=[],
            coverage_stats=coverage_stats,
            output_path=output,
            operators=["gray"],
            year=2024,
        )
        assert result.exists()
        assert output.stat().st_size > 5_000

    def test_auto_generates_filename(self, tmp_path: Path) -> None:
        from fcc_ad_tracker.stage5_model.excel_writer import generate_workbook
        from unittest.mock import patch, MagicMock

        with patch("fcc_ad_tracker.stage5_model.excel_writer.get_settings") as mock_s:
            settings = MagicMock()
            settings.output.output_dir = tmp_path / "output"
            mock_s.return_value = settings

            result = generate_workbook(
                operator_summary=[],
                dma_detail=[],
                velocity_data=[],
                cycle_comparison=[],
                raw_data=[],
                coverage_stats=[],
                output_path=None,
                operators=["gray"],
                year=2026,
            )

        assert result.exists()
        assert "gray" in result.name
        assert "2026" in result.name
        assert result.suffix == ".xlsx"
