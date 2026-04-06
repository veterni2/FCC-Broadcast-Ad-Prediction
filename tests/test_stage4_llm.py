"""Tests for Stage 4: LLM extraction schemas and validation."""

from __future__ import annotations

import json

from fcc_ad_tracker.stage4_llm.schemas import (
    DocumentType,
    ExtractionConfidence,
    LineItem,
    PoliticalAdExtraction,
)


def test_extraction_schema_defaults() -> None:
    """Schema defaults produce valid model with minimal input."""
    extraction = PoliticalAdExtraction(
        document_type=DocumentType.INVOICE,
    )
    assert extraction.document_type == DocumentType.INVOICE
    assert extraction.gross_amount is None
    assert extraction.net_amount is None
    assert extraction.gross_or_net_flag == "neither"
    assert extraction.extraction_confidence == ExtractionConfidence.MEDIUM
    assert extraction.confidence_notes == []


def test_extraction_with_amounts() -> None:
    """Schema correctly stores gross and net amounts."""
    extraction = PoliticalAdExtraction(
        document_type=DocumentType.INVOICE,
        advertiser_name="Cruz for Senate",
        gross_amount=15750.00,
        net_amount=13387.50,
        agency_commission=2362.50,
        gross_or_net_flag="both",
        flight_start="10/01/2024",
        flight_end="10/15/2024",
    )
    assert extraction.gross_amount == 15750.00
    assert extraction.net_amount == 13387.50
    assert extraction.gross_or_net_flag == "both"


def test_extraction_failed_nulls_amounts() -> None:
    """When confidence is FAILED, amounts should be null."""
    extraction = PoliticalAdExtraction(
        document_type=DocumentType.CONTRACT,
        extraction_confidence=ExtractionConfidence.FAILED,
        confidence_notes=["not_a_political_ad"],
    )
    assert extraction.gross_amount is None
    assert extraction.net_amount is None
    assert extraction.extraction_confidence == ExtractionConfidence.FAILED


def test_extraction_roundtrip_json() -> None:
    """Schema serializes to JSON and back without data loss."""
    original = PoliticalAdExtraction(
        document_type=DocumentType.INVOICE,
        advertiser_name="Texans for Progress",
        gross_amount=8500.00,
        gross_or_net_flag="gross_only",
        flight_start="09/15/2024",
        flight_end="09/30/2024",
        line_items=[
            LineItem(
                description="M-F 6A-10A",
                class_of_time="NP",
                num_spots=20,
                rate_per_spot=425.00,
                line_total=8500.00,
            ),
        ],
        total_spots=20,
        extraction_confidence=ExtractionConfidence.HIGH,
    )

    json_str = original.model_dump_json()
    restored = PoliticalAdExtraction.model_validate_json(json_str)

    assert restored.gross_amount == original.gross_amount
    assert restored.line_items[0].rate_per_spot == 425.00
    assert restored.total_spots == 20
