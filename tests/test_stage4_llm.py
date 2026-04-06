"""Tests for Stage 4: LLM extraction schemas, client, and validation."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

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


# ---------------------------------------------------------------------------
# LLMClient (Anthropic API mocked)
# ---------------------------------------------------------------------------


def _make_mock_anthropic(tool_input: dict):
    """Build a mock anthropic.Anthropic client that returns a tool_use block."""
    tool_use_block = MagicMock()
    tool_use_block.type = "tool_use"
    tool_use_block.input = tool_input

    response = MagicMock()
    response.content = [tool_use_block]
    response.usage.input_tokens = 500
    response.usage.output_tokens = 100

    mock_client = MagicMock()
    mock_client.messages.create.return_value = response
    return mock_client


class TestLLMClient:
    def _make_client(self, mock_anthropic_client):
        """Instantiate LLMClient with a pre-built mock Anthropic client."""
        with patch("fcc_ad_tracker.stage4_llm.client.get_settings") as mock_settings, \
             patch("fcc_ad_tracker.stage4_llm.client.anthropic.Anthropic") as mock_cls:
            settings = MagicMock()
            settings.llm.api_key.get_secret_value.return_value = "sk-test"
            settings.llm.model = "claude-test-model"
            settings.llm.max_tokens = 1024
            settings.llm.temperature = 0.0
            mock_settings.return_value = settings
            mock_cls.return_value = mock_anthropic_client

            from fcc_ad_tracker.stage4_llm.client import LLMClient
            return LLMClient()

    def test_extract_without_hint_sends_plain_text(self) -> None:
        """Without a document_type_hint, the user content is the raw text."""
        tool_input = {
            "document_type": "INVOICE",
            "gross_amount": 10000.0,
            "net_amount": None,
            "gross_or_net_flag": "gross_only",
            "extraction_confidence": "high",
            "confidence_notes": [],
            "line_items": [],
        }
        mock_ant = _make_mock_anthropic(tool_input)
        client = self._make_client(mock_ant)

        result, usage = client.extract("Invoice text here")

        call_kwargs = mock_ant.messages.create.call_args.kwargs
        user_msg = call_kwargs["messages"][0]["content"]
        # No hint prefix should appear
        assert "[CONTEXT:" not in user_msg
        assert user_msg == "Invoice text here"

    def test_extract_with_hint_prepends_context(self) -> None:
        """With a document_type_hint, the user content includes the CONTEXT prefix."""
        tool_input = {
            "document_type": "INVOICE",
            "gross_amount": 5000.0,
            "net_amount": None,
            "gross_or_net_flag": "gross_only",
            "extraction_confidence": "high",
            "confidence_notes": [],
            "line_items": [],
        }
        mock_ant = _make_mock_anthropic(tool_input)
        client = self._make_client(mock_ant)

        result, usage = client.extract("Invoice text here", document_type_hint="INVOICE")

        call_kwargs = mock_ant.messages.create.call_args.kwargs
        user_msg = call_kwargs["messages"][0]["content"]
        assert "[CONTEXT:" in user_msg
        assert "document_type=INVOICE" in user_msg
        assert "Invoice text here" in user_msg

    def test_extract_returns_validated_schema(self) -> None:
        """The returned object is a valid PoliticalAdExtraction model."""
        tool_input = {
            "document_type": "CONTRACT",
            "gross_amount": 25000.0,
            "net_amount": 21250.0,
            "agency_commission": 3750.0,
            "gross_or_net_flag": "both",
            "extraction_confidence": "high",
            "confidence_notes": [],
            "line_items": [],
        }
        mock_ant = _make_mock_anthropic(tool_input)
        client = self._make_client(mock_ant)

        result, usage = client.extract("Contract text", document_type_hint="CONTRACT")

        assert isinstance(result, PoliticalAdExtraction)
        assert result.document_type == DocumentType.CONTRACT
        assert result.gross_amount == 25000.0
        assert result.net_amount == 21250.0

    def test_extract_tracks_cost(self) -> None:
        """Cost accumulates correctly across calls."""
        tool_input = {
            "document_type": "INVOICE",
            "gross_amount": None,
            "net_amount": None,
            "gross_or_net_flag": "neither",
            "extraction_confidence": "failed",
            "confidence_notes": ["not_a_political_ad"],
            "line_items": [],
        }
        mock_ant = _make_mock_anthropic(tool_input)
        client = self._make_client(mock_ant)

        assert client.total_cost == 0.0
        client.extract("Doc A")
        assert client.total_cost > 0.0
        cost_after_1 = client.total_cost
        client.extract("Doc B")
        assert client.total_cost > cost_after_1  # accumulates
