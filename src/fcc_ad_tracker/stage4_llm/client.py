"""Anthropic API client wrapper for structured document extraction.

Uses Claude's tool use (tool_choice forced) to guarantee schema-compliant
extraction results via Pydantic models. Forcing a named tool guarantees the
response contains a tool_use block — stop_reason will be "tool_use" rather
than "end_turn" — so the model cannot return free-form text instead of JSON.

Cost tracking is built in — each call logs input/output tokens
and estimated USD cost. The extractor enforces a per-run budget.
"""

from __future__ import annotations

from typing import Optional

import anthropic

from ..config.settings import get_settings
from ..utils.logging import get_logger
from .prompts import EXTRACTION_SYSTEM_PROMPT
from .schemas import PoliticalAdExtraction

log = get_logger("llm_client")

# Pricing per million tokens (Claude Sonnet as of early 2026)
_INPUT_COST_PER_MTOK = 3.00
_OUTPUT_COST_PER_MTOK = 15.00


def _build_tool_schema(model_class) -> dict:
    """Build Anthropic tool input_schema from a Pydantic model."""
    schema = model_class.model_json_schema()
    # Remove Pydantic-specific top-level keys that Anthropic doesn't need
    schema.pop("title", None)
    return schema


class LLMClient:
    """Anthropic Claude client for political ad document extraction.

    Usage:
        client = LLMClient()
        result, cost = client.extract(document_text)
    """

    def __init__(self) -> None:
        settings = get_settings().llm
        self._client = anthropic.Anthropic(
            api_key=settings.api_key.get_secret_value(),
        )
        self._model = settings.model
        self._max_tokens = settings.max_tokens
        self._temperature = settings.temperature
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._total_cost = 0.0
        self._call_count = 0
        # Precompute tool schema once from the Pydantic model
        self._tool_schema = _build_tool_schema(PoliticalAdExtraction)

    def extract(self, document_text: str) -> tuple[PoliticalAdExtraction, dict]:
        """Extract structured data from a political ad document.

        Args:
            document_text: Raw text from the PDF (OCR or direct extraction).

        Returns:
            Tuple of (PoliticalAdExtraction result, usage_stats dict).
            usage_stats contains: input_tokens, output_tokens, estimated_cost_usd.
        """
        # Truncate very long documents to stay within context window
        max_chars = 80_000  # ~20K tokens
        if len(document_text) > max_chars:
            log.warning(
                f"Document text truncated from {len(document_text)} to {max_chars} chars"
            )
            document_text = document_text[:max_chars]

        tool_spec = {
            "name": "extract_political_ad_data",
            "description": (
                "Extract structured political advertising data from the document text. "
                "Return exactly the fields you can identify with confidence. "
                "Set fields to null when information is absent or ambiguous."
            ),
            "input_schema": self._tool_schema,
        }

        response = self._client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            temperature=self._temperature,
            system=EXTRACTION_SYSTEM_PROMPT,
            tools=[tool_spec],
            tool_choice={"type": "tool", "name": "extract_political_ad_data"},
            messages=[{"role": "user", "content": document_text}],
        )

        # tool_choice forces a tool_use block; stop_reason will be "tool_use"
        # rather than "end_turn". Extract the tool use result block.
        tool_use_block = next(
            (b for b in response.content if b.type == "tool_use"),
            None,
        )
        if tool_use_block is None:
            raise ValueError(
                f"No tool_use block in response. Content types: "
                f"{[b.type for b in response.content]}"
            )
        result = PoliticalAdExtraction.model_validate(tool_use_block.input)

        # Track costs
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        cost = (
            input_tokens * _INPUT_COST_PER_MTOK / 1_000_000
            + output_tokens * _OUTPUT_COST_PER_MTOK / 1_000_000
        )

        self._total_input_tokens += input_tokens
        self._total_output_tokens += output_tokens
        self._total_cost += cost
        self._call_count += 1

        usage_stats = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "estimated_cost_usd": round(cost, 6),
        }

        log.debug(
            f"Extraction #{self._call_count}: "
            f"{input_tokens} in / {output_tokens} out / ${cost:.4f}"
        )

        return result, usage_stats

    @property
    def total_cost(self) -> float:
        """Total estimated cost across all calls in this session."""
        return self._total_cost

    @property
    def total_calls(self) -> int:
        """Total number of extraction calls made."""
        return self._call_count

    @property
    def stats(self) -> dict:
        """Summary statistics for this client session."""
        return {
            "total_calls": self._call_count,
            "total_input_tokens": self._total_input_tokens,
            "total_output_tokens": self._total_output_tokens,
            "total_cost_usd": round(self._total_cost, 4),
        }
