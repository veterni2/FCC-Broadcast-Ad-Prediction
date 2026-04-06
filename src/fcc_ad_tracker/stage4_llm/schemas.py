"""Pydantic models for LLM structured extraction.

These schemas define the exact JSON structure that Claude returns
when extracting data from political ad documents. They are passed
to the Anthropic API via structured outputs (output_config) to
guarantee schema compliance.

CRITICAL DESIGN RULES:
- Every dollar amount field is Optional. If not extractable, it MUST be None.
- gross_amount and net_amount are independent. NEVER calculate one from the other.
- document_type (INVOICE vs CONTRACT) is required and must be accurate.
- extraction_confidence='failed' means all amounts should be None.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class DocumentType(str, Enum):
    """Whether the document is an invoice (realized revenue) or contract (forward commitment).

    INVOICE: Billed/paid — spots have already aired. Primary revenue source.
    CONTRACT: Ordered/booked — may be revised or cancelled. Forward pipeline only.
    """
    INVOICE = "INVOICE"
    CONTRACT = "CONTRACT"


class ExtractionConfidence(str, Enum):
    """Overall confidence in the extraction quality.

    HIGH: All key fields clearly extracted, amounts unambiguous.
    MEDIUM: Most fields extracted, some uncertainty on specific values.
    LOW: Significant OCR degradation or ambiguity, but amounts are plausible.
    FAILED: Document is not a political ad, or amounts cannot be determined.
            When FAILED, all dollar amounts MUST be null.
    """
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    FAILED = "failed"


class LineItem(BaseModel):
    """A single line item from an invoice or contract.

    Represents one row in a spot schedule or invoice detail section.
    """
    description: str = Field(
        description="Spot description, program name, or daypart."
    )
    class_of_time: Optional[str] = Field(
        None,
        description=(
            "Class of time purchased. Common values: "
            "'P' (preemptible), 'NP' (non-preemptible), "
            "'F' (fixed/non-preemptible), 'C' (preemptible Class C). "
            "Only if explicitly stated."
        ),
    )
    num_spots: Optional[int] = Field(
        None,
        description="Number of spots in this line item.",
    )
    rate_per_spot: Optional[float] = Field(
        None,
        description="Rate per individual spot in USD.",
    )
    line_total: Optional[float] = Field(
        None,
        description="Total amount for this line item in USD.",
    )
    flight_start: Optional[str] = Field(
        None,
        description="Start date for this line item's flight, format MM/DD/YYYY.",
    )
    flight_end: Optional[str] = Field(
        None,
        description="End date for this line item's flight, format MM/DD/YYYY.",
    )


class PoliticalAdExtraction(BaseModel):
    """Structured extraction from a political advertising document.

    This is the primary output schema for Stage 4 LLM extraction.
    Every field is extracted ONLY if explicitly present in the document.
    Nothing is calculated, imputed, or inferred.
    """

    # --- Document classification ---
    document_type: DocumentType = Field(
        description=(
            "INVOICE if this document bills for spots that already aired "
            "(look for: 'invoice', 'statement', billed amounts, air dates). "
            "CONTRACT if this document orders/books future spots "
            "(look for: 'order', 'contract', 'estimate', scheduled dates)."
        ),
    )

    # --- Advertiser / candidate ---
    advertiser_name: Optional[str] = Field(
        None,
        description=(
            "Advertiser, campaign committee, PAC, or issue group name "
            "exactly as stated on the document. Do not normalize or abbreviate."
        ),
    )
    candidate_name: Optional[str] = Field(
        None,
        description="Candidate name if explicitly stated on the document.",
    )
    office_type: Optional[str] = Field(
        None,
        description=(
            "Office being sought. Use lowercase-hyphenated format: "
            "us-president, us-senate, us-house, governor, "
            "state-legislature, local, ballot-measure, issue-ad, other. "
            "Only if explicitly stated on the document."
        ),
    )

    # --- Station ---
    station_callsign: Optional[str] = Field(
        None,
        description="Station call sign as printed on the document (e.g. WFAA, KWTX).",
    )

    # --- Dollar amounts (NEVER calculate one from another) ---
    gross_amount: Optional[float] = Field(
        None,
        description=(
            "Total GROSS amount in USD (before agency commission). "
            "Only if the document explicitly labels an amount as gross, "
            "or if it is the only total and agency commission is shown separately."
        ),
    )
    net_amount: Optional[float] = Field(
        None,
        description=(
            "Total NET amount in USD (after agency commission deduction). "
            "Only if the document explicitly labels an amount as net, "
            "or if an agency commission has been subtracted from a total."
        ),
    )
    agency_commission: Optional[float] = Field(
        None,
        description=(
            "Agency commission amount in USD, or percentage (e.g. 15%). "
            "Only if explicitly shown on the document."
        ),
    )
    gross_or_net_flag: str = Field(
        "neither",
        description=(
            "What amounts were found: "
            "'both' = gross and net both present, "
            "'gross_only' = only gross amount found, "
            "'net_only' = only net amount found, "
            "'neither' = no dollar amounts could be extracted."
        ),
    )

    # --- Time class / rates ---
    class_of_time: Optional[str] = Field(
        None,
        description="Overall class of time if a single class applies to the entire order.",
    )
    lowest_unit_rate: Optional[float] = Field(
        None,
        description="Lowest unit rate (LUR) if stated on the document.",
    )

    # --- Line items ---
    line_items: list[LineItem] = Field(
        default_factory=list,
        description="Individual line items if the document is itemized.",
    )
    total_spots: Optional[int] = Field(
        None,
        description=(
            "Total number of spots. Use the explicitly stated total "
            "if present, or sum of line item spots if clearly additive."
        ),
    )

    # --- Dates ---
    flight_start: Optional[str] = Field(
        None,
        description="Overall flight start date, format MM/DD/YYYY.",
    )
    flight_end: Optional[str] = Field(
        None,
        description="Overall flight end date, format MM/DD/YYYY.",
    )
    invoice_date: Optional[str] = Field(
        None,
        description="Invoice issue date, format MM/DD/YYYY.",
    )
    invoice_period_start: Optional[str] = Field(
        None,
        description="Billing period start date, format MM/DD/YYYY.",
    )
    invoice_period_end: Optional[str] = Field(
        None,
        description="Billing period end date, format MM/DD/YYYY.",
    )

    # --- Confidence ---
    extraction_confidence: ExtractionConfidence = Field(
        ExtractionConfidence.MEDIUM,
        description=(
            "Overall confidence in this extraction. "
            "Set to 'failed' if the document is not a political ad "
            "or if amounts cannot be determined — when 'failed', "
            "all dollar amount fields MUST be null."
        ),
    )
    confidence_notes: list[str] = Field(
        default_factory=list,
        description=(
            "Quality flags explaining any issues. Examples: "
            "'amount_ambiguous', 'date_unclear', 'ocr_degraded', "
            "'multiple_invoices_in_doc', 'document_type_ambiguous', "
            "'handwritten_annotations', 'partial_page_visible'."
        ),
    )
