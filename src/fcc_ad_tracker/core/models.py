"""Domain models and enums used across all pipeline stages.

These are internal data flow models — distinct from the Stage 4 LLM
extraction schemas in stage4_llm/schemas.py.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ExtractionStatus(str, Enum):
    """Status of LLM extraction for a document."""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"


class DocumentType(str, Enum):
    """Whether a filing document is an invoice or a contract.

    INVOICE = realized revenue (billed/paid, spots already aired).
    CONTRACT = forward commitment (ordered/booked, may be revised/cancelled).
    These must NEVER be mixed in the same revenue line.
    """
    INVOICE = "INVOICE"
    CONTRACT = "CONTRACT"


class OfficeType(str, Enum):
    """Office type, parsed from the FCC OPIF folder path."""
    US_PRESIDENT = "us-president"
    US_SENATE = "us-senate"
    US_HOUSE = "us-house"
    GOVERNOR = "governor"
    STATE_LEGISLATURE = "state-legislature"
    LOCAL = "local"
    BALLOT_MEASURE = "ballot-measure"
    ISSUE_AD = "issue-ad"
    OTHER = "other"


class RaceLevel(str, Enum):
    """Race level, parsed from the FCC OPIF folder path."""
    FEDERAL = "federal"
    STATE = "state"
    LOCAL = "local"
    NON_CANDIDATE = "non-candidate-issue-ads"


class ExtractionMethod(str, Enum):
    """How text was extracted from a PDF."""
    PYMUPDF = "pymupdf"
    PADDLEOCR = "paddleocr"
    TESSERACT = "tesseract"


class RevenueDateSource(str, Enum):
    """Source of the date used for revenue quarter attribution."""
    FLIGHT = "flight"
    INVOICE_PERIOD = "invoice_period"


class GrossNetFlag(str, Enum):
    """What dollar amounts were extractable from the document."""
    BOTH = "both"
    GROSS_ONLY = "gross_only"
    NET_ONLY = "net_only"
    NEITHER = "neither"


# ---------------------------------------------------------------------------
# Domain Models
# ---------------------------------------------------------------------------


class Station(BaseModel):
    """A broadcast TV station mapped to its parent operator."""
    callsign: str = Field(description="FCC call sign, e.g. WFAA")
    facility_id: Optional[str] = Field(None, description="FCC facility ID")
    entity_id: Optional[str] = Field(None, description="FCC entity ID")
    operator_name: str = Field(description="Parent company, e.g. 'Gray Television'")
    dma_rank: Optional[int] = Field(None, description="Nielsen DMA rank (1 = NYC)")
    dma_name: Optional[str] = Field(None, description="Nielsen DMA name")
    dma_code: Optional[str] = Field(None, description="Nielsen DMA code")
    community_state: Optional[str] = Field(None, description="State abbreviation")
    network_affil: Optional[str] = Field(None, description="Network: ABC, CBS, NBC, FOX, etc.")


class DocumentRecord(BaseModel):
    """A political file document discovered on FCC OPIF.

    Mirrors the documents SQLite table. Processing status fields
    track the document through all pipeline stages.
    """
    doc_uuid: str
    folder_uuid: Optional[str] = None
    folder_id: Optional[str] = None
    file_manager_id: Optional[str] = None
    callsign: str
    operator_name: str
    dma_name: Optional[str] = None
    dma_rank: Optional[int] = None
    year: int
    race_level: Optional[str] = None
    office_type: Optional[str] = None
    candidate_name: Optional[str] = None
    campaign_year: Optional[int] = None
    political_file_type: str = "PA"
    file_name: Optional[str] = None
    file_extension: Optional[str] = None
    file_size: Optional[int] = None
    create_ts: Optional[str] = None
    last_update_ts: Optional[str] = None

    # Processing status
    pdf_downloaded: bool = False
    pdf_path: Optional[str] = None
    text_extracted: bool = False
    extraction_method: Optional[ExtractionMethod] = None
    char_count: Optional[int] = None
    page_count: Optional[int] = None
    llm_processed: bool = False
    extraction_status: Optional[ExtractionStatus] = None


class CoverageMetrics(BaseModel):
    """Coverage statistics for an operator or DMA.

    These metrics MUST be shown alongside any revenue figure —
    the user must always see the data quality before the number.
    """
    operator_name: Optional[str] = None
    dma_name: Optional[str] = None
    total_documents_attempted: int = 0
    total_documents_extracted: int = 0
    total_documents_failed: int = 0
    coverage_rate: float = 0.0  # extracted / attempted
    total_dollars_extracted: float = 0.0
    invoice_dollars: float = 0.0
    contract_dollars: float = 0.0


class RunLogEntry(BaseModel):
    """Audit trail entry for a pipeline run."""
    stage: str
    operators: Optional[str] = None
    year: Optional[int] = None
    docs_processed: int = 0
    docs_failed: int = 0
    total_cost_usd: float = 0.0
    notes: Optional[str] = None
