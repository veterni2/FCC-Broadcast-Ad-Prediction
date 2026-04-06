"""Custom exception hierarchy for the FCC Ad Tracker pipeline."""


class FCCTrackerError(Exception):
    """Base exception for all FCC Ad Tracker errors."""


# --- Stage 1: Station Enumeration ---


class StationEnumerationError(FCCTrackerError):
    """Error during station enumeration."""


class OperatorNotFoundError(StationEnumerationError):
    """Operator name not found in the operator mapping CSV."""

    def __init__(self, operator: str) -> None:
        self.operator = operator
        super().__init__(f"Operator '{operator}' not found in operator_stations.csv")


# --- Stage 2: Crawling ---


class CrawlerError(FCCTrackerError):
    """Error during FCC OPIF crawling."""


class OPIFAPIError(CrawlerError):
    """FCC OPIF API returned an unexpected response."""

    def __init__(self, status_code: int, detail: str = "") -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"OPIF API error {status_code}: {detail}")


class RateLimitExceededError(CrawlerError):
    """Too many requests to the FCC API."""


# --- Stage 3: PDF Processing ---


class PDFProcessingError(FCCTrackerError):
    """Error during PDF download, text extraction, or OCR."""


class PDFDownloadError(PDFProcessingError):
    """Failed to download a PDF from the FCC."""

    def __init__(self, doc_uuid: str, reason: str = "") -> None:
        self.doc_uuid = doc_uuid
        super().__init__(f"Failed to download PDF {doc_uuid}: {reason}")


class OCRError(PDFProcessingError):
    """OCR processing failed for a document."""


# --- Stage 4: LLM Extraction ---


class ExtractionError(FCCTrackerError):
    """Error during LLM-based data extraction."""


class CostBudgetExceeded(ExtractionError):
    """LLM API cost budget has been exceeded for this run.

    When raised, partial results should already be committed to the database.
    """

    def __init__(self, spent: float, budget: float) -> None:
        self.spent = spent
        self.budget = budget
        super().__init__(
            f"Cost budget exceeded: ${spent:.2f} spent of ${budget:.2f} budget. "
            "Partial results have been saved."
        )


class ExtractionValidationError(ExtractionError):
    """Extracted data failed validation checks."""


# --- Stage 5: Model ---


class ModelError(FCCTrackerError):
    """Error during financial model generation."""


class CoverageWarning(ModelError):
    """Coverage is below acceptable thresholds.

    This is a warning-level issue — the model is still generated
    but with prominent coverage disclaimers.
    """

    def __init__(self, operator: str, coverage_pct: float) -> None:
        self.operator = operator
        self.coverage_pct = coverage_pct
        super().__init__(
            f"Low coverage for {operator}: {coverage_pct:.1f}% of documents extracted"
        )


class DivergenceWarning(ModelError):
    """Extracted totals diverge >30% from reported actuals.

    Raised by the validate command when comparing to 10-K data.
    """

    def __init__(self, operator: str, extracted: float, reported: float) -> None:
        self.operator = operator
        self.extracted = extracted
        self.reported = reported
        pct = abs(extracted - reported) / reported * 100
        super().__init__(
            f"DIVERGENCE WARNING: {operator} extracted ${extracted:,.0f} vs "
            f"reported ${reported:,.0f} ({pct:.1f}% difference)"
        )
