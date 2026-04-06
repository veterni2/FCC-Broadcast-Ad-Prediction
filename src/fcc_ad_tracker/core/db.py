"""SQLite database manager for the document registry.

Provides connection management (WAL mode), schema creation,
and helper methods for all pipeline stages.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

from ..config.settings import get_settings

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS stations (
    callsign        TEXT PRIMARY KEY,
    facility_id     TEXT,
    entity_id       TEXT,
    operator_name   TEXT NOT NULL,
    dma_rank        INTEGER,
    dma_name        TEXT,
    dma_code        TEXT,
    community_state TEXT,
    network_affil   TEXT,
    last_updated    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS documents (
    doc_uuid            TEXT PRIMARY KEY,
    folder_uuid         TEXT,
    folder_id           TEXT,
    file_manager_id     TEXT,
    callsign            TEXT NOT NULL REFERENCES stations(callsign),
    operator_name       TEXT NOT NULL,
    dma_name            TEXT,
    dma_rank            INTEGER,
    year                INTEGER NOT NULL,
    race_level          TEXT,
    office_type         TEXT,
    candidate_name      TEXT,
    document_type       TEXT,   -- 'INVOICE' | 'CONTRACT' | NULL (from URL path)
    campaign_year       INTEGER,
    political_file_type TEXT DEFAULT 'PA',
    file_name           TEXT,
    file_extension      TEXT,
    file_size           INTEGER,
    create_ts           TEXT,
    last_update_ts      TEXT,
    crawled_at          TEXT NOT NULL DEFAULT (datetime('now')),
    pdf_downloaded      INTEGER DEFAULT 0,
    pdf_path            TEXT,
    text_extracted      INTEGER DEFAULT 0,
    extraction_method   TEXT,
    char_count          INTEGER,
    page_count          INTEGER,
    llm_processed       INTEGER DEFAULT 0,
    llm_processed_at    TEXT,
    extraction_status   TEXT
);

CREATE INDEX IF NOT EXISTS idx_docs_callsign ON documents(callsign);
CREATE INDEX IF NOT EXISTS idx_docs_operator ON documents(operator_name);
CREATE INDEX IF NOT EXISTS idx_docs_year ON documents(year);
CREATE INDEX IF NOT EXISTS idx_docs_status
    ON documents(pdf_downloaded, text_extracted, llm_processed);

CREATE TABLE IF NOT EXISTS extracted_text (
    doc_uuid        TEXT PRIMARY KEY REFERENCES documents(doc_uuid),
    raw_text        TEXT,
    ocr_confidence  REAL,
    extracted_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS extractions (
    extraction_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_uuid                TEXT NOT NULL REFERENCES documents(doc_uuid),
    document_type           TEXT NOT NULL,
    advertiser_name         TEXT,
    office_type_extracted   TEXT,
    gross_amount            REAL,
    net_amount              REAL,
    agency_commission       REAL,
    gross_or_net_flag       TEXT,
    class_of_time           TEXT,
    num_spots               INTEGER,
    lowest_unit_rate        REAL,
    actual_rate             REAL,
    flight_start            TEXT,
    flight_end              TEXT,
    invoice_date            TEXT,
    invoice_period_start    TEXT,
    invoice_period_end      TEXT,
    station_callsign        TEXT,
    dma_extracted           TEXT,
    revenue_quarter         TEXT,
    revenue_date_source     TEXT,
    revenue_date_unknown    INTEGER DEFAULT 0,
    extraction_confidence   TEXT DEFAULT 'medium',
    confidence_notes        TEXT,
    input_tokens            INTEGER,
    output_tokens           INTEGER,
    estimated_cost_usd      REAL,
    extracted_at            TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(doc_uuid, document_type, advertiser_name, flight_start)
);

CREATE INDEX IF NOT EXISTS idx_ext_doc ON extractions(doc_uuid);
CREATE INDEX IF NOT EXISTS idx_ext_quarter ON extractions(revenue_quarter);
CREATE INDEX IF NOT EXISTS idx_ext_doctype ON extractions(document_type);

CREATE TABLE IF NOT EXISTS crawl_state (
    callsign        TEXT PRIMARY KEY REFERENCES stations(callsign),
    last_crawled_at TEXT NOT NULL,
    last_doc_ts     TEXT,
    docs_found      INTEGER DEFAULT 0,
    errors          TEXT
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at    TEXT,
    stage           TEXT NOT NULL,
    operators       TEXT,
    year            INTEGER,
    docs_processed  INTEGER DEFAULT 0,
    docs_failed     INTEGER DEFAULT 0,
    total_cost_usd  REAL DEFAULT 0,
    notes           TEXT
);
"""


# ---------------------------------------------------------------------------
# Database Manager
# ---------------------------------------------------------------------------


class DatabaseManager:
    """SQLite database manager with WAL mode and helper methods.

    Usage:
        db = DatabaseManager()
        db.initialize()

        with db.transaction() as conn:
            conn.execute("INSERT INTO ...")
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._db_path = db_path or get_settings().db.db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        """Create a new connection with WAL mode and foreign keys."""
        conn = sqlite3.connect(str(self._db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        """Create all tables and indexes if they don't exist."""
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA_SQL)
            # Migration: add document_type column if it was not in the original schema
            cols = {row[1] for row in conn.execute("PRAGMA table_info(documents)").fetchall()}
            if "document_type" not in cols:
                conn.execute("ALTER TABLE documents ADD COLUMN document_type TEXT")
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for a database transaction.

        Commits on success, rolls back on exception.
        """
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def read(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for read-only database access."""
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    # -------------------------------------------------------------------
    # Station helpers
    # -------------------------------------------------------------------

    def upsert_station(self, station: dict[str, Any]) -> None:
        """Insert or update a station record."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO stations (
                    callsign, facility_id, entity_id, operator_name,
                    dma_rank, dma_name, dma_code, community_state,
                    network_affil, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(callsign) DO UPDATE SET
                    facility_id = excluded.facility_id,
                    entity_id = excluded.entity_id,
                    operator_name = excluded.operator_name,
                    dma_rank = excluded.dma_rank,
                    dma_name = excluded.dma_name,
                    dma_code = excluded.dma_code,
                    community_state = excluded.community_state,
                    network_affil = excluded.network_affil,
                    last_updated = datetime('now')
                """,
                (
                    station["callsign"],
                    station.get("facility_id"),
                    station.get("entity_id"),
                    station["operator_name"],
                    station.get("dma_rank"),
                    station.get("dma_name"),
                    station.get("dma_code"),
                    station.get("community_state"),
                    station.get("network_affil"),
                ),
            )

    def get_stations(self, operator: Optional[str] = None, top_dma: Optional[int] = None) -> list[dict]:
        """Get stations, optionally filtered by operator and DMA rank."""
        query = "SELECT * FROM stations WHERE 1=1"
        params: list[Any] = []

        if operator:
            query += " AND LOWER(operator_name) LIKE ?"
            params.append(f"%{operator.lower()}%")
        if top_dma:
            query += " AND dma_rank <= ?"
            params.append(top_dma)

        query += " ORDER BY dma_rank ASC"

        with self.read() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    # -------------------------------------------------------------------
    # Document helpers
    # -------------------------------------------------------------------

    def upsert_document(self, doc: dict[str, Any]) -> bool:
        """Insert a document record. Returns True if new, False if exists."""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT doc_uuid FROM documents WHERE doc_uuid = ?",
                (doc["doc_uuid"],),
            ).fetchone()

            if existing:
                return False

            conn.execute(
                """
                INSERT INTO documents (
                    doc_uuid, folder_uuid, folder_id, file_manager_id,
                    callsign, operator_name, dma_name, dma_rank,
                    year, race_level, office_type, candidate_name,
                    document_type, campaign_year, political_file_type, file_name,
                    file_extension, file_size, create_ts, last_update_ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc["doc_uuid"],
                    doc.get("folder_uuid"),
                    doc.get("folder_id"),
                    doc.get("file_manager_id"),
                    doc["callsign"],
                    doc["operator_name"],
                    doc.get("dma_name"),
                    doc.get("dma_rank"),
                    doc["year"],
                    doc.get("race_level"),
                    doc.get("office_type"),
                    doc.get("candidate_name"),
                    doc.get("document_type"),  # INVOICE | CONTRACT from URL path
                    doc.get("campaign_year"),
                    doc.get("political_file_type", "PA"),
                    doc.get("file_name"),
                    doc.get("file_extension"),
                    doc.get("file_size"),
                    doc.get("create_ts"),
                    doc.get("last_update_ts"),
                ),
            )
            return True

    def get_undownloaded_docs(
        self, operator: Optional[str] = None, year: Optional[int] = None, limit: Optional[int] = None
    ) -> list[dict]:
        """Get documents that haven't been downloaded yet."""
        query = "SELECT * FROM documents WHERE pdf_downloaded = 0"
        params: list[Any] = []

        if operator:
            query += " AND LOWER(operator_name) LIKE ?"
            params.append(f"%{operator.lower()}%")
        if year:
            query += " AND year = ?"
            params.append(year)

        query += " ORDER BY create_ts DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with self.read() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_unextracted_docs(
        self, operator: Optional[str] = None, year: Optional[int] = None, limit: Optional[int] = None
    ) -> list[dict]:
        """Get documents downloaded but not yet text-extracted."""
        query = "SELECT * FROM documents WHERE pdf_downloaded = 1 AND text_extracted = 0"
        params: list[Any] = []

        if operator:
            query += " AND LOWER(operator_name) LIKE ?"
            params.append(f"%{operator.lower()}%")
        if year:
            query += " AND year = ?"
            params.append(year)

        query += " ORDER BY create_ts DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with self.read() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_unprocessed_docs(
        self, operator: Optional[str] = None, year: Optional[int] = None, limit: Optional[int] = None
    ) -> list[dict]:
        """Get documents with text extracted but not yet LLM processed."""
        query = "SELECT d.*, et.raw_text FROM documents d JOIN extracted_text et ON d.doc_uuid = et.doc_uuid WHERE d.text_extracted = 1 AND d.llm_processed = 0"
        params: list[Any] = []

        if operator:
            query += " AND LOWER(d.operator_name) LIKE ?"
            params.append(f"%{operator.lower()}%")
        if year:
            query += " AND d.year = ?"
            params.append(year)

        query += " ORDER BY d.create_ts DESC"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with self.read() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def mark_downloaded(self, doc_uuid: str, pdf_path: str) -> None:
        """Mark a document as downloaded."""
        with self.transaction() as conn:
            conn.execute(
                "UPDATE documents SET pdf_downloaded = 1, pdf_path = ? WHERE doc_uuid = ?",
                (pdf_path, doc_uuid),
            )

    def mark_text_extracted(
        self,
        doc_uuid: str,
        raw_text: str,
        method: str,
        char_count: int,
        page_count: int,
        ocr_confidence: Optional[float] = None,
    ) -> None:
        """Mark a document as text-extracted and store the raw text."""
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE documents SET
                    text_extracted = 1,
                    extraction_method = ?,
                    char_count = ?,
                    page_count = ?
                WHERE doc_uuid = ?
                """,
                (method, char_count, page_count, doc_uuid),
            )
            conn.execute(
                """
                INSERT INTO extracted_text (doc_uuid, raw_text, ocr_confidence)
                VALUES (?, ?, ?)
                ON CONFLICT(doc_uuid) DO UPDATE SET
                    raw_text = excluded.raw_text,
                    ocr_confidence = excluded.ocr_confidence,
                    extracted_at = datetime('now')
                """,
                (doc_uuid, raw_text, ocr_confidence),
            )

    def mark_llm_processed(self, doc_uuid: str, status: str) -> None:
        """Mark a document as LLM-processed with the given status."""
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE documents SET
                    llm_processed = 1,
                    llm_processed_at = datetime('now'),
                    extraction_status = ?
                WHERE doc_uuid = ?
                """,
                (status, doc_uuid),
            )

    def insert_extraction(self, extraction: dict[str, Any]) -> None:
        """Insert an LLM extraction result."""
        confidence_notes = extraction.get("confidence_notes")
        if isinstance(confidence_notes, list):
            confidence_notes = json.dumps(confidence_notes)

        with self.transaction() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO extractions (
                    doc_uuid, document_type, advertiser_name,
                    office_type_extracted, gross_amount, net_amount,
                    agency_commission, gross_or_net_flag, class_of_time,
                    num_spots, lowest_unit_rate, actual_rate,
                    flight_start, flight_end, invoice_date,
                    invoice_period_start, invoice_period_end,
                    station_callsign, dma_extracted, revenue_quarter,
                    revenue_date_source, revenue_date_unknown,
                    extraction_confidence, confidence_notes,
                    input_tokens, output_tokens, estimated_cost_usd
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    extraction["doc_uuid"],
                    extraction["document_type"],
                    extraction.get("advertiser_name"),
                    extraction.get("office_type_extracted"),
                    extraction.get("gross_amount"),
                    extraction.get("net_amount"),
                    extraction.get("agency_commission"),
                    extraction.get("gross_or_net_flag"),
                    extraction.get("class_of_time"),
                    extraction.get("num_spots"),
                    extraction.get("lowest_unit_rate"),
                    extraction.get("actual_rate"),
                    extraction.get("flight_start"),
                    extraction.get("flight_end"),
                    extraction.get("invoice_date"),
                    extraction.get("invoice_period_start"),
                    extraction.get("invoice_period_end"),
                    extraction.get("station_callsign"),
                    extraction.get("dma_extracted"),
                    extraction.get("revenue_quarter"),
                    extraction.get("revenue_date_source"),
                    extraction.get("revenue_date_unknown", 0),
                    extraction.get("extraction_confidence", "medium"),
                    confidence_notes,
                    extraction.get("input_tokens"),
                    extraction.get("output_tokens"),
                    extraction.get("estimated_cost_usd"),
                ),
            )

    # -------------------------------------------------------------------
    # Crawl state helpers
    # -------------------------------------------------------------------

    def update_crawl_state(self, callsign: str, docs_found: int, last_doc_ts: Optional[str] = None, errors: Optional[str] = None) -> None:
        """Update the crawl state for a station."""
        now = datetime.now(timezone.utc).isoformat()
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO crawl_state (callsign, last_crawled_at, last_doc_ts, docs_found, errors)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(callsign) DO UPDATE SET
                    last_crawled_at = excluded.last_crawled_at,
                    last_doc_ts = COALESCE(excluded.last_doc_ts, crawl_state.last_doc_ts),
                    docs_found = excluded.docs_found,
                    errors = excluded.errors
                """,
                (callsign, now, last_doc_ts, docs_found, errors),
            )

    def get_crawl_state(self, callsign: str) -> Optional[dict]:
        """Get the crawl state for a station."""
        with self.read() as conn:
            row = conn.execute(
                "SELECT * FROM crawl_state WHERE callsign = ?", (callsign,)
            ).fetchone()
            return dict(row) if row else None

    # -------------------------------------------------------------------
    # Run log helpers
    # -------------------------------------------------------------------

    def start_run(self, stage: str, operators: Optional[str] = None, year: Optional[int] = None) -> int:
        """Start a new run log entry. Returns the run_id."""
        with self.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO run_log (stage, operators, year) VALUES (?, ?, ?)",
                (stage, operators, year),
            )
            return cursor.lastrowid  # type: ignore[return-value]

    def complete_run(
        self, run_id: int, docs_processed: int = 0, docs_failed: int = 0,
        total_cost_usd: float = 0.0, notes: Optional[str] = None
    ) -> None:
        """Complete a run log entry."""
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE run_log SET
                    completed_at = datetime('now'),
                    docs_processed = ?,
                    docs_failed = ?,
                    total_cost_usd = ?,
                    notes = ?
                WHERE run_id = ?
                """,
                (docs_processed, docs_failed, total_cost_usd, notes, run_id),
            )

    # -------------------------------------------------------------------
    # Model / aggregation queries
    # -------------------------------------------------------------------

    def get_extractions_for_model(
        self,
        operator: Optional[str] = None,
        year: Optional[int] = None,
        document_type: Optional[str] = None,
    ) -> list[dict]:
        """Get extraction records for the financial model.

        Only returns records where extraction was successful AND
        revenue_date_unknown is False.
        """
        query = """
            SELECT e.*, d.callsign, d.operator_name, d.dma_name, d.dma_rank,
                   d.race_level, d.office_type AS folder_office_type,
                   d.candidate_name AS folder_candidate_name, d.campaign_year
            FROM extractions e
            JOIN documents d ON e.doc_uuid = d.doc_uuid
            WHERE d.extraction_status = 'success'
              AND e.revenue_date_unknown = 0
        """
        params: list[Any] = []

        if operator:
            query += " AND LOWER(d.operator_name) LIKE ?"
            params.append(f"%{operator.lower()}%")
        if year:
            query += " AND d.year = ?"
            params.append(year)
        if document_type:
            query += " AND e.document_type = ?"
            params.append(document_type)

        query += " ORDER BY e.flight_start ASC"

        with self.read() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_pipeline_status(self) -> dict[str, Any]:
        """Get a summary of the pipeline status across all stages."""
        with self.read() as conn:
            stations = conn.execute("SELECT COUNT(*) as cnt FROM stations").fetchone()
            total_docs = conn.execute("SELECT COUNT(*) as cnt FROM documents").fetchone()
            downloaded = conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE pdf_downloaded = 1").fetchone()
            text_done = conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE text_extracted = 1").fetchone()
            llm_done = conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE llm_processed = 1").fetchone()
            success = conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE extraction_status = 'success'").fetchone()
            failed = conn.execute("SELECT COUNT(*) as cnt FROM documents WHERE extraction_status = 'failed'").fetchone()
            total_cost = conn.execute("SELECT COALESCE(SUM(total_cost_usd), 0) as total FROM run_log").fetchone()

            return {
                "stations": stations["cnt"],
                "total_documents": total_docs["cnt"],
                "pdfs_downloaded": downloaded["cnt"],
                "text_extracted": text_done["cnt"],
                "llm_processed": llm_done["cnt"],
                "extraction_success": success["cnt"],
                "extraction_failed": failed["cnt"],
                "total_cost_usd": total_cost["total"],
            }
