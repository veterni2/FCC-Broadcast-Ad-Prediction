"""Tests for the SQLite database manager."""

from __future__ import annotations

from fcc_ad_tracker.core.db import DatabaseManager


def test_initialize_creates_tables(tmp_db: DatabaseManager) -> None:
    """Schema creation produces all expected tables."""
    with tmp_db.read() as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = sorted(row["name"] for row in tables)

    assert "stations" in table_names
    assert "documents" in table_names
    assert "extracted_text" in table_names
    assert "extractions" in table_names
    assert "crawl_state" in table_names
    assert "run_log" in table_names


def test_upsert_station(tmp_db: DatabaseManager, sample_station: dict) -> None:
    """Station upsert inserts and then updates."""
    tmp_db.upsert_station(sample_station)
    stations = tmp_db.get_stations()
    assert len(stations) == 1
    assert stations[0]["callsign"] == "WFAA"
    assert stations[0]["operator_name"] == "Gray Television"

    # Update
    sample_station["network_affil"] = "CBS"
    tmp_db.upsert_station(sample_station)
    stations = tmp_db.get_stations()
    assert len(stations) == 1
    assert stations[0]["network_affil"] == "CBS"


def test_upsert_document(tmp_db: DatabaseManager, sample_station: dict, sample_document: dict) -> None:
    """Document upsert inserts new and rejects duplicates."""
    tmp_db.upsert_station(sample_station)

    is_new = tmp_db.upsert_document(sample_document)
    assert is_new is True

    is_new = tmp_db.upsert_document(sample_document)
    assert is_new is False


def test_get_undownloaded_docs(tmp_db: DatabaseManager, sample_station: dict, sample_document: dict) -> None:
    """Undownloaded docs query returns the right records."""
    tmp_db.upsert_station(sample_station)
    tmp_db.upsert_document(sample_document)

    docs = tmp_db.get_undownloaded_docs()
    assert len(docs) == 1
    assert docs[0]["doc_uuid"] == "test-uuid-001"

    # Mark as downloaded
    tmp_db.mark_downloaded("test-uuid-001", "/path/to/file.pdf")
    docs = tmp_db.get_undownloaded_docs()
    assert len(docs) == 0


def test_pipeline_status(tmp_db: DatabaseManager) -> None:
    """Pipeline status returns correct counts."""
    status = tmp_db.get_pipeline_status()
    assert status["stations"] == 0
    assert status["total_documents"] == 0
    assert status["total_cost_usd"] == 0.0


def test_run_log(tmp_db: DatabaseManager) -> None:
    """Run log tracks pipeline runs."""
    run_id = tmp_db.start_run(stage="test", operators="gray", year=2024)
    assert run_id > 0

    tmp_db.complete_run(run_id, docs_processed=10, docs_failed=2, total_cost_usd=0.15)

    with tmp_db.read() as conn:
        row = conn.execute("SELECT * FROM run_log WHERE run_id = ?", (run_id,)).fetchone()
        assert row["docs_processed"] == 10
        assert row["docs_failed"] == 2
        assert row["total_cost_usd"] == 0.15
        assert row["completed_at"] is not None
