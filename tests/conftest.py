"""Shared test fixtures for FCC Ad Tracker tests."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from fcc_ad_tracker.core.db import DatabaseManager


@pytest.fixture
def tmp_db(tmp_path: Path) -> DatabaseManager:
    """Create a temporary database for testing."""
    db = DatabaseManager(db_path=tmp_path / "test.db")
    db.initialize()
    return db


@pytest.fixture
def sample_station() -> dict:
    """A sample Gray TV station record."""
    return {
        "callsign": "WFAA",
        "facility_id": "72054",
        "entity_id": "E12345",
        "operator_name": "Gray Television",
        "dma_rank": 5,
        "dma_name": "Dallas-Fort Worth",
        "dma_code": "623",
        "community_state": "TX",
        "network_affil": "ABC",
    }


@pytest.fixture
def sample_document(sample_station: dict) -> dict:
    """A sample political file document record."""
    return {
        "doc_uuid": "test-uuid-001",
        "folder_uuid": "folder-uuid-001",
        "folder_id": "12345",
        "file_manager_id": "67890",
        "callsign": sample_station["callsign"],
        "operator_name": sample_station["operator_name"],
        "dma_name": sample_station["dma_name"],
        "dma_rank": sample_station["dma_rank"],
        "year": 2024,
        "race_level": "federal",
        "office_type": "us-senate",
        "candidate_name": "ted-cruz",
        "campaign_year": 2024,
        "political_file_type": "PA",
        "file_name": "Invoice_Oct2024.pdf",
        "file_extension": "pdf",
        "file_size": 125000,
        "create_ts": "2024-10-15T14:30:00Z",
    }


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"
