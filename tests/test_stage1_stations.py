"""Tests for Stage 1: Station enumeration."""

from __future__ import annotations

import csv
import tempfile
from pathlib import Path

import pytest

from fcc_ad_tracker.core.db import DatabaseManager
from fcc_ad_tracker.stage1_stations.operator_map import load_operator_stations


def _create_test_csv(tmp_path: Path) -> Path:
    """Create a minimal operator_stations.csv for testing."""
    csv_path = tmp_path / "test_operators.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "callsign", "facility_id", "entity_id", "operator_name",
                "dma_rank", "dma_name", "dma_code", "community_state", "network_affil",
            ],
        )
        writer.writeheader()
        writer.writerow({
            "callsign": "WFAA", "facility_id": "72054", "entity_id": "E001",
            "operator_name": "Gray Television", "dma_rank": "5",
            "dma_name": "Dallas-Fort Worth", "dma_code": "623",
            "community_state": "TX", "network_affil": "ABC",
        })
        writer.writerow({
            "callsign": "KHOU", "facility_id": "72055", "entity_id": "E002",
            "operator_name": "Gray Television", "dma_rank": "8",
            "dma_name": "Houston", "dma_code": "618",
            "community_state": "TX", "network_affil": "CBS",
        })
        writer.writerow({
            "callsign": "KXAN", "facility_id": "72056", "entity_id": "E003",
            "operator_name": "Nexstar Media Group", "dma_rank": "40",
            "dma_name": "Austin", "dma_code": "635",
            "community_state": "TX", "network_affil": "NBC",
        })
    return csv_path


def test_load_all_stations(tmp_path: Path) -> None:
    """Loading without filters returns all stations."""
    csv_path = _create_test_csv(tmp_path)
    stations = load_operator_stations(csv_path=csv_path)
    assert len(stations) == 3


def test_load_with_operator_filter(tmp_path: Path) -> None:
    """Operator filter returns only matching stations."""
    csv_path = _create_test_csv(tmp_path)
    stations = load_operator_stations(csv_path=csv_path, operator_filter="gray")
    assert len(stations) == 2
    assert all(s.operator_name == "Gray Television" for s in stations)


def test_load_with_dma_filter(tmp_path: Path) -> None:
    """DMA rank filter excludes stations in smaller DMAs."""
    csv_path = _create_test_csv(tmp_path)
    stations = load_operator_stations(csv_path=csv_path, top_dma=10)
    assert len(stations) == 2
    assert all(s.dma_rank is not None and s.dma_rank <= 10 for s in stations)


def test_load_combined_filters(tmp_path: Path) -> None:
    """Combining operator and DMA filters."""
    csv_path = _create_test_csv(tmp_path)
    stations = load_operator_stations(
        csv_path=csv_path, operator_filter="gray", top_dma=6
    )
    assert len(stations) == 1
    assert stations[0].callsign == "WFAA"


def test_missing_csv_raises() -> None:
    """Missing CSV file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_operator_stations(csv_path=Path("/nonexistent/path.csv"))
