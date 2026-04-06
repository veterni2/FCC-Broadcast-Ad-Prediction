"""Curated operator-to-station mapping.

The mapping from station call sign to parent operator MUST come from
a pre-built lookup table sourced from FCC Form 323 ownership reports
or a verified third-party source.

DO NOT infer operator from station name, call sign pattern, or market.
Ownership changes frequently (M&A, bankruptcy, asset sales).

The lookup table is stored as static/operator_stations.csv, version-controlled
in git, and updated manually when ownership changes occur.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from ..core.exceptions import OperatorNotFoundError
from ..core.models import Station
from ..utils.logging import get_logger

log = get_logger("operator_map")

# Path to the versioned operator mapping CSV
_DEFAULT_CSV_PATH = Path(__file__).parent.parent.parent.parent / "static" / "operator_stations.csv"


def load_operator_stations(
    csv_path: Optional[Path] = None,
    operator_filter: Optional[str] = None,
    top_dma: Optional[int] = None,
) -> list[Station]:
    """Load the operator-to-station mapping from the curated CSV.

    Args:
        csv_path: Path to operator_stations.csv. Defaults to static/operator_stations.csv.
        operator_filter: If set, only return stations for this operator (case-insensitive partial match).
        top_dma: If set, only return stations in DMAs ranked <= this value.

    Returns:
        List of Station models.

    Raises:
        FileNotFoundError: If the CSV file doesn't exist.
        OperatorNotFoundError: If operator_filter matches zero stations.
    """
    csv_path = csv_path or _DEFAULT_CSV_PATH

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Operator mapping CSV not found at {csv_path}. "
            "Create it from FCC Form 323 data or a verified station list."
        )

    stations: list[Station] = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip comment rows (callsign starts with '#') — used for
            # human-readable section headers inside the CSV
            callsign_raw = row.get("callsign", "").strip()
            if callsign_raw.startswith("#"):
                continue

            # Apply operator filter
            if operator_filter:
                op_name = row.get("operator_name", "").lower()
                if operator_filter.lower() not in op_name:
                    continue

            # Apply DMA rank filter
            dma_rank_str = row.get("dma_rank", "")
            dma_rank = int(dma_rank_str) if dma_rank_str.strip() else None
            if top_dma and dma_rank and dma_rank > top_dma:
                continue

            station = Station(
                callsign=row["callsign"].strip().upper(),
                facility_id=row.get("facility_id", "").strip() or None,
                entity_id=row.get("entity_id", "").strip() or None,
                operator_name=row["operator_name"].strip(),
                dma_rank=dma_rank,
                dma_name=row.get("dma_name", "").strip() or None,
                dma_code=row.get("dma_code", "").strip() or None,
                community_state=row.get("community_state", "").strip() or None,
                network_affil=row.get("network_affil", "").strip() or None,
            )
            stations.append(station)

    if operator_filter and not stations:
        raise OperatorNotFoundError(operator_filter)

    log.info(
        f"Loaded {len(stations)} stations"
        + (f" for operator '{operator_filter}'" if operator_filter else "")
        + (f" in top {top_dma} DMAs" if top_dma else "")
    )
    return stations


def get_operators(csv_path: Optional[Path] = None) -> list[str]:
    """Get the list of unique operator names in the mapping CSV."""
    csv_path = csv_path or _DEFAULT_CSV_PATH

    if not csv_path.exists():
        return []

    operators: set[str] = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("callsign") or "").strip().startswith("#"):
                continue
            op = row.get("operator_name", "") or ""
            op = op.strip()
            if op:
                operators.add(op)

    return sorted(operators)
