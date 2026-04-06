"""Station enumerator — build stations.json and populate SQLite.

Cross-references the curated operator mapping CSV with FCC LMS data
to produce a validated list of stations for the target operators.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from ..core.db import DatabaseManager
from ..core.models import Station
from ..utils.logging import get_logger
from .operator_map import load_operator_stations

log = get_logger("enumerator")


def enumerate_stations(
    db: DatabaseManager,
    operators: list[str],
    top_dma: Optional[int] = None,
    output_path: Optional[Path] = None,
) -> list[Station]:
    """Enumerate stations for the given operators and populate the database.

    Args:
        db: Database manager instance.
        operators: List of operator names to enumerate (e.g., ['gray', 'nexstar']).
        top_dma: Only include stations in the top N DMAs.
        output_path: Path to write stations.json. Defaults to data/stations.json.

    Returns:
        List of Station models for the matched stations.
    """
    all_stations: list[Station] = []

    for operator in operators:
        try:
            stations = load_operator_stations(
                operator_filter=operator,
                top_dma=top_dma,
            )
            all_stations.extend(stations)
            log.info(f"Found {len(stations)} stations for operator '{operator}'")
        except FileNotFoundError as e:
            log.error(str(e))
            continue

    if not all_stations:
        log.warning("No stations found for the given operators.")
        return []

    # Deduplicate by callsign (in case of overlapping operator filters)
    seen: set[str] = set()
    unique_stations: list[Station] = []
    for station in all_stations:
        if station.callsign not in seen:
            seen.add(station.callsign)
            unique_stations.append(station)

    # Populate database
    for station in unique_stations:
        db.upsert_station(station.model_dump())

    log.info(f"Registered {len(unique_stations)} stations in database")

    # Write stations.json
    if output_path is None:
        output_path = db.db_path.parent / "stations.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            [s.model_dump() for s in unique_stations],
            f,
            indent=2,
        )
    log.info(f"Wrote {output_path}")

    return unique_stations
