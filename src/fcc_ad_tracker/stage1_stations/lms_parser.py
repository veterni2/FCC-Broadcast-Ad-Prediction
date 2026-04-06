"""Parse FCC LMS (Licensing and Management System) bulk data files.

The LMS public database files are available at:
https://opendata.fcc.gov/Media/LMS-Public-Database-Files/nsck-y87u

These are pipe-delimited text files with 2-character record type codes.
This module extracts facility records for TV broadcast stations.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from ..utils.logging import get_logger

log = get_logger("lms_parser")


def parse_lms_facility_file(filepath: Path) -> list[dict]:
    """Parse an LMS facility data file.

    Args:
        filepath: Path to the pipe-delimited LMS file.

    Returns:
        List of station records with fields:
        facility_id, callsign, service_type, community_state, etc.
    """
    stations: list[dict] = []

    if not filepath.exists():
        log.error(f"LMS file not found: {filepath}")
        return stations

    log.info(f"Parsing LMS file: {filepath}")

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) < 5:
                continue

            # Extract relevant fields based on LMS file format
            # The exact column positions depend on the specific LMS export
            # This will be refined when we have actual LMS data
            try:
                record = {
                    "facility_id": row[0].strip() if len(row) > 0 else None,
                    "callsign": row[1].strip() if len(row) > 1 else None,
                    "service_type": row[2].strip() if len(row) > 2 else None,
                    "community_state": row[3].strip() if len(row) > 3 else None,
                    "network_affil": row[4].strip() if len(row) > 4 else None,
                }

                # Only keep TV stations
                if record.get("service_type") in ("TV", "DT", "TX"):
                    stations.append(record)

            except (IndexError, ValueError) as e:
                log.debug(f"Skipping malformed row: {e}")
                continue

    log.info(f"Parsed {len(stations)} TV station records from LMS")
    return stations
