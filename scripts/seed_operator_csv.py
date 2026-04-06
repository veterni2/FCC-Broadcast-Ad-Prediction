"""Seed script for generating the initial operator_stations.csv.

Sources:
- FCC Form 323 ownership reports
- Gray Television station census (graymedia.com/assets/pdf/)
- TVNewsCheck station ownership database
- BIA Advisory station data

This script is a template — actual data must be manually curated
from the sources above. Operator assignment is NEVER inferred
from call sign patterns.
"""

from __future__ import annotations

import csv
from pathlib import Path


def main() -> None:
    """Generate a template operator_stations.csv."""
    output_path = Path(__file__).parent.parent / "static" / "operator_stations.csv"

    print(f"Operator stations CSV already exists at: {output_path}")
    print("To update, manually edit the CSV with verified ownership data.")
    print()
    print("Sources for operator mapping:")
    print("  1. FCC Form 323: https://www.fcc.gov/media/radio/ownership-report-form-323")
    print("  2. Gray TV stations: https://gray.tv/stations")
    print("  3. TVNewsCheck: https://tvnewscheck.com/station-ownership/")
    print("  4. BIA Advisory: https://www.biaadvisory.com/")


if __name__ == "__main__":
    main()
