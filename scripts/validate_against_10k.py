"""Validate extracted political ad revenue against 10-K reported actuals.

Compares the sum of extracted INVOICE gross dollars for a given operator
and year against the political advertising revenue reported in the company's
SEC filings and earnings releases.

Divergence > 30% surfaces a warning. This is expected during the early
cycle (low station coverage) and should narrow as filings accumulate.
"""

from __future__ import annotations

import argparse
import sys

# Known reported political ad revenue from 10-K filings (USD, full dollars)
# Source: company earnings releases and SEC filings
# Set full_year to an integer dollar amount once the filing is available.
KNOWN_ACTUALS = {
    ("gray", 2024): {
        "q3": None,  # To be filled from earnings release
        "q4": None,
        "full_year": None,
        "source": "Gray Television 2024 10-K",
    },
    ("nexstar", 2024): {
        "q3": None,
        "q4": None,
        "full_year": None,
        "source": "Nexstar Media Group 2024 10-K, ~$750M per management guidance",
    },
}


def main() -> None:
    """Run validation against known actuals."""
    parser = argparse.ArgumentParser(
        description="Validate extracted INVOICE gross against 10-K reported actuals."
    )
    parser.add_argument(
        "--operator",
        required=True,
        help="Operator name to filter (e.g. 'gray', 'nexstar')",
    )
    parser.add_argument(
        "--year",
        required=True,
        type=int,
        help="Campaign year to validate (e.g. 2024)",
    )
    args = parser.parse_args()

    operator: str = args.operator
    year: int = args.year
    operator_key = operator.lower()

    # Import here so errors surface clearly before DB connection
    try:
        from fcc_ad_tracker.core.db import DatabaseManager
        from fcc_ad_tracker.config.settings import get_settings
    except ImportError as exc:
        print(
            f"ERROR: Could not import fcc_ad_tracker: {exc}\n"
            "Make sure the package is installed: pip install -e ."
        )
        sys.exit(1)

    # Connect to the database
    try:
        db = DatabaseManager()
        db.initialize()
    except Exception as exc:
        print(
            f"ERROR: Could not connect to database: {exc}\n"
            "Run the pipeline first (fcc-tracker run) to populate the database."
        )
        sys.exit(1)

    # Pull all successful INVOICE extractions for this operator + year
    try:
        records = db.get_extractions_for_model(
            operator=operator,
            year=year,
            document_type="INVOICE",
        )
    except Exception as exc:
        print(f"ERROR: Database query failed: {exc}")
        sys.exit(1)

    # Sum gross_amount; skip records where it is None
    extracted_total = sum(
        e["gross_amount"] for e in records if e.get("gross_amount") is not None
    )
    docs_with_amounts = sum(
        1 for e in records if e.get("gross_amount") is not None
    )
    docs_attempted = len(records)
    coverage_rate = (
        docs_with_amounts / docs_attempted * 100 if docs_attempted else 0.0
    )

    # Look up known actual
    actual_entry = KNOWN_ACTUALS.get((operator_key, year))
    actual: int | None = actual_entry["full_year"] if actual_entry else None

    # --- Print results ---
    print(f"Operator: {operator.title()} | Year: {year}")
    print(f"Extracted INVOICE gross:    ${extracted_total:,.0f}")
    if actual is not None:
        print(f"Reported 10-K full year:    ${actual:,.0f}")
    else:
        print(f"Reported 10-K full year:    $TBD (not yet populated)")
    print(f"Documents with amounts:     {docs_with_amounts:,}")
    print(f"Documents attempted:        {docs_attempted:,}")
    print(f"Coverage rate:              {coverage_rate:.1f}%")

    if actual is not None and extracted_total > 0:
        divergence = abs(extracted_total - actual) / actual
        sign = "+" if extracted_total >= actual else "-"
        print(f"Divergence: {sign}{divergence * 100:.1f}%")
        if divergence > 0.30:
            print(
                "\u26a0 WARNING: Divergence exceeds 30% threshold. "
                "Review coverage before using in credit analysis."
            )
        else:
            print("\u2713 Within 30% threshold")
    else:
        print(
            "Actual not yet populated. "
            "Add to KNOWN_ACTUALS dict from SEC filings."
        )


if __name__ == "__main__":
    main()
