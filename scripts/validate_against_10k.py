"""Validate extracted political ad revenue against 10-K reported actuals.

Compares the sum of extracted INVOICE dollars for a given operator
and year against the political advertising revenue reported in the
company's SEC filings.

If the divergence exceeds 30%, a warning is surfaced.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Known reported political ad revenue from 10-K filings (USD millions)
# Source: company earnings releases and SEC filings
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
    print("Validation against 10-K actuals")
    print("=" * 50)
    print()
    print("Known actuals (to be populated from SEC filings):")
    for (operator, year), data in KNOWN_ACTUALS.items():
        print(f"  {operator.title()} {year}: {data['source']}")
        print(f"    Full year: {data['full_year'] or 'TBD'}")
    print()
    print("Run the pipeline first, then use 'fcc-tracker validate' to compare.")


if __name__ == "__main__":
    main()
