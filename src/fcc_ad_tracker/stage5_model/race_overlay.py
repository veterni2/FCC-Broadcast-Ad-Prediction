"""Cook Political Report race ratings DMA overlay.

Joins extracted revenue data with race competitiveness ratings
to add a competitive_rating column. This is an OPTIONAL overlay —
if data/race_ratings.csv does not exist, the module is skipped
and competitive_rating is simply omitted from the output.

The race_ratings.csv must be manually created from Cook's public web pages.
Cook does not provide a free CSV download.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Optional

from ..utils.logging import get_logger

log = get_logger("race_overlay")

_DEFAULT_RATINGS_PATH = Path(__file__).parent.parent.parent.parent / "data" / "race_ratings.csv"


def load_race_ratings(csv_path: Optional[Path] = None) -> dict[str, dict]:
    """Load Cook Political Report race ratings.

    Args:
        csv_path: Path to race_ratings.csv. Defaults to data/race_ratings.csv.

    Returns:
        Dict mapping (state, office_type) tuples to rating info,
        or empty dict if file doesn't exist.
    """
    csv_path = csv_path or _DEFAULT_RATINGS_PATH

    if not csv_path.exists():
        log.info(
            "Race ratings CSV not found — competitive overlay will be skipped. "
            "Create data/race_ratings.csv from Cook Political Report data."
        )
        return {}

    ratings: dict[str, dict] = {}

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = f"{row.get('state', '')}-{row.get('office_type', '')}-{row.get('district', '')}"
            ratings[key] = {
                "rating": row.get("rating", ""),
                "incumbent": row.get("incumbent", ""),
                "challenger": row.get("challenger", ""),
            }

    log.info(f"Loaded {len(ratings)} race ratings")
    return ratings
