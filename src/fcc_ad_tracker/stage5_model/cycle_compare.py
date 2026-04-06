"""Cross-cycle comparison (2022 vs 2024 vs 2026).

Joins data across election cycles for the same DMAs and office types.
Normalizes by week-of-cycle (weeks before election day), not calendar date,
to handle the ramp-up pattern correctly.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional

from ..core.db import DatabaseManager
from ..utils.dates import get_quarter_date_range, week_of_cycle
from ..utils.logging import get_logger

log = get_logger("cycle_compare")


def compare_cycles(
    db: DatabaseManager,
    operator: Optional[str] = None,
    cycles: Optional[list[int]] = None,
) -> list[dict]:
    """Compare political ad revenue across election cycles.

    Fetches INVOICE extractions for each cycle year, maps each extraction's
    revenue_quarter to a week-of-cycle value (negative = weeks before election
    day), groups by (week_of_cycle, operator_name), and merges into a single
    record per (week_of_cycle, operator_name) with per-cycle gross columns and
    year-over-year growth rates.

    Args:
        db: Database manager instance.
        operator: Operator filter (passed through to db query).
        cycles: List of election years to compare. Defaults to [2022, 2024, 2026].

    Returns:
        List of dicts sorted by (operator_name, week_of_cycle):
        [
            {
                "week_of_cycle": -8,
                "operator_name": "Gray Television",
                "2022_gross": 2.1,
                "2024_gross": 4.5,
                "2026_gross": 1.2,
                "yoy_growth_2024_vs_2022": 1.14,
                "yoy_growth_2026_vs_2024": None,
            },
            ...
        ]
    """
    cycles = cycles or [2022, 2024, 2026]

    # Accumulate gross_amount by (cycle_year, week_of_cycle, operator_name).
    # Structure: cycle_data[cycle_year][(week_of_cycle, operator_name)] = total_gross
    cycle_data: dict[int, dict[tuple[int, str], float]] = {y: defaultdict(float) for y in cycles}

    for cycle_year in cycles:
        extractions = db.get_extractions_for_model(
            operator=operator,
            year=cycle_year,
            document_type="INVOICE",
        )
        log.info(f"Cycle {cycle_year}: fetched {len(extractions)} INVOICE extractions")

        for ext in extractions:
            revenue_quarter = ext.get("revenue_quarter")
            if not revenue_quarter:
                # No quarter attribution — skip per zero-imputation policy
                continue

            gross = ext.get("gross_amount")
            if gross is None:
                # NULL amounts excluded, never imputed
                continue

            operator_name: str = ext.get("operator_name") or "Unknown"

            # Compute the midpoint date of the quarter
            try:
                q_start, q_end = get_quarter_date_range(revenue_quarter)
            except Exception:
                log.debug(f"Could not parse revenue_quarter '{revenue_quarter}' — skipping")
                continue

            midpoint = q_start + (q_end - q_start) / 2

            # Compute weeks-before-election for the cycle year (election date is
            # inferred from the cycle year by week_of_cycle when not supplied).
            woc = week_of_cycle(midpoint)

            cycle_data[cycle_year][(woc, operator_name)] += float(gross)

    # Collect all (week_of_cycle, operator_name) keys across all cycles
    all_keys: set[tuple[int, str]] = set()
    for year_data in cycle_data.values():
        all_keys.update(year_data.keys())

    records: list[dict] = []
    for woc, op_name in all_keys:
        row: dict = {
            "week_of_cycle": woc,
            "operator_name": op_name,
        }

        gross_by_year: dict[int, Optional[float]] = {}
        for cy in cycles:
            val = cycle_data[cy].get((woc, op_name))
            # Store None when there is no data for this cycle (no aggregated value)
            # defaultdict returns 0.0 via __missing__ only when the key was set;
            # .get() returns None when key was never touched — correct sentinel.
            gross_by_year[cy] = val
            row[f"{cy}_gross"] = val

        # Growth rates: only computable when both values are non-None and prior > 0
        sorted_cycles = sorted(cycles)
        for i in range(1, len(sorted_cycles)):
            prior_year = sorted_cycles[i - 1]
            curr_year = sorted_cycles[i]
            prior_val = gross_by_year.get(prior_year)
            curr_val = gross_by_year.get(curr_year)
            growth_key = f"yoy_growth_{curr_year}_vs_{prior_year}"
            if prior_val and curr_val is not None and prior_val != 0:
                row[growth_key] = (curr_val / prior_val) - 1
            else:
                row[growth_key] = None

        records.append(row)

    records.sort(key=lambda r: (r["operator_name"], r["week_of_cycle"]))
    log.info(f"compare_cycles returning {len(records)} records for cycles {cycles}")
    return records
