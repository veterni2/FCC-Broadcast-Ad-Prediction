"""Date parsing and quarter attribution utilities.

Revenue is attributed to quarters by flight_date (when spots aired),
NOT by invoice_date or upload_ts. This module implements the
three-tier date attribution hierarchy.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

from .logging import get_logger

log = get_logger("dates")


# Common date formats found in FCC political ad documents
_DATE_FORMATS = [
    "%m/%d/%Y",      # 01/15/2024
    "%m/%d/%y",      # 01/15/24
    "%m-%d-%Y",      # 01-15-2024
    "%m-%d-%y",      # 01-15-24
    "%Y-%m-%d",      # 2024-01-15
    "%B %d, %Y",     # January 15, 2024
    "%b %d, %Y",     # Jan 15, 2024
    "%m/%d",         # 01/15 (no year -- requires context)
    "%d-%b-%Y",      # 15-Jan-2024
    "%d-%b-%y",      # 15-Jan-24
]


def parse_date(date_str: Optional[str], context_year: Optional[int] = None) -> Optional[date]:
    """Parse a date string from a political ad document.

    Tries multiple common date formats. If the format lacks a year
    and context_year is provided, uses that year.

    Args:
        date_str: The date string to parse.
        context_year: Year to assume if the format lacks one.

    Returns:
        A date object, or None if parsing fails.
    """
    if not date_str or not date_str.strip():
        return None

    cleaned = date_str.strip()

    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(cleaned, fmt)
            # Handle two-digit years or missing years
            if "%Y" not in fmt and "%y" not in fmt and context_year:
                parsed = parsed.replace(year=context_year)
            return parsed.date()
        except ValueError:
            continue

    log.debug(f"Failed to parse date: '{date_str}'")
    return None


def date_to_quarter(d: date) -> str:
    """Convert a date to a quarter string like '2024-Q3'.

    Args:
        d: The date to convert.

    Returns:
        Quarter string in format 'YYYY-QN'.
    """
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{quarter}"


def attribute_revenue_quarter(
    flight_start: Optional[str] = None,
    flight_end: Optional[str] = None,
    invoice_period_start: Optional[str] = None,
    invoice_period_end: Optional[str] = None,
    context_year: Optional[int] = None,
) -> tuple[Optional[str], Optional[str], bool]:
    """Determine the revenue quarter using the three-tier hierarchy.

    Hierarchy:
    1. Flight dates (when spots actually aired) -- primary
    2. Invoice period dates -- fallback
    3. Unknown -- EXCLUDED from revenue model

    Args:
        flight_start: Flight start date string.
        flight_end: Flight end date string.
        invoice_period_start: Invoice period start date string.
        invoice_period_end: Invoice period end date string.
        context_year: Year context for date parsing.

    Returns:
        Tuple of (revenue_quarter, revenue_date_source, revenue_date_unknown).
        revenue_quarter: e.g. '2024-Q3' or None.
        revenue_date_source: 'flight' or 'invoice_period' or None.
        revenue_date_unknown: True if no usable dates found.
    """
    # Tier 1: Flight dates
    fs = parse_date(flight_start, context_year)
    fe = parse_date(flight_end, context_year)

    if fs:
        # Use midpoint of flight if both dates available, otherwise start
        if fe and fe > fs:
            midpoint = fs + (fe - fs) / 2
            return date_to_quarter(midpoint), "flight", False
        return date_to_quarter(fs), "flight", False

    if fe:
        return date_to_quarter(fe), "flight", False

    # Tier 2: Invoice period dates
    ips = parse_date(invoice_period_start, context_year)
    ipe = parse_date(invoice_period_end, context_year)

    if ips:
        if ipe and ipe > ips:
            midpoint = ips + (ipe - ips) / 2
            return date_to_quarter(midpoint), "invoice_period", False
        return date_to_quarter(ips), "invoice_period", False

    if ipe:
        return date_to_quarter(ipe), "invoice_period", False

    # Tier 3: Unknown — EXCLUDE from revenue model
    return None, None, True


def get_quarter_date_range(quarter: str) -> tuple[date, date]:
    """Get the start and end dates for a quarter string.

    Args:
        quarter: Quarter string like '2024-Q3'.

    Returns:
        Tuple of (start_date, end_date).
    """
    year_str, q_str = quarter.split("-")
    year = int(year_str)
    q = int(q_str[1])

    start_month = (q - 1) * 3 + 1
    start = date(year, start_month, 1)

    if q < 4:
        end = date(year, start_month + 3, 1) - timedelta(days=1)
    else:
        end = date(year, 12, 31)

    return start, end


def get_iso_week(d: date) -> str:
    """Get ISO week string for filing velocity tracking.

    Args:
        d: The date.

    Returns:
        ISO week string like '2024-W35'.
    """
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def week_of_cycle(d: date, election_date: Optional[date] = None) -> int:
    """Calculate weeks until election day for cycle comparison.

    Normalizes dates across cycles by expressing them as
    'weeks before election day' (negative = before, 0 = election week).

    Args:
        d: The date to normalize.
        election_date: Election day. If None, uses the first Tuesday
            after the first Monday in November of the same year.

    Returns:
        Signed integer: negative means weeks before election.
    """
    if election_date is None:
        # First Tuesday after first Monday in November
        nov1 = date(d.year, 11, 1)
        # Find first Monday
        days_until_monday = (7 - nov1.weekday()) % 7
        if nov1.weekday() == 0:
            days_until_monday = 0
        first_monday = nov1 + timedelta(days=days_until_monday)
        election_date = first_monday + timedelta(days=1)

    delta = (d - election_date).days
    return delta // 7
