"""Weekly filing velocity time series.

Counts new documents and summed dollar amounts per ISO week,
grouped by operator. Useful as a leading indicator of political
ad revenue ahead of quarterly earnings releases.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Optional

from ..core.db import DatabaseManager
from ..utils.dates import get_iso_week
from ..utils.logging import get_logger

log = get_logger("velocity")


def compute_filing_velocity(
    db: DatabaseManager,
    operator: Optional[str] = None,
    year: Optional[int] = None,
) -> list[dict]:
    """Compute weekly filing velocity by operator.

    Queries the documents table directly and joins to extractions for INVOICE
    gross amounts. Each row represents one ISO week per operator, with a
    running cumulative document count.

    Args:
        db: Database manager instance.
        operator: Operator filter (partial match, case-insensitive).
        year: Campaign year filter.

    Returns:
        List of weekly records sorted by (operator_name, iso_week):
        {iso_week, operator_name, doc_count, cumulative_docs, invoice_gross}
    """
    query = """
        SELECT d.operator_name, d.callsign, d.create_ts, d.doc_uuid,
               e.gross_amount, e.document_type, e.revenue_date_unknown
        FROM documents d
        LEFT JOIN extractions e ON d.doc_uuid = e.doc_uuid
            AND e.document_type = 'INVOICE' AND e.revenue_date_unknown = 0
        WHERE d.pdf_downloaded = 1
    """
    params: list = []

    if operator:
        query += " AND LOWER(d.operator_name) LIKE ?"
        params.append(f"%{operator.lower()}%")
    if year:
        query += " AND d.year = ?"
        params.append(year)

    with db.read() as conn:
        rows = conn.execute(query, params).fetchall()

    log.info(f"Loaded {len(rows)} document rows for velocity computation")

    # -----------------------------------------------------------------------
    # Parse rows and aggregate per (operator_name, iso_week)
    # Track unique doc_uuids per week to avoid double-counting documents
    # that have multiple extraction rows.
    # -----------------------------------------------------------------------

    # week_docs[(operator_name, iso_week)] -> set of doc_uuids seen this week
    week_docs: dict[tuple, set] = defaultdict(set)

    # week_gross[(operator_name, iso_week)] -> running gross sum for INVOICE docs
    week_gross: dict[tuple, float] = defaultdict(float)

    # Track which (doc_uuid, operator_name) pairs have already contributed
    # their INVOICE gross to avoid double-adding for the same doc + same week.
    doc_gross_seen: set[tuple] = set()

    for row in rows:
        create_ts: Optional[str] = row["create_ts"]
        if not create_ts:
            continue

        # Parse create_ts — try ISO format first, then common variants
        parsed_date: Optional[date] = None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed_date = datetime.strptime(create_ts[:19], fmt).date()
                break
            except (ValueError, TypeError):
                continue

        if parsed_date is None:
            log.debug(f"Skipping unparseable create_ts: '{create_ts}'")
            continue

        iso_week = get_iso_week(parsed_date)
        op: str = row["operator_name"] or "Unknown"
        doc_uuid: str = row["doc_uuid"]
        key = (op, iso_week)

        # Count unique docs per week
        week_docs[key].add(doc_uuid)

        # Sum INVOICE gross — only once per doc_uuid to avoid double-counting
        # multiple extraction rows for the same document.
        gross_val = row["gross_amount"]
        doc_key = (doc_uuid, op, iso_week)
        if gross_val is not None and doc_key not in doc_gross_seen:
            week_gross[key] += gross_val
            doc_gross_seen.add(doc_key)

    # -----------------------------------------------------------------------
    # Build flat records and compute cumulative docs per operator
    # -----------------------------------------------------------------------

    # Collect all (operator, iso_week) combinations
    all_keys = set(week_docs.keys())

    # Group by operator
    ops_weeks: dict[str, list[str]] = defaultdict(list)
    for op, iso_week in all_keys:
        ops_weeks[op].append(iso_week)

    result: list[dict] = []

    for op, weeks in ops_weeks.items():
        weeks_sorted = sorted(weeks)
        cumulative = 0
        for iso_week in weeks_sorted:
            key = (op, iso_week)
            doc_count = len(week_docs[key])
            cumulative += doc_count
            result.append({
                "iso_week": iso_week,
                "operator_name": op,
                "doc_count": doc_count,
                "cumulative_docs": cumulative,
                "invoice_gross": week_gross[key],
            })

    result.sort(key=lambda x: (x["operator_name"], x["iso_week"]))

    log.info(f"Velocity computation complete: {len(result)} week-operator rows")
    return result
