"""Political file crawler orchestrator.

Coordinates the BrowserManager to discover new political ad documents
for all target stations, deduplicates against the SQLite registry,
and manages incremental crawl state.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from ..core.db import DatabaseManager
from ..core.models import Station
from ..utils.logging import get_logger
from .browser import BrowserManager

log = get_logger("crawler")


async def crawl_stations(
    db: DatabaseManager,
    stations: list[Station],
    year: int,
    operators_str: str = "",
    incremental: bool = False,
) -> dict[str, int]:
    """Crawl FCC OPIF for political ad documents across all stations.

    Opens a single browser instance for the entire run, creates one page per
    station, discovers documents via :class:`BrowserManager`, deduplicates
    against the SQLite registry, and persists crawl state.

    Args:
        db: Initialised :class:`~fcc_ad_tracker.core.db.DatabaseManager`.
        stations: Stations to crawl (all have been validated by Stage 1).
        year: Campaign year to crawl (e.g. ``2026``).
        operators_str: Human-readable operator filter label for logging.
        incremental: When ``True``, pass each station's ``last_crawled_at``
            timestamp to the browser so only newer documents are processed.

    Returns:
        Dict with aggregate crawl statistics::

            {
                "new_docs": int,
                "skipped_existing": int,
                "errors": int,
                "total_stations": int,
            }
    """
    totals: dict[str, int] = {
        "new_docs": 0,
        "skipped_existing": 0,
        "errors": 0,
        "total_stations": len(stations),
    }

    if not stations:
        log.warning("crawl_stations called with empty station list")
        return totals

    log.info(
        "Starting crawl: %d stations, year=%s, incremental=%s, operators=%r",
        len(stations),
        year,
        incremental,
        operators_str,
    )

    async with BrowserManager() as browser:
        for station in stations:
            callsign = station.callsign
            station_new = 0
            station_existing = 0
            station_error_msg: Optional[str] = None

            # Determine incremental cutoff
            incremental_since: Optional[str] = None
            if incremental:
                crawl_state = db.get_crawl_state(callsign)
                if crawl_state:
                    incremental_since = crawl_state.get("last_crawled_at")
                    if incremental_since:
                        log.debug(
                            "Incremental crawl for %s: skipping docs before %s",
                            callsign,
                            incremental_since,
                        )

            log.info(
                "Crawling station %s (%s, DMA #%s — %s)",
                callsign,
                station.operator_name,
                station.dma_rank,
                station.dma_name,
            )

            try:
                docs = await browser.get_political_files(
                    callsign=callsign,
                    year=year,
                    incremental_since=incremental_since,
                )
            except Exception as exc:
                log.error("Browser error crawling %s: %s", callsign, exc, exc_info=True)
                totals["errors"] += 1
                station_error_msg = str(exc)
                # Record failed crawl state so the run is auditable
                db.update_crawl_state(
                    callsign=callsign,
                    docs_found=0,
                    errors=station_error_msg,
                )
                continue

            log.info(
                "%s: browser returned %d candidate document(s)", callsign, len(docs)
            )

            for doc in docs:
                # Inject station-level metadata that the browser cannot know
                doc["operator_name"] = station.operator_name
                doc["dma_name"] = station.dma_name
                doc["dma_rank"] = station.dma_rank
                doc["campaign_year"] = year

                # Ensure callsign is upper-cased consistently
                doc["callsign"] = callsign.upper()

                # year is set by the browser from the URL, but guard against None
                if not doc.get("year"):
                    doc["year"] = year

                try:
                    is_new = db.upsert_document(doc)
                except Exception as exc:
                    log.error(
                        "db.upsert_document failed for doc_uuid=%s (%s): %s",
                        doc.get("doc_uuid"),
                        callsign,
                        exc,
                    )
                    totals["errors"] += 1
                    station_error_msg = str(exc)
                    continue

                if is_new:
                    station_new += 1
                else:
                    station_existing += 1

            log.info(
                "%s: new=%d, existing=%d", callsign, station_new, station_existing
            )

            totals["new_docs"] += station_new
            totals["skipped_existing"] += station_existing

            # Persist crawl state for this station
            docs_found_total = station_new + station_existing
            # Find the most recent create_ts among newly discovered docs for
            # use as last_doc_ts (best-effort; may be None)
            last_doc_ts: Optional[str] = _most_recent_ts(docs)

            db.update_crawl_state(
                callsign=callsign,
                docs_found=docs_found_total,
                last_doc_ts=last_doc_ts,
                errors=station_error_msg,
            )

    log.info(
        "Crawl finished: new_docs=%d, skipped=%d, errors=%d, stations=%d",
        totals["new_docs"],
        totals["skipped_existing"],
        totals["errors"],
        totals["total_stations"],
    )
    return totals


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _most_recent_ts(docs: list[dict]) -> Optional[str]:
    """Return the most recent ``create_ts`` value among *docs*, or ``None``.

    Values that cannot be parsed as ISO datetimes are ignored.
    """
    from datetime import datetime

    best: Optional[datetime] = None
    best_raw: Optional[str] = None

    for doc in docs:
        ts = doc.get("create_ts")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            continue
        if best is None or dt > best:
            best = dt
            best_raw = ts

    return best_raw
