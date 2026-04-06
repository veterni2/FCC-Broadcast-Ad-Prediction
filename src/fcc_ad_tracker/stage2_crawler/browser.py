"""Playwright browser manager for FCC OPIF political file scraping.

The FCC OPIF site at publicfiles.fcc.gov renders political file folder
hierarchies as server-rendered HTML with jQuery DataTables. There is no
JSON API for folder traversal — each level loads a full HTML page.

URL hierarchy (the source of truth for race metadata):
    /tv-profile/{callsign}/political-files
        /{year}/{year_uuid}/
            /federal/
                /us-senate/{candidate-slug}/invoices/{folder_uuid}/
                /us-senate/{candidate-slug}/contracts/{folder_uuid}/
                /us-house/{candidate-slug}/invoices/{folder_uuid}/
            /state/governor/{candidate-slug}/invoices/{folder_uuid}/
            /local/{...}/invoices/{folder_uuid}/
            /non-candidate-issue-ads/{folder_uuid}/

File download URL (confirmed working):
    GET https://publicfiles.fcc.gov/api/manager/download/{folder_uuid}/{file_uuid}.pdf
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Page, BrowserContext

from ..config.settings import get_settings
from ..utils.logging import get_logger
from ..utils.rate_limiter import RateLimiter

log = get_logger("browser")

_BASE_URL = "https://publicfiles.fcc.gov"

# UUID v4 pattern (also tolerates v1/other variants from FCC)
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# All known office-type slug values used in the OPIF URL hierarchy
_KNOWN_OFFICE_TYPES: set[str] = {
    "us-senate",
    "us-house",
    "president",
    "governor",
    "state-senate",
    "state-house",
    "state-attorney-general",
    "lt-governor",
    "secretary-of-state",
    "state-treasurer",
    "state-comptroller",
    "state-commissioner",
    "local-offices",
    "mayor",
    "city-council",
    "ballot-measure",
    "other-office",
    "other-state",
}

_KNOWN_RACE_LEVELS: set[str] = {"federal", "state", "local", "non-candidate-issue-ads"}

_DOC_TYPE_MAP: dict[str, str] = {
    "invoices": "INVOICE",
    "contracts": "CONTRACT",
}

# File-size unit multipliers
_SIZE_UNITS: dict[str, int] = {
    "b": 1,
    "kb": 1_024,
    "mb": 1_024**2,
    "gb": 1_024**3,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_uuid(s: str) -> bool:
    """Return True if *s* matches the UUID hex-and-dashes format."""
    return bool(_UUID_RE.match(s))


def parse_path_metadata(url: str, callsign: str) -> dict[str, Any]:
    """Parse OPIF URL path segments after ``/political-files/`` into metadata.

    Supports the full URL hierarchy:

    * ``/{year}/{year_uuid}/federal/{office_type}/{candidate-slug}/{doc_type}/{folder_uuid}/``
    * ``/{year}/{year_uuid}/state/{office_type}/{candidate-slug}/{doc_type}/{folder_uuid}/``
    * ``/{year}/{year_uuid}/local/.../{doc_type}/{folder_uuid}/``
    * ``/{year}/{year_uuid}/non-candidate-issue-ads/{folder_uuid}/``

    Returns a dict with keys:

    ``year``, ``year_uuid``, ``race_level``, ``office_type``,
    ``candidate_slug``, ``doc_type``, ``folder_uuid``

    Missing / inapplicable keys are set to ``None``.
    """
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")

    marker = f"/tv-profile/{callsign.lower()}/political-files"
    lower_path = path.lower()
    idx = lower_path.find(marker)
    if idx == -1:
        # Try case-insensitive callsign match already baked in lower_path
        # Fall back: look for /political-files/ anywhere
        pf_idx = lower_path.find("/political-files")
        if pf_idx == -1:
            log.debug("parse_path_metadata: /political-files not found in %s", url)
            return {}
        after = path[pf_idx + len("/political-files"):].lstrip("/")
    else:
        after = path[idx + len(marker):].lstrip("/")

    segments = [s for s in after.split("/") if s]

    meta: dict[str, Any] = {
        "year": None,
        "year_uuid": None,
        "race_level": None,
        "office_type": None,
        "candidate_slug": None,
        "doc_type": None,
        "folder_uuid": None,
    }

    if not segments:
        return meta

    # Segment 0: year (integer) or year string
    try:
        meta["year"] = int(segments[0])
    except ValueError:
        meta["year"] = None

    if len(segments) < 2:
        return meta

    # Segment 1: may be a UUID (year_uuid) or directly the race level
    pos = 1
    if _is_uuid(segments[pos]):
        meta["year_uuid"] = segments[pos]
        pos += 1

    if pos >= len(segments):
        return meta

    race_level_candidate = segments[pos].lower()
    if race_level_candidate in _KNOWN_RACE_LEVELS:
        meta["race_level"] = race_level_candidate
        pos += 1
    else:
        # Unknown race level — record it anyway and continue
        meta["race_level"] = race_level_candidate
        pos += 1

    if pos >= len(segments):
        return meta

    # Handle non-candidate-issue-ads: next segment should be folder UUID
    if meta["race_level"] == "non-candidate-issue-ads":
        if _is_uuid(segments[pos]):
            meta["folder_uuid"] = segments[pos]
            meta["doc_type"] = "non-candidate"
        return meta

    # All other levels: next segment is office_type
    office_candidate = segments[pos].lower()
    if office_candidate in _KNOWN_OFFICE_TYPES:
        meta["office_type"] = office_candidate
        pos += 1
    else:
        # Record it even if unrecognised
        meta["office_type"] = office_candidate
        pos += 1

    if pos >= len(segments):
        return meta

    # Next non-UUID segment that is NOT a doc_type keyword = candidate slug
    if segments[pos].lower() not in ("invoices", "contracts", "nab") and not _is_uuid(segments[pos]):
        meta["candidate_slug"] = segments[pos]
        pos += 1

    if pos >= len(segments):
        return meta

    # doc_type: invoices / contracts / nab
    seg_lower = segments[pos].lower()
    if seg_lower in _DOC_TYPE_MAP:
        meta["doc_type"] = seg_lower  # keep raw slug; caller maps via _DOC_TYPE_MAP
        pos += 1
    elif seg_lower == "nab":
        meta["doc_type"] = "nab"
        pos += 1

    if pos >= len(segments):
        return meta

    # folder UUID
    if _is_uuid(segments[pos]):
        meta["folder_uuid"] = segments[pos]

    return meta


def _parse_file_size(size_str: str) -> Optional[int]:
    """Parse a human-readable file size string to bytes.

    Examples: ``"125 KB"`` → ``128_000``, ``"2.3 MB"`` → ``2_411_724``.
    Returns ``None`` if parsing fails.
    """
    if not size_str:
        return None
    size_str = size_str.strip()
    m = re.match(r"([\d,.]+)\s*([a-zA-Z]*)", size_str)
    if not m:
        return None
    number_str = m.group(1).replace(",", "")
    unit = m.group(2).lower() if m.group(2) else "b"
    try:
        number = float(number_str)
    except ValueError:
        return None
    multiplier = _SIZE_UNITS.get(unit, 1)
    return int(number * multiplier)


# ---------------------------------------------------------------------------
# BrowserManager
# ---------------------------------------------------------------------------


class BrowserManager:
    """Manages a Playwright Chromium browser for FCC OPIF crawling.

    Usage::

        async with BrowserManager() as browser:
            docs = await browser.get_political_files("WFAA", 2026)
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._rate_limiter = RateLimiter(
            requests_per_second=settings.fcc.rate_limit_rps,
            burst=1,
        )
        self._playwright = None
        self._browser = None

    async def __aenter__(self) -> "BrowserManager":
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        log.info("Playwright Chromium browser launched")
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._browser:
            await self._browser.close()
            log.info("Browser closed")
        if self._playwright:
            await self._playwright.stop()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_political_files(
        self,
        callsign: str,
        year: int,
        incremental_since: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Discover all political file documents for *callsign* in *year*.

        Opens a new browser page, crawls the full OPIF folder hierarchy for
        the station, closes the page, and returns accumulated document records.

        Args:
            callsign: Station call sign (e.g. ``"WFAA"``).
            year: Campaign year (e.g. ``2026``).
            incremental_since: ISO datetime string; documents older than this
                are skipped during accumulation (best-effort — FCC upload dates
                are not always reliable).

        Returns:
            List of document record dicts suitable for ``db.upsert_document()``.
        """
        assert self._browser is not None, "BrowserManager must be used as async context manager"
        page = await self._browser.new_page()
        try:
            docs = await self._crawl_station(page, callsign, year, incremental_since)
        finally:
            await page.close()
        return docs

    # ------------------------------------------------------------------
    # Internal crawl logic
    # ------------------------------------------------------------------

    async def _crawl_station(
        self,
        page: Page,
        callsign: str,
        year: int,
        incremental_since: Optional[str],
    ) -> list[dict[str, Any]]:
        """Navigate to the station root and fan out into the target year."""
        base_url = f"{_BASE_URL}/tv-profile/{callsign.lower()}/political-files"
        log.info("Crawling %s year=%s — starting at %s", callsign, year, base_url)

        await self._navigate(page, base_url)

        # Collect all hrefs on this page that link into political-files for
        # the correct station, then filter to those matching the target year.
        year_links = await self._extract_subfolder_links(page, callsign, base_url)

        # Filter links that contain the target year segment in the path
        year_str = str(year)
        target_links: list[str] = []
        for link in year_links:
            parsed = urlparse(link)
            # Path looks like /tv-profile/{callsign}/political-files/{year}/...
            # We want paths whose first segment after /political-files/ is the year
            after_pf = _after_political_files(parsed.path, callsign)
            segs = [s for s in after_pf.split("/") if s]
            if segs and segs[0] == year_str:
                target_links.append(link)

        if not target_links:
            # Possibly there are year folders listed directly: try constructing
            # the URL and proceeding
            candidate = f"{base_url}/{year}/"
            log.debug("No year links found on root; trying direct URL %s", candidate)
            target_links = [candidate]

        log.info("Found %d year-folder entry point(s) for %s/%s", len(target_links), callsign, year)

        all_docs: list[dict[str, Any]] = []
        visited: set[str] = {base_url}

        for link in target_links:
            await self._crawl_recursive(
                page, link, callsign, visited, all_docs, incremental_since, depth=0
            )

        log.info(
            "Crawl complete for %s/%s — %d documents discovered", callsign, year, len(all_docs)
        )
        return all_docs

    async def _crawl_recursive(
        self,
        page: Page,
        url: str,
        callsign: str,
        visited: set[str],
        all_docs: list[dict[str, Any]],
        incremental_since: Optional[str],
        depth: int = 0,
    ) -> None:
        """Recursively navigate the OPIF folder hierarchy.

        At each page:
        1. Check for file rows (DataTable rows with download links).
           If found — this is a leaf folder; harvest docs and return.
        2. Otherwise find subfolder links and recurse into each.

        Args:
            page: Active Playwright page.
            url: URL to navigate to.
            callsign: Station call sign for link filtering.
            visited: Set of already-visited URLs (mutated in place).
            all_docs: Accumulator for discovered document records (mutated).
            incremental_since: Optional ISO datetime cutoff.
            depth: Current recursion depth (safety cap at 10).
        """
        if depth > 10:
            log.warning("Max recursion depth reached at %s — skipping", url)
            return

        # Normalise URL (strip trailing slash variations for dedup)
        norm = url.rstrip("/")
        if norm in visited:
            return
        visited.add(norm)
        visited.add(norm + "/")

        log.debug("depth=%d navigating %s", depth, url)
        try:
            await self._navigate(page, url)
        except Exception as exc:
            log.warning("Navigation failed for %s: %s", url, exc)
            return

        meta = parse_path_metadata(url, callsign)

        # Try to extract file rows first — if the page has them, it's a leaf
        file_rows = await self._extract_file_rows(page, meta, callsign)
        if file_rows:
            # Filter by incremental_since if provided
            if incremental_since:
                file_rows = _filter_incremental(file_rows, incremental_since)
            all_docs.extend(file_rows)
            log.debug(
                "Leaf folder at depth=%d: %d files found (%s)", depth, len(file_rows), url
            )
            return

        # Not a leaf — look for subfolder links and recurse
        subfolders = await self._extract_subfolder_links(page, callsign, url)
        if not subfolders:
            log.debug("No files and no subfolders at %s (depth=%d)", url, depth)
            return

        for sub_url in subfolders:
            await self._crawl_recursive(
                page, sub_url, callsign, visited, all_docs, incremental_since, depth + 1
            )

    async def _navigate(self, page: Page, url: str) -> None:
        """Rate-limited page navigation with domcontentloaded fallback.

        Attempts ``networkidle`` wait (30 s); on timeout falls back to
        ``domcontentloaded`` plus a short sleep to let DataTables settle.
        """
        await self._rate_limiter.acquire()
        try:
            await page.goto(url, wait_until="networkidle", timeout=30_000)
        except Exception as primary_exc:
            log.debug(
                "networkidle timeout for %s (%s) — falling back to domcontentloaded",
                url,
                primary_exc,
            )
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                await asyncio.sleep(2)
            except Exception as fallback_exc:
                raise RuntimeError(
                    f"Navigation failed for {url}: {fallback_exc}"
                ) from fallback_exc

    async def _extract_subfolder_links(
        self,
        page: Page,
        callsign: str,
        current_url: str,
    ) -> list[str]:
        """Extract all unique subfolder links from the current page.

        Filters ``<a href>`` elements to those whose href:
        * contains ``/tv-profile/{callsign}/political-files/``
        * does NOT contain ``/api/manager/download/``
        * is not the current URL itself

        Returns deduplicated list of absolute URLs.
        """
        pf_needle = f"/tv-profile/{callsign.lower()}/political-files/"
        current_norm = current_url.rstrip("/")

        try:
            hrefs: list[str] = await page.eval_on_selector_all(
                "a[href]",
                """(anchors) => anchors.map(a => a.href)""",
            )
        except Exception as exc:
            log.debug("eval_on_selector_all failed on %s: %s", current_url, exc)
            return []

        seen: set[str] = set()
        result: list[str] = []
        for href in hrefs:
            if not href:
                continue
            parsed = urlparse(href)
            path_lower = parsed.path.lower()
            if pf_needle not in path_lower:
                continue
            if "/api/manager/download/" in path_lower:
                continue
            norm = href.rstrip("/")
            if norm == current_norm:
                continue
            if norm in seen:
                continue
            seen.add(norm)
            result.append(href)

        return result

    async def _extract_file_rows(
        self,
        page: Page,
        meta: dict[str, Any],
        callsign: str,
    ) -> list[dict[str, Any]]:
        """Extract document rows from a DataTable on the current page.

        Waits up to 5 seconds for a ``<table>`` element; returns ``[]`` if none
        appears. Iterates through DataTable pages (clicking the "next" paginate
        button) and harvests name, size, date, and download href from each row.

        Only returns records if at least one row has a download link matching
        ``/api/manager/download/``.
        """
        try:
            await page.wait_for_selector("table", timeout=5_000)
        except Exception:
            return []

        rows: list[dict[str, Any]] = []

        while True:
            # Collect rows from the current DataTable page
            try:
                raw_rows: list[dict] = await page.eval_on_selector_all(
                    "table tbody tr",
                    """(trs) => trs.map(tr => {
                        const cells = tr.querySelectorAll('td');
                        const link = tr.querySelector('a[href*="/api/manager/download/"]');
                        return {
                            name: cells[0] ? cells[0].innerText.trim() : '',
                            size: cells[1] ? cells[1].innerText.trim() : '',
                            date: cells[2] ? cells[2].innerText.trim() : '',
                            download_href: link ? link.href : ''
                        };
                    })""",
                )
            except Exception as exc:
                log.debug("tbody row extraction failed: %s", exc)
                break

            for raw in raw_rows:
                if not raw.get("download_href"):
                    continue
                rec = self._build_doc_record(raw["download_href"], raw, meta, callsign)
                if rec:
                    rows.append(rec)

            # Check if there is a non-disabled "next" paginate button
            try:
                next_btn = await page.query_selector(
                    ".paginate_button.next:not(.disabled)"
                )
            except Exception:
                next_btn = None

            if not next_btn:
                break

            try:
                await next_btn.click()
                # Brief wait for DataTable to re-render
                await asyncio.sleep(0.8)
            except Exception as exc:
                log.debug("Failed to click next paginate button: %s", exc)
                break

        # Only treat as a file-listing page if we found at least one download link
        if not any(r.get("download_url") for r in rows):
            return []

        return rows

    def _build_doc_record(
        self,
        download_href: str,
        row: dict[str, Any],
        meta: dict[str, Any],
        callsign: str,
    ) -> Optional[dict[str, Any]]:
        """Build a document record dict from a DataTable row.

        Parses the download URL:
            ``https://publicfiles.fcc.gov/api/manager/download/{folder_uuid}/{file_uuid}.pdf``

        to extract ``folder_uuid`` and ``doc_uuid`` (file UUID without ``.pdf``).

        Returns ``None`` if the download URL cannot be parsed.
        """
        parsed = urlparse(download_href)
        parts = [p for p in parsed.path.split("/") if p]

        # Locate the "download" sentinel in the path
        try:
            dl_idx = next(i for i, p in enumerate(parts) if p.lower() == "download")
        except StopIteration:
            log.debug("No 'download' segment in href %s", download_href)
            return None

        if dl_idx + 2 >= len(parts):
            log.debug("Not enough path segments after 'download' in %s", download_href)
            return None

        folder_uuid_from_url = parts[dl_idx + 1]
        file_uuid_raw = parts[dl_idx + 2]
        file_uuid = re.sub(r"\.pdf$", "", file_uuid_raw, flags=re.IGNORECASE)

        if not _is_uuid(folder_uuid_from_url) or not _is_uuid(file_uuid):
            log.debug(
                "UUIDs look invalid in %s (folder=%s file=%s)",
                download_href,
                folder_uuid_from_url,
                file_uuid,
            )
            return None

        # Prefer folder_uuid from URL; fall back to meta if identical info
        folder_uuid = folder_uuid_from_url

        file_name: str = row.get("name", "") or ""
        file_extension: Optional[str] = None
        if "." in file_name:
            file_extension = file_name.rsplit(".", 1)[-1].lower()

        file_size = _parse_file_size(row.get("size", ""))
        create_ts: Optional[str] = row.get("date") or None

        # Map raw doc_type slug → canonical document_type string
        raw_doc_type = meta.get("doc_type") or ""
        document_type: Optional[str] = _DOC_TYPE_MAP.get(raw_doc_type)

        return {
            "doc_uuid": file_uuid,
            "folder_uuid": folder_uuid,
            "folder_id": None,
            "file_manager_id": None,
            "callsign": callsign.upper(),
            "year": meta.get("year"),
            "race_level": meta.get("race_level"),
            "office_type": meta.get("office_type"),
            "candidate_name": meta.get("candidate_slug"),
            "doc_type": raw_doc_type,
            "document_type": document_type,
            "political_file_type": "PA",
            "file_name": file_name or None,
            "file_extension": file_extension,
            "file_size": file_size,
            "create_ts": create_ts,
            "download_url": (
                f"{_BASE_URL}/api/manager/download/{folder_uuid}/{file_uuid}.pdf"
            ),
            # operator_name, dma_name, dma_rank, campaign_year are injected
            # by the crawler after this method returns
            "operator_name": None,
            "dma_name": None,
            "dma_rank": None,
            "campaign_year": None,
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _after_political_files(path: str, callsign: str) -> str:
    """Return the path portion after ``/political-files/`` for a given callsign."""
    marker = f"/tv-profile/{callsign.lower()}/political-files"
    lower = path.lower()
    idx = lower.find(marker)
    if idx == -1:
        pf = "/political-files"
        pf_idx = lower.find(pf)
        if pf_idx == -1:
            return ""
        return path[pf_idx + len(pf):]
    return path[idx + len(marker):]


def _filter_incremental(
    docs: list[dict[str, Any]], incremental_since: str
) -> list[dict[str, Any]]:
    """Best-effort filter: drop docs whose create_ts precedes incremental_since.

    If create_ts is absent or unparseable the document is kept (safe default).
    """
    from datetime import datetime

    try:
        cutoff = datetime.fromisoformat(incremental_since.replace("Z", "+00:00"))
    except Exception:
        return docs

    result = []
    for doc in docs:
        ts = doc.get("create_ts")
        if not ts:
            result.append(doc)
            continue
        try:
            doc_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if doc_dt >= cutoff:
                result.append(doc)
        except Exception:
            result.append(doc)

    return result
