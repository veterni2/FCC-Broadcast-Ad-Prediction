"""FCC OPIF API client for political file discovery.

Wraps the FCC search API and download endpoints with rate limiting.
Uses httpx for async HTTP with automatic retry on transient errors.

API Endpoints:
- Search: GET https://publicfiles.fcc.gov/api/service/political/filing/search
- Download: GET https://publicfiles.fcc.gov/api/manager/download

The search API supports filtering by:
- political_file_type: 'PA' (political advertisement)
- source_service_code: 'TV'
- campaign_year: e.g. '2026'
- callsign: station call sign
"""

from __future__ import annotations

from typing import Any, Optional

import httpx

from ..config.settings import get_settings
from ..utils.logging import get_logger
from ..utils.rate_limiter import RateLimiter

log = get_logger("opif_client")


class OPIFClient:
    """Async client for the FCC OPIF political files API.

    Usage:
        async with OPIFClient() as client:
            results = await client.search_political_files(
                callsign="WFAA",
                campaign_year=2026,
            )
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._base_url = settings.fcc.base_url
        self._search_url = settings.fcc.search_url
        self._rate_limiter = RateLimiter(
            requests_per_second=settings.fcc.rate_limit_rps,
        )
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> OPIFClient:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "User-Agent": "FCC-Ad-Tracker/0.1.0 (political-file-research)",
                "Accept": "application/json",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def search_political_files(
        self,
        callsign: Optional[str] = None,
        campaign_year: Optional[int] = None,
        political_file_type: str = "PA",
        source_service_code: str = "TV",
        page: int = 0,
        page_size: int = 100,
    ) -> dict[str, Any]:
        """Search for political file documents on OPIF.

        Args:
            callsign: Filter by station call sign.
            campaign_year: Filter by campaign year.
            political_file_type: File type filter (default 'PA').
            source_service_code: Service code filter (default 'TV').
            page: Page number for pagination.
            page_size: Results per page.

        Returns:
            API response dict with results and pagination info.
        """
        assert self._client is not None, "Client not initialized. Use 'async with'."

        await self._rate_limiter.acquire()

        # Build filter parameter
        filters: list[dict[str, str]] = []
        if political_file_type:
            filters.append({"political_file_type": political_file_type})
        if source_service_code:
            filters.append({"source_service_code": source_service_code})
        if campaign_year:
            filters.append({"campaign_year": str(campaign_year)})
        if callsign:
            filters.append({"callsign": callsign.upper()})

        params: dict[str, Any] = {
            "page": page,
            "size": page_size,
        }
        if filters:
            import json
            params["f"] = json.dumps(filters)

        log.debug(f"Searching OPIF: {params}")

        response = await self._client.get(self._search_url, params=params)
        response.raise_for_status()

        return response.json()

    async def download_file(
        self,
        folder_id: str,
        file_manager_id: str,
    ) -> bytes:
        """Download a political file document (PDF) from OPIF.

        Args:
            folder_id: The folder ID containing the file.
            file_manager_id: The file manager ID of the document.

        Returns:
            Raw bytes of the downloaded file.
        """
        assert self._client is not None, "Client not initialized. Use 'async with'."

        await self._rate_limiter.acquire()

        url = f"{self._base_url}/api/manager/download"
        params = {
            "folder_id": folder_id,
            "file_manager_id": file_manager_id,
        }

        log.debug(f"Downloading file: folder_id={folder_id}, file_manager_id={file_manager_id}")

        response = await self._client.get(url, params=params)
        response.raise_for_status()

        return response.content

    @property
    def rate_limiter_stats(self) -> dict[str, float]:
        """Return rate limiter statistics."""
        return self._rate_limiter.stats
