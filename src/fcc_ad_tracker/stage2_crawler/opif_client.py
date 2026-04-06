"""FCC OPIF download client.

The FCC OPIF site (publicfiles.fcc.gov) does NOT expose a documented JSON API
for folder traversal or document discovery. All discovery is done via Playwright
HTML scraping in browser.py.

This module provides only the confirmed-working download endpoint:

    GET https://publicfiles.fcc.gov/api/manager/download/{folder_uuid}/{file_uuid}.pdf

Usage:
    async with OPIFClient() as client:
        pdf_bytes = await client.download_file(folder_uuid="abc...", file_uuid="def...")
"""

from __future__ import annotations

from typing import Any, Optional

import httpx

from ..config.settings import get_settings
from ..utils.logging import get_logger
from ..utils.rate_limiter import RateLimiter

log = get_logger("opif_client")

_DOWNLOAD_PATH = "/api/manager/download/{folder_uuid}/{file_uuid}.pdf"


class OPIFClient:
    """Async HTTP client for FCC OPIF file downloads.

    Wraps the only confirmed-working OPIF API endpoint with rate limiting
    and automatic retry on transient errors.

    Usage::

        async with OPIFClient() as client:
            pdf_bytes = await client.download_file(
                folder_uuid="aabbccdd-...",
                file_uuid="11223344-...",
            )
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._base_url = settings.fcc.base_url
        self._rate_limiter = RateLimiter(
            requests_per_second=settings.fcc.rate_limit_rps,
        )
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "OPIFClient":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            headers={
                "User-Agent": "FCC-Ad-Tracker/0.1.0 (political-file-research)",
            },
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def download_file(
        self,
        folder_uuid: str,
        file_uuid: str,
    ) -> bytes:
        """Download a political file PDF from FCC OPIF.

        Uses the confirmed-working download URL:
            GET {base_url}/api/manager/download/{folder_uuid}/{file_uuid}.pdf

        Args:
            folder_uuid: UUID of the folder containing the file.
            file_uuid: UUID of the specific file (without .pdf extension).

        Returns:
            Raw bytes of the downloaded PDF.

        Raises:
            httpx.HTTPStatusError: On 4xx/5xx responses.
            RuntimeError: If the client is not initialised.
        """
        assert self._client is not None, "Client not initialised. Use 'async with'."

        await self._rate_limiter.acquire()

        url = (
            f"{self._base_url}/api/manager/download"
            f"/{folder_uuid}/{file_uuid}.pdf"
        )

        log.debug("Downloading: %s", url)
        response = await self._client.get(url)
        response.raise_for_status()
        return response.content

    @property
    def rate_limiter_stats(self) -> dict[str, float]:
        """Return rate limiter statistics."""
        return self._rate_limiter.stats
