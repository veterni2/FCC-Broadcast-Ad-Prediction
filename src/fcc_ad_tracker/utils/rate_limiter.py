"""Rate limiter for HTTP requests to the FCC OPIF API.

Implements a token-bucket style rate limiter that can be used
with both sync and async code paths.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

from .logging import get_logger

log = get_logger("rate_limiter")


class RateLimiter:
    """Token-bucket rate limiter.

    Args:
        requests_per_second: Maximum sustained request rate.
        burst: Maximum burst size (defaults to 1 for strict limiting).
    """

    def __init__(self, requests_per_second: float = 1.5, burst: int = 1) -> None:
        self._rps = requests_per_second
        self._interval = 1.0 / requests_per_second
        self._burst = burst
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._total_waits = 0
        self._total_wait_time = 0.0

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rps)
        self._last_refill = now

    async def acquire(self) -> None:
        """Acquire a rate limit token, waiting if necessary."""
        async with self._lock:
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            # Need to wait
            deficit = 1.0 - self._tokens
            wait_time = deficit / self._rps
            self._total_waits += 1
            self._total_wait_time += wait_time

            if self._total_waits % 50 == 0:
                log.debug(
                    f"Rate limiter: {self._total_waits} waits, "
                    f"{self._total_wait_time:.1f}s total wait time"
                )

            await asyncio.sleep(wait_time)
            self._refill()
            self._tokens -= 1.0

    def acquire_sync(self) -> None:
        """Synchronous version of acquire for non-async code paths."""
        self._refill()

        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return

        deficit = 1.0 - self._tokens
        wait_time = deficit / self._rps
        self._total_waits += 1
        self._total_wait_time += wait_time
        time.sleep(wait_time)
        self._refill()
        self._tokens -= 1.0

    @property
    def stats(self) -> dict[str, float]:
        """Return rate limiter statistics."""
        return {
            "total_waits": self._total_waits,
            "total_wait_time_seconds": round(self._total_wait_time, 2),
            "requests_per_second": self._rps,
        }
