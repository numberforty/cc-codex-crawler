"""HTTP helpers with exponential backoff."""
from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from aiohttp import ClientResponse, ClientSession, ClientTimeout
import aiohttp

BACKOFF_DELAYS = [30, 120, 600, 1800]
logger = logging.getLogger(__name__)


async def open_url(session: ClientSession, url: str, timeout: int) -> ClientResponse:
    """Open ``url`` and return the response object."""
    return await session.get(url, timeout=ClientTimeout(total=timeout))


async def fetch_with_backoff(
    session: ClientSession, url: str, max_retries: int, timeout: int
) -> AsyncIterator[ClientResponse]:
    retries = 0
    backoff_idx = 0
    while True:
        try:
            resp = await open_url(session, url, timeout)
            if resp.status == 503:
                raise aiohttp.ClientResponseError(
                    resp.request_info, resp.history, status=resp.status
                )
            resp.raise_for_status()
            yield resp
            return
        except Exception as exc:  # noqa: BLE001
            if retries >= max_retries:
                logger.error("Giving up on %s: %s", url, exc)
                raise
            delay = BACKOFF_DELAYS[min(backoff_idx, len(BACKOFF_DELAYS) - 1)]
            logger.warning("Retry %s for %s in %ss", retries + 1, url, delay)
            await asyncio.sleep(delay)
            retries += 1
            backoff_idx += 1
