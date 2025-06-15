# Streaming processor for Common Crawl index files
from __future__ import annotations

import asyncio
import os
import logging
from typing import AsyncIterator, Iterable, Optional

from aiohttp import ClientSession, ClientTimeout
import aiohttp
import aiofiles
import zlib

from config import Config, load_config
from json_utils import parse_json_line
from http_utils import BACKOFF_DELAYS


logger = logging.getLogger(__name__)


async def _iter_gzip_lines_from_response(
    resp: aiohttp.ClientResponse,
) -> AsyncIterator[str]:
    """Yield decoded lines from a gzip-compressed HTTP response."""
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    buf = b""
    async for chunk in resp.content.iter_chunked(16384):
        part = decompressor.decompress(chunk)
        if part:
            buf += part
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                yield line.decode("utf-8")
    buf += decompressor.flush()
    if buf:
        yield buf.decode("utf-8")


async def _iter_gzip_lines_from_file(path: str) -> AsyncIterator[str]:
    """Yield lines from a local gzip compressed file."""
    async with aiofiles.open(path, "rb") as fh:
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        buf = b""
        while True:
            chunk = await fh.read(16384)
            if not chunk:
                break
            part = decompressor.decompress(chunk)
            if part:
                buf += part
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    yield line.decode("utf-8")
        buf += decompressor.flush()
        if buf:
            yield buf.decode("utf-8")


async def fetch_lines(
    session: ClientSession, url: str, config: Config
) -> AsyncIterator[str]:
    """Fetch ``url`` and yield decompressed lines with retry and backoff."""
    if os.path.exists(url):
        async for line in _iter_gzip_lines_from_file(url):
            yield line
        return

    retries = 0
    backoff_idx = 0
    while True:
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    logger.error("URL not found: %s", url)
                    return
                if resp.status == 503:
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=resp.status
                    )
                resp.raise_for_status()
                async for line in _iter_gzip_lines_from_response(resp):
                    yield line
                return
        except Exception as exc:  # noqa: BLE001
            if retries >= config.max_retries:
                logger.error("Failed to fetch %s: %s", url, exc)
                raise
            delay = BACKOFF_DELAYS[min(backoff_idx, len(BACKOFF_DELAYS) - 1)]
            logger.warning("Retry %s for %s in %ss", retries + 1, url, delay)
            await asyncio.sleep(delay)
            retries += 1
            backoff_idx += 1


async def producer(queue: asyncio.Queue[str], urls: Iterable[str]) -> None:
    for url in urls:
        await queue.put(url)
    await queue.put(None)  # sentinel


async def consumer(
    queue: asyncio.Queue[Optional[str]],
    session: ClientSession,
    config: Config,
) -> None:
    while True:
        url = await queue.get()
        if url is None:
            queue.task_done()
            break
        logger.info("Processing %s", url)
        count = 0
        try:
            async for line in fetch_lines(session, url, config):
                record = parse_json_line(line)
                if not record:
                    continue
                logger.info(
                    "URL: %s TS:%s", record.get("url"), record.get("timestamp")
                )
                count += 1
                if count >= config.record_limit:
                    logger.info("Record limit reached for %s", url)
                    break
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed processing %s: %s", url, exc)
        finally:
            queue.task_done()


async def run(config: Config) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    queue: asyncio.Queue[Optional[str]] = asyncio.Queue()
    async with ClientSession(timeout=ClientTimeout(total=config.timeout)) as session:
        cons = [
            asyncio.create_task(consumer(queue, session, config))
            for _ in range(config.max_concurrent_tasks)
        ]
        await producer(queue, config.urls)
        await queue.join()
        for _ in cons:
            await queue.put(None)
        await queue.join()
        for c in cons:
            await c


def main(config_path: str) -> None:
    config = load_config(config_path)
    asyncio.run(run(config))


if __name__ == "__main__":  # pragma: no cover
    import sys

    if len(sys.argv) != 2:
        print("Usage: python streaming_processor.py config.yaml")
        raise SystemExit(1)
    main(sys.argv[1])
