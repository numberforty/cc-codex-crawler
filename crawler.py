import io
import json
import os
import time

import requests
from warcio.archiveiterator import ArchiveIterator

from utils import (
    list_warc_keys,
    list_warc_keys_http,
    load_state,
    save_file,
    save_state,
    stream_and_extract,
    stream_and_extract_http,
)

# Configuration from environment variables with sensible defaults
S3_BUCKET = os.getenv("S3_BUCKET", "commoncrawl")
CRAWL_PREFIX = os.getenv("CRAWL_PREFIX", "crawl-data")
DEFAULT_CRAWL = "CC-MAIN-2024-22"
DEFAULT_EXT = ".mp3"

# Parse target extensions into a Python set
_default_exts = ".py,.js,.java,.cpp,.go"
TARGET_EXTENSIONS = {
    ext.strip()
    for ext in os.getenv("TARGET_EXTENSIONS", _default_exts).split(",")
    if ext.strip()
}

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
MAX_WARCS = int(os.getenv("MAX_WARCS", "10"))
SAMPLES_PER_EXT = int(os.getenv("SAMPLES_PER_EXT", "1000"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "1.0"))
USER_AGENT = os.getenv("USER_AGENT", "cc-codex-crawler/1.0")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
STATE_FILE = os.getenv("STATE_FILE", "crawler_state.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def main() -> None:
    """Entry point for the CLI crawler.

    The function parses command line arguments, configures logging and then
    iterates over WARC files from the Common Crawl. Matching records are saved
    to ``OUTPUT_DIR`` using :func:`save_file`.
    """

    import argparse
    import logging
    import threading
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import boto3

    parser = argparse.ArgumentParser(description="CC Codex crawler")
    parser.add_argument(
        "--warcs",
        type=int,
        default=MAX_WARCS,
        help="Number of WARC files to process",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=SAMPLES_PER_EXT,
        help="Number of files to save per extension",
    )
    parser.add_argument(
        "--rate-limit",
        dest="rate_limit",
        type=float,
        default=RATE_LIMIT_SECONDS,
        help="Seconds to sleep between requests",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help="Maximum number of concurrent workers",
    )
    parser.add_argument(
        "--mode",
        choices=["aws", "http", "index"],
        default="aws",
        help="Access S3 via the AWS SDK, direct HTTPS or the CDX index",
    )
    parser.add_argument(
        "--extensions",
        dest="extensions",
        help="Extension for index mode (e.g., .mp3)",
    )

    args = parser.parse_args()

    # Configure logging: console handler and file handler
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler("crawler.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if args.mode == "index":
        crawl = os.getenv("CRAWL_PREFIX", DEFAULT_CRAWL)
        ext = args.extensions or DEFAULT_EXT
        idx_url = f"http://index.commoncrawl.org/{crawl}-index?url=*{ext}&output=json"

        attempt = 0
        backoff = 1.0
        while attempt < 3:
            try:
                resp = requests.get(idx_url, timeout=10)
                resp.raise_for_status()
                break
            except (
                requests.RequestException
            ) as exc:  # pragma: no cover - network failure
                attempt += 1
                if attempt >= 3:
                    logger.warning("Failed to query index: %s", exc)
                    return
                time.sleep(backoff)
                backoff *= 2

        count = 0
        for line in resp.text.splitlines():
            if count >= args.samples:
                break
            entry = json.loads(line)
            filename = entry["filename"]
            offset = int(entry["offset"])
            length = int(entry["length"])
            url = entry["url"]

            warc_url = f"https://data.commoncrawl.org/{filename}"
            headers = {"Range": f"bytes={offset}-{offset + length - 1}"}

            attempt2 = 0
            backoff2 = 1.0
            while attempt2 < 3:
                try:
                    r = requests.get(warc_url, headers=headers, timeout=10)
                    r.raise_for_status()
                    break
                except (
                    requests.RequestException
                ) as exc:  # pragma: no cover - network failure
                    attempt2 += 1
                    if attempt2 >= 3:
                        logger.warning("Failed to fetch range: %s", exc)
                        r = None
                        break
                    time.sleep(backoff2)
                    backoff2 *= 2

            if not r:
                continue

            stream = io.BytesIO(r.content)
            for rec in ArchiveIterator(stream, arc2warc=True):
                content_type = rec.http_headers.get_header("Content-Type", "")
                if (
                    rec.rec_type == "response"
                    and content_type.startswith("audio/")
                    and url.endswith(ext)
                ):
                    data = rec.content_stream().read()
                    try:
                        save_file(data, url, OUTPUT_DIR)
                        count += 1
                        logger.info("Saved %s (%d/%d)", url, count, args.samples)
                    except Exception as exc:  # pragma: no cover - disk failure
                        logger.warning("Failed to save %s: %s", url, exc)
                    break
        return

    use_http = args.mode == "http"
    s3_client = None
    if args.mode == "aws":
        s3_client = boto3.client("s3")

    try:
        if use_http:
            prefix = os.getenv("CRAWL_PREFIX", CRAWL_PREFIX).strip("/")
            warc_keys = list_warc_keys_http(prefix, args.warcs)
        else:
            warc_keys = list_warc_keys(s3_client, S3_BUCKET, CRAWL_PREFIX, args.warcs)
    except Exception as exc:  # pragma: no cover - network failure
        if not use_http:
            logger.warning("Falling back to HTTP listing due to: %s", exc)
            try:
                prefix = os.getenv("CRAWL_PREFIX", CRAWL_PREFIX).strip("/")
                warc_keys = list_warc_keys_http(prefix, args.warcs)
                use_http = True
            except Exception as exc2:
                logger.warning("Failed to list WARC files: %s", exc2)
                return
        else:
            logger.warning("Failed to list WARC files: %s", exc)
            return

    completed_keys = load_state(STATE_FILE)
    if completed_keys:
        warc_keys = [k for k in warc_keys if k not in completed_keys]

    if not warc_keys:
        logger.info("No new WARC files to process")
        return

    saved_counts = defaultdict(int)
    lock = threading.Lock()
    state_lock = threading.Lock()

    def process_warc(key: str) -> None:
        logger.info("Processing %s", key)
        try:
            if use_http:
                iterator = stream_and_extract_http(
                    key,
                    TARGET_EXTENSIONS,
                    args.rate_limit,
                    USER_AGENT,
                )
            else:
                iterator = stream_and_extract(
                    s3_client,
                    S3_BUCKET,
                    key,
                    TARGET_EXTENSIONS,
                    args.rate_limit,
                    USER_AGENT,
                )
            for url, data in iterator:
                ext = next(
                    (e for e in TARGET_EXTENSIONS if url.endswith(e)),
                    None,
                )
                if not ext:
                    continue
                with lock:
                    if saved_counts[ext] >= args.samples:
                        continue
                try:
                    path = save_file(data, url, OUTPUT_DIR)
                    with lock:
                        saved_counts[ext] += 1
                    logger.info("Saved %s (%s)", path, ext)
                except Exception as exc:  # pragma: no cover - disk failure
                    logger.warning("Failed to save %s: %s", url, exc)
        except Exception as exc:  # pragma: no cover - streaming failure
            logger.warning("Error processing %s: %s", key, exc)
        finally:
            with state_lock:
                completed_keys.add(key)
                save_state(STATE_FILE, completed_keys)

    max_workers = min(args.workers, len(warc_keys)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_warc, k) for k in warc_keys]
        for fut in as_completed(futures):
            fut.result()


if __name__ == "__main__":
    main()
