"""Utility functions for the CC Codex crawler."""

import os
from typing import Dict, List, Set

# Timeout in seconds for all network requests performed by this module. The
# value can be overridden by setting the ``REQUEST_TIMEOUT`` environment
# variable.
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", 30))


def extension_from_url(url: str) -> str:
    """Return the file extension from ``url`` if present.

    The function strips query parameters and fragments, then checks for
    common compound extensions such as ``.tar.gz``. If no extension can be
    determined an empty string is returned.
    """

    import os
    from urllib.parse import urlparse

    path = urlparse(url).path
    if not path or path.endswith("/"):
        return ""

    filename = os.path.basename(path)

    for ext in (".tar.gz", ".tar.bz2", ".tar.xz"):
        if filename.endswith(ext):
            return ext

    _, ext = os.path.splitext(filename)
    return ext




def list_warc_keys(
    s3_client,
    bucket: str,
    prefix: str,
    max_keys: int,
) -> List[str]:
    """Return up to ``max_keys`` WARC file keys from an S3 prefix.

    The function iterates over an S3 paginator and collects object keys
    that end with ``".warc.gz"``. Only ``max_keys`` keys are returned
    at most.

    Parameters
    ----------
    s3_client : boto3.client
        Configured boto3 S3 client.
    bucket : str
        Name of the S3 bucket to query.
    prefix : str
        Prefix under which to search for WARC files.
    max_keys : int
        Maximum number of keys to return.
    """
    keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj.get("Key")
            if key and key.endswith(".warc.gz"):
                keys.append(key)
                if len(keys) >= max_keys:
                    return keys

    return keys


def stream_and_extract(
    s3_client,
    bucket: str,
    key: str,
    target_exts,
    rate_limit: float,
    user_agent: str,
) -> None:
    """Stream a gzipped WARC file from S3 and yield matching records.

    The function downloads the specified WARC file using a pre-signed URL
    and ``warcio``'s :class:`~warcio.archiveiterator.ArchiveIterator` to
    iterate over the contained records. Only response records whose URL path
    ends with one of ``target_exts`` are yielded.

    Network errors are retried with exponential backoff (up to three
    attempts) and the function sleeps ``rate_limit`` seconds after each S3
    GET request.

    Parameters
    ----------
    s3_client : boto3.client
        Configured boto3 S3 client.
    bucket : str
        Name of the S3 bucket containing the WARC file.
    key : str
        Object key of the WARC file in the bucket.
    target_exts : Iterable[str]
        Collection of extensions to match against the URL path.
    rate_limit : float
        Number of seconds to sleep between S3 GET requests.
    user_agent : str
        Custom ``User-Agent`` header for the GET request.
    """

    import gzip
    import time
    from urllib.parse import urlparse

    import requests
    from botocore.exceptions import BotoCoreError, ClientError
    from warcio.archiveiterator import ArchiveIterator

    attempt = 0
    backoff = 1.0

    while attempt < 3:
        try:
            url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=3600,
            )

            headers = {"User-Agent": user_agent}
            with requests.get(
                url, stream=True, headers=headers, timeout=REQUEST_TIMEOUT
            ) as resp:
                resp.raise_for_status()

                with gzip.GzipFile(fileobj=resp.raw) as gz:
                    for record in ArchiveIterator(gz):
                        if record.rec_type != "response":
                            continue

                        uri = record.rec_headers.get_header("WARC-Target-URI")
                        if not uri:
                            continue

                        path = urlparse(uri).path
                        if any(path.endswith(ext) for ext in target_exts):
                            yield uri, record.content_stream().read()

            time.sleep(rate_limit)
            break
        except (BotoCoreError, ClientError, requests.RequestException):
            attempt += 1
            if attempt >= 3:
                raise
            time.sleep(backoff)
            backoff *= 2


def save_file(data: bytes, url: str, output_dir: str) -> str:
    """Save ``data`` to ``output_dir`` using a name derived from ``url``.

    The function strips query parameters and fragments from the URL and
    sanitizes the resulting path to create a safe filename. If the derived
    filename already exists in ``output_dir``, a numeric suffix is appended
    to avoid overwriting existing files.

    Parameters
    ----------
    data : bytes
        Binary data to write to disk.
    url : str
        Source URL of the data. The path portion (without query or fragment)
        is used to derive the filename.
    output_dir : str
        Directory in which to save the file.

    Returns
    -------
    str
        The full path of the written file.
    """

    import os
    import re
    from urllib.parse import urlparse

    parsed = urlparse(url)
    base = os.path.basename(parsed.path)

    # Fallback to a generic name when the path ends with a slash
    if not base:
        base = "file"

    # Sanitize filename to avoid directory traversal or invalid characters
    base = re.sub(r"[^a-zA-Z0-9._-]", "_", base)

    name, ext = os.path.splitext(base)
    candidate = base
    counter = 1
    while os.path.exists(os.path.join(output_dir, candidate)):
        candidate = f"{name}_{counter}{ext}"
        counter += 1

    file_path = os.path.join(output_dir, candidate)
    with open(file_path, "wb") as fh:
        fh.write(data)

    return file_path


def load_state(path: str) -> Set[str]:
    """Load completed WARC keys from ``path`` if it exists."""
    import json
    import os

    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            data = data.get("completed", [])
    except Exception:
        return set()
    return {str(x) for x in data}


def save_state(path: str, completed: Set[str]) -> None:
    """Write ``completed`` WARC keys to ``path``."""
    import json

    tmp = {"completed": sorted(completed)}
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(tmp, fh, indent=2)


def list_warc_keys_http(prefix: str, max_keys: int) -> List[str]:
    """Return up to ``max_keys`` WARC file keys using HTTPS listing.

    The function downloads ``warc.paths.gz`` from the Common Crawl bucket
    and extracts the first ``max_keys`` entries.
    """

    import gzip
    import io
    import time

    import requests

    base_url = "https://data.commoncrawl.org"
    norm = prefix.strip("/")
    appended_latest = False
    if "CC-MAIN" not in norm:
        norm = f"{norm}/CC-MAIN-LATEST"
        appended_latest = True
    url = f"{base_url}/{norm}/warc.paths.gz"

    attempt = 0
    backoff = 1.0

    while attempt < 3:
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 404 and appended_latest:
                raise RuntimeError(
                    "CC-MAIN-LATEST not found. Set CRAWL_PREFIX to a specific crawl, "
                    "e.g., 'crawl-data/CC-MAIN-2024-22'."
                )
            resp.raise_for_status()

            keys: List[str] = []
            with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
                for line in gz:
                    key = line.decode("utf-8").strip()
                    if key.endswith(".warc.gz"):
                        keys.append(f"crawl-data/{key}")
                        if len(keys) >= max_keys:
                            break
            return keys
        except requests.RequestException:
            attempt += 1
            if attempt >= 3:
                raise
            time.sleep(backoff)
            backoff *= 2

    return []


def stream_and_extract_http(
    key: str,
    target_exts,
    rate_limit: float,
    user_agent: str,
) -> None:
    """Stream a gzipped WARC file via HTTPS and yield matching records."""

    import gzip
    import time
    from urllib.parse import urlparse

    import requests
    from warcio.archiveiterator import ArchiveIterator

    url = f"https://data.commoncrawl.org/{key}"

    attempt = 0
    backoff = 1.0

    while attempt < 3:
        try:
            headers = {"User-Agent": user_agent}
            with requests.get(
                url, stream=True, headers=headers, timeout=REQUEST_TIMEOUT
            ) as resp:
                resp.raise_for_status()
                with gzip.GzipFile(fileobj=resp.raw) as gz:
                    for record in ArchiveIterator(gz):
                        if record.rec_type != "response":
                            continue
                        uri = record.rec_headers.get_header("WARC-Target-URI")
                        if not uri:
                            continue
                        path = urlparse(uri).path
                        if any(path.endswith(ext) for ext in target_exts):
                            yield uri, record.content_stream().read()
            time.sleep(rate_limit)
            break
        except requests.RequestException:
            attempt += 1
            if attempt >= 3:
                raise
            time.sleep(backoff)
            backoff *= 2
