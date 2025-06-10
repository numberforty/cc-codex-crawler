"""Utility functions for the CC Codex crawler."""

from typing import Dict, List, Set


def fetch_and_parse_robots(base_url: str, user_agent: str):
    """Retrieve and parse ``/robots.txt`` for ``base_url``.

    The function downloads the robots.txt file using ``requests`` with
    a custom ``User-Agent`` header. Network errors are retried with
    exponential backoff (up to three attempts). The contents are parsed
    using :class:`urllib.robotparser.RobotFileParser`.

    Parameters
    ----------
    base_url : str
        Base URL from which to fetch ``/robots.txt``. The scheme must be
        included.
    user_agent : str
        ``User-Agent`` string used for the HTTP request.

    Returns
    -------
    urllib.robotparser.RobotFileParser
        Parsed robots.txt object.
    """

    import time
    from urllib.parse import urljoin
    from urllib.robotparser import RobotFileParser

    import requests

    robots_url = urljoin(base_url, "/robots.txt")
    attempt = 0
    backoff = 1.0

    while attempt < 3:
        try:
            headers = {"User-Agent": user_agent}
            resp = requests.get(robots_url, headers=headers, timeout=10)
            resp.raise_for_status()

            parser = RobotFileParser()
            parser.set_url(robots_url)
            parser.parse(resp.text.splitlines())
            return parser
        except requests.RequestException:
            attempt += 1
            if attempt >= 3:
                raise
            time.sleep(backoff)
            backoff *= 2


class DomainRateLimiter:
    """Per-domain rate limiter using ``crawl-delay`` directives."""

    def __init__(self, default_delay: float = 1.0, user_agent: str = "*") -> None:
        """Create a new :class:`DomainRateLimiter` instance.

        Parameters
        ----------
        default_delay : float
            Fallback delay in seconds when ``robots.txt`` does not specify one.
        user_agent : str
            ``User-Agent`` string for ``robots.txt`` requests.
        """

        self.default_delay = default_delay
        self.user_agent = user_agent
        self._last_access: Dict[str, float] = {}
        self._delays: Dict[str, float] = {}

    def _get_delay(self, url: str) -> float:
        """Return the crawl delay for ``url``.

        The ``robots.txt`` of the host is fetched on first access. Subsequent
        calls reuse the cached value. Any network error results in
        ``default_delay`` being used.
        """

        from urllib.parse import urlparse

        parsed = urlparse(url)
        host = parsed.netloc
        if host in self._delays:
            return self._delays[host]

        base_url = f"{parsed.scheme}://{host}"
        try:
            parser = fetch_and_parse_robots(base_url, self.user_agent)
            delay = parser.crawl_delay(self.user_agent)
            if delay is None:
                delay = self.default_delay
        except Exception:
            delay = self.default_delay

        self._delays[host] = delay
        return delay

    def wait(self, url: str) -> None:
        """Sleep if the host was accessed too recently."""

        import time
        from urllib.parse import urlparse

        parsed = urlparse(url)
        host = parsed.netloc
        if not host:
            return

        delay = self._get_delay(url)
        last = self._last_access.get(host)
        if last is not None:
            elapsed = time.time() - last
            if elapsed < delay:
                time.sleep(delay - elapsed)

        self._last_access[host] = time.time()


def list_warc_keys(s3_client, bucket: str, prefix: str, max_keys: int) -> List[str]:
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
            with requests.get(url, stream=True, headers=headers) as resp:
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
