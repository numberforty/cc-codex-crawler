"""Utility functions for the CC Codex crawler."""

from typing import List


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
