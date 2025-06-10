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
