"""Utility functions for the fetcher."""

from __future__ import annotations

import os
import re
from urllib.parse import urlparse

# Timeout in seconds for network requests. Can be overridden via the
# ``REQUEST_TIMEOUT`` environment variable.
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", 30))


def extension_from_url(url: str) -> str:
    """Return the file extension from ``url`` if present."""
    path = urlparse(url).path
    if not path or path.endswith("/"):
        return ""
    filename = os.path.basename(path)
    for ext in (".tar.gz", ".tar.bz2", ".tar.xz"):
        if filename.endswith(ext):
            return ext
    _, ext = os.path.splitext(filename)
    return ext


def save_file(data: bytes, url: str, output_dir: str) -> str:
    """Save ``data`` to ``output_dir`` under a name derived from ``url``."""
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.basename(urlparse(url).path) or "file"
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
