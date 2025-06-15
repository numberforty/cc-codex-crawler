"""Utilities for parsing JSON lines with simple repair logic."""
from __future__ import annotations

import logging
from typing import Any, Optional

import orjson

logger = logging.getLogger(__name__)


def parse_json_line(line: str) -> Optional[dict[str, Any]]:
    """Parse a JSON object from ``line`` with basic error recovery."""
    try:
        start = line.index("{")
    except ValueError:
        return None

    js = line[start:].strip()
    try:
        return orjson.loads(js)
    except orjson.JSONDecodeError:
        close = js.rfind("}")
        if close != -1:
            try:
                return orjson.loads(js[: close + 1])
            except orjson.JSONDecodeError:
                pass
        logger.debug("Failed to parse JSON line: %s", line)
        return None
