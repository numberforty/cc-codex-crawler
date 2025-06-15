"""Configuration loading for the streaming processor."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

import yaml


@dataclass
class Config:
    urls: List[str] = field(default_factory=list)
    max_concurrent_tasks: int = 10
    max_retries: int = 4
    timeout: int = 10
    record_limit: int = 1000


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return Config(
        urls=data.get("urls", []),
        max_concurrent_tasks=int(data.get("max_concurrent_tasks", 10)),
        max_retries=int(data.get("max_retries", 4)),
        timeout=int(data.get("timeout", 10)),
        record_limit=int(data.get("record_limit", 1000)),
    )
