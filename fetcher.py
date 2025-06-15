#!/usr/bin/env python3
"""Simplified Common Crawl fetcher using index files.

This module implements a very small subset of the features provided by
`tballison/commoncrawl-fetcher-lite`.  It allows selecting records from
Common Crawl index segments via a JSON configuration file and downloads
matching files with HTTP range requests.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional

import requests
from warcio.archiveiterator import ArchiveIterator

from utils import REQUEST_TIMEOUT, save_file

BASE_URL = "https://data.commoncrawl.org"


@dataclass
class RecordSelector:
    must: Dict[str, List[str]]
    must_not: Dict[str, List[str]]
    should: Dict[str, List[str]]

    @classmethod
    def from_dict(cls, data: Dict) -> "RecordSelector":
        def _load(
            section: Optional[Dict[str, List[Dict[str, str]]]]
        ) -> Dict[str, List[str]]:
            out: Dict[str, List[str]] = {}
            if not section:
                return out
            for field, conditions in section.items():
                out[field] = [c.get("match") for c in conditions if "match" in c]
            return out

        return cls(
            must=_load(data.get("must")),
            must_not=_load(data.get("must_not")),
            should=_load(data.get("should")),
        )

    def matches(self, record: Dict[str, str]) -> bool:
        for field, values in self.must.items():
            if record.get(field) not in values:
                return False
        for field, values in self.must_not.items():
            if record.get(field) in values:
                return False
        if not self.should:
            return True
        for field, values in self.should.items():
            if record.get(field) in values:
                return True
        return False


def _open_gzip_stream(path_or_url: str) -> Iterator[bytes]:
    if os.path.exists(path_or_url):
        fh = open(path_or_url, "rb")
    else:
        if "://" not in path_or_url:
            path_or_url = f"{BASE_URL}/{path_or_url.lstrip('/')}"
        resp = requests.get(path_or_url, stream=True, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        fh = resp.raw
    with gzip.GzipFile(fileobj=fh) as gz:
        for line in gz:
            yield line


def _iter_index_paths(entry: str) -> Iterator[str]:
    prefix = BASE_URL
    cleaned = entry.strip()
    if os.path.exists(cleaned):
        for line in _open_gzip_stream(cleaned):
            yield line.decode("utf-8").strip()
    else:

        url = f"{prefix}/{cleaned.lstrip('/')}"

        for line in _open_gzip_stream(url):
            yield line.decode("utf-8").strip()


def _iter_records(index_path: str) -> Iterator[Dict[str, str]]:
    for line in _open_gzip_stream(index_path):
        decoded = line.decode("utf-8")
        try:
            urlkey, rest = decoded.split(" ", 1)
            timestamp, js = rest.split(" ", 1)
            record = json.loads(js)
            record["urlkey"] = urlkey
            record["timestamp"] = timestamp
            yield record
        except Exception:
            continue


def _fetch_warc_slice(record: Dict[str, str], data_dir: Optional[str] = None) -> bytes:
    filename = record["filename"]
    offset = int(record["offset"])
    length = int(record["length"])
    if data_dir and os.path.exists(os.path.join(data_dir, os.path.basename(filename))):
        path = os.path.join(data_dir, os.path.basename(filename))
        with open(path, "rb") as fh:
            fh.seek(offset)
            return fh.read(length)
    url = f"{BASE_URL}/{filename}"
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.content


def _extract_file(warc_bytes: bytes) -> Optional[tuple[str, bytes]]:
    with gzip.GzipFile(fileobj=io.BytesIO(warc_bytes)) as gz:
        for rec in ArchiveIterator(gz):
            if rec.rec_type != "response":
                continue
            uri = rec.rec_headers.get_header("WARC-Target-URI")
            if not uri:
                continue
            return uri, rec.content_stream().read()
    return None


def process_config(config_path: str, data_dir: Optional[str] = None) -> None:
    with open(config_path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    dry_run = bool(cfg.get("dryRun"))
    selector = RecordSelector.from_dict(cfg.get("recordSelector", {}))
    index_entries = cfg.get("indices", {}).get("paths", [])
    out_dir = cfg.get("outputDir", "docs")
    os.makedirs(out_dir, exist_ok=True)

    for entry in index_entries:
        for index_path in _iter_index_paths(entry):
            for record in _iter_records(index_path):
                if not selector.matches(record):
                    continue
                if dry_run:
                    print(record.get("url"))
                    continue
                try:
                    chunk = _fetch_warc_slice(record, data_dir)
                    extracted = _extract_file(chunk)
                    if not extracted:
                        continue
                    url, data = extracted
                    save_file(data, url, out_dir)
                except Exception as exc:
                    print(f"Failed {record.get('url')}: {exc}")
                time.sleep(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch files from Common Crawl")
    parser.add_argument("config", help="JSON configuration file")
    parser.add_argument(
        "--data-dir",
        help="Optional local directory containing referenced WARC files",
    )

    args = parser.parse_args()
    process_config(args.config, args.data_dir)
