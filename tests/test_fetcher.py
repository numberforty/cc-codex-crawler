import gzip
import io
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from warcio.statusandheaders import StatusAndHeaders  # noqa: E402
from warcio.warcwriter import WARCWriter  # noqa: E402

import fetcher  # noqa: E402


def _create_warc(path: Path, content_type: str, url: str) -> int:
    with path.open("wb") as fh:
        gz = gzip.GzipFile(fileobj=fh, mode="wb")
        writer = WARCWriter(gz, gzip=False)
        headers = StatusAndHeaders("200 OK", [("Content-Type", content_type)])
        record = writer.create_warc_record(
            url,
            "response",
            payload=io.BytesIO(b"data"),
            http_headers=headers,
        )
        writer.write_record(record)
        gz.close()
    return path.stat().st_size


def test_process_config_local(tmp_path, monkeypatch):
    warc = tmp_path / "sample.warc.gz"
    length = _create_warc(warc, "video/mp4", "http://example.com/test.mp4")

    index = tmp_path / "index.gz"
    line = (
        "com,example)/test.mp4 20250101000000 "
        + json.dumps(
            {
                "url": "http://example.com/test.mp4",
                "mime": "video/mp4",
                "mime-detected": "video/mp4",
                "status": "200",
                "digest": "AAAA",
                "length": str(length),
                "offset": "0",
                "filename": warc.name,
            }
        )
        + "\n"
    )
    with gzip.open(index, "wb") as fh:
        fh.write(line.encode("utf-8"))

    paths = tmp_path / "cc-index.paths.gz"
    with gzip.open(paths, "wb") as fh:
        fh.write(f"{index.name}\n".encode("utf-8"))

    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "dryRun": False,
                "outputDir": str(tmp_path / "out"),
                "indices": {"paths": [str(paths)]},
                "recordSelector": {
                    "must": {"status": [{"match": "200"}]},
                    "should": {"mime-detected": [{"match": "video/mp4"}]},
                },
            }
        )
    )

    monkeypatch.chdir(tmp_path)
    fetcher.process_config(str(config), data_dir=str(tmp_path))

    out_files = list((tmp_path / "out").iterdir())
    assert len(out_files) == 1
    assert out_files[0].read_bytes() == b"data"

