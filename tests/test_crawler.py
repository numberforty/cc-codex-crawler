import gzip
import io
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import crawler  # noqa: E402
import utils  # noqa: E402
from warcio.statusandheaders import StatusAndHeaders  # noqa: E402
from warcio.warcwriter import WARCWriter  # noqa: E402


def _create_warc(path: Path, content_type: str, url: str) -> None:
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


def test_main_local(monkeypatch, tmp_path):
    warc_dir = tmp_path / "data"
    warc_dir.mkdir()
    warc = warc_dir / "audio.warc.gz"
    _create_warc(warc, "audio/mpeg", "http://example.com/sample.mp3")

    saved = []

    def fake_save(data: bytes, u: str, out: str) -> str:
        path = utils.save_file(data, u, out)
        saved.append(path)
        return path

    monkeypatch.setattr(crawler, "save_file", fake_save)
    monkeypatch.setattr(crawler, "STATE_FILE", str(tmp_path / "state.json"))
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    monkeypatch.setattr(crawler, "OUTPUT_DIR", str(out_dir))
    monkeypatch.setattr(crawler, "TARGET_EXTENSIONS", {".mp3"})

    argv = [
        "crawler.py",
        "--warcs",
        "1",
        "--samples",
        "1",
        "--warc-dir",
        str(warc_dir),
    ]
    monkeypatch.setattr(sys, "argv", argv)

    crawler.main()
    assert len(saved) == 1

