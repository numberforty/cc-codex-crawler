import gzip
import io
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from warcio.statusandheaders import StatusAndHeaders  # noqa: E402
from warcio.warcwriter import WARCWriter  # noqa: E402

import crawler  # noqa: E402
import utils  # noqa: E402


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


def _process_bytes(data: bytes, url: str, ext: str | None, output_dir: str) -> bool:
    stream = io.BytesIO(data)
    for rec in crawler.ArchiveIterator(stream, arc2warc=True):
        content_type = rec.http_headers.get_header("Content-Type", "")
        if rec.rec_type == "response" and content_type.startswith("audio/"):
            if ext and not url.endswith(ext):
                continue
            content = rec.content_stream().read()
            crawler.save_file(content, url, output_dir)
            return True
    return False


def test_index_mode_media_type(monkeypatch, tmp_path):
    audio_warc = tmp_path / "audio.warc.gz"
    text_warc = tmp_path / "text.warc.gz"
    url = "http://example.com/sample.mp3"
    _create_warc(audio_warc, "audio/mpeg", url)
    _create_warc(text_warc, "text/plain", url)

    saved = []

    def fake_save(data: bytes, u: str, out: str) -> str:
        path = utils.save_file(data, u, out)
        saved.append(path)
        return path

    monkeypatch.setattr(crawler, "save_file", fake_save)
    monkeypatch.setattr(crawler, "OUTPUT_DIR", str(tmp_path))

    assert _process_bytes(audio_warc.read_bytes(), url, None, str(tmp_path))
    assert len(saved) == 1

    assert not _process_bytes(text_warc.read_bytes(), url, None, str(tmp_path))
    assert len(saved) == 1
