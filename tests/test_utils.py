import gzip
import io
from pathlib import Path

import pytest
import requests
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

import utils


def _create_warc(
    path: Path, content_type: str = "text/plain", gzip_compress: bool = True
) -> None:
    """Create a minimal WARC file for testing."""

    with path.open("wb") as fh:
        target = gzip.GzipFile(fileobj=fh, mode="wb") if gzip_compress else fh
        writer = WARCWriter(target, gzip=False)
        headers = StatusAndHeaders("200 OK", [("Content-Type", content_type)])
        record = writer.create_warc_record(
            "http://example.com/test.mp3",
            "response",
            payload=io.BytesIO(b"audio"),
            http_headers=headers,
        )
        writer.write_record(record)
        if gzip_compress:
            target.close()


def test_extension_from_url_present():
    assert utils.extension_from_url("https://example.com/file.py") == ".py"
    assert utils.extension_from_url("https://x/y/file.tar.gz") in {".tar.gz", ".gz"}
    assert utils.extension_from_url("https://example.com/a/?q=1") == ""


def test_save_file_collision(tmp_path):
    url = "http://example.com/hello.py"
    first = utils.save_file(b"one", url, str(tmp_path))
    second = utils.save_file(b"two", url, str(tmp_path))
    assert Path(first).name == "hello.py"
    assert Path(second).name == "hello_1.py"
    assert (tmp_path / "hello.py").read_bytes() == b"one"
    assert (tmp_path / "hello_1.py").read_bytes() == b"two"


def test_list_local_warc_files(tmp_path):
    d = tmp_path / "a"
    d.mkdir()
    f1 = d / "one.warc"
    f1.write_bytes(b"data")
    f2 = d / "two.warc.gz"
    f2.write_bytes(b"data")
    files = utils.list_local_warc_files(str(tmp_path), 5)
    assert set(files) == {str(f1), str(f2)}


def test_stream_and_extract_local(tmp_path):
    warc = tmp_path / "sample.warc.gz"
    _create_warc(warc, "audio/mpeg")
    records = list(utils.stream_and_extract_local(str(warc), [".mp3"]))
    assert len(records) == 1
    assert records[0][0] == "http://example.com/test.mp3"
    assert records[0][1] == b"audio"


def test_stream_and_extract_local_plain(tmp_path):
    warc = tmp_path / "sample.warc"
    _create_warc(warc, "audio/mpeg", gzip_compress=False)
    records = list(utils.stream_and_extract_local(str(warc), [".mp3"]))
    assert len(records) == 1
    assert records[0][0] == "http://example.com/test.mp3"
    assert records[0][1] == b"audio"


def test_list_warc_keys_http(monkeypatch):
    data = (
        b"CC-MAIN-2025-21/segment/1.warc.gz\n"
        b"not-a-warc\n"
        b"crawl-data/CC-MAIN-2025-21/segment/2.warc.gz\n"
    )
    gz = gzip.compress(data)

    called = {}

    class FakeResp:
        def __init__(self, content, status_code=200):
            self.content = content
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError()

    def fake_get(url, timeout):
        called["url"] = url
        return FakeResp(gz)

    monkeypatch.setattr(requests, "get", fake_get)

    keys = utils.list_warc_keys_http("CC-MAIN-2025-21", 2)

    assert called["url"].endswith("crawl-data/CC-MAIN-2025-21/warc.paths.gz")
    assert keys == [
        "crawl-data/CC-MAIN-2025-21/segment/1.warc.gz",
        "crawl-data/CC-MAIN-2025-21/segment/2.warc.gz",
    ]


def test_list_warc_keys_http_404(monkeypatch):
    class FakeResp:
        def __init__(self, status_code):
            self.content = b""
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError()

    def fake_get(url, timeout):
        return FakeResp(404)

    monkeypatch.setattr(requests, "get", fake_get)

    with pytest.raises(RuntimeError):
        utils.list_warc_keys_http("CC-MAIN-2025-21", 1)


def test_download_warc_http(monkeypatch, tmp_path):
    dest = tmp_path / "file.warc.gz"
    called = {}

    class FakeResp:
        def __init__(self):
            self.status_code = 200

        def iter_content(self, chunk_size=8192):
            yield b"data"

        def raise_for_status(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    def fake_get(url, stream=True, headers=None, timeout=None):
        called["url"] = url
        return FakeResp()

    monkeypatch.setattr(requests, "get", fake_get)

    utils.download_warc_http("crawl-data/test/file.warc.gz", str(dest), rate_limit=0)

    assert dest.read_bytes() == b"data"
    assert called["url"].endswith("crawl-data/test/file.warc.gz")
