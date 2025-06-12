import gzip
import io
import os
import sys
from pathlib import Path

sys.path.insert(
    0,
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
)  # noqa: E402
import pytest  # noqa: E402
import requests  # noqa: E402
from warcio.statusandheaders import StatusAndHeaders  # noqa: E402
from warcio.warcwriter import WARCWriter  # noqa: E402

import utils  # noqa: E402

# Helper to create a small WARC file for testing


def _create_warc(path: Path):
    with path.open("wb") as fh:
        gz = gzip.GzipFile(fileobj=fh, mode="wb")
        writer = WARCWriter(gz, gzip=False)
        headers = StatusAndHeaders("200 OK", [("Content-Type", "text/plain")])
        record = writer.create_warc_record(
            "http://example.com/test.py",
            "response",
            payload=io.BytesIO(b"print(1)"),
            http_headers=headers,
        )
        writer.write_record(record)
        gz.close()


def test_extension_from_url_present():
    ext_func = getattr(utils, "extension_from_url", None)
    if ext_func is None:
        pytest.skip("extension_from_url not implemented")
    assert ext_func("https://example.com/code.py") == ".py"
    assert ext_func("https://example.com/path/file.tar.gz") in {".tar.gz", ".gz"}
    assert ext_func("https://example.com/a/b/?q=1") == ""


def test_save_file_collision(tmp_path):
    url = "http://example.com/hello.py"
    first = utils.save_file(b"one", url, str(tmp_path))
    second = utils.save_file(b"two", url, str(tmp_path))
    assert Path(first).name == "hello.py"
    assert Path(second).name == "hello_1.py"
    assert (tmp_path / "hello.py").read_bytes() == b"one"
    assert (tmp_path / "hello_1.py").read_bytes() == b"two"


def test_list_warc_keys():
    pages = [
        {"Contents": [{"Key": "a.warc.gz"}, {"Key": "b.txt"}]},
        {"Contents": [{"Key": "c.warc.gz"}]},
    ]

    class Paginator:
        def paginate(self, Bucket, Prefix):
            assert Bucket == "bucket"
            assert Prefix == "prefix"
            for p in pages:
                yield p

    class Client:
        def get_paginator(self, name):
            assert name == "list_objects_v2"
            return Paginator()

    result = utils.list_warc_keys(Client(), "bucket", "prefix", 2)
    assert result == ["a.warc.gz", "c.warc.gz"]


def test_stream_and_extract(tmp_path, monkeypatch):
    warc_path = tmp_path / "sample.warc.gz"
    _create_warc(warc_path)

    class FakeClient:
        def generate_presigned_url(self, op, Params, ExpiresIn):
            return "http://example.com/presigned"

    class DummyResp:
        def __init__(self, path):
            self.raw = open(path, "rb")
            self.status_code = 200

        def raise_for_status(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.raw.close()

    def fake_get(url, stream=True, headers=None, timeout=None):
        assert url == "http://example.com/presigned"
        assert timeout == utils.REQUEST_TIMEOUT
        return DummyResp(warc_path)

    monkeypatch.setattr("requests.get", fake_get)

    records = list(
        utils.stream_and_extract(
            FakeClient(),
            "bucket",
            "key",
            [".py"],
            rate_limit=0,
            user_agent="ua",
        )
    )
    assert len(records) == 1
    assert records[0][0] == "http://example.com/test.py"
    assert records[0][1] == b"print(1)"


def test_list_warc_keys_http(tmp_path, monkeypatch):
    path_file = tmp_path / "warc.paths.gz"
    content = b"a.warc.gz\nb.warc.gz\n"
    import gzip

    with gzip.open(path_file, "wb") as fh:
        fh.write(content)

    class Resp:
        def __init__(self, data):
            self.content = data
            self.status_code = 200

        def raise_for_status(self):
            pass

    calls = []

    def fake_get(url, timeout=10):
        calls.append(1)
        assert url.endswith("warc.paths.gz")
        if len(calls) == 1:
            raise requests.RequestException("fail")
        return Resp(path_file.read_bytes())

    monkeypatch.setattr("requests.get", fake_get)
    monkeypatch.setattr("time.sleep", lambda x: None)

    keys = utils.list_warc_keys_http("crawl-data/CC-MAIN-2020-50", 1)
    assert calls and len(calls) == 2
    assert keys == ["crawl-data/a.warc.gz"]


def test_list_warc_keys_http_prefixed_entries(tmp_path, monkeypatch):
    path_file = tmp_path / "warc.paths.gz"
    content = b"crawl-data/a.warc.gz\ncrawl-data/b.warc.gz\n"
    import gzip

    with gzip.open(path_file, "wb") as fh:
        fh.write(content)

    class Resp:
        def __init__(self, data):
            self.content = data
            self.status_code = 200

        def raise_for_status(self):
            pass

    def fake_get(url, timeout=10):
        assert url.endswith("warc.paths.gz")
        return Resp(path_file.read_bytes())

    monkeypatch.setattr("requests.get", fake_get)

    keys = utils.list_warc_keys_http("crawl-data/CC-MAIN-XXXX-XX", 2)
    assert keys == ["crawl-data/a.warc.gz", "crawl-data/b.warc.gz"]


def test_list_warc_keys_http_latest_404(monkeypatch):
    class Resp:
        def __init__(self):
            self.status_code = 404
            self.content = b""

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    def fake_get(url, timeout=10):
        return Resp()

    monkeypatch.setattr("requests.get", fake_get)

    with pytest.raises(RuntimeError):
        utils.list_warc_keys_http("crawl-data", 1)


def test_stream_and_extract_http(tmp_path, monkeypatch):
    warc_path = tmp_path / "sample.warc.gz"
    _create_warc(warc_path)

    class DummyResp:
        def __init__(self, path):
            self.raw = open(path, "rb")
            self.status_code = 200

        def raise_for_status(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.raw.close()

    calls = []

    def fake_get(url, stream=True, headers=None, timeout=None):
        calls.append(1)
        assert url == "https://data.commoncrawl.org/crawl-data/dir/sample.warc.gz"
        assert timeout == utils.REQUEST_TIMEOUT
        if len(calls) == 1:
            raise requests.RequestException("fail")
        return DummyResp(warc_path)

    monkeypatch.setattr("requests.get", fake_get)
    monkeypatch.setattr("time.sleep", lambda x: None)

    records = list(
        utils.stream_and_extract_http(
            "crawl-data/dir/sample.warc.gz",
            [".py"],
            rate_limit=0,
            user_agent="ua",
        )
    )
    assert calls and len(calls) == 2
    assert len(records) == 1
    assert records[0][0] == "http://example.com/test.py"
    assert records[0][1] == b"print(1)"


def test_latest_crawl_id_success(monkeypatch):
    class Resp:
        def __init__(self):
            self.status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return [{"id": "CC-MAIN-2025-21"}]

    monkeypatch.setattr("requests.get", lambda url, timeout=10: Resp())

    assert utils.latest_crawl_id() == "CC-MAIN-2025-21"


def test_latest_crawl_id_failure(monkeypatch):
    def raise_exc(url, timeout=10):
        raise requests.RequestException

    monkeypatch.setattr("requests.get", raise_exc)

    assert utils.latest_crawl_id() is None
