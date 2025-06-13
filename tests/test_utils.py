import gzip
import io
from pathlib import Path

import utils

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter


def _create_warc(path: Path, content_type: str = "text/plain") -> None:
    with path.open("wb") as fh:
        gz = gzip.GzipFile(fileobj=fh, mode="wb")
        writer = WARCWriter(gz, gzip=False)
        headers = StatusAndHeaders("200 OK", [("Content-Type", content_type)])
        record = writer.create_warc_record(
            "http://example.com/test.mp3",
            "response",
            payload=io.BytesIO(b"audio"),
            http_headers=headers,
        )
        writer.write_record(record)
        gz.close()


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
