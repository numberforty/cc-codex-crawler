from pathlib import Path

import utils


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
