import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config


def test_load_extension_filter(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("extension_filter: '.mp3'\n")
    c = config.load_config(str(cfg))
    assert c.extension_filter == '.mp3'
