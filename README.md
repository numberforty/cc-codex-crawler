# CC Codex Crawler

Programmer: Hasan Alqahtani

CC Codex Crawler scans local Common Crawl WARC files and saves matching
records to disk. Download your desired crawl segments manually and point the
crawler at the directory containing the ``*.warc`` files.

```
crawler.py --> utils.py --> OUTPUT_DIR
```

## Installation

Install the required dependencies with `pip`:

```bash
pip install -r requirements.txt
```

Optionally set an OpenAI API key if you plan to use the optional utilities:

```bash
export OPENAI_API_KEY=<your key>
```

## Running the crawler

The crawler operates on local files only. By default it searches for common
source code extensions, but this can be customised via environment variables.

The most common options are listed below:

| Variable | Description | Default |
|----------|-------------|---------|
| `LOCAL_WARC_DIR` | Directory containing downloaded WARC files | `E:\\` |
| `TARGET_EXTENSIONS` | Comma separated list of extensions to save | `.py,.js,.java,.cpp,.go` |
| `OUTPUT_DIR` | Directory for extracted files | `./output` |

A simple run processing five local WARC files:

```bash
python crawler.py --warc-dir E:\\WARC-CC-MAIN-2024-30 --warcs 5 --samples 100
```

## Example: downloading MP3 files

To gather a small corpus of audio files, override the target extensions and
request more WARC files. The following command fetches up to 50 MP3 samples:

```bash
TARGET_EXTENSIONS=.mp3 \
python crawler.py --warc-dir E:\\WARC-CC-MAIN-2024-30 --warcs 20 --samples 50
```

The crawler processes one WARC file at a time and moves on to the next only
when the requested number of samples hasn't yet been collected. It stops early
once enough matches are found or no further WARC files remain.

Typical log output looks like:

```
2025-06-11 00:00:00,000 - INFO - Processing crawl-data/.../sample.warc.gz
2025-06-11 00:00:10,000 - INFO - Saved output/track_001.mp3 (.mp3)
```

Only responses with an `audio/*` content type are written to disk.

## Documentation

This README covers installation and quick-start examples. See
[docs/USAGE.md](docs/USAGE.md) for extended usage instructions and additional
examples.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for
instructions. The repository uses `pre-commit` hooks and `pytest` for testing
all pull requests.
