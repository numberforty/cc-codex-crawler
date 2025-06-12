# CC Codex Crawler

Programmer: Hasan Alqahtani

CC Codex Crawler downloads files from the Common Crawl dataset and prepares them
for further analysis. It provides a command line utility that streams WARC files
and saves matching records to disk.

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

The crawler can operate either with AWS credentials or via direct HTTPS
requests. By default it searches for common source code extensions, but this can
be customised via environment variables.

The most common options are listed below:

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_BUCKET` | S3 bucket name | `commoncrawl` |
| `CRAWL_PREFIX` | Crawl prefix when using `--mode http` | `crawl-data` |
| `TARGET_EXTENSIONS` | Comma separated list of extensions to save | `.py,.js,.java,.cpp,.go` |
| `OUTPUT_DIR` | Directory for downloaded files | `./output` |

A simple crawl using AWS credentials:

```bash
python crawler.py --warcs 5 --samples 100
```

Using HTTPS mode without credentials (always specify a concrete crawl):

```bash
CRAWL_PREFIX=crawl-data/CC-MAIN-2024-22 \
python crawler.py --mode http --warcs 5 --samples 100
```

## Example: downloading MP3 files

To gather a small corpus of audio files, override the target extensions and
request more WARC files. The following command fetches up to 50 MP3 samples:

```bash
CRAWL_PREFIX=crawl-data/CC-MAIN-2024-22 \
TARGET_EXTENSIONS=.mp3 \
python crawler.py --mode http --warcs 20 --samples 50
```

Typical log output looks like:

```
2025-06-11 00:00:00,000 - INFO - Processing crawl-data/.../sample.warc.gz
2025-06-11 00:00:10,000 - INFO - Saved output/track_001.mp3 (.mp3)
```

Only responses with an `audio/*` content type are written to disk.

## CDX index mode

Instead of streaming entire WARC files you can fetch specific records using the
Common Crawl URL index. If `CRAWL_PREFIX` is not set the crawler retrieves the
latest crawl ID from `https://index.commoncrawl.org/collinfo.json`.

```bash
curl -s "https://index.commoncrawl.org/collinfo.json" | jq '.[0].id'
```

Once you know the crawl ID you can query it directly. Results are returned as
newline separated JSON objects. A short example:

```bash
curl "https://index.commoncrawl.org/CC-MAIN-2025-21-index?url=example.com&matchType=domain&output=json&limit=2" | head -n 2
{"urlkey": "com,example)/", "timestamp": "20250512012055", "url": "http://example.com/", ...}
{"urlkey": "com,example)/index.html", "timestamp": "20250512012603", "url": "http://example.com/index.html", ...}
```

Typical usage with the crawler:

```bash
CRAWL_PREFIX=CC-MAIN-2025-21 \
python crawler.py --mode index --index-url "example.com" --match-type domain --samples 50
```

Alternatively, use `--extensions` for simple extension filtering:

```bash
python crawler.py --mode index --extensions .mp3 --samples 50
```

## Documentation

This README covers installation and quick-start examples. See
[docs/USAGE.md](docs/USAGE.md) for extended usage instructions and additional
examples.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for
instructions. The repository uses `pre-commit` hooks and `pytest` for testing
all pull requests.
