# CC Codex Crawler

Programmer: Hasan Alqahtani

CC Codex Crawler provides a small Python utility for extracting files from the
Common Crawl dataset.  Earlier versions only operated on local WARC files.
The project now includes a lightweight fetcher inspired by
[`commoncrawl-fetcher-lite`](https://github.com/tballison/commoncrawl-fetcher-lite)
which downloads records based on the public indices.

## Installation

Install the required dependencies with `pip`:

```bash
pip install -r requirements.txt
```

Optionally set an OpenAI API key if you plan to use the optional utilities:

```bash
export OPENAI_API_KEY=<your key>
```

## Running the fetcher

The new `fetcher.py` script reads a JSON configuration that lists the desired
index files and filtering rules.  A minimal configuration looks like:

```json
{
  "dryRun": true,
  "indices": {"paths": ["crawl-data/CC-MAIN-2023-06/cc-index.paths.gz"]},
  "recordSelector": {
    "must": {"status": [{"match": "200"}]},
    "should": {"mime_detected": [{"match": "video/mp4"}]}
  }
}
```

Run the fetcher with:

```bash
python fetcher.py path/to/config.json
```

If `dryRun` is set to `false` the matching files are downloaded and stored in
the directory specified by `outputDir`.

### Local crawler

The previous local crawler is still available as `crawler.py`. It scans local
WARC files and saves matching records based on file extension. See
`docs/USAGE.md` for details.

## Documentation

This README covers installation and quick-start examples. See
[docs/USAGE.md](docs/USAGE.md) for extended usage instructions and additional
examples.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for
instructions. The repository uses `pre-commit` hooks and `pytest` for testing
all pull requests.
