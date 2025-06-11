# CC Codex Crawler

CC Codex Crawler downloads source files from the Common Crawl dataset and
prepares them for analysis or use with language models. The project contains a
command line crawler that streams WARC files from S3 and utilities for handling
the downloaded data.

```
crawler.py --> utils.py --> OUTPUT_DIR
```

## Quickstart

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. _(Optional)_ configure an OpenAI API key if you plan to use the
   optional utilities:
   ```bash
   export OPENAI_API_KEY=<your key>
   ```
3. Start crawling Common Crawl using AWS credentials:
   ```bash
   python crawler.py --warcs 5 --samples 100
   ```
4. Or run in HTTP mode without AWS credentials. **Set** `CRAWL_PREFIX` to a
   specific crawl (for example `crawl-data/CC-MAIN-2024-22`):
   ```bash
   CRAWL_PREFIX=crawl-data/CC-MAIN-2024-22 python crawler.py --mode http --warcs 5 --samples 100
   ```

## CLI Examples

* Process more files with multiple workers:
  ```bash
  python crawler.py --warcs 20 --samples 500 --workers 8
  ```
* Customize request rate:
  ```bash
  python crawler.py --rate-limit 0.5
  ```
* Override target extensions:
  ```bash
  TARGET_EXTENSIONS=.py,.js python crawler.py
  ```
* Run with AWS credentials (default):
  ```bash
  python crawler.py --warcs 20
  ```
* Run without AWS credentials (HTTP mode). Always set `CRAWL_PREFIX` to a
  specific crawl:
  ```bash
  CRAWL_PREFIX=crawl-data/CC-MAIN-2024-22 python crawler.py --mode http --warcs 20
  ```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for
instructions. The repository uses `pre-commit` hooks and `pytest` for testing
all pull requests.
