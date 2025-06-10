# CC Codex Crawler

CC Codex Crawler downloads source files from the Common Crawl dataset and
prepares them for analysis or use with language models. The project contains a
command line crawler that streams WARC files from S3 and utilities for handling
the downloaded data.

```
+------------------+          +-----------+
| codex_crawler.py |  --->    | crawler.py|
+------------------+          +-----------+
           |                        |
           v                        v
        OpenAI                  Common Crawl
           |                        |
           +-----------+------------+
                       |
                       v
                   utils.py
                       |
                       v
                    OUTPUT_DIR
```

## Quickstart

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set your OpenAI API key:
   ```bash
   export OPENAI_API_KEY=<your key>
   ```
3. Run the codex step (optional):
   ```bash
   python codex_crawler.py
   ```
4. Start crawling Common Crawl:
   ```bash
   python crawler.py --warcs 5 --samples 100
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

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for
instructions. The repository uses `pre-commit` hooks and `pytest` for testing
all pull requests.
