# Usage Guide

This document expands on the information in the project
[README](../README.md) and demonstrates how to configure the crawler for
different file types.

## Downloading MP3 Samples

The crawler targets source code files by default. To download MP3 files instead,
set the `TARGET_EXTENSIONS` environment variable and specify the desired number
of samples. Below are examples for both PowerShell and the Windows command
prompt. Each retrieves up to 50 MP3 files using direct HTTPS access.

```powershell
$env:CRAWL_PREFIX = "crawl-data/CC-MAIN-2024-22"
$env:TARGET_EXTENSIONS = ".mp3"
python crawler.py --mode http --warcs 20 --samples 50
```

```batch
set CRAWL_PREFIX=crawl-data/CC-MAIN-2024-22
set TARGET_EXTENSIONS=.mp3
python crawler.py --mode http --warcs 20 --samples 50
```

Typical output lines look like this:

```
2025-06-11 00:00:00,000 - INFO - Processing crawl-data/.../sample.warc.gz
2025-06-11 00:00:10,000 - INFO - Saved output/track_001.mp3 (.mp3)
```

Files are only written if the response's `Content-Type` header starts with `audio/`.

The downloaded files are written to the directory specified by `OUTPUT_DIR`
(`./output` by default).

## CDX index mode

When you only need a small sample you can use the CDX index to download
individual records without streaming full WARC files. The crawler automatically
uses the latest crawl ID from `https://index.commoncrawl.org/collinfo.json` if
`CRAWL_PREFIX` is not set.

```powershell
curl -s "https://index.commoncrawl.org/collinfo.json" | jq '.[0].id'
$env:CRAWL_PREFIX = "CC-MAIN-2025-21"
python crawler.py --mode index --index-url "example.com" --match-type domain --samples 50
```

Use `--index-url` to send an arbitrary pattern to the CDX index. Combine it with
`--match-type` when needed. For simple extension filtering you can instead use
the `--extensions` option:

```powershell
python crawler.py --mode index --extensions .mp3 --samples 50
```
