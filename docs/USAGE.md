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

The downloaded files are written to the directory specified by `OUTPUT_DIR`
(`./output` by default).

## CDX index mode

When you only need a small sample you can use the CDX index to download
individual records without streaming full WARC files:

```powershell
$env:CRAWL_PREFIX = "CC-MAIN-2024-22"
python crawler.py --mode index --samples 50 --extensions .mp3
```
