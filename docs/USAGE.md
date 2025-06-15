# Usage Guide

This document expands on the [README](../README.md) and demonstrates how to
run the simplified Common Crawl fetcher.

## Running the Fetcher

Create a JSON configuration file describing which records to extract from the
Common Crawl indices. A minimal example that lists MP4 files without actually
downloading them is provided at `config.json` in the repository root.

Run the fetcher by passing the configuration file:

```bash
python fetcher.py config.json
```

Set `"dryRun": false` in the configuration to download matching files. The
files will be written to the directory specified by `outputDir` (defaults to
`docs/`).
