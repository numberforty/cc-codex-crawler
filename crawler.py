import os

# Configuration from environment variables with sensible defaults
S3_BUCKET = os.getenv("S3_BUCKET", "commoncrawl")
CRAWL_PREFIX = os.getenv("CRAWL_PREFIX", "crawl-data")

# Parse target extensions into a Python set
_default_exts = ".py,.js,.java,.cpp,.go"
TARGET_EXTENSIONS = {
    ext.strip() for ext in os.getenv("TARGET_EXTENSIONS", _default_exts).split(",") if ext.strip()
}

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
MAX_WARCS = int(os.getenv("MAX_WARCS", "10"))
SAMPLES_PER_EXT = int(os.getenv("SAMPLES_PER_EXT", "1000"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "1.0"))
USER_AGENT = os.getenv("USER_AGENT", "cc-codex-crawler/1.0")

os.makedirs(OUTPUT_DIR, exist_ok=True)
