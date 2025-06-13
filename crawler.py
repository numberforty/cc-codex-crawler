import logging
import os
from collections import defaultdict

from utils import (
    list_local_warc_files,
    load_state,
    save_file,
    save_state,
    stream_and_extract_local,
)

# Configuration
LOCAL_WARC_DIR = os.getenv("LOCAL_WARC_DIR", "E:\\")
_default_exts = ".py,.js,.java,.cpp,.go"
TARGET_EXTENSIONS = {
    ext.strip()
    for ext in os.getenv("TARGET_EXTENSIONS", _default_exts).split(",")
    if ext.strip()
}
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
MAX_WARCS = int(os.getenv("MAX_WARCS", "10"))
SAMPLES_PER_EXT = int(os.getenv("SAMPLES_PER_EXT", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
STATE_FILE = os.getenv("STATE_FILE", "crawler_state.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def main() -> None:
    """Process local WARC files and save matching records."""

    import argparse

    parser = argparse.ArgumentParser(description="CC Codex crawler (local)")
    parser.add_argument(
        "--warcs",
        type=int,
        default=MAX_WARCS,
        help="Number of WARC files to process",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=SAMPLES_PER_EXT,
        help="Number of files to save per extension",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help="Maximum number of concurrent workers",
    )
    parser.add_argument(
        "--warc-dir",
        default=LOCAL_WARC_DIR,
        help="Directory containing local WARC files",
    )

    args = parser.parse_args()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler("crawler.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    warc_files = list_local_warc_files(args.warc_dir, args.warcs)

    completed = load_state(STATE_FILE)
    if completed:
        warc_files = [p for p in warc_files if p not in completed]

    if not warc_files:
        logger.info("No new WARC files to process")
        return

    saved_counts: defaultdict[str, int] = defaultdict(int)

    for path in warc_files:
        logger.info("Processing %s", path)
        try:
            iterator = stream_and_extract_local(path, TARGET_EXTENSIONS)
            for url, data in iterator:
                ext = next((e for e in TARGET_EXTENSIONS if url.endswith(e)), None)
                if not ext:
                    continue
                if saved_counts[ext] >= args.samples:
                    continue
                try:
                    target_path = save_file(data, url, OUTPUT_DIR)
                    saved_counts[ext] += 1
                    logger.info("Saved %s (%s)", target_path, ext)
                except Exception as exc:
                    logger.warning("Failed to save %s: %s", url, exc)
        except Exception as exc:
            logger.warning("Error processing %s: %s", path, exc)
        finally:
            completed.add(path)
            save_state(STATE_FILE, completed)

        if all(saved_counts[e] >= args.samples for e in TARGET_EXTENSIONS):
            logger.info("Collected required samples, stopping")
            break


if __name__ == "__main__":
    main()
