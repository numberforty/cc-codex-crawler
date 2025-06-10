# Recursively format all Python files using isort and Black

from __future__ import annotations

import sys
from pathlib import Path

import isort
from black import FileMode, WriteBack, format_file_in_place


def format_file(path: Path) -> bool:
    """Run isort and Black on a single file.

    Parameters
    ----------
    path : Path
        File path to format.

    Returns
    -------
    bool
        ``True`` if the file was modified.
    """

    changed = isort.file(str(path))
    changed |= format_file_in_place(path, fast=False, mode=FileMode(), write_back=WriteBack.YES)
    return changed


def main() -> None:
    root = Path('.')
    for py_file in sorted(root.rglob('*.py')):
        if py_file.resolve() == Path(__file__).resolve():
            continue
        try:
            if format_file(py_file):
                print(py_file)
        except Exception as exc:  # pragma: no cover - formatting failures
            print(f"Failed to format {py_file}: {exc}", file=sys.stderr)


if __name__ == '__main__':
    main()
