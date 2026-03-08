#!/usr/bin/env python3
"""Remove generated runtime artifacts while preserving source datasets."""

from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

REMOVE_DIR_CONTENTS = [
    ROOT / "uploads",
    ROOT / "outputs",
    ROOT / "logs",
]

REMOVE_DIR_NAMES = {
    "__pycache__",
    ".pytest_cache",
}

REMOVE_FILE_NAMES = {
    ".DS_Store",
    "tsconfig.tsbuildinfo",
}

SKIP_RECURSIVE_DIR_NAMES = {
    ".git",
    ".venv",
    "node_modules",
    ".next",
}

KEEP_OUTPUT_FILES = set()


def clean_directory_contents(path: Path) -> list[str]:
    removed: list[str] = []
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return removed
    for child in path.iterdir():
        if path.name == "outputs" and child.name in KEEP_OUTPUT_FILES:
            continue
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)
        removed.append(str(child.relative_to(ROOT)))
    path.mkdir(parents=True, exist_ok=True)
    return removed


def clean_recursive() -> list[str]:
    removed: list[str] = []
    for path in ROOT.rglob("*"):
        if not path.exists():
            continue
        if any(part in SKIP_RECURSIVE_DIR_NAMES for part in path.parts):
            continue
        if path.is_dir() and path.name in REMOVE_DIR_NAMES:
            shutil.rmtree(path, ignore_errors=True)
            removed.append(str(path.relative_to(ROOT)))
            continue
        if path.is_file() and path.name in REMOVE_FILE_NAMES:
            path.unlink(missing_ok=True)
            removed.append(str(path.relative_to(ROOT)))
    return removed


def main() -> None:
    removed: list[str] = []
    for directory in REMOVE_DIR_CONTENTS:
        removed.extend(clean_directory_contents(directory))
    removed.extend(clean_recursive())
    print(f"Removed {len(removed)} generated artifacts.")
    for entry in sorted(removed):
        print(entry)


if __name__ == "__main__":
    main()
