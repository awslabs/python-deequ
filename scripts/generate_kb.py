#!/usr/bin/env python3
"""Generate knowledge base for the PyDeequ bot from repository source.

Usage (from repo root):
    python3 scripts/generate_kb.py > kb.md
"""

import os
from pathlib import Path

REPO_ROOT = Path(".")
SRC_DIR = REPO_ROOT / "pydeequ"
TESTS_DIR = REPO_ROOT / "tests"
README = REPO_ROOT / "README.md"
PYPROJECT = REPO_ROOT / "pyproject.toml"

MAX_FILE_CHARS = 8000
MAX_TOTAL_CHARS = 500000


def read_safe(path, max_chars=None):
    try:
        text = path.read_text(errors="replace")
        if max_chars and len(text) > max_chars:
            text = text[:max_chars] + "\n... (truncated)"
        return text
    except Exception:
        return ""


def main():
    parts = []
    total = 0

    # README
    if README.exists():
        content = read_safe(README, MAX_FILE_CHARS)
        parts.append(f"# PyDeequ Knowledge Base\n\n## README\n\n{content}")
        total += len(content)

    # pyproject.toml
    if PYPROJECT.exists():
        content = read_safe(PYPROJECT, 3000)
        parts.append(f"## Build Configuration (pyproject.toml)\n\n```toml\n{content}\n```")
        total += len(content)

    # Source files
    if SRC_DIR.exists():
        parts.append("## Source Code Reference\n")
        for py_file in sorted(SRC_DIR.rglob("*.py")):
            if total >= MAX_TOTAL_CHARS:
                parts.append("\n... (KB size limit reached)")
                break
            rel = py_file.relative_to(REPO_ROOT)
            content = read_safe(py_file, MAX_FILE_CHARS)
            if content.strip():
                section = f"### `{rel}`\n\n```python\n{content}\n```\n"
                parts.append(section)
                total += len(section)

    # Test file listing (names only)
    if TESTS_DIR.exists():
        parts.append("## Test Files\n")
        for test_file in sorted(TESTS_DIR.rglob("*.py")):
            if total >= MAX_TOTAL_CHARS:
                break
            rel = test_file.relative_to(REPO_ROOT)
            lines = len(test_file.read_text(errors="replace").splitlines())
            parts.append(f"- `{rel}` ({lines} lines)")

    print("\n\n".join(parts))


if __name__ == "__main__":
    main()
