# -*- coding: utf-8 -*-
"""
Regenerate Python protobuf stubs from the deequ proto sources.

Per ADR-0005 and ADR-0006, the canonical .proto schema lives in the deequ
repo, split per surface (common.proto, verification.proto, analysis.proto,
column_profiler.proto, constraint_suggestion.proto). The generated _pb2.py
and _pb2.pyi files in `pydeequ/v2/proto/` are checked in alongside the
source (GraphFrames pattern). This script is a developer convenience: run
it manually whenever any .proto changes in the deequ repo, then commit
the diff.

Usage:
    DEEQU_PROTO_DIR=/path/to/deequ/src/main/protobuf python scripts/regen_proto.py

Backwards compatibility: the legacy DEEQU_PROTO_PATH (pointing at a single
.proto file) is still accepted; the script will use its parent directory.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_OUT_DIR = REPO_ROOT / "pydeequ" / "v2" / "proto"


def resolve_proto_dir() -> Path:
    """Locate the directory containing the .proto source files."""
    raw_dir = os.environ.get("DEEQU_PROTO_DIR")
    if raw_dir:
        path = Path(raw_dir).expanduser().resolve()
    else:
        # Backwards-compatible fallback: a single .proto path.
        raw_path = os.environ.get("DEEQU_PROTO_PATH")
        if not raw_path:
            sys.exit(
                "DEEQU_PROTO_DIR is required. Set it to the directory containing "
                "the deequ .proto sources, e.g.:\n"
                "  DEEQU_PROTO_DIR=/path/to/deequ/src/main/protobuf "
                "python scripts/regen_proto.py"
            )
        path = Path(raw_path).expanduser().resolve().parent
    if not path.is_dir():
        sys.exit(f"Proto source directory does not exist: {path}")
    return path


def copy_protos(src_dir: Path) -> list[Path]:
    """Copy all .proto files from src_dir to PROTO_OUT_DIR. Return their dst paths."""
    PROTO_OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Wipe stale .proto files in the destination before copying fresh ones.
    for stale in PROTO_OUT_DIR.glob("*.proto"):
        stale.unlink()
    sources = sorted(src_dir.glob("*.proto"))
    if not sources:
        sys.exit(f"No .proto files found in {src_dir}")
    dsts = []
    for src in sources:
        dst = PROTO_OUT_DIR / src.name
        shutil.copyfile(src, dst)
        dsts.append(dst)
        print(f"Copied {src.name} -> {dst}")
    return dsts


def run_protoc(protos: list[Path]) -> None:
    """Generate _pb2.py and _pb2.pyi for every .proto using grpcio-tools."""
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={PROTO_OUT_DIR}",
        f"--python_out={PROTO_OUT_DIR}",
        f"--pyi_out={PROTO_OUT_DIR}",
    ] + [str(p) for p in protos]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    for p in protos:
        pb2 = PROTO_OUT_DIR / f"{p.stem}_pb2.py"
        if not pb2.exists():
            sys.exit(f"protoc did not produce {pb2}")
        print(f"Generated stubs -> {pb2}")
    # Post-process: protoc emits absolute `import x_pb2 as y` for cross-file
    # references, which won't resolve when the stubs live inside a Python
    # package. Rewrite to relative imports.
    fix_cross_file_imports()


def fix_cross_file_imports() -> None:
    """Rewrite `import X_pb2 as Y` -> `from . import X_pb2 as Y` in generated .py files."""
    import re

    pattern = re.compile(r"^import (\w+_pb2) as (\w+)$", re.MULTILINE)
    for pb2 in PROTO_OUT_DIR.glob("*_pb2.py"):
        text = pb2.read_text()
        new_text = pattern.sub(r"from . import \1 as \2", text)
        if new_text != text:
            pb2.write_text(new_text)
            print(f"Fixed cross-file imports in {pb2.name}")


def cleanup_intermediate_protos() -> None:
    """Remove the .proto files we copied — they're not part of the wheel."""
    for stale in PROTO_OUT_DIR.glob("*.proto"):
        stale.unlink()


def main() -> None:
    src_dir = resolve_proto_dir()
    protos = copy_protos(src_dir)
    run_protoc(protos)
    cleanup_intermediate_protos()


if __name__ == "__main__":
    main()
