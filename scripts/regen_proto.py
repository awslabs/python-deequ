# -*- coding: utf-8 -*-
"""
Regenerate Python protobuf stubs from a deequ JAR.

Per ADR-0005 (in the deequ repo), the canonical `.proto` schema lives in
the deequ repo. The generated `_pb2.py` and `_pb2.pyi` files in
`pydeequ/v2/proto/` are checked in and committed alongside the source
(GraphFrames pattern). This script is a developer convenience: run it
manually whenever the schema changes in the deequ repo, then commit the
regenerated stubs.

Usage:
    DEEQU_JAR_PATH=/path/to/deequ_2.12-X.Y.Z.jar python scripts/regen_proto.py

CI also runs this script and asserts `git diff --exit-code pydeequ/v2/proto/`
to catch stale stubs.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

PROTO_RESOURCE_PATH = "META-INF/protobuf/deequ_connect.proto"

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTO_OUT_DIR = REPO_ROOT / "pydeequ" / "v2" / "proto"


def resolve_jar_path() -> Path:
    """Require an explicit local JAR path. No download fallback (per ADR-0005)."""
    raw = os.environ.get("DEEQU_JAR_PATH")
    if not raw:
        sys.exit(
            "DEEQU_JAR_PATH is required. Set it to a locally-built deequ JAR, e.g.:\n"
            "  DEEQU_JAR_PATH=/path/to/deequ/target/deequ_2.12-X.Y.Z.jar "
            "python scripts/regen_proto.py"
        )
    path = Path(raw).expanduser().resolve()
    if not path.exists():
        sys.exit(f"DEEQU_JAR_PATH points to a missing file: {path}")
    return path


def extract_proto(jar: Path) -> Path:
    """Extract META-INF/protobuf/deequ_connect.proto from the JAR.

    Note: until ADR-0005's drift-check pattern is wired into the deequ build,
    we can either pin to a JAR that does ship the .proto under META-INF/, or
    point this script at the schema directly via DEEQU_PROTO_PATH. Today the
    deequ JAR no longer ships the .proto (per ADR-0005) — pass the raw .proto
    via DEEQU_PROTO_PATH.
    """
    proto_override = os.environ.get("DEEQU_PROTO_PATH")
    if proto_override:
        src = Path(proto_override).expanduser().resolve()
        if not src.exists():
            sys.exit(f"DEEQU_PROTO_PATH points to a missing file: {src}")
        dst = PROTO_OUT_DIR / "deequ_connect.proto"
        shutil.copyfile(src, dst)
        print(f"Copied .proto -> {dst}")
        return dst

    PROTO_OUT_DIR.mkdir(parents=True, exist_ok=True)
    proto_dst = PROTO_OUT_DIR / "deequ_connect.proto"
    with zipfile.ZipFile(jar) as zf:
        names = set(zf.namelist())
        if PROTO_RESOURCE_PATH not in names:
            sys.exit(
                f"{jar} is missing {PROTO_RESOURCE_PATH}. "
                "Either rebuild the JAR with that resource or set DEEQU_PROTO_PATH "
                "to the .proto file directly."
            )
        with zf.open(PROTO_RESOURCE_PATH) as src, open(proto_dst, "wb") as dst:
            shutil.copyfileobj(src, dst)
    print(f"Extracted .proto -> {proto_dst}")
    return proto_dst


def run_protoc(proto: Path) -> None:
    """Generate _pb2.py and _pb2.pyi using grpcio-tools' bundled protoc."""
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={proto.parent}",
        f"--python_out={PROTO_OUT_DIR}",
        f"--pyi_out={PROTO_OUT_DIR}",
        str(proto),
    ]
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    pb2 = PROTO_OUT_DIR / "deequ_connect_pb2.py"
    if not pb2.exists():
        sys.exit(f"protoc did not produce {pb2}")
    print(f"Generated stubs -> {pb2}")


def main() -> None:
    if os.environ.get("DEEQU_PROTO_PATH"):
        # The .proto path was provided directly; skip JAR resolution.
        proto = extract_proto(jar=Path("/dev/null"))
    else:
        jar = resolve_jar_path()
        proto = extract_proto(jar)
    run_protoc(proto)


if __name__ == "__main__":
    main()
