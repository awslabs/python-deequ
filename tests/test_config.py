import pathlib
import warnings

import pytest
from pydeequ.configs import (
    IS_DEEQU_V1,
    SPARK_TO_DEEQU_COORD_MAPPING,
    _extract_major_minor_versions,
    _get_deequ_maven_config,
    _get_spark_version,
)


@pytest.mark.parametrize(
    "full_version, major_minor_version",
    [
        ("3.2.1", "3.2"),
        ("3.1", "3.1"),
        ("3.10.3", "3.10"),
        ("3.10", "3.10")
    ]
)
def test_extract_major_minor_versions(full_version, major_minor_version):
    assert _extract_major_minor_versions(full_version) == major_minor_version


def test_supported_spark_resolves_to_deequ_coord(monkeypatch):
    # _get_spark_version is lru_cached and reads SPARK_VERSION at call time.
    monkeypatch.setenv("SPARK_VERSION", "3.5")
    _get_spark_version.cache_clear()
    try:
        assert _get_deequ_maven_config() == "com.amazon.deequ:deequ:2.0.21-spark-3.5"
    finally:
        _get_spark_version.cache_clear()


@pytest.mark.parametrize("unsupported", ["3.3", "3.2", "3.1", "3.4"])
def test_unsupported_spark_raises(monkeypatch, unsupported):
    # Spark versions Deequ no longer publishes a build for must fail loudly.
    monkeypatch.setenv("SPARK_VERSION", unsupported)
    _get_spark_version.cache_clear()
    try:
        with pytest.raises(RuntimeError, match="incompatible Spark version"):
            _get_deequ_maven_config()
    finally:
        _get_spark_version.cache_clear()


def test_spark_4_1_deequ_coordinate(monkeypatch):
    monkeypatch.setenv("SPARK_VERSION", "4.1.2")
    _get_spark_version.cache_clear()

    try:
        assert _get_deequ_maven_config() == "com.amazon.deequ:deequ:2.0.18-spark-4.1"
        assert SPARK_TO_DEEQU_COORD_MAPPING["4.1"] == "com.amazon.deequ:deequ:2.0.18-spark-4.1"
    finally:
        _get_spark_version.cache_clear()


def test_repo_sources_have_no_invalid_escape_sequences():
    # The warning fires at compile time and leaves IS_DEEQU_V1's value unchanged, so no
    # assertion on an imported value can catch a regression.
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    sources = sorted(
        source
        for directory in ("pydeequ", "tests")
        for source in (repo_root / directory).rglob("*.py")
    )
    assert sources, f"no python sources found under {repo_root}"
    offenders = []
    for source in sources:
        name = source.relative_to(repo_root)
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                # Bytes, so compile() applies PEP 263 (BOM, coding cookie) as import would.
                compile(source.read_bytes(), str(source), "exec")
        except SyntaxError as error:
            offenders.append(f"{name}: {error}")
            continue
        offenders += [
            f"{name}: {entry.message}"
            for entry in caught
            # DeprecationWarning on Python < 3.12, SyntaxWarning from 3.12 on.
            if issubclass(entry.category, (SyntaxWarning, DeprecationWarning))
            and "escape" in str(entry.message)
        ]
    assert not offenders, f"invalid escape sequences: {'; '.join(offenders)}"


def test_supported_coordinates_are_not_deequ_v1():
    # suggestions.py selects JVM constructor arity from this flag.
    assert IS_DEEQU_V1 is False
    assert not any(
        coord.startswith("com.amazon.deequ:deequ:1") for coord in SPARK_TO_DEEQU_COORD_MAPPING.values()
    )
