import pytest
from pydeequ.configs import (
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
