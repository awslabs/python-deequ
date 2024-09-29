import os
from unittest import mock

import pyspark
import pytest

from pydeequ.configs import _extract_major_minor_versions, _get_spark_version


@pytest.fixture
def mock_env(monkeypatch):
    with mock.patch.dict(os.environ, clear=True):
        monkeypatch.delenv("SPARK_VERSION", raising=False)
        yield


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


@pytest.mark.parametrize(
    "spark_version, expected", [("3.2.1", "3.2"), ("3.1", "3.1"), ("3.10.3", "3.10"), ("3.10", "3.10")]
)
def test__get_spark_versione(spark_version, expected, mock_env):
    try:
        _get_spark_version.cache_clear()
        with mock.patch.object(pyspark, "__version__", spark_version):
            assert _get_spark_version() == expected
    finally:
        _get_spark_version.cache_clear()



@pytest.mark.parametrize(
    "spark_version, expected", [("3.2.1", "3.2"), ("3.1", "3.2"), ("3.10.3", "3.2"), ("3.10", "3.2")]
)
def test__get_spark_version_with_cache(spark_version, expected, mock_env):
    with mock.patch.object(pyspark, "__version__", spark_version):
        assert _get_spark_version() == expected
