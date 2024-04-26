# -*- coding: utf-8 -*-
from functools import lru_cache
import os
import re


SPARK_TO_DEEQU_COORD_MAPPING = {
    "3.3": "com.amazon.deequ:deequ:2.0.7-spark-3.3",
    "3.2": "com.amazon.deequ:deequ:2.0.7-spark-3.2",
    "3.1": "com.amazon.deequ:deequ:2.0.7-spark-3.1"
}


def _extract_major_minor_versions(full_version: str):
    major_minor_pattern = re.compile(r"(\d+\.\d+)\.*")
    match = re.match(major_minor_pattern, full_version)
    if match:
        return match.group(1)


@lru_cache(maxsize=None)
def _get_spark_version() -> str:
    try:
        spark_version = os.environ["SPARK_VERSION"]
    except KeyError:
        raise RuntimeError(f"SPARK_VERSION environment variable is required. Supported values are: {SPARK_TO_DEEQU_COORD_MAPPING.keys()}")

    return _extract_major_minor_versions(spark_version)


def _get_deequ_maven_config():
    spark_version = _get_spark_version()
    try:
        return SPARK_TO_DEEQU_COORD_MAPPING[spark_version[:3]]
    except KeyError:
        raise RuntimeError(
            f"Found incompatible Spark version {spark_version}; Use one of the Supported Spark versions for Deequ: {SPARK_TO_DEEQU_COORD_MAPPING.keys()}"
        )


SPARK_VERSION = _get_spark_version()
DEEQU_MAVEN_COORD = _get_deequ_maven_config()
IS_DEEQU_V1 = re.search("com\.amazon\.deequ\:deequ\:1.*", DEEQU_MAVEN_COORD) is not None
