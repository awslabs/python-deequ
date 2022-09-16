# -*- coding: utf-8 -*-
from functools import lru_cache
import subprocess
import re

SPARK_TO_DEEQU_COORD_MAPPING = {
    "3.2": "com.amazon.deequ:deequ:2.0.1-spark-3.2",
    "3.1": "com.amazon.deequ:deequ:2.0.0-spark-3.1",
    "3.0": "com.amazon.deequ:deequ:1.2.2-spark-3.0",
    "2.4": "com.amazon.deequ:deequ:1.1.0_spark-2.4-scala-2.11",
}


@lru_cache(maxsize=None)
def _get_spark_version() -> str:
    # Get version from a subprocess so we don't mess up with existing SparkContexts.
    command = [
        "python",
        "-c",
        "from pyspark import SparkContext; print(SparkContext.getOrCreate()._jsc.version())",
    ]
    output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    spark_version = output.stdout.decode().split("\n")[-2]
    return spark_version


def _get_deequ_maven_config():
    spark_version = _get_spark_version()
    try:
        return SPARK_TO_DEEQU_COORD_MAPPING[spark_version[:3]]
    except KeyError:
        raise RuntimeError(
            f"Found Incompatible Spark version {spark_version}; Use one of the Supported Spark versions for Deequ: {SPARK_TO_DEEQU_COORD_MAPPING.keys()}"
        )


DEEQU_MAVEN_COORD = _get_deequ_maven_config()
IS_DEEQU_V1 = re.search("com\.amazon\.deequ\:deequ\:1.*", DEEQU_MAVEN_COORD) is not None
