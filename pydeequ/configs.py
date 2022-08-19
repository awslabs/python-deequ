# -*- coding: utf-8 -*-
import logging

from functools import lru_cache
import subprocess

logger = logging.getLogger("logger")
configs = {
    "deequ_maven_coord": "com.amazon.deequ:deequ:2.0.1-spark-3.2",
    "deequ_maven_coord_spark3_2": "com.amazon.deequ:deequ:2.0.1-spark-3.2",
    "deequ_maven_coord_spark3": "com.amazon.deequ:deequ:1.2.2-spark-3.0",
    "deequ_maven_coord_spark2_4": "com.amazon.deequ:deequ:1.1.0_spark-2.4-scala-2.11",
    "f2j_maven_coord": "net.sourceforge.f2j:arpack_combined_all",
}


@lru_cache(maxsize=None)
def _get_spark_version() -> str:
    # Create the context on a subprocess so we don't
    # mess up tests and users contexts.
    command = [
        "python",
        "-c",
        "from pyspark import SparkContext; print(SparkContext.getOrCreate()._jsc.version())",
    ]
    output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    spark_version = output.stdout.decode().strip()[:3]
    return spark_version


def set_deequ_maven_config():
    spark_version = _get_spark_version()
    if spark_version is None:
        logger.error("Please set env variable SPARK_VERSION")
        logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
        return configs["deequ_maven_coord"]  # TODO
    elif spark_version[0:3] == "3.2":
        logger.info("Setting spark-3.2 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark3_2"]
    elif spark_version[0:3] == "3.0":
        logger.info("Setting spark-3.0 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark3"]
    elif spark_version[0:3] == "2.4":
        logger.info("Setting spark-2.4 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark2_4"]
    else:
        logger.error(f"Deequ is still not supported in spark version: {spark_version}")
        logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
        return configs["deequ_maven_coord"]  # TODO

    logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
    return configs["deequ_maven_coord"]
