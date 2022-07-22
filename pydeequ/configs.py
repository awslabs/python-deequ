# -*- coding: utf-8 -*-
import logging
import os

logger = logging.getLogger("logger")
configs = {
    "deequ_maven_coord_spark3_2": "com.amazon.deequ:deequ:2.0.1-spark-3.2",
    "f2j_maven_coord": "net.sourceforge.f2j:arpack_combined_all",
}


def _get_spark_version():
    # TODO - Change this later [Use Spark API's instead of env var]
    spark_version: str = os.getenv("SPARK_VERSION")
    return spark_version


def set_deequ_maven_config():
    spark_version = _get_spark_version()
    if spark_version is None:
        logger.error("Please set env variable SPARK_VERSION")
        logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
        return configs["deequ_maven_coord"]  # TODO
    if spark_version[0:3] == "3.2":
        logger.info("Setting spark-3.2 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark3_2"]
    else:
        logger.error(f"Deequ is still not supported in spark version: {spark_version}")
        logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
        return configs["deequ_maven_coord"]  # TODO

    logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
    return configs["deequ_maven_coord"]
