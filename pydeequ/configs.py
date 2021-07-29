# -*- coding: utf-8 -*-
import logging
import os

logger = logging.getLogger("logger")
configs = {
    "deequ_maven_coord": "com.amazon.deequ:deequ:1.2.2-spark-3.0",
    "deequ_maven_coord_spark3": "com.amazon.deequ:deequ:1.2.2-spark-3.0",
    # "deequ_maven_coord_spark2_4": "com.amazon.deequ:deequ:1.2.2-spark-2.4", # 1.2.2 is broken, rolling back to 1.1.0 with scala 11
    "deequ_maven_coord_spark2_4": "com.amazon.deequ:deequ:1.1.0_spark-2.4-scala-2.11",
    # "deequ_maven_coord_spark2_2": "com.amazon.deequ:deequ:1.2.2-spark-2.2",
    "deequ_maven_coord_spark2_2": "com.amazon.deequ:deequ:1.1.0_spark-2.2-scala-2.11",
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
    if spark_version[0:3] == "3.0":
        logger.info("Setting spark-3.0 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark3"]
    elif spark_version[0:3] == "2.4":
        logger.info("Setting spark-2.4 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark2_4"]
    elif spark_version[0:3] == "2.2":
        logger.info("Setting spark3 as default version of deequ")
        configs["deequ_maven_coord"] = configs["deequ_maven_coord_spark2_2"]
    else:
        logger.error(f"Deequ is still not supported in spark version: {spark_version}")
        logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
        return configs["deequ_maven_coord"]  # TODO

    logger.info(f"Using deequ: {configs['deequ_maven_coord']}")
    return configs["deequ_maven_coord"]
