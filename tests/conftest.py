# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name
import logging

from pydeequ import set_deequ_maven_config


# @pytest.yield_fixture(autouse=True)
def setup_pyspark():
    from pyspark.sql import SparkSession

    logger = logging.getLogger("logger")
    logger.info("\nSetup Airflow DB connections and DAG")

    # TODO: get Maven Coord from Configs

    deequ_maven_coord = set_deequ_maven_config()
    # This package is excluded because it causes an error in the SparkSession fig
    f2j_maven_coord = "net.sourceforge.f2j:arpack_combined_all"

    return (
        SparkSession.builder.master("local[*]")
        .config("spark.executor.memory", "2g")
        .config("spark.jars.packages", deequ_maven_coord)
        .config("spark.pyspark.python", "/usr/bin/python3")
        .config("spark.pyspark.driver.python", "/usr/bin/python3")
        .config("spark.jars.excludes", f2j_maven_coord)
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    )
    # , logger
    # logger.info("\nTeardown Airflow DB connections and DAG")
