# -*- coding: utf-8 -*-
# pylint: disable=redefined-outer-name
import logging

from pydeequ import deequ_maven_coord, f2j_maven_coord


# @pytest.yield_fixture(autouse=True)
def setup_pyspark():
    from pyspark.sql import SparkSession

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
