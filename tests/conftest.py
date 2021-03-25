import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def spark_session():
    deequ_maven_coord = "com.amazon.deequ:deequ:1.0.3"  # TODO: get Maven Coord from Configs
    f2j_maven_coord = "net.sourceforge.f2j:arpack_combined_all"  # This package is excluded because it causes an error in the SparkSession fig
    session = (SparkSession
               .builder
               .master('local[*]')
               .config("spark.pyspark.python", "/usr/bin/python")
               .config("spark.pyspark.driver.python", "/usr/bin/python")
               .config("spark.jars.packages", deequ_maven_coord)
               .config("spark.jars.excludes", f2j_maven_coord)
               .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
               .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
               .config("spark.sql.autoBroadcastJoinThreshold", "-1")
               .getOrCreate())

    yield session
    # tearDown
    session.stop()
    session.sparkContext._gateway.shutdown_callback_server()
