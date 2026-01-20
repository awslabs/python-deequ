# -*- coding: utf-8 -*-
# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""
PyDeequ - Python API for Deequ data quality library.

For PyDeequ 2.0 with DuckDB (no Spark required):
    import duckdb
    import pydeequ
    from pydeequ.v2.analyzers import Size, Completeness

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id")
    engine = pydeequ.connect(con, table="test")

For PyDeequ 2.0 (Spark Connect), use:
    from pydeequ.v2 import VerificationSuite, Check, CheckLevel
    from pydeequ.v2.predicates import eq, gte

For PyDeequ 1.x (Legacy Py4J), set SPARK_VERSION env var and use:
    from pydeequ import deequ_maven_coord
    from pydeequ.checks import Check, CheckLevel
"""
from typing import Any, Optional

__version__ = "2.0.0b1"


def connect(
    connection: Any,
    table: Optional[str] = None,
    dataframe: Optional[Any] = None,
):
    """
    Create an engine from a connection object with auto-detection.

    This function inspects the connection type and creates the appropriate
    engine backend. It supports:
    - DuckDB connections (duckdb.DuckDBPyConnection) - runs locally
    - Spark sessions (pyspark.sql.SparkSession) - uses Spark Connect

    Args:
        connection: A database connection or Spark session
        table: Table name for SQL-based backends (required for DuckDB)
        dataframe: DataFrame for Spark backend (alternative to table)

    Returns:
        An engine instance appropriate for the connection type

    Raises:
        ValueError: If connection type is not supported

    Example:
        # DuckDB (local, no Spark required)
        import duckdb
        import pydeequ

        con = duckdb.connect()
        con.execute("CREATE TABLE reviews AS SELECT * FROM 'reviews.csv'")
        engine = pydeequ.connect(con, table="reviews")

        # Spark Connect
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
        engine = pydeequ.connect(spark, dataframe=df)
    """
    from pydeequ.engines import connect as engines_connect
    return engines_connect(connection, table=table, dataframe=dataframe)

# Legacy imports are deferred to avoid requiring SPARK_VERSION for V2 users.
# V2 users should import from pydeequ.v2 directly.

_deequ_maven_coord = None
_f2j_maven_coord = "net.sourceforge.f2j:arpack_combined_all"


def __getattr__(name):
    """Lazy loading for legacy module attributes."""
    global _deequ_maven_coord

    if name == "deequ_maven_coord":
        if _deequ_maven_coord is None:
            from pydeequ.configs import DEEQU_MAVEN_COORD
            _deequ_maven_coord = DEEQU_MAVEN_COORD
        return _deequ_maven_coord

    if name == "f2j_maven_coord":
        return _f2j_maven_coord

    if name in ("AnalysisRunner", "Check", "CheckLevel", "ColumnProfilerRunner",
                "PyDeequSession", "DEEQU_MAVEN_COORD"):
        # Import legacy modules on demand
        if name == "AnalysisRunner":
            from pydeequ.analyzers import AnalysisRunner
            return AnalysisRunner
        elif name == "Check":
            from pydeequ.checks import Check
            return Check
        elif name == "CheckLevel":
            from pydeequ.checks import CheckLevel
            return CheckLevel
        elif name == "ColumnProfilerRunner":
            from pydeequ.profiles import ColumnProfilerRunner
            return ColumnProfilerRunner
        elif name == "DEEQU_MAVEN_COORD":
            from pydeequ.configs import DEEQU_MAVEN_COORD
            return DEEQU_MAVEN_COORD

    if name == "PyDeequSession":
        # Return the lazily-defined class
        return _get_pydeequ_session_class()

    raise AttributeError(f"module 'pydeequ' has no attribute '{name}'")


def _get_pydeequ_session_class():
    """Lazily create PyDeequSession class to avoid importing SparkSession at module load."""
    from pyspark.sql import SparkSession
    from pydeequ.analyzers import AnalysisRunner
    from pydeequ.checks import Check, CheckLevel
    from pydeequ.profiles import ColumnProfilerRunner

    class PyDeequSession:
        """
        For interacting with PyDeequ Modules at the "Runner" Level
        """

        def __init__(self, spark_session: SparkSession):
            self._spark_session = spark_session
            self._sc = spark_session.sparkContext
            self._jvm = spark_session._jvm

        def createColumnProfileRunner(self):
            return ColumnProfilerRunner(self._spark_session)

        def createAnalysisRunner(self):
            return AnalysisRunner(self._spark_session)

        def createCheck(self, level: CheckLevel, description: str, constraints=None):
            return Check(self._spark_session, level, description, constraints)

    return PyDeequSession
