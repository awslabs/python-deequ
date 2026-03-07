# -*- coding: utf-8 -*-
"""
VerificationSuite and AnalysisRunner for PyDeequ v2.

This module provides the main entry points for running data quality checks
and analysis. All runners take an engine in the constructor and use
``onData(table=..., dataframe=...)`` to bind data.

Example usage with DuckDB:
    import duckdb
    import pydeequ
    from pydeequ.v2.verification import VerificationSuite, AnalysisRunner
    from pydeequ.v2.checks import Check, CheckLevel
    from pydeequ.v2.analyzers import Size, Completeness
    from pydeequ.v2.predicates import eq, gte

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id, 'foo@bar.com' as email")
    engine = pydeequ.connect(con)

    check = (Check(CheckLevel.Error, "Data quality check")
        .isComplete("id")
        .hasCompleteness("email", gte(0.95)))

    result = (VerificationSuite(engine)
        .onData(table="test")
        .addCheck(check)
        .run())

Example usage with Spark Connect:
    from pyspark.sql import SparkSession
    import pydeequ
    from pydeequ.v2.verification import VerificationSuite, AnalysisRunner
    from pydeequ.v2.checks import Check, CheckLevel
    from pydeequ.v2.predicates import eq

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    engine = pydeequ.connect(spark)

    result = (VerificationSuite(engine)
        .onData(dataframe=df)
        .addCheck(check)
        .run())

    result.show()  # Result is a DataFrame
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

import pandas as pd

from pydeequ.v2.analyzers import _ConnectAnalyzer
from pydeequ.v2.checks import Check

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pydeequ.engines import BaseEngine


class VerificationSuite:
    """
    Main entry point for running data quality verification.

    Takes an engine in the constructor and uses ``onData()`` to bind data.

    Example:
        result = (VerificationSuite(engine)
            .onData(table="users")
            .addCheck(check)
            .run())
    """

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine

    def onData(
        self, *, table: Optional[str] = None, dataframe: "Optional[DataFrame]" = None
    ) -> "EngineVerificationRunBuilder":
        """
        Bind data for verification.

        Args:
            table: Table name (keyword-only)
            dataframe: DataFrame (keyword-only)

        Returns:
            EngineVerificationRunBuilder for method chaining
        """
        if table is not None and dataframe is not None:
            raise ValueError("Provide either 'table' or 'dataframe', not both")
        if table is not None:
            bound_engine = self._engine.for_table(table)
        elif dataframe is not None:
            bound_engine = self._engine.for_dataframe(dataframe)
        else:
            raise ValueError("Must provide either 'table' or 'dataframe'")
        return EngineVerificationRunBuilder(bound_engine)


class EngineVerificationRunBuilder:
    """Builder for configuring and executing engine-based verification."""

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine
        self._checks: List[Check] = []
        self._analyzers: List[_ConnectAnalyzer] = []

    def addCheck(self, check: Check) -> "EngineVerificationRunBuilder":
        self._checks.append(check)
        return self

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "EngineVerificationRunBuilder":
        self._analyzers.append(analyzer)
        return self

    def run(self) -> pd.DataFrame:
        """Execute the verification and return results as a pandas DataFrame."""
        results = self._engine.run_checks(self._checks)
        return self._engine.constraints_to_dataframe(results)


class AnalysisRunner:
    """
    Entry point for running analyzers without checks.

    Takes an engine in the constructor and uses ``onData()`` to bind data.

    Example:
        result = (AnalysisRunner(engine)
            .onData(table="users")
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("email"))
            .run())
    """

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine

    def onData(
        self, *, table: Optional[str] = None, dataframe: "Optional[DataFrame]" = None
    ) -> "EngineAnalysisRunBuilder":
        """
        Bind data for analysis.

        Args:
            table: Table name (keyword-only)
            dataframe: DataFrame (keyword-only)

        Returns:
            EngineAnalysisRunBuilder for method chaining
        """
        if table is not None and dataframe is not None:
            raise ValueError("Provide either 'table' or 'dataframe', not both")
        if table is not None:
            bound_engine = self._engine.for_table(table)
        elif dataframe is not None:
            bound_engine = self._engine.for_dataframe(dataframe)
        else:
            raise ValueError("Must provide either 'table' or 'dataframe'")
        return EngineAnalysisRunBuilder(bound_engine)


class EngineAnalysisRunBuilder:
    """Builder for configuring and executing engine-based analysis."""

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine
        self._analyzers: List[_ConnectAnalyzer] = []

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "EngineAnalysisRunBuilder":
        self._analyzers.append(analyzer)
        return self

    def run(self) -> pd.DataFrame:
        """Execute the analysis and return metrics as pandas DataFrame."""
        results = self._engine.compute_metrics(self._analyzers)
        return self._engine.metrics_to_dataframe(results)


# ---------------------------------------------------------------------------
# Private Spark Connect builders (used internally by SparkEngine)
# ---------------------------------------------------------------------------

class _SparkVerificationRunBuilder:
    """Internal builder for Spark Connect verification (protobuf-based)."""

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        self._spark = spark
        self._df = df
        self._checks: List[Check] = []
        self._analyzers: List[_ConnectAnalyzer] = []

    def addCheck(self, check: Check) -> "_SparkVerificationRunBuilder":
        self._checks.append(check)
        return self

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "_SparkVerificationRunBuilder":
        self._analyzers.append(analyzer)
        return self

    def run(self) -> "DataFrame":
        from google.protobuf import any_pb2
        from pydeequ.v2.proto import deequ_connect_pb2 as proto
        from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

        msg = proto.DeequVerificationRelation()
        for check in self._checks:
            msg.checks.append(check.to_proto())
        for analyzer in self._analyzers:
            msg.required_analyzers.append(analyzer.to_proto())

        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")
        plan = create_deequ_plan(extension)
        return dataframe_from_plan(plan, self._spark)


class _SparkAnalysisRunBuilder:
    """Internal builder for Spark Connect analysis (protobuf-based)."""

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        self._spark = spark
        self._df = df
        self._analyzers: List[_ConnectAnalyzer] = []

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "_SparkAnalysisRunBuilder":
        self._analyzers.append(analyzer)
        return self

    def run(self) -> "DataFrame":
        from google.protobuf import any_pb2
        from pydeequ.v2.proto import deequ_connect_pb2 as proto
        from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

        msg = proto.DeequAnalysisRelation()
        for analyzer in self._analyzers:
            msg.analyzers.append(analyzer.to_proto())

        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")
        plan = create_deequ_plan(extension)
        return dataframe_from_plan(plan, self._spark)


# Export all public symbols
__all__ = [
    "VerificationSuite",
    "EngineVerificationRunBuilder",
    "AnalysisRunner",
    "EngineAnalysisRunBuilder",
]
