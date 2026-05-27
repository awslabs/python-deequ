# -*- coding: utf-8 -*-
"""
VerificationSuite and AnalysisRunner for PyDeequ v2.

Each runner is a single class with ``onData``, ``addCheck``/``addAnalyzer``,
and ``run``. The data-binding seam lives at the engine
(``BaseEngine.for_table`` / ``for_dataframe``); the runner owns the
collected state and the call into the engine.

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

from typing import TYPE_CHECKING, Any, List, Optional

import pandas as pd

from pydeequ.v2.analyzers import _ConnectAnalyzer
from pydeequ.v2.checks import Check

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pydeequ.engines import BaseEngine


def _bind_engine(
    engine: "BaseEngine",
    *,
    table: Optional[str],
    dataframe: Optional[Any],
) -> "BaseEngine":
    """Resolve ``onData(table=…, dataframe=…)`` to a bound engine.

    Exactly one of ``table`` or ``dataframe`` must be provided.
    """
    if table is not None and dataframe is not None:
        raise ValueError("Provide either 'table' or 'dataframe', not both")
    if table is not None:
        return engine.for_table(table)
    if dataframe is not None:
        return engine.for_dataframe(dataframe)
    raise ValueError("Must provide either 'table' or 'dataframe'")


class VerificationSuite:
    """
    Run data-quality verification.

    Example:
        result = (VerificationSuite(engine)
            .onData(table="users")
            .addCheck(check)
            .run())
    """

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine
        self._checks: List[Check] = []

    def onData(
        self,
        *,
        table: Optional[str] = None,
        dataframe: "Optional[DataFrame]" = None,
    ) -> "VerificationSuite":
        """Bind data for verification (keyword-only)."""
        self._engine = _bind_engine(self._engine, table=table, dataframe=dataframe)
        return self

    def addCheck(self, check: Check) -> "VerificationSuite":
        self._checks.append(check)
        return self

    def run(self) -> pd.DataFrame:
        """Execute verification and return results as a pandas DataFrame."""
        results = self._engine.run_checks(self._checks)
        return self._engine.constraints_to_dataframe(results)


class AnalysisRunner:
    """
    Run analyzers without checks.

    Example:
        result = (AnalysisRunner(engine)
            .onData(table="users")
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("email"))
            .run())
    """

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine
        self._analyzers: List[_ConnectAnalyzer] = []

    def onData(
        self,
        *,
        table: Optional[str] = None,
        dataframe: "Optional[DataFrame]" = None,
    ) -> "AnalysisRunner":
        """Bind data for analysis (keyword-only)."""
        self._engine = _bind_engine(self._engine, table=table, dataframe=dataframe)
        return self

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "AnalysisRunner":
        self._analyzers.append(analyzer)
        return self

    def run(self) -> pd.DataFrame:
        """Execute analysis and return metrics as a pandas DataFrame."""
        results = self._engine.compute_metrics(self._analyzers)
        return self._engine.metrics_to_dataframe(results)


# Backwards-compatible aliases for the previously-exposed builder classes.
# The deepening collapses the runner and its builder into a single class —
# anything that imported the builder name still resolves.
EngineVerificationRunBuilder = VerificationSuite
EngineAnalysisRunBuilder = AnalysisRunner


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
