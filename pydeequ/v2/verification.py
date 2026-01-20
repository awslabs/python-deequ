# -*- coding: utf-8 -*-
"""
VerificationSuite for PyDeequ v2.

This module provides the main entry point for running data quality checks.
It supports two execution modes:

1. Engine-based (DuckDB, etc.) - uses pydeequ.connect()
2. Spark Connect - uses SparkSession with Deequ plugin

Example usage with DuckDB:
    import duckdb
    import pydeequ
    from pydeequ.v2.verification import VerificationSuite, AnalysisRunner
    from pydeequ.v2.checks import Check, CheckLevel
    from pydeequ.v2.predicates import gte, eq

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id, 'foo@bar.com' as email")
    engine = pydeequ.connect(con, table="test")

    check = (Check(CheckLevel.Error, "Data quality check")
        .isComplete("id")
        .hasCompleteness("email", gte(0.95)))

    result = VerificationSuite().on_engine(engine).addCheck(check).run()

Example usage with Spark Connect:
    from pyspark.sql import SparkSession
    from pydeequ.v2.verification import VerificationSuite
    from pydeequ.v2.checks import Check, CheckLevel
    from pydeequ.v2.predicates import gte, eq

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    check = (Check(CheckLevel.Error, "Data quality check")
        .isComplete("id")
        .hasCompleteness("email", gte(0.95)))

    result = (VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run())

    result.show()  # Result is a DataFrame
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

import pandas as pd
from google.protobuf import any_pb2

from pydeequ.v2.analyzers import _ConnectAnalyzer
from pydeequ.v2.checks import Check
from pydeequ.v2.proto import deequ_connect_pb2 as proto
from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pydeequ.engines import BaseEngine


class VerificationSuite:
    """
    Main entry point for running data quality verification.

    VerificationSuite allows you to define checks and analyzers to run.
    It supports two execution modes:

    1. Engine-based: Use on_engine() for DuckDB and other SQL backends
    2. Spark-based: Use onData() for Spark Connect

    Example (Engine-based):
        suite = VerificationSuite()
        result = (suite
            .on_engine(engine)
            .addCheck(check)
            .run())

    Example (Spark-based):
        suite = VerificationSuite(spark)
        result = (suite
            .onData(df)
            .addCheck(check)
            .run())
    """

    def __init__(self, spark: Optional["SparkSession"] = None):
        """
        Create a new VerificationSuite.

        Args:
            spark: Optional SparkSession for Spark Connect mode.
                   Not required for engine-based execution.
        """
        self._spark = spark

    def onData(self, df: "DataFrame") -> "VerificationRunBuilder":
        """
        Specify the DataFrame to run verification on (Spark mode).

        Args:
            df: DataFrame to verify

        Returns:
            VerificationRunBuilder for method chaining

        Raises:
            ValueError: If SparkSession was not provided in constructor
        """
        if self._spark is None:
            raise ValueError(
                "SparkSession required for onData(). "
                "Use VerificationSuite(spark).onData(df) or "
                "VerificationSuite().on_engine(engine) for engine-based execution."
            )
        return VerificationRunBuilder(self._spark, df)

    def on_engine(self, engine: "BaseEngine") -> "EngineVerificationRunBuilder":
        """
        Specify the engine to run verification on (Engine mode).

        Args:
            engine: BaseEngine instance (e.g., DuckDBEngine)

        Returns:
            EngineVerificationRunBuilder for method chaining
        """
        return EngineVerificationRunBuilder(engine)

    # Alias for consistency with other methods
    def on_table(self, table: str) -> "EngineVerificationRunBuilder":
        """
        Specify a table name for engine-based verification.

        Note: This method requires an engine to be set first via on_engine().
        For direct table access, use:
            pydeequ.connect(con, table="my_table")

        Raises:
            ValueError: This method is deprecated
        """
        raise ValueError(
            "on_table() requires an engine. Use: "
            "VerificationSuite().on_engine(pydeequ.connect(con, table='my_table'))"
        )


class VerificationRunBuilder:
    """
    Builder for configuring and executing a verification run.

    This class collects checks and analyzers, then executes them
    when run() is called.
    """

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        """
        Create a new VerificationRunBuilder.

        Args:
            spark: SparkSession
            df: DataFrame to verify
        """
        self._spark = spark
        self._df = df
        self._checks: List[Check] = []
        self._analyzers: List[_ConnectAnalyzer] = []

    def addCheck(self, check: Check) -> "VerificationRunBuilder":
        """
        Add a check to run.

        Args:
            check: Check to add

        Returns:
            self for method chaining
        """
        self._checks.append(check)
        return self

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "VerificationRunBuilder":
        """
        Add an analyzer to run (in addition to those required by checks).

        Args:
            analyzer: Analyzer to add

        Returns:
            self for method chaining
        """
        self._analyzers.append(analyzer)
        return self

    def run(self) -> "DataFrame":
        """
        Execute the verification and return results as a DataFrame.

        The result DataFrame contains columns:
        - check: Check description
        - check_level: Error or Warning
        - check_status: Success, Warning, or Error
        - constraint: Constraint description
        - constraint_status: Success or Failure
        - constraint_message: Details about failures

        Returns:
            DataFrame with verification results

        Raises:
            RuntimeError: If the Deequ plugin is not available on the server
        """
        # Build the protobuf message
        msg = proto.DeequVerificationRelation()

        # Add checks
        for check in self._checks:
            msg.checks.append(check.to_proto())

        # Add required analyzers
        for analyzer in self._analyzers:
            msg.required_analyzers.append(analyzer.to_proto())

        # Get the input DataFrame's plan as serialized bytes
        # We serialize just the Relation (plan.root), not the full Plan,
        # because Scala expects to parse it as a Relation
        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        # Wrap our Deequ message in a google.protobuf.Any
        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")

        # Create a proper LogicalPlan subclass with the extension
        plan = create_deequ_plan(extension)

        # Create DataFrame from the plan (handles Spark 3.x vs 4.x)
        return dataframe_from_plan(plan, self._spark)


class EngineVerificationRunBuilder:
    """
    Builder for configuring and executing engine-based verification.

    This class works with DuckDB and other SQL backends via the engine abstraction.
    """

    def __init__(self, engine: "BaseEngine"):
        """
        Create a new EngineVerificationRunBuilder.

        Args:
            engine: BaseEngine instance (e.g., DuckDBEngine)
        """
        self._engine = engine
        self._checks: List[Check] = []
        self._analyzers: List[_ConnectAnalyzer] = []

    def addCheck(self, check: Check) -> "EngineVerificationRunBuilder":
        """
        Add a check to run.

        Args:
            check: Check to add

        Returns:
            self for method chaining
        """
        self._checks.append(check)
        return self

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "EngineVerificationRunBuilder":
        """
        Add an analyzer to run (in addition to those required by checks).

        Args:
            analyzer: Analyzer to add

        Returns:
            self for method chaining
        """
        self._analyzers.append(analyzer)
        return self

    def run(self) -> pd.DataFrame:
        """
        Execute the verification and return results as a pandas DataFrame.

        The result DataFrame contains columns:
        - check: Check description
        - check_level: Error or Warning
        - check_status: Success, Warning, or Error
        - constraint: Constraint description
        - constraint_status: Success or Failure
        - constraint_message: Details about failures

        Returns:
            pandas DataFrame with verification results
        """
        # Run checks via engine
        results = self._engine.run_checks(self._checks)
        return self._engine.constraints_to_dataframe(results)


class AnalysisRunner:
    """
    Entry point for running analyzers without checks.

    Use this when you want to compute metrics without defining
    pass/fail constraints. Supports both engine-based and Spark-based execution.

    Example (Engine-based with DuckDB):
        from pydeequ.v2.analyzers import Size, Completeness, Mean
        import pydeequ

        engine = pydeequ.connect(con, table="my_table")
        result = (AnalysisRunner()
            .on_engine(engine)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("email"))
            .run())

    Example (Spark Connect):
        from pydeequ.v2.analyzers import Size, Completeness, Mean

        result = (AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("email"))
            .addAnalyzer(Mean("amount"))
            .run())
    """

    def __init__(self, spark: Optional["SparkSession"] = None):
        """
        Create a new AnalysisRunner.

        Args:
            spark: Optional SparkSession for Spark Connect mode.
                   Not required for engine-based execution.
        """
        self._spark = spark

    def onData(self, df: "DataFrame") -> "AnalysisRunBuilder":
        """
        Specify the DataFrame to analyze (Spark mode).

        Args:
            df: DataFrame to analyze

        Returns:
            AnalysisRunBuilder for method chaining

        Raises:
            ValueError: If SparkSession was not provided in constructor
        """
        if self._spark is None:
            raise ValueError(
                "SparkSession required for onData(). "
                "Use AnalysisRunner(spark).onData(df) or "
                "AnalysisRunner().on_engine(engine) for engine-based execution."
            )
        return AnalysisRunBuilder(self._spark, df)

    def on_engine(self, engine: "BaseEngine") -> "EngineAnalysisRunBuilder":
        """
        Specify the engine to run analysis on (Engine mode).

        Args:
            engine: BaseEngine instance (e.g., DuckDBEngine)

        Returns:
            EngineAnalysisRunBuilder for method chaining
        """
        return EngineAnalysisRunBuilder(engine)

    def on_table(self, table: str) -> "EngineAnalysisRunBuilder":
        """
        Specify a table name for engine-based analysis.

        Raises:
            ValueError: This method requires an engine
        """
        raise ValueError(
            "on_table() requires an engine. Use: "
            "AnalysisRunner().on_engine(pydeequ.connect(con, table='my_table'))"
        )


class AnalysisRunBuilder:
    """Builder for configuring and executing an analysis run."""

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        """
        Create a new AnalysisRunBuilder.

        Args:
            spark: SparkSession
            df: DataFrame to analyze
        """
        self._spark = spark
        self._df = df
        self._analyzers: List[_ConnectAnalyzer] = []

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "AnalysisRunBuilder":
        """
        Add an analyzer to run.

        Args:
            analyzer: Analyzer to add

        Returns:
            self for method chaining
        """
        self._analyzers.append(analyzer)
        return self

    def run(self) -> "DataFrame":
        """
        Execute the analysis and return metrics as DataFrame.

        Returns:
            DataFrame with computed metrics
        """
        # Build protobuf message
        msg = proto.DeequAnalysisRelation()
        for analyzer in self._analyzers:
            msg.analyzers.append(analyzer.to_proto())

        # Get the input DataFrame's plan as serialized bytes
        # We serialize just the Relation (plan.root), not the full Plan,
        # because Scala expects to parse it as a Relation
        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        # Wrap our Deequ message in a google.protobuf.Any
        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")

        # Create a proper LogicalPlan subclass with the extension
        plan = create_deequ_plan(extension)

        # Create DataFrame from the plan (handles Spark 3.x vs 4.x)
        return dataframe_from_plan(plan, self._spark)


class EngineAnalysisRunBuilder:
    """Builder for configuring and executing engine-based analysis."""

    def __init__(self, engine: "BaseEngine"):
        """
        Create a new EngineAnalysisRunBuilder.

        Args:
            engine: BaseEngine instance (e.g., DuckDBEngine)
        """
        self._engine = engine
        self._analyzers: List[_ConnectAnalyzer] = []

    def addAnalyzer(self, analyzer: _ConnectAnalyzer) -> "EngineAnalysisRunBuilder":
        """
        Add an analyzer to run.

        Args:
            analyzer: Analyzer to add

        Returns:
            self for method chaining
        """
        self._analyzers.append(analyzer)
        return self

    def run(self) -> pd.DataFrame:
        """
        Execute the analysis and return metrics as pandas DataFrame.

        Returns:
            pandas DataFrame with computed metrics
        """
        results = self._engine.compute_metrics(self._analyzers)
        return self._engine.metrics_to_dataframe(results)


# Export all public symbols
__all__ = [
    "VerificationSuite",
    "VerificationRunBuilder",
    "EngineVerificationRunBuilder",
    "AnalysisRunner",
    "AnalysisRunBuilder",
    "EngineAnalysisRunBuilder",
]
