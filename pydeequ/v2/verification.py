# -*- coding: utf-8 -*-
"""
VerificationSuite for Deequ Spark Connect.

This module provides the main entry point for running data quality checks
via Spark Connect. It builds protobuf messages and sends them to the
server-side Deequ plugin.

Example usage:
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

from typing import TYPE_CHECKING, List

from google.protobuf import any_pb2

from pydeequ.v2.analyzers import _ConnectAnalyzer
from pydeequ.v2.checks import Check
from pydeequ.v2.proto import deequ_connect_pb2 as proto
from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class VerificationSuite:
    """
    Main entry point for running data quality verification.

    VerificationSuite allows you to define checks and analyzers to run
    on a DataFrame. When run() is called, the checks and analyzers are
    serialized to protobuf and sent to the Spark Connect server where
    the Deequ plugin executes them.

    Example:
        suite = VerificationSuite(spark)
        result = (suite
            .onData(df)
            .addCheck(check)
            .run())
    """

    def __init__(self, spark: "SparkSession"):
        """
        Create a new VerificationSuite.

        Args:
            spark: SparkSession connected via Spark Connect
        """
        self._spark = spark

    def onData(self, df: "DataFrame") -> "VerificationRunBuilder":
        """
        Specify the DataFrame to run verification on.

        Args:
            df: DataFrame to verify

        Returns:
            VerificationRunBuilder for method chaining
        """
        return VerificationRunBuilder(self._spark, df)


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


class AnalysisRunner:
    """
    Entry point for running analyzers without checks.

    Use this when you want to compute metrics without defining
    pass/fail constraints.

    Example:
        from pydeequ.v2.analyzers import Size, Completeness, Mean

        result = (AnalysisRunner(spark)
            .onData(df)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("email"))
            .addAnalyzer(Mean("amount"))
            .run())
    """

    def __init__(self, spark: "SparkSession"):
        """
        Create a new AnalysisRunner.

        Args:
            spark: SparkSession connected via Spark Connect
        """
        self._spark = spark

    def onData(self, df: "DataFrame") -> "AnalysisRunBuilder":
        """
        Specify the DataFrame to analyze.

        Args:
            df: DataFrame to analyze

        Returns:
            AnalysisRunBuilder for method chaining
        """
        return AnalysisRunBuilder(self._spark, df)


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


# Export all public symbols
__all__ = [
    "VerificationSuite",
    "VerificationRunBuilder",
    "AnalysisRunner",
    "AnalysisRunBuilder",
]
