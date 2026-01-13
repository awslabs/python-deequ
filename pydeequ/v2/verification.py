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

from typing import TYPE_CHECKING, List, Optional

from google.protobuf import any_pb2

from pydeequ.v2.analyzers import _ConnectAnalyzer
from pydeequ.v2.checks import Check
from pydeequ.v2.proto import deequ_connect_pb2 as proto

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.connect.client import SparkConnectClient


def _create_deequ_plan(extension: any_pb2.Any):
    """
    Create a LogicalPlan subclass for Deequ that properly integrates with PySpark.

    We dynamically import and subclass LogicalPlan to avoid import issues
    when Spark Connect is not available.
    """
    import pyspark.sql.connect.proto as spark_proto
    from pyspark.sql.connect.plan import LogicalPlan

    class _DeequExtensionPlan(LogicalPlan):
        """
        Custom LogicalPlan for Deequ operations via Spark Connect.

        This plan wraps our protobuf message as a Relation extension,
        which is sent to the server and handled by DeequRelationPlugin.
        """

        def __init__(self, ext: any_pb2.Any):
            # Pass None as child - this is a leaf node
            super().__init__(child=None)
            self._extension = ext

        def plan(self, session: "SparkConnectClient") -> spark_proto.Relation:
            """Return the Relation proto for this plan."""
            rel = self._create_proto_relation()
            rel.extension.CopyFrom(self._extension)
            return rel

        def __repr__(self) -> str:
            return "DeequExtensionPlan"

    return _DeequExtensionPlan(extension)


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
            spark: SparkSession (can be either local or Spark Connect)
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
        verification_msg = self._build_verification_message()

        # Check if we're using Spark Connect or local Spark
        if self._is_spark_connect():
            return self._run_via_spark_connect(verification_msg)
        else:
            return self._run_via_local_spark(verification_msg)

    def _build_verification_message(self) -> proto.DeequVerificationRelation:
        """Build the protobuf verification message."""
        msg = proto.DeequVerificationRelation()

        # Add checks
        for check in self._checks:
            msg.checks.append(check.to_proto())

        # Add required analyzers
        for analyzer in self._analyzers:
            msg.required_analyzers.append(analyzer.to_proto())

        return msg

    def _is_spark_connect(self) -> bool:
        """Check if we're using Spark Connect."""
        # Spark Connect sessions have a different class
        session_class = type(self._spark).__name__
        # Also check for the remote attribute
        return (
            session_class == "SparkSession"
            and hasattr(self._spark, "_client")
            and self._spark._client is not None
        )

    def _run_via_spark_connect(
        self, msg: proto.DeequVerificationRelation
    ) -> "DataFrame":
        """
        Execute verification via Spark Connect plugin.

        This sends the protobuf message to the Deequ RelationPlugin
        on the Spark Connect server.
        """
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        # Get the input DataFrame's plan as serialized bytes
        # We serialize just the Relation (plan.root), not the full Plan,
        # because Scala expects to parse it as a Relation
        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        # Wrap our Deequ message in a google.protobuf.Any
        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")

        # Create a proper LogicalPlan subclass with the extension
        plan = _create_deequ_plan(extension)

        # Use DataFrame.withPlan to properly create the DataFrame
        return ConnectDataFrame.withPlan(plan, session=self._spark)

    def _run_via_local_spark(self, msg: proto.DeequVerificationRelation) -> "DataFrame":
        """
        Execute verification using local Spark (fallback for development).

        This uses the legacy Py4J bridge to call Deequ directly.
        This is useful for testing the Python API before the Spark Connect
        plugin is fully implemented.
        """
        # Import legacy modules
        from pydeequ.checks import Check as LegacyCheck
        from pydeequ.checks import CheckLevel as LegacyCheckLevel
        from pydeequ.verification import VerificationResult as LegacyVerificationResult
        from pydeequ.verification import VerificationSuite as LegacyVerificationSuite

        # Build legacy verification run
        legacy_suite = LegacyVerificationSuite(self._spark)
        run_builder = legacy_suite.onData(self._df)

        # Convert protobuf checks to legacy checks
        for check_proto in msg.checks:
            legacy_check = self._convert_check_proto_to_legacy(check_proto)
            run_builder = run_builder.addCheck(legacy_check)

        # Run and get results
        result = run_builder.run()

        # Return as DataFrame
        return LegacyVerificationResult.checkResultsAsDataFrame(self._spark, result)

    def _convert_check_proto_to_legacy(
        self, check_proto: proto.CheckMessage
    ) -> "LegacyCheck":
        """Convert a protobuf Check to a legacy Py4J Check."""
        from pydeequ.checks import Check as LegacyCheck
        from pydeequ.checks import CheckLevel as LegacyCheckLevel
        from pydeequ.scala_utils import ScalaFunction1

        # Map level
        level = (
            LegacyCheckLevel.Error
            if check_proto.level == proto.CheckMessage.Level.ERROR
            else LegacyCheckLevel.Warning
        )

        # Create legacy check
        legacy_check = LegacyCheck(self._spark, level, check_proto.description)

        # Add constraints
        for constraint in check_proto.constraints:
            legacy_check = self._add_legacy_constraint(legacy_check, constraint)

        return legacy_check

    def _add_legacy_constraint(
        self, check: "LegacyCheck", constraint: proto.ConstraintMessage
    ) -> "LegacyCheck":
        """Add a constraint to a legacy check by converting from protobuf."""

        # Build assertion function from predicate
        assertion = self._build_assertion_lambda(constraint.assertion)
        hint = constraint.hint if constraint.hint else None

        constraint_type = constraint.type

        # Map constraint types to legacy methods
        if constraint_type == "hasSize":
            return check.hasSize(assertion, hint)

        elif constraint_type == "isComplete":
            return check.isComplete(constraint.column, hint)

        elif constraint_type == "hasCompleteness":
            return check.hasCompleteness(constraint.column, assertion, hint)

        elif constraint_type == "areComplete":
            return check.areComplete(list(constraint.columns), hint)

        elif constraint_type == "haveCompleteness":
            return check.haveCompleteness(list(constraint.columns), assertion, hint)

        elif constraint_type == "isUnique":
            return check.isUnique(constraint.column, hint)

        elif constraint_type == "hasUniqueness":
            return check.hasUniqueness(list(constraint.columns), assertion, hint)

        elif constraint_type == "hasDistinctness":
            return check.hasDistinctness(list(constraint.columns), assertion, hint)

        elif constraint_type == "hasUniqueValueRatio":
            return check.hasUniqueValueRatio(list(constraint.columns), assertion, hint)

        elif constraint_type == "hasMin":
            return check.hasMin(constraint.column, assertion, hint)

        elif constraint_type == "hasMax":
            return check.hasMax(constraint.column, assertion, hint)

        elif constraint_type == "hasMean":
            return check.hasMean(constraint.column, assertion, hint)

        elif constraint_type == "hasSum":
            return check.hasSum(constraint.column, assertion, hint)

        elif constraint_type == "hasStandardDeviation":
            return check.hasStandardDeviation(constraint.column, assertion, hint)

        elif constraint_type == "hasApproxCountDistinct":
            return check.hasApproxCountDistinct(constraint.column, assertion, hint)

        elif constraint_type == "hasApproxQuantile":
            return check.hasApproxQuantile(
                constraint.column, constraint.quantile, assertion, hint
            )

        elif constraint_type == "hasCorrelation":
            cols = list(constraint.columns)
            return check.hasCorrelation(cols[0], cols[1], assertion, hint)

        elif constraint_type == "hasEntropy":
            return check.hasEntropy(constraint.column, assertion, hint)

        elif constraint_type == "hasMutualInformation":
            cols = list(constraint.columns)
            return check.hasMutualInformation(cols[0], cols[1], assertion, hint)

        elif constraint_type == "hasMinLength":
            return check.hasMinLength(constraint.column, assertion, hint)

        elif constraint_type == "hasMaxLength":
            return check.hasMaxLength(constraint.column, assertion, hint)

        elif constraint_type == "hasPattern":
            return check.hasPattern(
                constraint.column, constraint.pattern, assertion, hint=hint
            )

        elif constraint_type == "containsEmail":
            return check.containsEmail(constraint.column, assertion, hint)

        elif constraint_type == "containsURL":
            return check.containsURL(constraint.column, assertion, hint)

        elif constraint_type == "containsCreditCardNumber":
            return check.containsCreditCardNumber(constraint.column, assertion, hint)

        elif constraint_type == "containsSocialSecurityNumber":
            return check.containsSocialSecurityNumber(
                constraint.column, assertion, hint
            )

        elif constraint_type == "isPositive":
            return check.isPositive(constraint.column, assertion, hint)

        elif constraint_type == "isNonNegative":
            return check.isNonNegative(constraint.column, assertion, hint)

        elif constraint_type == "isLessThan":
            cols = list(constraint.columns)
            return check.isLessThan(cols[0], cols[1], assertion, hint)

        elif constraint_type == "isLessThanOrEqualTo":
            cols = list(constraint.columns)
            return check.isLessThanOrEqualTo(cols[0], cols[1], assertion, hint)

        elif constraint_type == "isGreaterThan":
            cols = list(constraint.columns)
            return check.isGreaterThan(cols[0], cols[1], assertion, hint)

        elif constraint_type == "isGreaterThanOrEqualTo":
            cols = list(constraint.columns)
            return check.isGreaterThanOrEqualTo(cols[0], cols[1], assertion, hint)

        elif constraint_type == "isContainedIn":
            return check.isContainedIn(
                constraint.column, list(constraint.allowed_values), assertion, hint
            )

        elif constraint_type == "satisfies":
            return check.satisfies(
                constraint.column_condition, constraint.constraint_name, assertion, hint
            )

        else:
            raise ValueError(f"Unknown constraint type: {constraint_type}")

    def _build_assertion_lambda(self, predicate: proto.PredicateMessage):
        """Convert a predicate protobuf to a Python lambda function."""
        op = predicate.operator
        value = predicate.value
        lower = predicate.lower_bound
        upper = predicate.upper_bound

        if op == proto.PredicateMessage.Operator.EQ:
            return lambda x: x == value
        elif op == proto.PredicateMessage.Operator.NE:
            return lambda x: x != value
        elif op == proto.PredicateMessage.Operator.GT:
            return lambda x: x > value
        elif op == proto.PredicateMessage.Operator.GE:
            return lambda x: x >= value
        elif op == proto.PredicateMessage.Operator.LT:
            return lambda x: x < value
        elif op == proto.PredicateMessage.Operator.LE:
            return lambda x: x <= value
        elif op == proto.PredicateMessage.Operator.BETWEEN:
            return lambda x: lower <= x <= upper
        else:
            # Default to is_one behavior
            return lambda x: x == 1.0


class AnalysisRunner:
    """
    Entry point for running analyzers without checks.

    Use this when you want to compute metrics without defining
    pass/fail constraints.

    Example:
        from pydeequ.connect.analyzers import Size, Completeness, Mean

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
            spark: SparkSession
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

        # Check if we're using Spark Connect or local Spark
        if self._is_spark_connect():
            return self._run_via_spark_connect(msg)
        else:
            return self._run_via_local_spark(msg)

    def _is_spark_connect(self) -> bool:
        """Check if we're using Spark Connect."""
        session_class = type(self._spark).__name__
        return (
            session_class == "SparkSession"
            and hasattr(self._spark, "_client")
            and self._spark._client is not None
        )

    def _run_via_spark_connect(self, msg: proto.DeequAnalysisRelation) -> "DataFrame":
        """Execute analysis via Spark Connect plugin."""
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        # Get the input DataFrame's plan as serialized bytes
        # We serialize just the Relation (plan.root), not the full Plan,
        # because Scala expects to parse it as a Relation
        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        # Wrap our Deequ message in a google.protobuf.Any
        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")

        # Create a proper LogicalPlan subclass with the extension
        plan = _create_deequ_plan(extension)

        # Use DataFrame.withPlan to properly create the DataFrame
        return ConnectDataFrame.withPlan(plan, session=self._spark)

    def _run_via_local_spark(self, msg: proto.DeequAnalysisRelation):
        """Execute analysis using local Spark."""
        from pydeequ.analyzers import AnalysisRunner as LegacyAnalysisRunner

        legacy_runner = LegacyAnalysisRunner(self._spark)
        run_builder = legacy_runner.onData(self._df)

        # Convert protobuf analyzers to legacy analyzers
        for analyzer_proto in msg.analyzers:
            legacy_analyzer = self._convert_analyzer_proto_to_legacy(analyzer_proto)
            if legacy_analyzer:
                run_builder = run_builder.addAnalyzer(legacy_analyzer)

        return run_builder.run()

    def _convert_analyzer_proto_to_legacy(self, analyzer_proto: proto.AnalyzerMessage):
        """Convert a protobuf Analyzer to a legacy Py4J Analyzer."""
        from pydeequ import analyzers as legacy_analyzers

        analyzer_type = analyzer_proto.type
        column = analyzer_proto.column
        columns = list(analyzer_proto.columns)
        where = analyzer_proto.where if analyzer_proto.where else None

        if analyzer_type == "Size":
            return legacy_analyzers.Size(where)
        elif analyzer_type == "Completeness":
            return legacy_analyzers.Completeness(column, where)
        elif analyzer_type == "Mean":
            return legacy_analyzers.Mean(column, where)
        elif analyzer_type == "Sum":
            return legacy_analyzers.Sum(column, where)
        elif analyzer_type == "Maximum":
            return legacy_analyzers.Maximum(column, where)
        elif analyzer_type == "Minimum":
            return legacy_analyzers.Minimum(column, where)
        elif analyzer_type == "StandardDeviation":
            return legacy_analyzers.StandardDeviation(column, where)
        elif analyzer_type == "Distinctness":
            return legacy_analyzers.Distinctness(columns, where)
        elif analyzer_type == "Uniqueness":
            return legacy_analyzers.Uniqueness(columns, where)
        elif analyzer_type == "UniqueValueRatio":
            return legacy_analyzers.UniqueValueRatio(columns, where)
        elif analyzer_type == "CountDistinct":
            return legacy_analyzers.CountDistinct(columns)
        elif analyzer_type == "ApproxCountDistinct":
            return legacy_analyzers.ApproxCountDistinct(column, where)
        elif analyzer_type == "ApproxQuantile":
            return legacy_analyzers.ApproxQuantile(
                column, analyzer_proto.quantile, analyzer_proto.relative_error, where
            )
        elif analyzer_type == "Correlation":
            return legacy_analyzers.Correlation(columns[0], columns[1], where)
        elif analyzer_type == "MutualInformation":
            return legacy_analyzers.MutualInformation(columns, where)
        elif analyzer_type == "MaxLength":
            return legacy_analyzers.MaxLength(column, where)
        elif analyzer_type == "MinLength":
            return legacy_analyzers.MinLength(column, where)
        elif analyzer_type == "PatternMatch":
            return legacy_analyzers.PatternMatch(
                column, analyzer_proto.pattern, where=where
            )
        elif analyzer_type == "Compliance":
            return legacy_analyzers.Compliance(column, analyzer_proto.pattern, where)
        elif analyzer_type == "Entropy":
            return legacy_analyzers.Entropy(column, where)
        elif analyzer_type == "Histogram":
            return legacy_analyzers.Histogram(
                column,
                max_detail_bins=analyzer_proto.max_detail_bins
                if analyzer_proto.max_detail_bins
                else None,
                where=where,
            )
        elif analyzer_type == "DataType":
            return legacy_analyzers.DataType(column, where)
        else:
            print(f"Warning: Unknown analyzer type: {analyzer_type}")
            return None


class AnalyzerContext:
    """
    Helper class for working with analyzer results.
    """

    @classmethod
    def successMetricsAsDataFrame(
        cls, spark: "SparkSession", analyzer_context, pandas: bool = False
    ) -> "DataFrame":
        """
        Get successful metrics as a DataFrame.

        Args:
            spark: SparkSession
            analyzer_context: Result from AnalysisRunner.run()
            pandas: If True, return a pandas DataFrame

        Returns:
            DataFrame with metrics
        """
        from pydeequ.analyzers import AnalyzerContext as LegacyAnalyzerContext

        return LegacyAnalyzerContext.successMetricsAsDataFrame(
            spark, analyzer_context, pandas=pandas
        )

    @classmethod
    def successMetricsAsJson(cls, spark: "SparkSession", analyzer_context):
        """
        Get successful metrics as JSON.

        Args:
            spark: SparkSession
            analyzer_context: Result from AnalysisRunner.run()

        Returns:
            JSON representation of metrics
        """
        from pydeequ.analyzers import AnalyzerContext as LegacyAnalyzerContext

        return LegacyAnalyzerContext.successMetricsAsJson(spark, analyzer_context)


# Export all public symbols
__all__ = [
    "VerificationSuite",
    "VerificationRunBuilder",
    "AnalysisRunner",
    "AnalysisRunBuilder",
    "AnalyzerContext",
]
