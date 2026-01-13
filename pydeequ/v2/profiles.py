# -*- coding: utf-8 -*-
"""
Column Profiler for Deequ Spark Connect.

This module provides column profiling capabilities that analyze DataFrame columns
to compute statistics like completeness, data type distribution, and optional
KLL sketch-based quantile estimation.

Example usage:
    from pyspark.sql import SparkSession
    from pydeequ.v2.profiles import ColumnProfilerRunner, KLLParameters

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Basic profiling
    profiles = (ColumnProfilerRunner(spark)
        .onData(df)
        .run())

    # With KLL profiling for quantile estimation
    profiles = (ColumnProfilerRunner(spark)
        .onData(df)
        .withKLLProfiling()
        .setKLLParameters(KLLParameters(sketch_size=2048))
        .run())

    profiles.show()  # Result is a DataFrame with one row per column
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Optional, Sequence

from google.protobuf import any_pb2

from pydeequ.v2.proto import deequ_connect_pb2 as proto

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _create_deequ_plan(extension: any_pb2.Any):
    """
    Create a LogicalPlan subclass for Deequ that properly integrates with PySpark.
    """
    import pyspark.sql.connect.proto as spark_proto
    from pyspark.sql.connect.plan import LogicalPlan

    class _DeequExtensionPlan(LogicalPlan):
        """Custom LogicalPlan for Deequ profiler operations."""

        def __init__(self, ext: any_pb2.Any):
            super().__init__(child=None)
            self._extension = ext

        def plan(self, session) -> spark_proto.Relation:
            """Return the Relation proto for this plan."""
            rel = self._create_proto_relation()
            rel.extension.CopyFrom(self._extension)
            return rel

        def __repr__(self) -> str:
            return "DeequProfilerPlan"

    return _DeequExtensionPlan(extension)


@dataclass
class KLLParameters:
    """
    Parameters for KLL sketch-based quantile estimation.

    KLL sketches provide memory-efficient approximate quantile computation
    for numeric columns.

    Attributes:
        sketch_size: Size of the KLL sketch (default: 2048)
        shrinking_factor: Factor for sketch compaction (default: 0.64)
        num_buckets: Number of histogram buckets for distribution (default: 64)
    """

    sketch_size: int = 2048
    shrinking_factor: float = 0.64
    num_buckets: int = 64

    def to_proto(self) -> proto.KLLParameters:
        """Convert to protobuf message."""
        return proto.KLLParameters(
            sketch_size=self.sketch_size,
            shrinking_factor=self.shrinking_factor,
            number_of_buckets=self.num_buckets,
        )


class ColumnProfilerRunner:
    """
    Entry point for running column profiling.

    ColumnProfilerRunner analyzes DataFrame columns to compute statistics
    including completeness, data type, distinct values, and optionally
    KLL sketches for numeric columns.

    Example:
        profiles = (ColumnProfilerRunner(spark)
            .onData(df)
            .restrictToColumns(["col1", "col2"])
            .withKLLProfiling()
            .run())
    """

    def __init__(self, spark: "SparkSession"):
        """
        Create a new ColumnProfilerRunner.

        Args:
            spark: SparkSession (can be either local or Spark Connect)
        """
        self._spark = spark

    def onData(self, df: "DataFrame") -> "ColumnProfilerRunBuilder":
        """
        Specify the DataFrame to profile.

        Args:
            df: DataFrame to profile

        Returns:
            ColumnProfilerRunBuilder for method chaining
        """
        return ColumnProfilerRunBuilder(self._spark, df)


class ColumnProfilerRunBuilder:
    """
    Builder for configuring and executing a column profiling run.

    This class collects profiling options and executes the profiling
    when run() is called.
    """

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        """
        Create a new ColumnProfilerRunBuilder.

        Args:
            spark: SparkSession
            df: DataFrame to profile
        """
        self._spark = spark
        self._df = df
        self._restrict_to_columns: Optional[Sequence[str]] = None
        self._low_cardinality_threshold: int = 0
        self._enable_kll: bool = False
        self._kll_parameters: Optional[KLLParameters] = None
        self._predefined_types: Optional[Dict[str, str]] = None

    def restrictToColumns(self, columns: Sequence[str]) -> "ColumnProfilerRunBuilder":
        """
        Restrict profiling to specific columns.

        Args:
            columns: List of column names to profile

        Returns:
            self for method chaining
        """
        self._restrict_to_columns = columns
        return self

    def withLowCardinalityHistogramThreshold(
        self, threshold: int
    ) -> "ColumnProfilerRunBuilder":
        """
        Set threshold for computing histograms.

        Columns with distinct values <= threshold will have histograms computed.

        Args:
            threshold: Maximum distinct values for histogram computation

        Returns:
            self for method chaining
        """
        self._low_cardinality_threshold = threshold
        return self

    def withKLLProfiling(self) -> "ColumnProfilerRunBuilder":
        """
        Enable KLL sketch profiling for numeric columns.

        KLL sketches provide approximate quantile estimation.

        Returns:
            self for method chaining
        """
        self._enable_kll = True
        return self

    def setKLLParameters(self, params: KLLParameters) -> "ColumnProfilerRunBuilder":
        """
        Set KLL sketch parameters.

        Args:
            params: KLLParameters configuration

        Returns:
            self for method chaining
        """
        self._kll_parameters = params
        return self

    def setPredefinedTypes(
        self, types: Dict[str, str]
    ) -> "ColumnProfilerRunBuilder":
        """
        Set predefined data types for columns.

        This overrides automatic type inference for specified columns.

        Args:
            types: Dictionary mapping column names to type names.
                   Supported types: "String", "Integer", "Long", "Double", "Boolean"

        Returns:
            self for method chaining
        """
        self._predefined_types = types
        return self

    def run(self) -> "DataFrame":
        """
        Execute the profiling and return results as a DataFrame.

        The result DataFrame contains columns:
        - column: Column name
        - completeness: Non-null ratio (0.0-1.0)
        - approx_distinct_values: Approximate cardinality
        - data_type: Detected/provided type
        - is_data_type_inferred: Whether type was inferred
        - type_counts: JSON string of type counts
        - histogram: JSON string of histogram (or null)
        - mean, minimum, maximum, sum, std_dev: Numeric stats (null for non-numeric)
        - approx_percentiles: JSON array of percentiles (null if not computed)
        - kll_buckets: JSON string of KLL buckets (null if KLL disabled)

        Returns:
            DataFrame with profiling results (one row per column)

        Raises:
            RuntimeError: If the Deequ plugin is not available on the server
        """
        # Build the protobuf message
        profiler_msg = self._build_profiler_message()

        # V2 only supports Spark Connect
        return self._run_via_spark_connect(profiler_msg)

    def _build_profiler_message(self) -> proto.DeequColumnProfilerRelation:
        """Build the protobuf profiler message."""
        msg = proto.DeequColumnProfilerRelation()

        # Set column restrictions
        if self._restrict_to_columns:
            msg.restrict_to_columns.extend(self._restrict_to_columns)

        # Set histogram threshold
        if self._low_cardinality_threshold > 0:
            msg.low_cardinality_histogram_threshold = self._low_cardinality_threshold

        # Set KLL profiling
        msg.enable_kll_profiling = self._enable_kll
        if self._kll_parameters:
            msg.kll_parameters.CopyFrom(self._kll_parameters.to_proto())

        # Set predefined types
        if self._predefined_types:
            for col, dtype in self._predefined_types.items():
                msg.predefined_types[col] = dtype

        return msg

    def _run_via_spark_connect(
        self, msg: proto.DeequColumnProfilerRelation
    ) -> "DataFrame":
        """Execute profiling via Spark Connect plugin."""
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame

        # Get the input DataFrame's plan as serialized bytes
        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        # Wrap our Deequ message in a google.protobuf.Any
        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")

        # Create a proper LogicalPlan subclass with the extension
        plan = _create_deequ_plan(extension)

        # Use DataFrame.withPlan to properly create the DataFrame
        return ConnectDataFrame.withPlan(plan, session=self._spark)


# Export all public symbols
__all__ = [
    "ColumnProfilerRunner",
    "ColumnProfilerRunBuilder",
    "KLLParameters",
]
