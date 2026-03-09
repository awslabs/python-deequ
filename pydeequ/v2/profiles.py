# -*- coding: utf-8 -*-
"""
Column Profiler for PyDeequ v2.

This module provides column profiling capabilities that analyze columns
to compute statistics like completeness, data type distribution, and optional
KLL sketch-based quantile estimation.

Example usage with DuckDB:
    import duckdb
    import pydeequ
    from pydeequ.v2.profiles import ColumnProfilerRunner

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id, 'foo' as name")
    engine = pydeequ.connect(con)

    profiles = (ColumnProfilerRunner(engine)
        .onData(table="test")
        .run())

Example usage with Spark Connect:
    from pyspark.sql import SparkSession
    import pydeequ
    from pydeequ.v2.profiles import ColumnProfilerRunner

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    engine = pydeequ.connect(spark)

    profiles = (ColumnProfilerRunner(engine)
        .onData(dataframe=df)
        .withKLLProfiling()
        .run())
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

import pandas as pd

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pydeequ.engines import BaseEngine


@dataclass
class KLLParameters:
    """
    Parameters for KLL sketch-based quantile estimation.

    Attributes:
        sketch_size: Size of the KLL sketch (default: 2048)
        shrinking_factor: Factor for sketch compaction (default: 0.64)
        num_buckets: Number of histogram buckets for distribution (default: 64)
    """

    sketch_size: int = 2048
    shrinking_factor: float = 0.64
    num_buckets: int = 64

    def to_proto(self):
        """Convert to protobuf message."""
        from pydeequ.v2.proto import deequ_connect_pb2 as proto
        return proto.KLLParameters(
            sketch_size=self.sketch_size,
            shrinking_factor=self.shrinking_factor,
            number_of_buckets=self.num_buckets,
        )


class ColumnProfilerRunner:
    """
    Entry point for running column profiling.

    Takes an engine in the constructor and uses ``onData()`` to bind data.

    Example:
        profiles = (ColumnProfilerRunner(engine)
            .onData(table="users")
            .restrictToColumns(["col1", "col2"])
            .run())
    """

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine

    def onData(
        self, *, table: Optional[str] = None, dataframe: "Optional[DataFrame]" = None
    ) -> "EngineColumnProfilerRunBuilder":
        """
        Bind data for profiling.

        Args:
            table: Table name (keyword-only)
            dataframe: DataFrame (keyword-only)

        Returns:
            EngineColumnProfilerRunBuilder for method chaining
        """
        if table is not None and dataframe is not None:
            raise ValueError("Provide either 'table' or 'dataframe', not both")
        if table is not None:
            bound_engine = self._engine.for_table(table)
        elif dataframe is not None:
            bound_engine = self._engine.for_dataframe(dataframe)
        else:
            raise ValueError("Must provide either 'table' or 'dataframe'")
        return EngineColumnProfilerRunBuilder(bound_engine)


class EngineColumnProfilerRunBuilder:
    """Builder for configuring and executing engine-based column profiling."""

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine
        self._restrict_to_columns: Optional[Sequence[str]] = None
        self._low_cardinality_threshold: int = 0

    def restrictToColumns(self, columns: Sequence[str]) -> "EngineColumnProfilerRunBuilder":
        self._restrict_to_columns = columns
        return self

    def withLowCardinalityHistogramThreshold(
        self, threshold: int
    ) -> "EngineColumnProfilerRunBuilder":
        self._low_cardinality_threshold = threshold
        return self

    def run(self) -> pd.DataFrame:
        """Execute the profiling and return results as a pandas DataFrame."""
        profiles = self._engine.profile_columns(
            columns=self._restrict_to_columns,
            low_cardinality_threshold=self._low_cardinality_threshold,
        )
        return self._engine.profiles_to_dataframe(profiles)


# ---------------------------------------------------------------------------
# Private Spark Connect builder (used internally by SparkEngine)
# ---------------------------------------------------------------------------

class _SparkColumnProfilerRunBuilder:
    """Internal builder for Spark Connect profiling (protobuf-based)."""

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        self._spark = spark
        self._df = df
        self._restrict_to_columns: Optional[Sequence[str]] = None
        self._low_cardinality_threshold: int = 0
        self._enable_kll: bool = False
        self._kll_parameters: Optional[KLLParameters] = None
        self._predefined_types: Optional[Dict[str, str]] = None

    def restrictToColumns(self, columns: Sequence[str]) -> "_SparkColumnProfilerRunBuilder":
        self._restrict_to_columns = columns
        return self

    def withLowCardinalityHistogramThreshold(
        self, threshold: int
    ) -> "_SparkColumnProfilerRunBuilder":
        self._low_cardinality_threshold = threshold
        return self

    def withKLLProfiling(self) -> "_SparkColumnProfilerRunBuilder":
        self._enable_kll = True
        return self

    def setKLLParameters(self, params: KLLParameters) -> "_SparkColumnProfilerRunBuilder":
        self._kll_parameters = params
        return self

    def setPredefinedTypes(
        self, types: Dict[str, str]
    ) -> "_SparkColumnProfilerRunBuilder":
        self._predefined_types = types
        return self

    def run(self) -> "DataFrame":
        from google.protobuf import any_pb2
        from pydeequ.v2.proto import deequ_connect_pb2 as proto
        from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

        msg = proto.DeequColumnProfilerRelation()

        if self._restrict_to_columns:
            msg.restrict_to_columns.extend(self._restrict_to_columns)
        if self._low_cardinality_threshold > 0:
            msg.low_cardinality_histogram_threshold = self._low_cardinality_threshold
        msg.enable_kll_profiling = self._enable_kll
        if self._kll_parameters:
            msg.kll_parameters.CopyFrom(self._kll_parameters.to_proto())
        if self._predefined_types:
            for col, dtype in self._predefined_types.items():
                msg.predefined_types[col] = dtype

        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")
        plan = create_deequ_plan(extension)
        return dataframe_from_plan(plan, self._spark)


# Export all public symbols
__all__ = [
    "ColumnProfilerRunner",
    "EngineColumnProfilerRunBuilder",
    "KLLParameters",
]
