# -*- coding: utf-8 -*-
"""
Tests for Column Profiler functionality.

These tests verify that the Column Profiler correctly analyzes DataFrame columns
and returns expected statistics.
"""

import json

import pytest
from pyspark.sql import Row

from pydeequ.v2.profiles import ColumnProfilerRunner, KLLParameters, _SparkColumnProfilerRunBuilder


class TestBasicProfiling:
    """Test basic profiling metrics."""

    def test_completeness_calculation(self, engine, profiler_df):
        """Test completeness is correctly calculated."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        # id column is complete (8/8 = 1.0)
        assert rows["id"]["completeness"] == 1.0

        # salary has 1 null out of 8 (7/8 = 0.875)
        assert abs(rows["salary"]["completeness"] - 7 / 8) < 0.001

        # age has 1 null out of 8
        assert abs(rows["age"]["completeness"] - 7 / 8) < 0.001

    def test_data_type_inference(self, engine, profiler_df):
        """Test data types are correctly inferred."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        # Check data types contain expected type indicators
        # Deequ returns "Integral" for integer types
        assert (
            "Integral" in rows["id"]["data_type"]
            or "Integer" in rows["id"]["data_type"]
            or "Long" in rows["id"]["data_type"]
        )
        assert "String" in rows["name"]["data_type"]
        # Deequ returns "Fractional" for double types
        assert (
            "Fractional" in rows["salary"]["data_type"]
            or "Double" in rows["salary"]["data_type"]
        )
        assert "Boolean" in rows["active"]["data_type"]

    def test_approx_distinct_values(self, engine, profiler_df):
        """Test approximate distinct value count."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        # id should have 8 distinct values
        assert rows["id"]["approx_distinct_values"] == 8

        # active (boolean) should have 2 distinct values
        assert rows["active"]["approx_distinct_values"] == 2

    def test_all_columns_profiled(self, engine, profiler_df):
        """Test that all columns are profiled by default."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = result.to_dict('records')

        expected_columns = {"id", "name", "age", "salary", "active", "email", "score"}
        profiled_columns = {r["column"] for r in rows}

        assert profiled_columns == expected_columns


class TestNumericProfiling:
    """Test numeric column profiling."""

    def test_numeric_statistics(self, engine, profiler_df):
        """Test mean, min, max, sum, stddev for numeric columns."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        age_profile = rows["age"]
        # age values: 30, 25, 35, 28, None, 45, 32, 29
        # min=25, max=45
        assert age_profile["minimum"] == 25.0
        assert age_profile["maximum"] == 45.0
        assert age_profile["mean"] is not None
        assert age_profile["std_dev"] is not None

    def test_non_numeric_has_null_stats(self, engine, profiler_df):
        """Test non-numeric columns have null/NaN for numeric stats."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        name_profile = rows["name"]
        # Spark returns NaN for non-numeric stats, DuckDB returns None
        import math
        assert name_profile["mean"] is None or math.isnan(name_profile["mean"])
        assert name_profile["minimum"] is None or math.isnan(name_profile["minimum"])
        assert name_profile["maximum"] is None or math.isnan(name_profile["maximum"])


class TestKLLProfiling:
    """Test KLL sketch profiling (Spark-specific, uses internal builder)."""

    def test_kll_disabled_by_default(self, engine, profiler_df):
        """Test KLL is not computed by default."""
        result = ColumnProfilerRunner(engine).onData(dataframe=profiler_df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        assert rows["age"]["kll_buckets"] is None

    def test_kll_enabled(self, spark, profiler_df):
        """Test KLL buckets are computed when enabled."""
        result = (
            _SparkColumnProfilerRunBuilder(spark, profiler_df).withKLLProfiling().run()
        )
        rows = {r["column"]: r for r in result.collect()}

        # Numeric columns should have KLL buckets
        assert rows["age"]["kll_buckets"] is not None
        assert rows["salary"]["kll_buckets"] is not None
        # Non-numeric should not
        assert rows["name"]["kll_buckets"] is None

    def test_kll_custom_parameters(self, spark, profiler_df):
        """Test custom KLL parameters are applied."""
        params = KLLParameters(sketch_size=1024, shrinking_factor=0.5, num_buckets=32)
        result = (
            _SparkColumnProfilerRunBuilder(spark, profiler_df)
            .withKLLProfiling()
            .setKLLParameters(params)
            .run()
        )
        # Just verify it runs without error
        assert result.count() > 0


class TestProfilerOptions:
    """Test profiler configuration options."""

    def test_restrict_to_columns(self, engine, profiler_df):
        """Test restricting profiling to specific columns."""
        result = (
            ColumnProfilerRunner(engine)
            .onData(dataframe=profiler_df)
            .restrictToColumns(["id", "name"])
            .run()
        )

        columns = [r["column"] for r in result.to_dict('records')]
        assert set(columns) == {"id", "name"}

    def test_low_cardinality_histogram(self, engine, profiler_df):
        """Test histogram is computed for low cardinality columns."""
        result = (
            ColumnProfilerRunner(engine)
            .onData(dataframe=profiler_df)
            .withLowCardinalityHistogramThreshold(10)
            .run()
        )
        rows = {r["column"]: r for r in result.to_dict('records')}

        # active (2 values) should have histogram
        assert rows["active"]["histogram"] is not None
        # Verify histogram is valid JSON
        histogram = json.loads(rows["active"]["histogram"])
        assert len(histogram) > 0

    def test_predefined_types(self, spark, profiler_df):
        """Test predefined types override inference (Spark-specific)."""
        result = (
            _SparkColumnProfilerRunBuilder(spark, profiler_df)
            .setPredefinedTypes({"id": "String"})
            .run()
        )
        rows = {r["column"]: r for r in result.collect()}

        assert rows["id"]["is_data_type_inferred"] is False


class TestProfilerEdgeCases:
    """Test edge cases for profiler."""

    def test_all_null_column(self, spark, engine):
        """Test profiling column with all nulls."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("value", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [(1, None), (2, None)],
            schema=schema,
        )
        result = ColumnProfilerRunner(engine).onData(dataframe=df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        assert rows["value"]["completeness"] == 0.0

    def test_single_row(self, spark, engine):
        """Test profiling single row DataFrame."""
        df = spark.createDataFrame([Row(id=1, value=100)])
        result = ColumnProfilerRunner(engine).onData(dataframe=df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        assert rows["value"]["minimum"] == 100.0
        assert rows["value"]["maximum"] == 100.0
        assert rows["value"]["completeness"] == 1.0

    def test_large_dataframe(self, spark, engine):
        """Test profiling larger DataFrame."""
        df = spark.createDataFrame(
            [Row(id=i, value=i * 10, category=f"cat_{i % 5}") for i in range(1000)]
        )
        result = ColumnProfilerRunner(engine).onData(dataframe=df).run()
        rows = {r["column"]: r for r in result.to_dict('records')}

        # Allow some approximation error for HyperLogLog-based distinct count
        assert rows["id"]["approx_distinct_values"] >= 950
        assert rows["category"]["approx_distinct_values"] == 5


class TestKLLParametersUnit:
    """Unit tests for KLLParameters (no Spark needed)."""

    def test_default_parameters(self):
        """Test default KLL parameters."""
        params = KLLParameters()
        assert params.sketch_size == 2048
        assert params.shrinking_factor == 0.64
        assert params.num_buckets == 64

    def test_custom_parameters(self):
        """Test custom KLL parameters."""
        params = KLLParameters(sketch_size=1024, shrinking_factor=0.5, num_buckets=32)
        assert params.sketch_size == 1024
        assert params.shrinking_factor == 0.5
        assert params.num_buckets == 32

    def test_to_proto(self):
        """Test conversion to protobuf."""
        params = KLLParameters(sketch_size=512, shrinking_factor=0.7, num_buckets=16)
        proto_msg = params.to_proto()

        # Proto uses snake_case field names
        assert proto_msg.sketch_size == 512
        assert proto_msg.shrinking_factor == 0.7
        assert proto_msg.number_of_buckets == 16
