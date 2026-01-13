# -*- coding: utf-8 -*-
"""
Tests for Analyzers using Spark Connect.

These tests verify the core analyzer functionality of PyDeequ v2.
"""

import pytest
from pyspark.sql import Row

from pydeequ.v2.verification import AnalysisRunner
from pydeequ.v2.analyzers import (
    Size,
    Completeness,
    Mean,
    Sum,
    Minimum,
    Maximum,
    StandardDeviation,
    ApproxCountDistinct,
    Distinctness,
    Uniqueness,
    UniqueValueRatio,
    Entropy,
    MinLength,
    MaxLength,
    Correlation,
    ApproxQuantile,
    PatternMatch,
    Compliance,
)


class TestBasicAnalyzers:
    """Test basic analyzer types."""

    def test_size(self, spark, sample_df):
        """Test Size analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Size()).run()

        rows = result.collect()
        size_row = [r for r in rows if r["name"] == "Size"][0]
        assert size_row["value"] == 3.0

    def test_completeness(self, spark, sample_df):
        """Test Completeness analyzer on complete column."""
        result = (
            AnalysisRunner(spark).onData(sample_df).addAnalyzer(Completeness("a")).run()
        )

        rows = result.collect()
        assert rows[0]["value"] == 1.0

    def test_completeness_with_nulls(self, spark, sample_df):
        """Test Completeness analyzer on column with nulls."""
        result = (
            AnalysisRunner(spark).onData(sample_df).addAnalyzer(Completeness("c")).run()
        )

        rows = result.collect()
        assert abs(rows[0]["value"] - 2 / 3) < 0.001

    def test_mean(self, spark, sample_df):
        """Test Mean analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Mean("b")).run()

        rows = result.collect()
        assert rows[0]["value"] == 2.0

    def test_sum(self, spark, sample_df):
        """Test Sum analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Sum("b")).run()

        rows = result.collect()
        assert rows[0]["value"] == 6.0

    def test_minimum(self, spark, sample_df):
        """Test Minimum analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Minimum("b")).run()

        rows = result.collect()
        assert rows[0]["value"] == 1.0

    def test_maximum(self, spark, sample_df):
        """Test Maximum analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Maximum("b")).run()

        rows = result.collect()
        assert rows[0]["value"] == 3.0

    def test_standard_deviation(self, spark, sample_df):
        """Test StandardDeviation analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(StandardDeviation("b"))
            .run()
        )

        rows = result.collect()
        # std of [1,2,3] is approximately 0.816
        assert abs(rows[0]["value"] - 0.816496580927726) < 0.001


class TestDistinctnessAnalyzers:
    """Test distinctness-related analyzers."""

    def test_approx_count_distinct(self, spark, sample_df):
        """Test ApproxCountDistinct analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(ApproxCountDistinct("b"))
            .run()
        )

        rows = result.collect()
        assert rows[0]["value"] == 3.0

    def test_distinctness(self, spark, sample_df):
        """Test Distinctness analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Distinctness(["b"]))
            .run()
        )

        rows = result.collect()
        assert rows[0]["value"] == 1.0  # All values are distinct

    def test_distinctness_non_unique(self, spark, sample_df):
        """Test Distinctness analyzer on non-unique column."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Distinctness(["d"]))
            .run()
        )

        rows = result.collect()
        # Column d has all same values, so 1 distinct / 3 rows = 1/3
        assert abs(rows[0]["value"] - 1 / 3) < 0.001

    def test_uniqueness(self, spark, sample_df):
        """Test Uniqueness analyzer."""
        result = (
            AnalysisRunner(spark).onData(sample_df).addAnalyzer(Uniqueness(["b"])).run()
        )

        rows = result.collect()
        assert rows[0]["value"] == 1.0

    def test_unique_value_ratio(self, spark, sample_df):
        """Test UniqueValueRatio analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(UniqueValueRatio(["b"]))
            .run()
        )

        rows = result.collect()
        assert rows[0]["value"] == 1.0


class TestStringAnalyzers:
    """Test string-related analyzers."""

    def test_min_length(self, spark, sample_df):
        """Test MinLength analyzer."""
        result = (
            AnalysisRunner(spark).onData(sample_df).addAnalyzer(MinLength("a")).run()
        )

        rows = result.collect()
        # "foo", "bar", "baz" all have length 3
        assert rows[0]["value"] == 3.0

    def test_max_length(self, spark, sample_df):
        """Test MaxLength analyzer."""
        result = (
            AnalysisRunner(spark).onData(sample_df).addAnalyzer(MaxLength("a")).run()
        )

        rows = result.collect()
        assert rows[0]["value"] == 3.0

    def test_pattern_match(self, spark, sample_df):
        """Test PatternMatch analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(PatternMatch("a", r"ba.*"))
            .run()
        )

        rows = result.collect()
        # "bar" and "baz" match, "foo" doesn't = 2/3
        assert abs(rows[0]["value"] - 2 / 3) < 0.001


class TestStatisticalAnalyzers:
    """Test statistical analyzers."""

    def test_entropy(self, spark, sample_df):
        """Test Entropy analyzer."""
        result = AnalysisRunner(spark).onData(sample_df).addAnalyzer(Entropy("a")).run()

        rows = result.collect()
        # 3 distinct values with equal frequency -> log(3) ~ 1.099
        assert abs(rows[0]["value"] - 1.0986122886681096) < 0.001

    def test_correlation(self, spark, sample_df):
        """Test Correlation analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Correlation("b", "c"))
            .run()
        )

        rows = result.collect()
        # b=[1,2,3], c=[5,6,None] -> perfect correlation on non-null pairs
        assert rows[0]["value"] == 1.0

    def test_approx_quantile(self, spark, sample_df):
        """Test ApproxQuantile analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(ApproxQuantile("b", 0.5))
            .run()
        )

        rows = result.collect()
        # Median of [1,2,3] is 2
        assert rows[0]["value"] == 2.0

    def test_compliance(self, spark, sample_df):
        """Test Compliance analyzer."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Compliance("positive_b", "b > 0"))
            .run()
        )

        rows = result.collect()
        # All values are positive
        assert rows[0]["value"] == 1.0


class TestMultipleAnalyzers:
    """Test running multiple analyzers together."""

    def test_multiple_analyzers(self, spark, sample_df):
        """Test running multiple analyzers in one run."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness("a"))
            .addAnalyzer(Mean("b"))
            .addAnalyzer(Maximum("b"))
            .addAnalyzer(Minimum("b"))
            .run()
        )

        rows = result.collect()

        # Check we got results for all analyzers
        names = [r["name"] for r in rows]
        assert "Size" in names
        assert "Completeness" in names
        assert "Mean" in names
        assert "Maximum" in names
        assert "Minimum" in names

    def test_multiple_completeness(self, spark, sample_df):
        """Test Completeness on multiple columns."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Completeness("a"))
            .addAnalyzer(Completeness("b"))
            .addAnalyzer(Completeness("c"))
            .run()
        )

        rows = result.collect()
        values = {r["instance"]: r["value"] for r in rows}

        assert values["a"] == 1.0
        assert values["b"] == 1.0
        assert abs(values["c"] - 2 / 3) < 0.001


class TestAnalyzerWithWhere:
    """Test analyzers with where clause filtering."""

    def test_size_with_where(self, spark, sample_df):
        """Test Size analyzer with where clause."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Size(where="b > 1"))
            .run()
        )

        rows = result.collect()
        # Only rows where b > 1 (b=2 and b=3) = 2 rows
        assert rows[0]["value"] == 2.0

    def test_completeness_with_where(self, spark, sample_df):
        """Test Completeness analyzer with where clause."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Completeness("c", where="b <= 2"))
            .run()
        )

        rows = result.collect()
        # Rows where b <= 2: (b=1, c=5), (b=2, c=6) -> both have c values
        assert rows[0]["value"] == 1.0

    def test_mean_with_where(self, spark, sample_df):
        """Test Mean analyzer with where clause."""
        result = (
            AnalysisRunner(spark)
            .onData(sample_df)
            .addAnalyzer(Mean("b", where="b > 1"))
            .run()
        )

        rows = result.collect()
        # Mean of [2, 3] = 2.5
        assert rows[0]["value"] == 2.5
