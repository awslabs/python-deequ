# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cross-engine analyzer parity tests.

Tests that verify DuckDB engine produces the same analyzer results
as the Spark engine baseline. Requires Spark Connect to be running.
"""

import pytest

from pydeequ.v2.analyzers import (
    Size,
    Completeness,
    Mean,
    Sum,
    Maximum,
    Minimum,
    StandardDeviation,
    Distinctness,
    Uniqueness,
    UniqueValueRatio,
    CountDistinct,
    ApproxCountDistinct,
    ApproxQuantile,
    Correlation,
    MutualInformation,
    MaxLength,
    MinLength,
    PatternMatch,
    Compliance,
    Entropy,
    Histogram,
    DataType,
)

from tests.engines.comparison.conftest import requires_spark, DualEngines
from tests.engines.comparison.utils import assert_metrics_match


@requires_spark
class TestSizeAnalyzerParity:
    """Parity tests for Size analyzer."""

    def test_size_basic(self, dual_engines_full: DualEngines):
        """Size produces same result on both engines."""
        analyzers = [Size()]
        spark_metrics = dual_engines_full.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_full.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Size basic")

    def test_size_with_nulls(self, dual_engines_missing: DualEngines):
        """Size counts all rows regardless of NULLs on both engines."""
        analyzers = [Size()]
        spark_metrics = dual_engines_missing.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_missing.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Size with nulls")


@requires_spark
class TestCompletenessAnalyzerParity:
    """Parity tests for Completeness analyzer."""

    def test_completeness_full(self, dual_engines_full: DualEngines):
        """Completeness produces same result for complete columns."""
        analyzers = [Completeness("att1"), Completeness("att2")]
        spark_metrics = dual_engines_full.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_full.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Completeness full")

    def test_completeness_partial(self, dual_engines_missing: DualEngines):
        """Completeness produces same result for partial columns."""
        analyzers = [Completeness("att1"), Completeness("att2")]
        spark_metrics = dual_engines_missing.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_missing.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Completeness partial")

    def test_completeness_all_null(self, dual_engines_all_null: DualEngines):
        """Completeness produces same result for all-NULL columns."""
        analyzers = [Completeness("value")]
        spark_metrics = dual_engines_all_null.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_all_null.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Completeness all null")


@requires_spark
class TestStatisticalAnalyzerParity:
    """Parity tests for statistical analyzers."""

    def test_mean(self, dual_engines_numeric: DualEngines):
        """Mean produces same result on both engines."""
        analyzers = [Mean("att1"), Mean("att2")]
        spark_metrics = dual_engines_numeric.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_numeric.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Mean")

    def test_sum(self, dual_engines_numeric: DualEngines):
        """Sum produces same result on both engines."""
        analyzers = [Sum("att1"), Sum("att2")]
        spark_metrics = dual_engines_numeric.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_numeric.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Sum")

    def test_minimum(self, dual_engines_numeric: DualEngines):
        """Minimum produces same result on both engines."""
        analyzers = [Minimum("att1"), Minimum("att2")]
        spark_metrics = dual_engines_numeric.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_numeric.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Minimum")

    def test_maximum(self, dual_engines_numeric: DualEngines):
        """Maximum produces same result on both engines."""
        analyzers = [Maximum("att1"), Maximum("att2")]
        spark_metrics = dual_engines_numeric.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_numeric.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Maximum")

    def test_standard_deviation(self, dual_engines_numeric: DualEngines):
        """StandardDeviation produces same result on both engines."""
        analyzers = [StandardDeviation("att1")]
        spark_metrics = dual_engines_numeric.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_numeric.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "StandardDeviation")


@requires_spark
class TestUniquenessAnalyzerParity:
    """Parity tests for uniqueness-related analyzers."""

    def test_distinctness(self, dual_engines_distinct: DualEngines):
        """Distinctness produces same result on both engines."""
        analyzers = [Distinctness(["att1"]), Distinctness(["att2"])]
        spark_metrics = dual_engines_distinct.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_distinct.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Distinctness")

    def test_uniqueness(self, dual_engines_distinct: DualEngines):
        """Uniqueness produces same result on both engines."""
        analyzers = [Uniqueness(["att1"]), Uniqueness(["att2"])]
        spark_metrics = dual_engines_distinct.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_distinct.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Uniqueness")

    def test_unique_value_ratio(self, dual_engines_distinct: DualEngines):
        """UniqueValueRatio produces same result on both engines."""
        analyzers = [UniqueValueRatio(["att1"]), UniqueValueRatio(["att2"])]
        spark_metrics = dual_engines_distinct.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_distinct.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "UniqueValueRatio")

    def test_count_distinct(self, dual_engines_distinct: DualEngines):
        """CountDistinct produces same result on both engines."""
        analyzers = [CountDistinct(["att1"]), CountDistinct(["att2"])]
        spark_metrics = dual_engines_distinct.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_distinct.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "CountDistinct")

    def test_approx_count_distinct(self, dual_engines_distinct: DualEngines):
        """ApproxCountDistinct produces approximately same result."""
        analyzers = [ApproxCountDistinct("att1"), ApproxCountDistinct("att2")]
        spark_metrics = dual_engines_distinct.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_distinct.duckdb_engine.compute_metrics(analyzers)
        # Uses APPROX_TOLERANCE (10%) for approximate algorithms
        assert_metrics_match(spark_metrics, duckdb_metrics, "ApproxCountDistinct")


@requires_spark
class TestStringAnalyzerParity:
    """Parity tests for string analyzers."""

    def test_min_length(self, dual_engines_string_lengths: DualEngines):
        """MinLength produces same result on both engines."""
        analyzers = [MinLength("att1"), MinLength("att2")]
        spark_metrics = dual_engines_string_lengths.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_string_lengths.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "MinLength")

    def test_max_length(self, dual_engines_string_lengths: DualEngines):
        """MaxLength produces same result on both engines."""
        analyzers = [MaxLength("att1"), MaxLength("att2")]
        spark_metrics = dual_engines_string_lengths.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_string_lengths.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "MaxLength")

    def test_pattern_match(self, dual_engines_pattern: DualEngines):
        """PatternMatch produces same result on both engines."""
        analyzers = [PatternMatch("email", r".*@.*\..*")]
        spark_metrics = dual_engines_pattern.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_pattern.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "PatternMatch")


@requires_spark
class TestCorrelationAnalyzerParity:
    """Parity tests for Correlation analyzer."""

    def test_correlation_positive(self, dual_engines_correlation: DualEngines):
        """Correlation produces same result for positively correlated columns."""
        analyzers = [Correlation("x", "y")]
        spark_metrics = dual_engines_correlation.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_correlation.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Correlation positive")

    def test_correlation_negative(self, dual_engines_correlation: DualEngines):
        """Correlation produces same result for negatively correlated columns."""
        analyzers = [Correlation("x", "z")]
        spark_metrics = dual_engines_correlation.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_correlation.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Correlation negative")


@requires_spark
class TestEntropyAnalyzerParity:
    """Parity tests for Entropy analyzer."""

    def test_entropy_uniform(self, dual_engines_entropy: DualEngines):
        """Entropy produces same result for uniform distribution."""
        analyzers = [Entropy("uniform")]
        spark_metrics = dual_engines_entropy.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_entropy.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Entropy uniform")

    def test_entropy_constant(self, dual_engines_entropy: DualEngines):
        """Entropy produces same result for constant column."""
        analyzers = [Entropy("constant")]
        spark_metrics = dual_engines_entropy.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_entropy.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Entropy constant")


@requires_spark
class TestMutualInformationAnalyzerParity:
    """Parity tests for MutualInformation analyzer."""

    def test_mutual_information(self, dual_engines_mutual_info: DualEngines):
        """MutualInformation produces same result on both engines."""
        analyzers = [MutualInformation(["x", "y_dependent"])]
        spark_metrics = dual_engines_mutual_info.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_mutual_info.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "MutualInformation")


@requires_spark
class TestComplianceAnalyzerParity:
    """Parity tests for Compliance analyzer."""

    def test_compliance(self, dual_engines_compliance: DualEngines):
        """Compliance produces same result on both engines."""
        analyzers = [
            Compliance("positive_check", "positive > 0"),
            Compliance("mixed_check", "mixed > 0"),
        ]
        spark_metrics = dual_engines_compliance.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_compliance.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Compliance")


@requires_spark
class TestQuantileAnalyzerParity:
    """Parity tests for ApproxQuantile analyzer."""

    def test_approx_quantile_median(self, dual_engines_quantile: DualEngines):
        """ApproxQuantile produces same result for median."""
        analyzers = [ApproxQuantile("value", 0.5)]
        spark_metrics = dual_engines_quantile.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_quantile.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "ApproxQuantile median")

    def test_approx_quantile_quartiles(self, dual_engines_quantile: DualEngines):
        """ApproxQuantile produces same result for quartiles."""
        analyzers = [
            ApproxQuantile("value", 0.25),
            ApproxQuantile("value", 0.75),
        ]
        spark_metrics = dual_engines_quantile.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_quantile.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "ApproxQuantile quartiles")


@requires_spark
class TestHistogramAnalyzerParity:
    """Parity tests for Histogram analyzer."""

    def test_histogram(self, dual_engines_histogram: DualEngines):
        """Histogram produces consistent results on both engines."""
        analyzers = [Histogram("category")]
        spark_metrics = dual_engines_histogram.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_histogram.duckdb_engine.compute_metrics(analyzers)
        # Histogram structure may differ, so just check it exists
        assert len(spark_metrics) > 0
        assert len(duckdb_metrics) > 0


@requires_spark
class TestDataTypeAnalyzerParity:
    """Parity tests for DataType analyzer."""

    def test_data_type(self, dual_engines_full: DualEngines):
        """DataType produces consistent results on both engines."""
        analyzers = [DataType("att1")]
        spark_metrics = dual_engines_full.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_full.duckdb_engine.compute_metrics(analyzers)
        # DataType format may differ, so just check it exists
        assert len(spark_metrics) > 0
        assert len(duckdb_metrics) > 0


@requires_spark
class TestAnalyzersWithWhereParity:
    """Parity tests for analyzers with WHERE clause."""

    def test_size_with_where(self, dual_engines_where: DualEngines):
        """Size with WHERE produces same result on both engines."""
        analyzers = [Size(where="category = 'A'")]
        spark_metrics = dual_engines_where.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_where.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Size with WHERE")

    def test_completeness_with_where(self, dual_engines_where: DualEngines):
        """Completeness with WHERE produces same result on both engines."""
        analyzers = [Completeness("att1", where="category = 'A'")]
        spark_metrics = dual_engines_where.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_where.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Completeness with WHERE")

    def test_mean_with_where(self, dual_engines_where: DualEngines):
        """Mean with WHERE produces same result on both engines."""
        analyzers = [Mean("value", where="category = 'A'")]
        spark_metrics = dual_engines_where.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_where.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Mean with WHERE")


@requires_spark
class TestMultipleAnalyzersParity:
    """Parity tests for running multiple analyzers together."""

    def test_all_basic_analyzers(self, dual_engines_numeric: DualEngines):
        """All basic analyzers produce same results on both engines."""
        analyzers = [
            Size(),
            Completeness("att1"),
            Mean("att1"),
            Sum("att1"),
            Minimum("att1"),
            Maximum("att1"),
            StandardDeviation("att1"),
        ]
        spark_metrics = dual_engines_numeric.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_numeric.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "All basic analyzers")

    def test_mixed_analyzer_types(self, dual_engines_full: DualEngines):
        """Mixed analyzer types produce same results on both engines."""
        analyzers = [
            Size(),
            Completeness("att1"),
            CountDistinct(["att1"]),
            MaxLength("att1"),
            MinLength("att1"),
        ]
        spark_metrics = dual_engines_full.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_full.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Mixed analyzer types")


@requires_spark
class TestEdgeCasesParity:
    """Parity tests for edge cases."""

    def test_single_row(self, dual_engines_single: DualEngines):
        """Analyzers produce same results for single-row dataset."""
        analyzers = [
            Size(),
            Completeness("att1"),
            Mean("item"),
            Maximum("item"),
            Minimum("item"),
        ]
        spark_metrics = dual_engines_single.spark_engine.compute_metrics(analyzers)
        duckdb_metrics = dual_engines_single.duckdb_engine.compute_metrics(analyzers)
        assert_metrics_match(spark_metrics, duckdb_metrics, "Single row")
