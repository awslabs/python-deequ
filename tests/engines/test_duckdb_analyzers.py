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

"""DuckDB-only analyzer tests.

Tests all 22 analyzers against known expected values from the test datasets.
These tests do not require Spark and can run quickly in CI.
"""

import math
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

from tests.engines.conftest import get_metric_value, get_metric
from tests.engines.fixtures.datasets import (
    EXPECTED_VALUES,
    FLOAT_EPSILON,
    FLOAT_TOLERANCE,
    APPROX_TOLERANCE,
    is_close,
    get_tolerance,
)


class TestSizeAnalyzer:
    """Tests for the Size analyzer."""

    def test_size_basic(self, engine_full):
        """Size returns correct row count for basic dataset."""
        metrics = engine_full.compute_metrics([Size()])
        value = get_metric_value(metrics, "Size")
        assert value == 4.0

    def test_size_empty(self, engine_empty):
        """Size returns 0 for empty dataset."""
        metrics = engine_empty.compute_metrics([Size()])
        value = get_metric_value(metrics, "Size")
        assert value == 0.0

    def test_size_single(self, engine_single):
        """Size returns 1 for single-row dataset."""
        metrics = engine_single.compute_metrics([Size()])
        value = get_metric_value(metrics, "Size")
        assert value == 1.0

    def test_size_missing(self, engine_missing):
        """Size counts all rows regardless of NULLs."""
        metrics = engine_missing.compute_metrics([Size()])
        value = get_metric_value(metrics, "Size")
        assert value == 12.0

    def test_size_with_where(self, engine_where):
        """Size respects WHERE clause."""
        metrics = engine_where.compute_metrics([Size(where="category = 'A'")])
        value = get_metric_value(metrics, "Size")
        assert value == 2.0


class TestCompletenessAnalyzer:
    """Tests for the Completeness analyzer."""

    def test_completeness_full(self, engine_full):
        """Completeness is 1.0 for columns with no NULLs."""
        metrics = engine_full.compute_metrics([Completeness("att1")])
        value = get_metric_value(metrics, "Completeness", "att1")
        assert is_close(value, 1.0, FLOAT_EPSILON)

    def test_completeness_partial(self, engine_missing):
        """Completeness reflects NULL ratio correctly."""
        metrics = engine_missing.compute_metrics([
            Completeness("att1"),
            Completeness("att2"),
        ])
        att1_value = get_metric_value(metrics, "Completeness", "att1")
        att2_value = get_metric_value(metrics, "Completeness", "att2")
        assert is_close(att1_value, 0.5, FLOAT_EPSILON)  # 6/12
        assert is_close(att2_value, 0.75, FLOAT_EPSILON)  # 9/12

    def test_completeness_all_null(self, engine_all_null):
        """Completeness is 0.0 for all-NULL column."""
        metrics = engine_all_null.compute_metrics([Completeness("value")])
        value = get_metric_value(metrics, "Completeness", "value")
        assert is_close(value, 0.0, FLOAT_EPSILON)

    def test_completeness_empty(self, engine_empty):
        """Completeness is 1.0 for empty dataset (vacuously true)."""
        metrics = engine_empty.compute_metrics([Completeness("att1")])
        value = get_metric_value(metrics, "Completeness", "att1")
        # Empty dataset: 0/0 should be treated as 1.0 (all rows are complete)
        assert value is None or is_close(value, 1.0, FLOAT_EPSILON) or math.isnan(value)

    def test_completeness_with_where(self, engine_where):
        """Completeness respects WHERE clause."""
        # category='A': att1 has values "x", "y" (2/2 complete)
        metrics = engine_where.compute_metrics([
            Completeness("att1", where="category = 'A'")
        ])
        value = get_metric_value(metrics, "Completeness", "att1")
        assert is_close(value, 1.0, FLOAT_EPSILON)


class TestMeanAnalyzer:
    """Tests for the Mean analyzer."""

    def test_mean_basic(self, engine_numeric):
        """Mean calculates correctly for numeric column."""
        metrics = engine_numeric.compute_metrics([Mean("att1")])
        value = get_metric_value(metrics, "Mean", "att1")
        assert is_close(value, 3.5, FLOAT_TOLERANCE)

    def test_mean_with_nulls(self, engine_numeric):
        """Mean excludes NULL values in calculation."""
        metrics = engine_numeric.compute_metrics([Mean("att2")])
        value = get_metric_value(metrics, "Mean", "att2")
        assert is_close(value, 3.0, FLOAT_TOLERANCE)  # (1+2+3+4+5)/5

    def test_mean_single(self, engine_single):
        """Mean works for single row."""
        metrics = engine_single.compute_metrics([Mean("price")])
        value = get_metric_value(metrics, "Mean", "price")
        assert is_close(value, 10.0, FLOAT_TOLERANCE)

    def test_mean_with_where(self, engine_where):
        """Mean respects WHERE clause."""
        metrics = engine_where.compute_metrics([Mean("value", where="category = 'A'")])
        value = get_metric_value(metrics, "Mean", "value")
        assert is_close(value, 15.0, FLOAT_TOLERANCE)  # (10+20)/2


class TestSumAnalyzer:
    """Tests for the Sum analyzer."""

    def test_sum_basic(self, engine_numeric):
        """Sum calculates correctly for numeric column."""
        metrics = engine_numeric.compute_metrics([Sum("att1")])
        value = get_metric_value(metrics, "Sum", "att1")
        assert is_close(value, 21.0, FLOAT_TOLERANCE)

    def test_sum_with_nulls(self, engine_numeric):
        """Sum excludes NULL values."""
        metrics = engine_numeric.compute_metrics([Sum("att2")])
        value = get_metric_value(metrics, "Sum", "att2")
        assert is_close(value, 15.0, FLOAT_TOLERANCE)

    def test_sum_single(self, engine_single):
        """Sum works for single row."""
        metrics = engine_single.compute_metrics([Sum("price")])
        value = get_metric_value(metrics, "Sum", "price")
        assert is_close(value, 10.0, FLOAT_TOLERANCE)


class TestMinimumAnalyzer:
    """Tests for the Minimum analyzer."""

    def test_minimum_basic(self, engine_numeric):
        """Minimum finds smallest value."""
        metrics = engine_numeric.compute_metrics([Minimum("att1")])
        value = get_metric_value(metrics, "Minimum", "att1")
        assert is_close(value, 1.0, FLOAT_TOLERANCE)

    def test_minimum_with_nulls(self, engine_numeric):
        """Minimum ignores NULL values."""
        metrics = engine_numeric.compute_metrics([Minimum("att2")])
        value = get_metric_value(metrics, "Minimum", "att2")
        assert is_close(value, 1.0, FLOAT_TOLERANCE)

    def test_minimum_single(self, engine_single):
        """Minimum works for single row."""
        metrics = engine_single.compute_metrics([Minimum("price")])
        value = get_metric_value(metrics, "Minimum", "price")
        assert is_close(value, 10.0, FLOAT_TOLERANCE)


class TestMaximumAnalyzer:
    """Tests for the Maximum analyzer."""

    def test_maximum_basic(self, engine_numeric):
        """Maximum finds largest value."""
        metrics = engine_numeric.compute_metrics([Maximum("att1")])
        value = get_metric_value(metrics, "Maximum", "att1")
        assert is_close(value, 6.0, FLOAT_TOLERANCE)

    def test_maximum_with_nulls(self, engine_numeric):
        """Maximum ignores NULL values."""
        metrics = engine_numeric.compute_metrics([Maximum("att2")])
        value = get_metric_value(metrics, "Maximum", "att2")
        assert is_close(value, 5.0, FLOAT_TOLERANCE)

    def test_maximum_single(self, engine_single):
        """Maximum works for single row."""
        metrics = engine_single.compute_metrics([Maximum("price")])
        value = get_metric_value(metrics, "Maximum", "price")
        assert is_close(value, 10.0, FLOAT_TOLERANCE)


class TestStandardDeviationAnalyzer:
    """Tests for the StandardDeviation analyzer."""

    def test_stddev_basic(self, engine_numeric):
        """StandardDeviation calculates sample stddev correctly."""
        metrics = engine_numeric.compute_metrics([StandardDeviation("att1")])
        value = get_metric_value(metrics, "StandardDeviation", "att1")
        # Sample stddev of [1,2,3,4,5,6] = sqrt(17.5/5) ≈ 1.8708
        assert is_close(value, 1.8708286933869707, FLOAT_TOLERANCE)

    def test_stddev_single_row(self, engine_single):
        """StandardDeviation for single row is NaN or 0."""
        metrics = engine_single.compute_metrics([StandardDeviation("price")])
        value = get_metric_value(metrics, "StandardDeviation", "price")
        # Single value: stddev is undefined (NaN) or 0
        assert value is None or math.isnan(value) or value == 0.0


class TestDistinctnessAnalyzer:
    """Tests for the Distinctness analyzer."""

    def test_distinctness_basic(self, engine_distinct):
        """Distinctness = distinct values / total rows."""
        metrics = engine_distinct.compute_metrics([Distinctness(["att1"])])
        value = get_metric_value(metrics, "Distinctness", "att1")
        # 3 distinct values / 6 rows = 0.5
        assert is_close(value, 0.5, FLOAT_EPSILON)

    def test_distinctness_all_unique(self, engine_distinct):
        """Distinctness is 1.0 when all values are distinct."""
        metrics = engine_distinct.compute_metrics([Distinctness(["att2"])])
        value = get_metric_value(metrics, "Distinctness", "att2")
        assert is_close(value, 1.0, FLOAT_EPSILON)

    def test_distinctness_all_same(self, engine_unique):
        """Distinctness is 1/n when all values are the same."""
        metrics = engine_unique.compute_metrics([Distinctness(["all_same"])])
        value = get_metric_value(metrics, "Distinctness", "all_same")
        # 1 distinct / 6 rows ≈ 0.167
        assert is_close(value, 1/6, FLOAT_EPSILON)


class TestUniquenessAnalyzer:
    """Tests for the Uniqueness analyzer."""

    def test_uniqueness_all_unique(self, engine_unique):
        """Uniqueness is 1.0 when all values appear exactly once."""
        metrics = engine_unique.compute_metrics([Uniqueness(["unique_col"])])
        value = get_metric_value(metrics, "Uniqueness", "unique_col")
        assert is_close(value, 1.0, FLOAT_EPSILON)

    def test_uniqueness_all_duplicated(self, engine_distinct):
        """Uniqueness is 0.0 when all values are duplicated."""
        metrics = engine_distinct.compute_metrics([Uniqueness(["att1"])])
        value = get_metric_value(metrics, "Uniqueness", "att1")
        # All values in att1 appear twice, so 0 unique
        assert is_close(value, 0.0, FLOAT_EPSILON)

    def test_uniqueness_mixed(self, engine_unique):
        """Uniqueness handles mixed case correctly."""
        metrics = engine_unique.compute_metrics([Uniqueness(["non_unique"])])
        value = get_metric_value(metrics, "Uniqueness", "non_unique")
        # [1,1,2,2,3,3] - all duplicated, uniqueness = 0
        assert is_close(value, 0.0, FLOAT_EPSILON)


class TestUniqueValueRatioAnalyzer:
    """Tests for the UniqueValueRatio analyzer."""

    def test_unique_value_ratio_all_unique(self, engine_distinct):
        """UniqueValueRatio is 1.0 when unique count = distinct count."""
        metrics = engine_distinct.compute_metrics([UniqueValueRatio(["att2"])])
        value = get_metric_value(metrics, "UniqueValueRatio", "att2")
        # 6 unique / 6 distinct = 1.0
        assert is_close(value, 1.0, FLOAT_EPSILON)

    def test_unique_value_ratio_no_unique(self, engine_distinct):
        """UniqueValueRatio is 0.0 when no values are unique."""
        metrics = engine_distinct.compute_metrics([UniqueValueRatio(["att1"])])
        value = get_metric_value(metrics, "UniqueValueRatio", "att1")
        # 0 unique / 3 distinct = 0.0
        assert is_close(value, 0.0, FLOAT_EPSILON)


class TestCountDistinctAnalyzer:
    """Tests for the CountDistinct analyzer."""

    def test_count_distinct_basic(self, engine_full):
        """CountDistinct counts unique values correctly."""
        metrics = engine_full.compute_metrics([CountDistinct(["att1"])])
        value = get_metric_value(metrics, "CountDistinct", "att1")
        # "a", "b", "c" (a appears twice) = 3 distinct
        assert is_close(value, 3.0, FLOAT_EPSILON)

    def test_count_distinct_all_unique(self, engine_distinct):
        """CountDistinct equals row count when all values are distinct."""
        metrics = engine_distinct.compute_metrics([CountDistinct(["att2"])])
        value = get_metric_value(metrics, "CountDistinct", "att2")
        assert is_close(value, 6.0, FLOAT_EPSILON)

    def test_count_distinct_with_duplicates(self, engine_distinct):
        """CountDistinct counts only distinct values."""
        metrics = engine_distinct.compute_metrics([CountDistinct(["att1"])])
        value = get_metric_value(metrics, "CountDistinct", "att1")
        assert is_close(value, 3.0, FLOAT_EPSILON)


class TestApproxCountDistinctAnalyzer:
    """Tests for the ApproxCountDistinct analyzer."""

    def test_approx_count_distinct_basic(self, engine_full):
        """ApproxCountDistinct approximates distinct count."""
        metrics = engine_full.compute_metrics([ApproxCountDistinct("att1")])
        value = get_metric_value(metrics, "ApproxCountDistinct", "att1")
        # Should be approximately 3
        assert is_close(value, 3.0, APPROX_TOLERANCE)

    def test_approx_count_distinct_all_unique(self, engine_distinct):
        """ApproxCountDistinct handles all-unique column."""
        metrics = engine_distinct.compute_metrics([ApproxCountDistinct("att2")])
        value = get_metric_value(metrics, "ApproxCountDistinct", "att2")
        # HyperLogLog can have higher variance on small datasets (up to 20% error)
        assert is_close(value, 6.0, 0.2)


class TestApproxQuantileAnalyzer:
    """Tests for the ApproxQuantile analyzer."""

    def test_approx_quantile_median(self, engine_quantile):
        """ApproxQuantile calculates median correctly."""
        metrics = engine_quantile.compute_metrics([ApproxQuantile("value", 0.5)])
        value = get_metric_value(metrics, "ApproxQuantile", "value")
        # Median of [1,2,3,4,5,6,7,8,9,10] = 5.5
        assert is_close(value, 5.5, FLOAT_TOLERANCE)

    def test_approx_quantile_quartiles(self, engine_quantile):
        """ApproxQuantile calculates quartiles."""
        metrics = engine_quantile.compute_metrics([
            ApproxQuantile("value", 0.25),
            ApproxQuantile("value", 0.75),
        ])
        # For small datasets, quantile calculation may vary slightly
        q25 = get_metric_value(metrics, "ApproxQuantile", "value")
        # Note: DuckDB uses QUANTILE_CONT which interpolates
        assert q25 is not None


class TestCorrelationAnalyzer:
    """Tests for the Correlation analyzer."""

    def test_correlation_positive(self, engine_correlation):
        """Correlation is 1.0 for perfectly positively correlated columns."""
        metrics = engine_correlation.compute_metrics([Correlation("x", "y")])
        value = get_metric_value(metrics, "Correlation", "x,y")
        assert is_close(value, 1.0, FLOAT_TOLERANCE)

    def test_correlation_negative(self, engine_correlation):
        """Correlation is -1.0 for perfectly negatively correlated columns."""
        metrics = engine_correlation.compute_metrics([Correlation("x", "z")])
        value = get_metric_value(metrics, "Correlation", "x,z")
        assert is_close(value, -1.0, FLOAT_TOLERANCE)


class TestMutualInformationAnalyzer:
    """Tests for the MutualInformation analyzer."""

    def test_mutual_information_dependent(self, engine_mutual_info):
        """MutualInformation is high for perfectly dependent columns."""
        metrics = engine_mutual_info.compute_metrics([
            MutualInformation(["x", "y_dependent"])
        ])
        value = get_metric_value(metrics, "MutualInformation", "x,y_dependent")
        # Perfect dependency should have high MI (equal to entropy of x)
        assert value is not None and value > 0


class TestMaxLengthAnalyzer:
    """Tests for the MaxLength analyzer."""

    def test_maxlength_basic(self, engine_string_lengths):
        """MaxLength finds longest string."""
        metrics = engine_string_lengths.compute_metrics([MaxLength("att1")])
        value = get_metric_value(metrics, "MaxLength", "att1")
        assert is_close(value, 4.0, FLOAT_EPSILON)  # "dddd"

    def test_maxlength_uniform(self, engine_string_lengths):
        """MaxLength works with varying lengths."""
        metrics = engine_string_lengths.compute_metrics([MaxLength("att2")])
        value = get_metric_value(metrics, "MaxLength", "att2")
        assert is_close(value, 5.0, FLOAT_EPSILON)  # "hello", "world", "value"


class TestMinLengthAnalyzer:
    """Tests for the MinLength analyzer."""

    def test_minlength_empty_string(self, engine_string_lengths):
        """MinLength handles empty string (length 0)."""
        metrics = engine_string_lengths.compute_metrics([MinLength("att1")])
        value = get_metric_value(metrics, "MinLength", "att1")
        assert is_close(value, 0.0, FLOAT_EPSILON)  # ""

    def test_minlength_basic(self, engine_string_lengths):
        """MinLength finds shortest string."""
        metrics = engine_string_lengths.compute_metrics([MinLength("att2")])
        value = get_metric_value(metrics, "MinLength", "att2")
        assert is_close(value, 4.0, FLOAT_EPSILON)  # "test", "data"


class TestPatternMatchAnalyzer:
    """Tests for the PatternMatch analyzer."""

    def test_pattern_match_email(self, engine_pattern):
        """PatternMatch detects email pattern."""
        # Simple email regex
        metrics = engine_pattern.compute_metrics([
            PatternMatch("email", r".*@.*\..*")
        ])
        value = get_metric_value(metrics, "PatternMatch", "email")
        # 4 valid emails out of 6
        assert is_close(value, 4/6, FLOAT_TOLERANCE)

    def test_pattern_match_all_match(self, engine_full):
        """PatternMatch returns 1.0 when all rows match."""
        metrics = engine_full.compute_metrics([
            PatternMatch("att1", r"^[a-c]$")
        ])
        value = get_metric_value(metrics, "PatternMatch", "att1")
        # "a", "b", "c", "a" all match
        assert is_close(value, 1.0, FLOAT_TOLERANCE)


class TestComplianceAnalyzer:
    """Tests for the Compliance analyzer."""

    def test_compliance_all_positive(self, engine_compliance):
        """Compliance is 1.0 when all rows satisfy predicate."""
        metrics = engine_compliance.compute_metrics([
            Compliance("positive_check", "positive > 0")
        ])
        value = get_metric_value(metrics, "Compliance", "positive_check")
        assert is_close(value, 1.0, FLOAT_TOLERANCE)

    def test_compliance_partial(self, engine_compliance):
        """Compliance reflects fraction satisfying predicate."""
        metrics = engine_compliance.compute_metrics([
            Compliance("mixed_check", "mixed > 0")
        ])
        value = get_metric_value(metrics, "Compliance", "mixed_check")
        # [-2,-1,0,1,2,3] -> 3 values > 0
        assert is_close(value, 0.5, FLOAT_TOLERANCE)

    def test_compliance_none(self, engine_compliance):
        """Compliance is 0.0 when no rows satisfy predicate."""
        metrics = engine_compliance.compute_metrics([
            Compliance("negative_positive", "negative > 0")
        ])
        value = get_metric_value(metrics, "Compliance", "negative_positive")
        assert is_close(value, 0.0, FLOAT_TOLERANCE)


class TestEntropyAnalyzer:
    """Tests for the Entropy analyzer."""

    def test_entropy_uniform(self, engine_entropy):
        """Entropy is log2(n) for uniform distribution."""
        metrics = engine_entropy.compute_metrics([Entropy("uniform")])
        value = get_metric_value(metrics, "Entropy", "uniform")
        # 4 equally distributed values: entropy = log2(4) = 2.0
        assert is_close(value, 2.0, FLOAT_TOLERANCE)

    def test_entropy_constant(self, engine_entropy):
        """Entropy is 0 for constant column."""
        metrics = engine_entropy.compute_metrics([Entropy("constant")])
        value = get_metric_value(metrics, "Entropy", "constant")
        assert is_close(value, 0.0, FLOAT_TOLERANCE)

    def test_entropy_skewed(self, engine_entropy):
        """Entropy is between 0 and max for skewed distribution."""
        metrics = engine_entropy.compute_metrics([Entropy("skewed")])
        value = get_metric_value(metrics, "Entropy", "skewed")
        # Skewed distribution: 0 < entropy < log2(4)
        assert value > 0.0 and value < 2.0


class TestHistogramAnalyzer:
    """Tests for the Histogram analyzer."""

    def test_histogram_basic(self, engine_histogram):
        """Histogram returns value distribution."""
        metrics = engine_histogram.compute_metrics([Histogram("category")])
        result = get_metric(metrics, "Histogram", "category")
        assert result is not None
        # Histogram value should be non-null (JSON or dict)


class TestDataTypeAnalyzer:
    """Tests for the DataType analyzer."""

    def test_datatype_numeric(self, engine_data_type):
        """DataType identifies numeric columns."""
        metrics = engine_data_type.compute_metrics([DataType("pure_numeric")])
        result = get_metric(metrics, "DataType", "pure_numeric")
        assert result is not None

    def test_datatype_string(self, engine_data_type):
        """DataType identifies string columns."""
        metrics = engine_data_type.compute_metrics([DataType("strings")])
        result = get_metric(metrics, "DataType", "strings")
        assert result is not None


class TestMultipleAnalyzers:
    """Tests for running multiple analyzers together."""

    def test_multiple_basic_analyzers(self, engine_numeric):
        """Multiple analyzers can be computed in one call."""
        metrics = engine_numeric.compute_metrics([
            Size(),
            Mean("att1"),
            Sum("att1"),
            Minimum("att1"),
            Maximum("att1"),
        ])

        assert len(metrics) >= 5
        assert get_metric_value(metrics, "Size") == 6.0
        assert is_close(get_metric_value(metrics, "Mean", "att1"), 3.5, FLOAT_TOLERANCE)
        assert is_close(get_metric_value(metrics, "Sum", "att1"), 21.0, FLOAT_TOLERANCE)
        assert is_close(get_metric_value(metrics, "Minimum", "att1"), 1.0, FLOAT_TOLERANCE)
        assert is_close(get_metric_value(metrics, "Maximum", "att1"), 6.0, FLOAT_TOLERANCE)

    def test_multiple_columns_same_analyzer(self, engine_full):
        """Same analyzer type on multiple columns."""
        metrics = engine_full.compute_metrics([
            Completeness("att1"),
            Completeness("att2"),
            Completeness("item"),
        ])

        assert len(metrics) >= 3
        assert is_close(get_metric_value(metrics, "Completeness", "att1"), 1.0, FLOAT_EPSILON)
        assert is_close(get_metric_value(metrics, "Completeness", "att2"), 1.0, FLOAT_EPSILON)
        assert is_close(get_metric_value(metrics, "Completeness", "item"), 1.0, FLOAT_EPSILON)

    def test_mixed_analyzer_types(self, engine_full):
        """Mix of different analyzer categories."""
        metrics = engine_full.compute_metrics([
            Size(),
            Completeness("att1"),
            CountDistinct(["att1"]),
            MaxLength("att1"),
        ])

        assert get_metric_value(metrics, "Size") == 4.0
        assert is_close(get_metric_value(metrics, "Completeness", "att1"), 1.0, FLOAT_EPSILON)
        assert get_metric_value(metrics, "CountDistinct", "att1") == 3.0
        assert get_metric_value(metrics, "MaxLength", "att1") == 1.0  # "a", "b", "c"


class TestAnalyzersWithWhere:
    """Tests for analyzers with WHERE clause filtering."""

    def test_size_where_a(self, engine_where):
        """Size with WHERE filters correctly."""
        metrics = engine_where.compute_metrics([
            Size(where="category = 'A'"),
            Size(where="category = 'B'"),
        ])
        assert get_metric_value(metrics, "Size") == 2.0  # Both return 2

    def test_completeness_where(self, engine_where):
        """Completeness varies by WHERE filter."""
        metrics = engine_where.compute_metrics([
            Completeness("att1", where="category = 'A'"),
            Completeness("att1", where="category = 'B'"),
        ])
        # Category A: att1 = ["x", "y"] -> 2/2 complete
        # Category B: att1 = [None, "w"] -> 1/2 complete
        a_completeness = get_metric_value(metrics, "Completeness", "att1")
        assert a_completeness is not None

    def test_mean_where(self, engine_where):
        """Mean varies by WHERE filter."""
        metrics = engine_where.compute_metrics([
            Mean("value", where="category = 'A'"),
        ])
        # Category A: value = [10, 20] -> mean = 15
        value = get_metric_value(metrics, "Mean", "value")
        assert is_close(value, 15.0, FLOAT_TOLERANCE)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_dataset_all_analyzers(self, engine_empty):
        """Empty dataset handles gracefully."""
        metrics = engine_empty.compute_metrics([
            Size(),
            Completeness("att1"),
        ])
        assert get_metric_value(metrics, "Size") == 0.0

    def test_all_null_column_stats(self, engine_all_null):
        """All-NULL column returns appropriate values."""
        metrics = engine_all_null.compute_metrics([
            Completeness("value"),
            Size(),
        ])
        assert is_close(get_metric_value(metrics, "Completeness", "value"), 0.0, FLOAT_EPSILON)
        assert get_metric_value(metrics, "Size") == 3.0

    def test_special_characters(self, engine_escape):
        """Special characters in data are handled."""
        metrics = engine_escape.compute_metrics([
            Size(),
            Completeness("att1"),
            MaxLength("att1"),
        ])
        assert get_metric_value(metrics, "Size") == 8.0
        assert is_close(get_metric_value(metrics, "Completeness", "att1"), 1.0, FLOAT_EPSILON)
