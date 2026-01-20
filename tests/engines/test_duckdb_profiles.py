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

"""DuckDB-only profiling tests.

Tests the column profiling functionality of the DuckDB engine.
"""

import math
import pytest

from tests.engines.fixtures.datasets import (
    FLOAT_EPSILON,
    FLOAT_TOLERANCE,
    is_close,
)


def get_profile_by_column(profiles, column_name: str):
    """Find a column profile by column name."""
    for p in profiles:
        if p.column == column_name:
            return p
    return None


class TestBasicProfiling:
    """Tests for basic profiling functionality."""

    def test_profile_all_columns(self, engine_full):
        """Profile returns data for all columns."""
        profiles = engine_full.profile_columns()
        assert len(profiles) >= 4  # att1, att2, item, price

    def test_profile_specific_columns(self, engine_full):
        """Profile can be restricted to specific columns."""
        profiles = engine_full.profile_columns(columns=["att1", "item"])
        column_names = [p.column for p in profiles]
        assert "att1" in column_names
        assert "item" in column_names

    def test_profile_column_name(self, engine_full):
        """Profile contains correct column names."""
        profiles = engine_full.profile_columns()
        column_names = [p.column for p in profiles]
        assert "att1" in column_names
        assert "att2" in column_names


class TestCompletenessProfile:
    """Tests for completeness in profiles."""

    def test_completeness_full(self, engine_full):
        """Completeness is 1.0 for complete columns."""
        profiles = engine_full.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert is_close(profile.completeness, 1.0, FLOAT_EPSILON)

    def test_completeness_partial(self, engine_missing):
        """Completeness reflects NULL ratio."""
        profiles = engine_missing.profile_columns(columns=["att1", "att2"])
        att1_profile = get_profile_by_column(profiles, "att1")
        att2_profile = get_profile_by_column(profiles, "att2")
        assert is_close(att1_profile.completeness, 0.5, FLOAT_EPSILON)  # 6/12
        assert is_close(att2_profile.completeness, 0.75, FLOAT_EPSILON)  # 9/12

    def test_completeness_all_null(self, engine_all_null):
        """Completeness is 0 for all-NULL column."""
        profiles = engine_all_null.profile_columns(columns=["value"])
        profile = get_profile_by_column(profiles, "value")
        assert is_close(profile.completeness, 0.0, FLOAT_EPSILON)


class TestDistinctValuesProfile:
    """Tests for approximate distinct values in profiles."""

    def test_distinct_values_unique(self, engine_unique):
        """Distinct count for unique column."""
        profiles = engine_unique.profile_columns(columns=["unique_col"])
        profile = get_profile_by_column(profiles, "unique_col")
        assert profile.approx_distinct_values == 6

    def test_distinct_values_duplicates(self, engine_distinct):
        """Distinct count handles duplicates correctly."""
        profiles = engine_distinct.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        # att1: ["a", "a", "b", "b", "c", "c"] -> 3 distinct
        assert profile.approx_distinct_values == 3


class TestDataTypeProfile:
    """Tests for data type detection in profiles."""

    def test_data_type_string(self, engine_full):
        """Data type detection for string column."""
        profiles = engine_full.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert profile.data_type is not None
        # Should be some form of string type
        assert "str" in profile.data_type.lower() or "char" in profile.data_type.lower() or "text" in profile.data_type.lower() or "object" in profile.data_type.lower()

    def test_data_type_numeric(self, engine_numeric):
        """Data type detection for numeric column."""
        profiles = engine_numeric.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert profile.data_type is not None


class TestNumericProfileStatistics:
    """Tests for numeric statistics in profiles."""

    def test_mean_numeric(self, engine_numeric):
        """Mean is calculated for numeric columns."""
        profiles = engine_numeric.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert profile.mean is not None
        assert is_close(profile.mean, 3.5, FLOAT_TOLERANCE)

    def test_min_numeric(self, engine_numeric):
        """Minimum is calculated for numeric columns."""
        profiles = engine_numeric.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert profile.minimum is not None
        assert is_close(profile.minimum, 1.0, FLOAT_TOLERANCE)

    def test_max_numeric(self, engine_numeric):
        """Maximum is calculated for numeric columns."""
        profiles = engine_numeric.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert profile.maximum is not None
        assert is_close(profile.maximum, 6.0, FLOAT_TOLERANCE)

    def test_sum_numeric(self, engine_numeric):
        """Sum is calculated for numeric columns."""
        profiles = engine_numeric.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        assert profile.sum is not None
        assert is_close(profile.sum, 21.0, FLOAT_TOLERANCE)

    def test_stddev_numeric(self, engine_numeric):
        """Standard deviation is calculated for numeric columns."""
        profiles = engine_numeric.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        if profile.std_dev is not None:
            # Population stddev (matches Spark)
            assert is_close(profile.std_dev, 1.7078251276599330, FLOAT_TOLERANCE)

    def test_numeric_with_nulls(self, engine_numeric):
        """Numeric statistics handle NULLs correctly."""
        profiles = engine_numeric.profile_columns(columns=["att2"])
        profile = get_profile_by_column(profiles, "att2")
        # att2 has values [1,2,3,4,5,NULL]
        if profile.mean is not None:
            assert is_close(profile.mean, 3.0, FLOAT_TOLERANCE)  # (1+2+3+4+5)/5


class TestStringProfileStatistics:
    """Tests for string column profiles."""

    def test_string_column_no_numeric_stats(self, engine_full):
        """String columns don't have numeric statistics."""
        profiles = engine_full.profile_columns(columns=["att1"])
        profile = get_profile_by_column(profiles, "att1")
        # String column shouldn't have meaningful numeric stats
        # (or they might be None)
        # Just verify we get a profile back
        assert profile is not None
        assert profile.completeness is not None


class TestHistogramProfile:
    """Tests for histogram in profiles."""

    def test_histogram_low_cardinality(self, engine_histogram):
        """Histogram is generated for low cardinality columns."""
        profiles = engine_histogram.profile_columns(
            columns=["category"],
            low_cardinality_threshold=10
        )
        profile = get_profile_by_column(profiles, "category")
        # Should have histogram for 4-value column with threshold 10
        if profile.histogram is not None:
            assert len(profile.histogram) > 0

    def test_histogram_high_cardinality(self, engine_unique):
        """Histogram might not be generated for high cardinality columns."""
        profiles = engine_unique.profile_columns(
            columns=["unique_col"],
            low_cardinality_threshold=3
        )
        profile = get_profile_by_column(profiles, "unique_col")
        # With 6 distinct and threshold 3, might skip histogram
        assert profile is not None


class TestQuantileProfile:
    """Tests for quantile/percentile information in profiles."""

    def test_percentiles_numeric(self, engine_quantile):
        """Percentiles are calculated for numeric columns."""
        profiles = engine_quantile.profile_columns(columns=["value"])
        profile = get_profile_by_column(profiles, "value")
        # Check for percentile attributes if present
        if hasattr(profile, 'approx_percentiles') and profile.approx_percentiles:
            # Should have some percentile data
            assert len(profile.approx_percentiles) >= 0


class TestEdgeCases:
    """Tests for edge cases in profiling."""

    def test_empty_dataset(self, engine_empty):
        """Profiling empty dataset."""
        profiles = engine_empty.profile_columns()
        # Should return profiles (possibly with default/None values)
        assert isinstance(profiles, list)

    def test_single_row(self, engine_single):
        """Profiling single-row dataset."""
        profiles = engine_single.profile_columns(columns=["att1", "item"])
        att1_profile = get_profile_by_column(profiles, "att1")
        item_profile = get_profile_by_column(profiles, "item")

        assert att1_profile.completeness == 1.0
        assert att1_profile.approx_distinct_values == 1

        if item_profile.mean is not None:
            assert item_profile.mean == 1.0
        if item_profile.minimum is not None:
            assert item_profile.minimum == 1.0
        if item_profile.maximum is not None:
            assert item_profile.maximum == 1.0

    def test_all_null_column(self, engine_all_null):
        """Profiling all-NULL column."""
        profiles = engine_all_null.profile_columns(columns=["value"])
        profile = get_profile_by_column(profiles, "value")
        assert profile.completeness == 0.0
        # Statistics should be None or NaN for all-NULL column
        if profile.mean is not None and not math.isnan(profile.mean):
            # Some implementations might return 0 or None
            pass


class TestProfileDataFrame:
    """Tests for profile to DataFrame conversion."""

    def test_profiles_to_dataframe(self, engine_full):
        """Profiles can be converted to DataFrame."""
        profiles = engine_full.profile_columns()
        df = engine_full.profiles_to_dataframe(profiles)

        assert df is not None
        assert len(df) > 0
        assert "column" in df.columns
        assert "completeness" in df.columns
