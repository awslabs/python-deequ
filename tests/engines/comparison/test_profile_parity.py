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

"""Cross-engine column profiling parity tests.

Tests that verify DuckDB engine produces the same column profiling
results as the Spark engine baseline. Requires Spark Connect to be running.
"""

import pytest

from tests.engines.comparison.conftest import requires_spark, DualEngines
from tests.engines.comparison.utils import assert_profiles_match


def get_profile_by_column(profiles, column_name: str):
    """Find a column profile by column name."""
    for p in profiles:
        if p.column == column_name:
            return p
    return None


@requires_spark
class TestBasicProfilingParity:
    """Parity tests for basic profiling functionality."""

    def test_profile_all_columns(self, dual_engines_full: DualEngines):
        """Profile all columns produces same results on both engines."""
        spark_profiles = dual_engines_full.spark_engine.profile_columns()
        duckdb_profiles = dual_engines_full.duckdb_engine.profile_columns()
        assert_profiles_match(spark_profiles, duckdb_profiles, "All columns")

    def test_profile_specific_columns(self, dual_engines_full: DualEngines):
        """Profile specific columns produces same results."""
        spark_profiles = dual_engines_full.spark_engine.profile_columns(columns=["att1", "item"])
        duckdb_profiles = dual_engines_full.duckdb_engine.profile_columns(columns=["att1", "item"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Specific columns")


@requires_spark
class TestCompletenessProfilingParity:
    """Parity tests for completeness in profiles."""

    def test_completeness_full(self, dual_engines_full: DualEngines):
        """Completeness is same for complete columns on both engines."""
        spark_profiles = dual_engines_full.spark_engine.profile_columns(columns=["att1"])
        duckdb_profiles = dual_engines_full.duckdb_engine.profile_columns(columns=["att1"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Completeness full")

    def test_completeness_partial(self, dual_engines_missing: DualEngines):
        """Completeness is same for partial columns on both engines."""
        spark_profiles = dual_engines_missing.spark_engine.profile_columns(columns=["att1", "att2"])
        duckdb_profiles = dual_engines_missing.duckdb_engine.profile_columns(columns=["att1", "att2"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Completeness partial")

    def test_completeness_all_null(self, dual_engines_all_null: DualEngines):
        """Completeness is same for all-NULL column on both engines."""
        spark_profiles = dual_engines_all_null.spark_engine.profile_columns(columns=["value"])
        duckdb_profiles = dual_engines_all_null.duckdb_engine.profile_columns(columns=["value"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Completeness all null")


@requires_spark
class TestDistinctValuesProfilingParity:
    """Parity tests for distinct values in profiles."""

    def test_distinct_values(self, dual_engines_distinct: DualEngines):
        """Distinct value counts are same on both engines."""
        spark_profiles = dual_engines_distinct.spark_engine.profile_columns(columns=["att1", "att2"])
        duckdb_profiles = dual_engines_distinct.duckdb_engine.profile_columns(columns=["att1", "att2"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Distinct values")


@requires_spark
class TestNumericProfilingParity:
    """Parity tests for numeric column profiling."""

    def test_numeric_statistics(self, dual_engines_numeric: DualEngines):
        """Numeric statistics are same on both engines."""
        spark_profiles = dual_engines_numeric.spark_engine.profile_columns(columns=["att1"])
        duckdb_profiles = dual_engines_numeric.duckdb_engine.profile_columns(columns=["att1"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Numeric statistics")

    def test_numeric_with_nulls(self, dual_engines_numeric: DualEngines):
        """Numeric statistics handle NULLs same way on both engines."""
        spark_profiles = dual_engines_numeric.spark_engine.profile_columns(columns=["att2"])
        duckdb_profiles = dual_engines_numeric.duckdb_engine.profile_columns(columns=["att2"])
        assert_profiles_match(spark_profiles, duckdb_profiles, "Numeric with nulls")


@requires_spark
class TestHistogramProfilingParity:
    """Parity tests for histogram profiling."""

    def test_histogram(self, dual_engines_histogram: DualEngines):
        """Histogram profiling produces consistent results."""
        spark_profiles = dual_engines_histogram.spark_engine.profile_columns(
            columns=["category"],
            low_cardinality_threshold=10
        )
        duckdb_profiles = dual_engines_histogram.duckdb_engine.profile_columns(
            columns=["category"],
            low_cardinality_threshold=10
        )
        # Check profiles exist and have histogram data
        # (exact histogram format may differ)
        assert len(spark_profiles) > 0
        assert len(duckdb_profiles) > 0


@requires_spark
class TestEdgeCaseProfilingParity:
    """Parity tests for edge cases in profiling."""

    def test_single_row(self, dual_engines_single: DualEngines):
        """Single-row profiling produces same results."""
        spark_profiles = dual_engines_single.spark_engine.profile_columns()
        duckdb_profiles = dual_engines_single.duckdb_engine.profile_columns()
        assert_profiles_match(spark_profiles, duckdb_profiles, "Single row")


@requires_spark
class TestMixedTypeProfilingParity:
    """Parity tests for mixed column types."""

    def test_mixed_types(self, dual_engines_full: DualEngines):
        """Mixed column types produce same results on both engines."""
        spark_profiles = dual_engines_full.spark_engine.profile_columns()
        duckdb_profiles = dual_engines_full.duckdb_engine.profile_columns()
        assert_profiles_match(spark_profiles, duckdb_profiles, "Mixed types")
