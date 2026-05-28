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

"""Cross-engine constraint parity tests.

Tests that verify DuckDB engine produces the same constraint evaluation
results as the Spark engine baseline. Requires Spark Connect to be running.
"""

import pytest

from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.predicates import eq, gt, gte, lt, lte, between, is_one

from tests.engines.comparison.conftest import requires_spark, DualEngines
from tests.engines.comparison.utils import assert_constraints_match


@requires_spark
class TestSizeConstraintParity:
    """Parity tests for size constraints."""

    def test_has_size_success(self, dual_engines_full: DualEngines):
        """hasSize produces same result on both engines when passing."""
        check = Check(CheckLevel.Error, "size check").hasSize(eq(4))
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasSize success")

    def test_has_size_failure(self, dual_engines_full: DualEngines):
        """hasSize produces same result on both engines when failing."""
        check = Check(CheckLevel.Error, "size check").hasSize(eq(100))
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasSize failure")


@requires_spark
class TestCompletenessConstraintParity:
    """Parity tests for completeness constraints."""

    def test_is_complete_success(self, dual_engines_full: DualEngines):
        """isComplete produces same result on both engines when passing."""
        check = Check(CheckLevel.Error, "complete").isComplete("att1")
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isComplete success")

    def test_is_complete_failure(self, dual_engines_missing: DualEngines):
        """isComplete produces same result on both engines when failing."""
        check = Check(CheckLevel.Error, "complete").isComplete("att1")
        spark_results = dual_engines_missing.spark_engine.run_checks([check])
        duckdb_results = dual_engines_missing.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isComplete failure")

    def test_has_completeness(self, dual_engines_missing: DualEngines):
        """hasCompleteness produces same result on both engines."""
        check = Check(CheckLevel.Error, "threshold").hasCompleteness("att1", gte(0.5))
        spark_results = dual_engines_missing.spark_engine.run_checks([check])
        duckdb_results = dual_engines_missing.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasCompleteness")

    def test_are_complete(self, dual_engines_full: DualEngines):
        """areComplete produces same result on both engines."""
        check = Check(CheckLevel.Error, "multi").areComplete(["att1", "att2"])
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "areComplete")


@requires_spark
class TestUniquenessConstraintParity:
    """Parity tests for uniqueness constraints."""

    def test_is_unique_success(self, dual_engines_unique: DualEngines):
        """isUnique produces same result on both engines when passing."""
        check = Check(CheckLevel.Error, "unique").isUnique("unique_col")
        spark_results = dual_engines_unique.spark_engine.run_checks([check])
        duckdb_results = dual_engines_unique.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isUnique success")

    def test_is_unique_failure(self, dual_engines_unique: DualEngines):
        """isUnique produces same result on both engines when failing."""
        check = Check(CheckLevel.Error, "not unique").isUnique("non_unique")
        spark_results = dual_engines_unique.spark_engine.run_checks([check])
        duckdb_results = dual_engines_unique.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isUnique failure")

    def test_has_uniqueness(self, dual_engines_distinct: DualEngines):
        """hasUniqueness produces same result on both engines."""
        check = Check(CheckLevel.Error, "uniqueness").hasUniqueness(["att2"], is_one())
        spark_results = dual_engines_distinct.spark_engine.run_checks([check])
        duckdb_results = dual_engines_distinct.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasUniqueness")

    def test_has_distinctness(self, dual_engines_distinct: DualEngines):
        """hasDistinctness produces same result on both engines."""
        check = Check(CheckLevel.Error, "distinct").hasDistinctness(["att1"], gte(0.5))
        spark_results = dual_engines_distinct.spark_engine.run_checks([check])
        duckdb_results = dual_engines_distinct.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasDistinctness")


@requires_spark
class TestStatisticalConstraintParity:
    """Parity tests for statistical constraints."""

    def test_has_min(self, dual_engines_numeric: DualEngines):
        """hasMin produces same result on both engines."""
        check = Check(CheckLevel.Error, "min").hasMin("att1", eq(1))
        spark_results = dual_engines_numeric.spark_engine.run_checks([check])
        duckdb_results = dual_engines_numeric.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasMin")

    def test_has_max(self, dual_engines_numeric: DualEngines):
        """hasMax produces same result on both engines."""
        check = Check(CheckLevel.Error, "max").hasMax("att1", eq(6))
        spark_results = dual_engines_numeric.spark_engine.run_checks([check])
        duckdb_results = dual_engines_numeric.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasMax")

    def test_has_mean(self, dual_engines_numeric: DualEngines):
        """hasMean produces same result on both engines."""
        check = Check(CheckLevel.Error, "mean").hasMean("att1", eq(3.5))
        spark_results = dual_engines_numeric.spark_engine.run_checks([check])
        duckdb_results = dual_engines_numeric.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasMean")

    def test_has_sum(self, dual_engines_numeric: DualEngines):
        """hasSum produces same result on both engines."""
        check = Check(CheckLevel.Error, "sum").hasSum("att1", eq(21))
        spark_results = dual_engines_numeric.spark_engine.run_checks([check])
        duckdb_results = dual_engines_numeric.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasSum")

    def test_has_standard_deviation(self, dual_engines_numeric: DualEngines):
        """hasStandardDeviation produces same result on both engines."""
        check = Check(CheckLevel.Error, "stddev").hasStandardDeviation("att1", between(1.5, 2.0))
        spark_results = dual_engines_numeric.spark_engine.run_checks([check])
        duckdb_results = dual_engines_numeric.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasStandardDeviation")


@requires_spark
class TestCorrelationConstraintParity:
    """Parity tests for correlation constraints."""

    def test_has_correlation(self, dual_engines_correlation: DualEngines):
        """hasCorrelation produces same result on both engines."""
        check = Check(CheckLevel.Error, "corr").hasCorrelation("x", "y", is_one())
        spark_results = dual_engines_correlation.spark_engine.run_checks([check])
        duckdb_results = dual_engines_correlation.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasCorrelation")


@requires_spark
class TestEntropyConstraintParity:
    """Parity tests for entropy constraints."""

    def test_has_entropy(self, dual_engines_entropy: DualEngines):
        """hasEntropy produces same result on both engines."""
        check = Check(CheckLevel.Error, "entropy").hasEntropy("uniform", eq(2.0))
        spark_results = dual_engines_entropy.spark_engine.run_checks([check])
        duckdb_results = dual_engines_entropy.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasEntropy")


@requires_spark
class TestStringConstraintParity:
    """Parity tests for string constraints."""

    def test_has_min_length(self, dual_engines_string_lengths: DualEngines):
        """hasMinLength produces same result on both engines."""
        check = Check(CheckLevel.Error, "min len").hasMinLength("att1", eq(0))
        spark_results = dual_engines_string_lengths.spark_engine.run_checks([check])
        duckdb_results = dual_engines_string_lengths.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasMinLength")

    def test_has_max_length(self, dual_engines_string_lengths: DualEngines):
        """hasMaxLength produces same result on both engines."""
        check = Check(CheckLevel.Error, "max len").hasMaxLength("att1", lte(5))
        spark_results = dual_engines_string_lengths.spark_engine.run_checks([check])
        duckdb_results = dual_engines_string_lengths.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasMaxLength")

    def test_has_pattern(self, dual_engines_pattern: DualEngines):
        """hasPattern produces same result on both engines."""
        check = Check(CheckLevel.Error, "pattern").hasPattern("email", r".*@.*\..*", gte(0.5))
        spark_results = dual_engines_pattern.spark_engine.run_checks([check])
        duckdb_results = dual_engines_pattern.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "hasPattern")


@requires_spark
class TestNumericConstraintParity:
    """Parity tests for numeric value constraints."""

    def test_is_positive(self, dual_engines_compliance: DualEngines):
        """isPositive produces same result on both engines."""
        check = Check(CheckLevel.Error, "positive").isPositive("positive", is_one())
        spark_results = dual_engines_compliance.spark_engine.run_checks([check])
        duckdb_results = dual_engines_compliance.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isPositive")

    def test_is_non_negative(self, dual_engines_compliance: DualEngines):
        """isNonNegative produces same result on both engines."""
        check = Check(CheckLevel.Error, "non-neg").isNonNegative("positive", is_one())
        spark_results = dual_engines_compliance.spark_engine.run_checks([check])
        duckdb_results = dual_engines_compliance.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isNonNegative")


@requires_spark
class TestColumnComparisonConstraintParity:
    """Parity tests for column comparison constraints."""

    def test_is_less_than(self, dual_engines_correlation: DualEngines):
        """isLessThan produces same result on both engines."""
        check = Check(CheckLevel.Error, "less").isLessThan("x", "y")
        spark_results = dual_engines_correlation.spark_engine.run_checks([check])
        duckdb_results = dual_engines_correlation.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isLessThan")

    def test_is_greater_than(self, dual_engines_correlation: DualEngines):
        """isGreaterThan produces same result on both engines."""
        check = Check(CheckLevel.Error, "greater").isGreaterThan("y", "x")
        spark_results = dual_engines_correlation.spark_engine.run_checks([check])
        duckdb_results = dual_engines_correlation.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isGreaterThan")


@requires_spark
class TestContainedInConstraintParity:
    """Parity tests for isContainedIn constraint."""

    def test_is_contained_in_success(self, dual_engines_contained_in: DualEngines):
        """isContainedIn produces same result on both engines when passing."""
        check = Check(CheckLevel.Error, "contained").isContainedIn(
            "status", ["active", "inactive", "pending"], is_one()
        )
        spark_results = dual_engines_contained_in.spark_engine.run_checks([check])
        duckdb_results = dual_engines_contained_in.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isContainedIn success")

    def test_is_contained_in_failure(self, dual_engines_contained_in: DualEngines):
        """isContainedIn produces same result on both engines when failing."""
        check = Check(CheckLevel.Error, "not contained").isContainedIn(
            "category", ["A", "B", "C"], is_one()
        )
        spark_results = dual_engines_contained_in.spark_engine.run_checks([check])
        duckdb_results = dual_engines_contained_in.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "isContainedIn failure")


@requires_spark
class TestSatisfiesConstraintParity:
    """Parity tests for satisfies constraint."""

    def test_satisfies(self, dual_engines_compliance: DualEngines):
        """satisfies produces same result on both engines."""
        check = Check(CheckLevel.Error, "satisfies").satisfies(
            "positive > 0", "positive_check", assertion=is_one()
        )
        spark_results = dual_engines_compliance.spark_engine.run_checks([check])
        duckdb_results = dual_engines_compliance.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "satisfies")


@requires_spark
class TestMultipleConstraintsParity:
    """Parity tests for multiple constraints."""

    def test_multiple_constraints_all_pass(self, dual_engines_full: DualEngines):
        """Multiple passing constraints produce same results."""
        check = (Check(CheckLevel.Error, "multi pass")
                .hasSize(eq(4))
                .isComplete("att1")
                .isComplete("att2"))
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "Multiple pass")

    def test_multiple_constraints_some_fail(self, dual_engines_missing: DualEngines):
        """Mixed pass/fail constraints produce same results."""
        check = (Check(CheckLevel.Error, "multi mixed")
                .hasSize(eq(12))  # Pass
                .isComplete("att1"))  # Fail
        spark_results = dual_engines_missing.spark_engine.run_checks([check])
        duckdb_results = dual_engines_missing.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "Multiple mixed")


@requires_spark
class TestCheckLevelsParity:
    """Parity tests for check levels."""

    def test_error_level(self, dual_engines_full: DualEngines):
        """Error level produces same results on both engines."""
        check = Check(CheckLevel.Error, "error").hasSize(eq(100))
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "Error level")

    def test_warning_level(self, dual_engines_full: DualEngines):
        """Warning level produces same results on both engines."""
        check = Check(CheckLevel.Warning, "warning").hasSize(eq(100))
        spark_results = dual_engines_full.spark_engine.run_checks([check])
        duckdb_results = dual_engines_full.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "Warning level")


@requires_spark
class TestConstraintsWithWhereParity:
    """Parity tests for constraints with WHERE clause."""

    def test_completeness_with_where(self, dual_engines_where: DualEngines):
        """Completeness with WHERE produces same result on both engines."""
        check = Check(CheckLevel.Error, "filtered").hasCompleteness(
            "att1", is_one()
        ).where("category = 'A'")
        spark_results = dual_engines_where.spark_engine.run_checks([check])
        duckdb_results = dual_engines_where.duckdb_engine.run_checks([check])
        assert_constraints_match(spark_results, duckdb_results, "Completeness with WHERE")
