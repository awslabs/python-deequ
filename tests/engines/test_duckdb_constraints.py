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

"""DuckDB-only constraint tests.

Tests all 32 constraint types against known expected values from the test datasets.
These tests do not require Spark and can run quickly in CI.
"""

import pytest

from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.predicates import eq, gt, gte, lt, lte, between, is_one
from pydeequ.v2.verification import VerificationSuite
from pydeequ.engines import ConstraintStatus, CheckStatus


def get_constraint_result(results, constraint_substring: str):
    """Find a constraint result by substring match on constraint name."""
    for r in results:
        if constraint_substring in r.constraint:
            return r
    return None


def get_check_result(results, check_description: str):
    """Find results for a specific check by description."""
    return [r for r in results if r.check_description == check_description]


class TestSizeConstraint:
    """Tests for hasSize constraint."""

    def test_has_size_success(self, engine_full):
        """hasSize succeeds when size equals expected."""
        check = Check(CheckLevel.Error, "size check").hasSize(eq(4))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_size_failure(self, engine_full):
        """hasSize fails when size doesn't match."""
        check = Check(CheckLevel.Error, "size check").hasSize(eq(10))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_size_range(self, engine_full):
        """hasSize with between predicate."""
        check = Check(CheckLevel.Error, "size range").hasSize(between(3, 5))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_size_empty(self, engine_empty):
        """hasSize correctly reports 0 for empty dataset."""
        check = Check(CheckLevel.Error, "empty size").hasSize(eq(0))
        results = engine_empty.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestCompletenessConstraints:
    """Tests for completeness-related constraints."""

    def test_is_complete_success(self, engine_full):
        """isComplete succeeds for non-NULL column."""
        check = Check(CheckLevel.Error, "complete").isComplete("att1")
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_complete_failure(self, engine_missing):
        """isComplete fails for column with NULLs."""
        check = Check(CheckLevel.Error, "complete").isComplete("att1")
        results = engine_missing.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_completeness_success(self, engine_missing):
        """hasCompleteness succeeds when threshold is met."""
        # att1 is 50% complete, check for >= 50%
        check = Check(CheckLevel.Error, "partial complete").hasCompleteness("att1", gte(0.5))
        results = engine_missing.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_completeness_failure(self, engine_missing):
        """hasCompleteness fails when threshold not met."""
        # att1 is 50% complete, check for >= 90%
        check = Check(CheckLevel.Error, "high threshold").hasCompleteness("att1", gte(0.9))
        results = engine_missing.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_are_complete_success(self, engine_full):
        """areComplete succeeds when all columns are complete."""
        check = Check(CheckLevel.Error, "multi complete").areComplete(["att1", "att2"])
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_are_complete_failure(self, engine_missing):
        """areComplete fails when any column has NULLs."""
        check = Check(CheckLevel.Error, "multi complete").areComplete(["att1", "att2"])
        results = engine_missing.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_have_completeness_success(self, engine_missing):
        """haveCompleteness succeeds for combined column threshold."""
        check = Check(CheckLevel.Error, "combined").haveCompleteness(["att1", "att2"], gte(0.5))
        results = engine_missing.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestUniquenessConstraints:
    """Tests for uniqueness-related constraints."""

    def test_is_unique_success(self, engine_unique):
        """isUnique succeeds when all values are unique."""
        check = Check(CheckLevel.Error, "unique").isUnique("unique_col")
        results = engine_unique.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_unique_failure(self, engine_unique):
        """isUnique fails when there are duplicates."""
        check = Check(CheckLevel.Error, "not unique").isUnique("non_unique")
        results = engine_unique.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_uniqueness_success(self, engine_unique):
        """hasUniqueness succeeds when threshold met."""
        check = Check(CheckLevel.Error, "uniqueness").hasUniqueness(["unique_col"], is_one())
        results = engine_unique.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_uniqueness_failure(self, engine_distinct):
        """hasUniqueness fails when uniqueness is below threshold."""
        # att1 has all duplicates, uniqueness = 0
        check = Check(CheckLevel.Error, "low uniqueness").hasUniqueness(["att1"], gte(0.5))
        results = engine_distinct.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_distinctness_success(self, engine_distinct):
        """hasDistinctness succeeds when threshold met."""
        # att2 has 6 distinct / 6 rows = 1.0
        check = Check(CheckLevel.Error, "distinct").hasDistinctness(["att2"], is_one())
        results = engine_distinct.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_distinctness_partial(self, engine_distinct):
        """hasDistinctness with partial distinctness."""
        # att1 has 3 distinct / 6 rows = 0.5
        check = Check(CheckLevel.Error, "partial distinct").hasDistinctness(["att1"], gte(0.5))
        results = engine_distinct.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_unique_value_ratio_success(self, engine_distinct):
        """hasUniqueValueRatio succeeds for all-unique column."""
        check = Check(CheckLevel.Error, "uvr").hasUniqueValueRatio(["att2"], is_one())
        results = engine_distinct.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_unique_value_ratio_zero(self, engine_distinct):
        """hasUniqueValueRatio for all-duplicated column."""
        # att1: 0 unique / 3 distinct = 0
        check = Check(CheckLevel.Error, "uvr zero").hasUniqueValueRatio(["att1"], eq(0))
        results = engine_distinct.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestStatisticalConstraints:
    """Tests for statistical constraints."""

    def test_has_min_success(self, engine_numeric):
        """hasMin succeeds when minimum matches."""
        check = Check(CheckLevel.Error, "min").hasMin("att1", eq(1))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_min_failure(self, engine_numeric):
        """hasMin fails when minimum doesn't match."""
        check = Check(CheckLevel.Error, "min fail").hasMin("att1", eq(5))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_max_success(self, engine_numeric):
        """hasMax succeeds when maximum matches."""
        check = Check(CheckLevel.Error, "max").hasMax("att1", eq(6))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_max_failure(self, engine_numeric):
        """hasMax fails when maximum doesn't match."""
        check = Check(CheckLevel.Error, "max fail").hasMax("att1", eq(100))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_mean_success(self, engine_numeric):
        """hasMean succeeds when mean matches."""
        check = Check(CheckLevel.Error, "mean").hasMean("att1", eq(3.5))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_mean_range(self, engine_numeric):
        """hasMean with range predicate."""
        check = Check(CheckLevel.Error, "mean range").hasMean("att1", between(3.0, 4.0))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_sum_success(self, engine_numeric):
        """hasSum succeeds when sum matches."""
        check = Check(CheckLevel.Error, "sum").hasSum("att1", eq(21))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_standard_deviation_success(self, engine_numeric):
        """hasStandardDeviation with range check."""
        check = Check(CheckLevel.Error, "stddev").hasStandardDeviation("att1", between(1.5, 2.0))
        results = engine_numeric.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_approx_count_distinct_success(self, engine_full):
        """hasApproxCountDistinct succeeds when count is approximately correct."""
        check = Check(CheckLevel.Error, "approx distinct").hasApproxCountDistinct("att1", between(2, 4))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestQuantileConstraints:
    """Tests for quantile constraints."""

    def test_has_approx_quantile_median(self, engine_quantile):
        """hasApproxQuantile for median."""
        check = Check(CheckLevel.Error, "median").hasApproxQuantile("value", 0.5, between(5.0, 6.0))
        results = engine_quantile.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestCorrelationConstraints:
    """Tests for correlation constraints."""

    def test_has_correlation_positive(self, engine_correlation):
        """hasCorrelation for perfectly correlated columns."""
        check = Check(CheckLevel.Error, "positive corr").hasCorrelation("x", "y", is_one())
        results = engine_correlation.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_correlation_negative(self, engine_correlation):
        """hasCorrelation for negative correlation."""
        check = Check(CheckLevel.Error, "negative corr").hasCorrelation("x", "z", eq(-1))
        results = engine_correlation.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestEntropyConstraints:
    """Tests for entropy constraints."""

    def test_has_entropy_uniform(self, engine_entropy):
        """hasEntropy for uniform distribution."""
        # ln(4) ≈ 1.386 (matches Spark's natural log convention)
        check = Check(CheckLevel.Error, "entropy").hasEntropy("uniform", between(1.38, 1.39))
        results = engine_entropy.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_entropy_constant(self, engine_entropy):
        """hasEntropy for constant column (entropy=0)."""
        check = Check(CheckLevel.Error, "zero entropy").hasEntropy("constant", eq(0))
        results = engine_entropy.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestMutualInformationConstraints:
    """Tests for mutual information constraints."""

    def test_has_mutual_information(self, engine_mutual_info):
        """hasMutualInformation for dependent columns."""
        check = Check(CheckLevel.Error, "mi").hasMutualInformation("x", "y_dependent", gt(0))
        results = engine_mutual_info.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestStringLengthConstraints:
    """Tests for string length constraints."""

    def test_has_min_length_success(self, engine_string_lengths):
        """hasMinLength for empty string (0)."""
        check = Check(CheckLevel.Error, "min length").hasMinLength("att1", eq(0))
        results = engine_string_lengths.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_min_length_failure(self, engine_string_lengths):
        """hasMinLength fails when min length is higher."""
        check = Check(CheckLevel.Error, "min length fail").hasMinLength("att1", gte(2))
        results = engine_string_lengths.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_has_max_length_success(self, engine_string_lengths):
        """hasMaxLength succeeds when max is correct."""
        check = Check(CheckLevel.Error, "max length").hasMaxLength("att1", eq(4))
        results = engine_string_lengths.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_max_length_bound(self, engine_string_lengths):
        """hasMaxLength with upper bound."""
        check = Check(CheckLevel.Error, "max bound").hasMaxLength("att1", lte(5))
        results = engine_string_lengths.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestPatternConstraints:
    """Tests for pattern matching constraints."""

    def test_has_pattern_success(self, engine_full):
        """hasPattern succeeds when pattern matches all rows."""
        check = Check(CheckLevel.Error, "pattern").hasPattern("att1", r"^[a-c]$", is_one())
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_pattern_partial(self, engine_pattern):
        """hasPattern with partial match threshold."""
        # Email pattern matches 4/6 rows
        check = Check(CheckLevel.Error, "email pattern").hasPattern("email", r".*@.*\..*", gte(0.5))
        results = engine_pattern.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_has_pattern_failure(self, engine_pattern):
        """hasPattern fails when match rate is below threshold."""
        check = Check(CheckLevel.Error, "strict pattern").hasPattern("email", r".*@.*\..*", is_one())
        results = engine_pattern.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure


class TestEmailUrlConstraints:
    """Tests for email and URL pattern constraints."""

    def test_contains_email_success(self, engine_pattern):
        """containsEmail with threshold."""
        check = Check(CheckLevel.Error, "email").containsEmail("email", gte(0.5))
        results = engine_pattern.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_contains_url_failure(self, engine_pattern):
        """containsURL fails for non-URL column."""
        check = Check(CheckLevel.Error, "url").containsURL("email", gte(0.5))
        results = engine_pattern.run_checks([check])
        result = results[0]
        # No URLs in email column
        assert result.constraint_status == ConstraintStatus.Failure


class TestNumericConstraints:
    """Tests for numeric value constraints."""

    def test_is_positive_success(self, engine_compliance):
        """isPositive succeeds for all-positive column."""
        check = Check(CheckLevel.Error, "positive").isPositive("positive", is_one())
        results = engine_compliance.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_positive_failure(self, engine_compliance):
        """isPositive fails for negative column."""
        check = Check(CheckLevel.Error, "not positive").isPositive("negative", gte(0.5))
        results = engine_compliance.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Failure

    def test_is_non_negative_success(self, engine_compliance):
        """isNonNegative for positive column."""
        check = Check(CheckLevel.Error, "non-neg").isNonNegative("positive", is_one())
        results = engine_compliance.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_non_negative_partial(self, engine_compliance):
        """isNonNegative with partial compliance."""
        # mixed: [-2,-1,0,1,2,3] -> 4/6 non-negative
        check = Check(CheckLevel.Error, "partial non-neg").isNonNegative("mixed", gte(0.5))
        results = engine_compliance.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestColumnComparisonConstraints:
    """Tests for column comparison constraints."""

    def test_is_less_than(self, engine_correlation):
        """isLessThan for ordered columns."""
        # x = [1,2,3,4,5], y = [2,4,6,8,10], so x < y always
        check = Check(CheckLevel.Error, "less than").isLessThan("x", "y")
        results = engine_correlation.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_less_than_or_equal_to(self, engine_correlation):
        """isLessThanOrEqualTo for ordered columns."""
        check = Check(CheckLevel.Error, "lte").isLessThanOrEqualTo("x", "y")
        results = engine_correlation.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_greater_than(self, engine_correlation):
        """isGreaterThan for reverse-ordered columns."""
        # y > x always
        check = Check(CheckLevel.Error, "greater than").isGreaterThan("y", "x")
        results = engine_correlation.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_greater_than_or_equal_to(self, engine_correlation):
        """isGreaterThanOrEqualTo for ordered columns."""
        check = Check(CheckLevel.Error, "gte").isGreaterThanOrEqualTo("y", "x")
        results = engine_correlation.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestContainedInConstraint:
    """Tests for isContainedIn constraint."""

    def test_is_contained_in_success(self, engine_contained_in):
        """isContainedIn succeeds when all values are in allowed set."""
        check = Check(CheckLevel.Error, "contained").isContainedIn(
            "status", ["active", "inactive", "pending"], is_one()
        )
        results = engine_contained_in.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_is_contained_in_failure(self, engine_contained_in):
        """isContainedIn fails when some values are not in set."""
        check = Check(CheckLevel.Error, "not contained").isContainedIn(
            "category", ["A", "B", "C"], is_one()
        )
        results = engine_contained_in.run_checks([check])
        result = results[0]
        # "D" is not in the allowed set
        assert result.constraint_status == ConstraintStatus.Failure

    def test_is_contained_in_partial(self, engine_contained_in):
        """isContainedIn with threshold for partial match."""
        check = Check(CheckLevel.Error, "partial contained").isContainedIn(
            "category", ["A", "B", "C"], gte(0.8)
        )
        results = engine_contained_in.run_checks([check])
        result = results[0]
        # 5/6 = 0.833 in allowed set
        assert result.constraint_status == ConstraintStatus.Success


class TestSatisfiesConstraint:
    """Tests for satisfies constraint."""

    def test_satisfies_simple(self, engine_compliance):
        """satisfies with simple predicate."""
        check = Check(CheckLevel.Error, "satisfies").satisfies("positive > 0", "positive_check", is_one())
        results = engine_compliance.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_satisfies_complex(self, engine_compliance):
        """satisfies with complex predicate."""
        check = Check(CheckLevel.Error, "complex").satisfies(
            "mixed >= -2 AND mixed <= 3", "range_check", is_one()
        )
        results = engine_compliance.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_satisfies_partial(self, engine_compliance):
        """satisfies with partial compliance."""
        check = Check(CheckLevel.Error, "partial satisfies").satisfies("mixed > 0", "partial_check", gte(0.4))
        results = engine_compliance.run_checks([check])
        result = results[0]
        # 3/6 = 0.5 > 0.4
        assert result.constraint_status == ConstraintStatus.Success


class TestCheckLevels:
    """Tests for check levels (Error vs Warning)."""

    def test_error_level_failure(self, engine_full):
        """Error level check results in Error status on failure."""
        check = Check(CheckLevel.Error, "error check").hasSize(eq(100))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.check_level == "Error"
        assert result.check_status == CheckStatus.Error

    def test_warning_level_failure(self, engine_full):
        """Warning level check results in Warning status on failure."""
        check = Check(CheckLevel.Warning, "warning check").hasSize(eq(100))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.check_level == "Warning"
        assert result.check_status == CheckStatus.Warning

    def test_error_level_success(self, engine_full):
        """Error level check results in Success status on pass."""
        check = Check(CheckLevel.Error, "pass check").hasSize(eq(4))
        results = engine_full.run_checks([check])
        result = results[0]
        assert result.check_status == CheckStatus.Success


class TestMultipleConstraints:
    """Tests for multiple constraints in one check."""

    def test_all_pass(self, engine_full):
        """All constraints pass results in Success."""
        check = (Check(CheckLevel.Error, "all pass")
                .hasSize(eq(4))
                .isComplete("att1")
                .isComplete("att2"))
        results = engine_full.run_checks([check])
        assert all(r.constraint_status == ConstraintStatus.Success for r in results)
        assert results[0].check_status == CheckStatus.Success

    def test_some_fail(self, engine_missing):
        """Some constraints fail results in overall failure."""
        check = (Check(CheckLevel.Error, "some fail")
                .hasSize(eq(12))  # Pass
                .isComplete("att1")  # Fail
                .hasCompleteness("att2", gte(0.5)))  # Pass
        results = engine_missing.run_checks([check])
        # Check that at least one constraint failed
        failed = [r for r in results if r.constraint_status == ConstraintStatus.Failure]
        assert len(failed) >= 1
        # Overall check should fail
        assert results[0].check_status == CheckStatus.Error

    def test_multiple_checks(self, engine_numeric):
        """Multiple checks can be run together."""
        check1 = Check(CheckLevel.Error, "size check").hasSize(eq(6))
        check2 = Check(CheckLevel.Error, "mean check").hasMean("att1", eq(3.5))
        check3 = Check(CheckLevel.Warning, "sum check").hasSum("att1", eq(21))

        results = engine_numeric.run_checks([check1, check2, check3])
        # All should pass
        assert len(results) == 3
        assert all(r.constraint_status == ConstraintStatus.Success for r in results)


class TestConstraintsWithWhere:
    """Tests for constraints with WHERE clause filtering."""

    @pytest.mark.skip(reason="WHERE clause support not yet implemented in Check API")
    def test_completeness_where(self, engine_where):
        """Completeness constraint with WHERE filter."""
        check = Check(CheckLevel.Error, "filtered completeness").hasCompleteness(
            "att1", is_one(), where="category = 'A'"
        )
        results = engine_where.run_checks([check])
        result = results[0]
        # Category A: att1 is complete
        assert result.constraint_status == ConstraintStatus.Success

    @pytest.mark.skip(reason="WHERE clause support not yet implemented in Check API")
    def test_size_where(self, engine_where):
        """Size constraint with WHERE filter."""
        check = Check(CheckLevel.Error, "filtered size").hasSize(
            eq(2), where="category = 'A'"
        )
        results = engine_where.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_dataset(self, engine_empty):
        """Constraints on empty dataset."""
        check = (Check(CheckLevel.Error, "empty check")
                .hasSize(eq(0)))
        results = engine_empty.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success

    def test_single_row(self, engine_single):
        """Constraints on single-row dataset."""
        check = (Check(CheckLevel.Error, "single row")
                .hasSize(eq(1))
                .isComplete("att1")
                .hasMin("item", eq(1))
                .hasMax("item", eq(1)))
        results = engine_single.run_checks([check])
        assert all(r.constraint_status == ConstraintStatus.Success for r in results)

    def test_all_null_column(self, engine_all_null):
        """Constraints on all-NULL column."""
        check = (Check(CheckLevel.Error, "all null")
                .hasCompleteness("value", eq(0)))
        results = engine_all_null.run_checks([check])
        result = results[0]
        assert result.constraint_status == ConstraintStatus.Success
