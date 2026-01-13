# -*- coding: utf-8 -*-
"""
Tests for Check constraints using Spark Connect.

These tests verify the core constraint functionality of PyDeequ v2.
"""

import pytest
from pyspark.sql import Row

from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.verification import VerificationSuite
from pydeequ.v2.predicates import eq, gt, gte, lt, lte, between


class TestCheckConstraints:
    """Test individual constraint types."""

    def test_hasSize(self, spark, sample_df):
        """Test hasSize constraint."""
        check = Check(CheckLevel.Error, "size check").hasSize(eq(3))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["constraint_status"] == "Success"

    def test_hasSize_failure(self, spark, sample_df):
        """Test hasSize constraint failure."""
        check = Check(CheckLevel.Error, "size check").hasSize(eq(5))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Failure"

    def test_isComplete(self, spark, sample_df):
        """Test isComplete constraint on complete column."""
        check = Check(CheckLevel.Error, "completeness check").isComplete("a")

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_isComplete_failure(self, spark, sample_df):
        """Test isComplete constraint on incomplete column."""
        check = Check(CheckLevel.Error, "completeness check").isComplete("c")

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Failure"

    def test_hasCompleteness(self, spark, sample_df):
        """Test hasCompleteness with threshold."""
        # Column c has 2/3 completeness
        check = Check(CheckLevel.Error, "completeness check").hasCompleteness(
            "c", gte(0.5)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_hasCompleteness_failure(self, spark, sample_df):
        """Test hasCompleteness failure."""
        check = Check(CheckLevel.Error, "completeness check").hasCompleteness(
            "c", gte(0.9)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Failure"

    def test_isUnique(self, spark, sample_df):
        """Test isUnique constraint."""
        check = Check(CheckLevel.Error, "uniqueness check").isUnique("b")

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_isUnique_failure(self, spark, sample_df):
        """Test isUnique constraint failure on non-unique column."""
        check = Check(CheckLevel.Error, "uniqueness check").isUnique("d")

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Failure"

    def test_hasUniqueness(self, spark, sample_df):
        """Test hasUniqueness with multiple columns."""
        check = Check(CheckLevel.Error, "uniqueness check").hasUniqueness(
            ["a", "b"], eq(1.0)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_hasMin(self, spark, sample_df):
        """Test hasMin constraint."""
        check = Check(CheckLevel.Error, "min check").hasMin("b", eq(1.0))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_hasMax(self, spark, sample_df):
        """Test hasMax constraint."""
        check = Check(CheckLevel.Error, "max check").hasMax("b", eq(3.0))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_hasMean(self, spark, sample_df):
        """Test hasMean constraint."""
        check = Check(CheckLevel.Error, "mean check").hasMean("b", eq(2.0))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_hasSum(self, spark, sample_df):
        """Test hasSum constraint."""
        check = Check(CheckLevel.Error, "sum check").hasSum("b", eq(6.0))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"

    def test_hasStandardDeviation(self, spark, sample_df):
        """Test hasStandardDeviation constraint."""
        # std of [1,2,3] is ~0.816
        check = Check(CheckLevel.Error, "std check").hasStandardDeviation(
            "b", between(0.8, 0.9)
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["constraint_status"] == "Success"


class TestCheckChaining:
    """Test chaining multiple constraints."""

    def test_multiple_constraints_all_pass(self, spark, sample_df):
        """Test multiple constraints that all pass."""
        check = (
            Check(CheckLevel.Error, "multi check")
            .hasSize(eq(3))
            .isComplete("a")
            .isUnique("b")
        )

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) == 3
        assert all(row["constraint_status"] == "Success" for row in rows)

    def test_multiple_constraints_some_fail(self, spark, sample_df):
        """Test multiple constraints with some failures."""
        check = (
            Check(CheckLevel.Error, "multi check")
            .hasSize(eq(3))  # pass
            .isComplete("c")  # fail (has null)
            .isUnique("d")
        )  # fail (all same value)

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert len(rows) == 3
        statuses = [row["constraint_status"] for row in rows]
        assert statuses.count("Success") == 1
        assert statuses.count("Failure") == 2


class TestCheckLevels:
    """Test check level (Error vs Warning)."""

    def test_error_level(self, spark, sample_df):
        """Test Error level check."""
        check = Check(CheckLevel.Error, "error check").hasSize(eq(3))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["check_level"] == "Error"

    def test_warning_level(self, spark, sample_df):
        """Test Warning level check."""
        check = Check(CheckLevel.Warning, "warning check").hasSize(eq(3))

        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()

        rows = result.collect()
        assert rows[0]["check_level"] == "Warning"


class TestPredicates:
    """Test different predicate types."""

    def test_eq_predicate(self, spark, sample_df):
        """Test eq (equals) predicate."""
        check = Check(CheckLevel.Error, "eq test").hasSize(eq(3))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_gt_predicate(self, spark, sample_df):
        """Test gt (greater than) predicate."""
        check = Check(CheckLevel.Error, "gt test").hasSize(gt(2))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_gte_predicate(self, spark, sample_df):
        """Test gte (greater than or equal) predicate."""
        check = Check(CheckLevel.Error, "gte test").hasSize(gte(3))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_lt_predicate(self, spark, sample_df):
        """Test lt (less than) predicate."""
        check = Check(CheckLevel.Error, "lt test").hasSize(lt(4))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_lte_predicate(self, spark, sample_df):
        """Test lte (less than or equal) predicate."""
        check = Check(CheckLevel.Error, "lte test").hasSize(lte(3))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_between_predicate(self, spark, sample_df):
        """Test between predicate."""
        check = Check(CheckLevel.Error, "between test").hasSize(between(2, 4))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"


class TestAdditionalConstraints:
    """Test additional constraint types."""

    def test_areComplete(self, spark, sample_df):
        """Test areComplete constraint."""
        check = Check(CheckLevel.Error, "are complete").areComplete(["a", "b"])
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_hasDistinctness(self, spark, sample_df):
        """Test hasDistinctness constraint."""
        # Column b has 3 distinct values out of 3 rows = 1.0 distinctness
        check = Check(CheckLevel.Error, "distinctness").hasDistinctness(["b"], eq(1.0))
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_hasApproxCountDistinct(self, spark, sample_df):
        """Test hasApproxCountDistinct constraint."""
        check = Check(CheckLevel.Error, "approx count").hasApproxCountDistinct(
            "b", eq(3.0)
        )
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_satisfies(self, spark, sample_df):
        """Test satisfies constraint with SQL expression."""
        check = Check(CheckLevel.Error, "satisfies").satisfies(
            "b > 0", "positive_b", eq(1.0)
        )
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_hasPattern(self, spark, extended_df):
        """Test hasPattern constraint."""
        # All emails match the pattern
        check = Check(CheckLevel.Error, "pattern").hasPattern(
            "email", r".*@.*\.com", eq(1.0)
        )
        result = VerificationSuite(spark).onData(extended_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_containsEmail(self, spark, extended_df):
        """Test containsEmail constraint."""
        check = Check(CheckLevel.Error, "email").containsEmail("email", eq(1.0))
        result = VerificationSuite(spark).onData(extended_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_containsCreditCardNumber(self, spark, extended_df):
        """Test containsCreditCardNumber constraint."""
        check = Check(CheckLevel.Error, "credit card").containsCreditCardNumber(
            "creditCard", eq(1.0)
        )
        result = VerificationSuite(spark).onData(extended_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_isNonNegative(self, spark, sample_df):
        """Test isNonNegative constraint."""
        check = Check(CheckLevel.Error, "non negative").isNonNegative("b")
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"

    def test_isPositive(self, spark, sample_df):
        """Test isPositive constraint."""
        check = Check(CheckLevel.Error, "positive").isPositive("b")
        result = VerificationSuite(spark).onData(sample_df).addCheck(check).run()
        assert result.collect()[0]["constraint_status"] == "Success"
