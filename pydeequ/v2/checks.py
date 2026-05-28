# -*- coding: utf-8 -*-
"""
Check class for Deequ Spark Connect.

This module provides a Spark Connect compatible Check class that builds
protobuf messages instead of using Py4J to call Scala code directly.

Example usage:
    from pydeequ.v2.checks import Check, CheckLevel
    from pydeequ.v2.predicates import gte, eq, between

    check = (Check(CheckLevel.Error, "Data quality check")
        .isComplete("id")
        .hasCompleteness("email", gte(0.95))
        .hasSize(eq(1000))
        .hasMean("amount", between(100, 500)))
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional, Sequence

from pydeequ.v2.predicates import Predicate, is_one
from pydeequ.v2.proto import deequ_connect_pb2 as proto


class CheckLevel(Enum):
    """Check severity level."""

    Error = "Error"
    Warning = "Warning"


class Check:
    """
    Check class for Spark Connect - builds protobuf messages.

    A Check is a collection of constraints that can be applied to a DataFrame.
    When the Check is run, each constraint is evaluated and the results are
    aggregated based on the Check's level (Error or Warning).

    Unlike the Py4J-based Check, this class does not require a SparkSession
    at construction time since it only builds protobuf messages.

    Example:
        check = (Check(CheckLevel.Error, "Data quality check")
            .isComplete("id")
            .hasCompleteness("email", gte(0.95))
            .hasSize(eq(1000)))
    """

    def __init__(self, level: CheckLevel, description: str):
        """
        Create a new Check.

        Args:
            level: The severity level (Error or Warning)
            description: Human-readable description of this check
        """
        self.level = level
        self.description = description
        self._constraints: List[proto.ConstraintMessage] = []

    def _add_constraint(
        self,
        constraint_type: str,
        column: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
        where: Optional[str] = None,
        pattern: Optional[str] = None,
        column_condition: Optional[str] = None,
        constraint_name: Optional[str] = None,
        allowed_values: Optional[Sequence[str]] = None,
        quantile: Optional[float] = None,
    ) -> "Check":
        """Internal method to add a constraint."""
        constraint = proto.ConstraintMessage(type=constraint_type)

        if column is not None:
            constraint.column = column
        if columns is not None:
            constraint.columns.extend(columns)
        if assertion is not None:
            constraint.assertion.CopyFrom(assertion.to_proto())
        if hint is not None:
            constraint.hint = hint
        if where is not None:
            constraint.where = where
        if pattern is not None:
            constraint.pattern = pattern
        if column_condition is not None:
            constraint.column_condition = column_condition
        if constraint_name is not None:
            constraint.constraint_name = constraint_name
        if allowed_values is not None:
            constraint.allowed_values.extend(allowed_values)
        if quantile is not None:
            constraint.quantile = quantile

        self._constraints.append(constraint)
        return self

    # ========================================================================
    # Size Constraints
    # ========================================================================

    def hasSize(self, assertion: Predicate, hint: Optional[str] = None) -> "Check":
        """
        Check that the DataFrame has a size satisfying the assertion.

        Args:
            assertion: Predicate to apply to the row count
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasSize(eq(1000))  # Must have exactly 1000 rows
            check.hasSize(gte(100))  # Must have at least 100 rows
        """
        return self._add_constraint("hasSize", assertion=assertion, hint=hint)

    # ========================================================================
    # Completeness Constraints
    # ========================================================================

    def isComplete(self, column: str, hint: Optional[str] = None) -> "Check":
        """
        Check that a column has no null values (100% complete).

        Args:
            column: Column name to check
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.isComplete("id")  # id column must have no nulls
        """
        return self._add_constraint(
            "isComplete", column=column, assertion=is_one(), hint=hint
        )

    def hasCompleteness(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that a column's completeness satisfies the assertion.

        Completeness is the fraction of non-null values (0.0 to 1.0).

        Args:
            column: Column name to check
            assertion: Predicate to apply to completeness value
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasCompleteness("email", gte(0.95))  # At least 95% complete
        """
        return self._add_constraint(
            "hasCompleteness", column=column, assertion=assertion, hint=hint
        )

    def areComplete(
        self, columns: Sequence[str], hint: Optional[str] = None
    ) -> "Check":
        """
        Check that all specified columns have no null values.

        Args:
            columns: Column names to check
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.areComplete(["id", "name", "email"])
        """
        return self._add_constraint(
            "areComplete", columns=columns, assertion=is_one(), hint=hint
        )

    def haveCompleteness(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that combined completeness of columns satisfies the assertion.

        Args:
            columns: Column names to check
            assertion: Predicate to apply to completeness value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "haveCompleteness", columns=columns, assertion=assertion, hint=hint
        )

    # ========================================================================
    # Uniqueness Constraints
    # ========================================================================

    def isUnique(self, column: str, hint: Optional[str] = None) -> "Check":
        """
        Check that a column has only unique values.

        Args:
            column: Column name to check
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.isUnique("id")  # id must be unique
        """
        return self._add_constraint("isUnique", column=column, hint=hint)

    def hasUniqueness(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that uniqueness of column(s) satisfies the assertion.

        Uniqueness is the fraction of unique values.

        Args:
            columns: Column names to check
            assertion: Predicate to apply to uniqueness value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasUniqueness", columns=columns, assertion=assertion, hint=hint
        )

    def hasDistinctness(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that distinctness of column(s) satisfies the assertion.

        Distinctness is the fraction of distinct values.

        Args:
            columns: Column names to check
            assertion: Predicate to apply to distinctness value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasDistinctness", columns=columns, assertion=assertion, hint=hint
        )

    def hasUniqueValueRatio(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that unique value ratio of column(s) satisfies the assertion.

        Args:
            columns: Column names to check
            assertion: Predicate to apply to ratio value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasUniqueValueRatio", columns=columns, assertion=assertion, hint=hint
        )

    # ========================================================================
    # Statistical Constraints
    # ========================================================================

    def hasMin(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the minimum value of a column satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to minimum value
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasMin("age", gte(0))  # Age must be non-negative
        """
        return self._add_constraint(
            "hasMin", column=column, assertion=assertion, hint=hint
        )

    def hasMax(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the maximum value of a column satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to maximum value
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasMax("price", lte(10000))  # Price must be <= 10000
        """
        return self._add_constraint(
            "hasMax", column=column, assertion=assertion, hint=hint
        )

    def hasMean(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the mean value of a column satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to mean value
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasMean("score", between(70, 90))
        """
        return self._add_constraint(
            "hasMean", column=column, assertion=assertion, hint=hint
        )

    def hasSum(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the sum of a column satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to sum value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasSum", column=column, assertion=assertion, hint=hint
        )

    def hasStandardDeviation(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the standard deviation of a column satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to std dev value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasStandardDeviation", column=column, assertion=assertion, hint=hint
        )

    def hasApproxCountDistinct(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the approximate count distinct satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to count distinct value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasApproxCountDistinct", column=column, assertion=assertion, hint=hint
        )

    def hasApproxQuantile(
        self,
        column: str,
        quantile: float,
        assertion: Predicate,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that an approximate quantile satisfies the assertion.

        Args:
            column: Column name to check
            quantile: Quantile to compute (0.0 to 1.0)
            assertion: Predicate to apply to quantile value
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasApproxQuantile("income", 0.5, between(30000, 80000))  # Median
        """
        return self._add_constraint(
            "hasApproxQuantile",
            column=column,
            quantile=quantile,
            assertion=assertion,
            hint=hint,
        )

    def hasCorrelation(
        self,
        column_a: str,
        column_b: str,
        assertion: Predicate,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that correlation between two columns satisfies the assertion.

        Args:
            column_a: First column name
            column_b: Second column name
            assertion: Predicate to apply to correlation value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasCorrelation",
            columns=[column_a, column_b],
            assertion=assertion,
            hint=hint,
        )

    def hasEntropy(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that the entropy of a column satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to entropy value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasEntropy", column=column, assertion=assertion, hint=hint
        )

    def hasMutualInformation(
        self,
        column_a: str,
        column_b: str,
        assertion: Predicate,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that mutual information between columns satisfies the assertion.

        Args:
            column_a: First column name
            column_b: Second column name
            assertion: Predicate to apply to mutual information value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasMutualInformation",
            columns=[column_a, column_b],
            assertion=assertion,
            hint=hint,
        )

    # ========================================================================
    # String Length Constraints
    # ========================================================================

    def hasMinLength(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that minimum string length satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to min length value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasMinLength", column=column, assertion=assertion, hint=hint
        )

    def hasMaxLength(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        """
        Check that maximum string length satisfies the assertion.

        Args:
            column: Column name to check
            assertion: Predicate to apply to max length value
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "hasMaxLength", column=column, assertion=assertion, hint=hint
        )

    # ========================================================================
    # Pattern & Content Constraints
    # ========================================================================

    def hasPattern(
        self,
        column: str,
        pattern: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that values match a regex pattern.

        Args:
            column: Column name to check
            pattern: Regex pattern to match
            assertion: Predicate to apply to match fraction (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.hasPattern("phone", r"^\\d{3}-\\d{3}-\\d{4}$")
        """
        return self._add_constraint(
            "hasPattern",
            column=column,
            pattern=pattern,
            assertion=assertion or is_one(),
            hint=hint,
        )

    def containsEmail(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that values contain valid email addresses.

        Args:
            column: Column name to check
            assertion: Predicate to apply to match fraction (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "containsEmail", column=column, assertion=assertion or is_one(), hint=hint
        )

    def containsURL(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that values contain valid URLs.

        Args:
            column: Column name to check
            assertion: Predicate to apply to match fraction (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "containsURL", column=column, assertion=assertion or is_one(), hint=hint
        )

    def containsCreditCardNumber(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that values contain valid credit card numbers.

        Args:
            column: Column name to check
            assertion: Predicate to apply to match fraction (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "containsCreditCardNumber",
            column=column,
            assertion=assertion or is_one(),
            hint=hint,
        )

    def containsSocialSecurityNumber(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that values contain valid SSNs.

        Args:
            column: Column name to check
            assertion: Predicate to apply to match fraction (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "containsSocialSecurityNumber",
            column=column,
            assertion=assertion or is_one(),
            hint=hint,
        )

    # ========================================================================
    # Comparison Constraints
    # ========================================================================

    def isPositive(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that all values in a column are positive.

        Args:
            column: Column name to check
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "isPositive", column=column, assertion=assertion or is_one(), hint=hint
        )

    def isNonNegative(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that all values in a column are non-negative.

        Args:
            column: Column name to check
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "isNonNegative", column=column, assertion=assertion or is_one(), hint=hint
        )

    def isLessThan(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that column_a < column_b for all rows.

        Args:
            column_a: First column name
            column_b: Second column name
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "isLessThan",
            columns=[column_a, column_b],
            assertion=assertion or is_one(),
            hint=hint,
        )

    def isLessThanOrEqualTo(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that column_a <= column_b for all rows.

        Args:
            column_a: First column name
            column_b: Second column name
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "isLessThanOrEqualTo",
            columns=[column_a, column_b],
            assertion=assertion or is_one(),
            hint=hint,
        )

    def isGreaterThan(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that column_a > column_b for all rows.

        Args:
            column_a: First column name
            column_b: Second column name
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "isGreaterThan",
            columns=[column_a, column_b],
            assertion=assertion or is_one(),
            hint=hint,
        )

    def isGreaterThanOrEqualTo(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that column_a >= column_b for all rows.

        Args:
            column_a: First column name
            column_b: Second column name
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining
        """
        return self._add_constraint(
            "isGreaterThanOrEqualTo",
            columns=[column_a, column_b],
            assertion=assertion or is_one(),
            hint=hint,
        )

    def isContainedIn(
        self,
        column: str,
        allowed_values: Sequence[str],
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that all values are in the allowed set.

        Args:
            column: Column name to check
            allowed_values: List of allowed values
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.isContainedIn("status", ["active", "inactive", "pending"])
        """
        return self._add_constraint(
            "isContainedIn",
            column=column,
            allowed_values=allowed_values,
            assertion=assertion or is_one(),
            hint=hint,
        )

    # ========================================================================
    # Custom Constraints
    # ========================================================================

    def satisfies(
        self,
        column_condition: str,
        constraint_name: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        """
        Check that rows satisfy a SQL condition.

        Args:
            column_condition: SQL WHERE clause condition
            constraint_name: Name for this constraint
            assertion: Predicate to apply to compliance (default: is_one)
            hint: Optional hint message for failures

        Returns:
            self for method chaining

        Example:
            check.satisfies("price > 0 AND quantity > 0", "positive_values")
        """
        return self._add_constraint(
            "satisfies",
            column_condition=column_condition,
            constraint_name=constraint_name,
            assertion=assertion or is_one(),
            hint=hint,
        )

    # ========================================================================
    # Filter (WHERE clause)
    # ========================================================================

    def where(self, filter_condition: str) -> "Check":
        """
        Apply a filter to the last added constraint.

        Args:
            filter_condition: SQL WHERE clause to filter rows

        Returns:
            self for method chaining

        Example:
            check.isComplete("email").where("status = 'active'")
        """
        if self._constraints:
            self._constraints[-1].where = filter_condition
        return self

    # ========================================================================
    # Serialization
    # ========================================================================

    def to_proto(self) -> proto.CheckMessage:
        """
        Convert this Check to a protobuf message.

        Returns:
            CheckMessage protobuf
        """
        level = (
            proto.CheckMessage.Level.ERROR
            if self.level == CheckLevel.Error
            else proto.CheckMessage.Level.WARNING
        )

        check_msg = proto.CheckMessage(level=level, description=self.description)
        check_msg.constraints.extend(self._constraints)

        return check_msg

    def __repr__(self) -> str:
        return f"Check(level={self.level.value}, description='{self.description}', constraints={len(self._constraints)})"


# Export all public symbols
__all__ = [
    "Check",
    "CheckLevel",
]
