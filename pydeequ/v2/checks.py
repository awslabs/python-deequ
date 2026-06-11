# -*- coding: utf-8 -*-
"""
Check class for Deequ Spark Connect.

Each builder method maps 1:1 to a `oneof` arm on `Constraint` in the
deequ_connect schema (see ADR-0001 in the deequ repo). Spec submessages
live at the top level and are reused across arms.
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
    """A named collection of constraints."""

    def __init__(self, level: CheckLevel, description: str):
        self.level = level
        self.description = description
        self._constraints: List[proto.Constraint] = []

    # ========================================================================
    # Internal helpers
    # ========================================================================

    def _append(self, constraint: proto.Constraint) -> "Check":
        self._constraints.append(constraint)
        return self

    @staticmethod
    def _column_assertion(
        column: str, assertion: Predicate, hint: Optional[str], where: Optional[str]
    ) -> proto.Constraint:
        c = proto.Constraint()
        if hint:
            c.hint = hint
        if where:
            c.where = where
        return c

    # ========================================================================
    # Size
    # ========================================================================

    def hasSize(self, assertion: Predicate, hint: Optional[str] = None) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_size.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    # ========================================================================
    # Completeness
    # ========================================================================

    def isComplete(self, column: str, hint: Optional[str] = None) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.is_complete.column = column
        return self._append(c)

    def hasCompleteness(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_completeness.column = column
        c.has_completeness.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    def areComplete(
        self, columns: Sequence[str], hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.are_complete.columns.extend(columns)
        return self._append(c)

    def haveCompleteness(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.have_completeness.columns.extend(columns)
        c.have_completeness.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    # ========================================================================
    # Uniqueness / distinctness
    # ========================================================================

    def isUnique(self, column: str, hint: Optional[str] = None) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.is_unique.column = column
        return self._append(c)

    def hasUniqueness(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_uniqueness.columns.extend(columns)
        c.has_uniqueness.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    def areUnique(
        self, columns: Sequence[str], hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.are_unique.columns.extend(columns)
        return self._append(c)

    def isPrimaryKey(
        self,
        column: str,
        additional_columns: Sequence[str] = (),
        hint: Optional[str] = None,
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.is_primary_key.column = column
        if additional_columns:
            c.is_primary_key.additional_columns.extend(additional_columns)
        return self._append(c)

    def hasDistinctness(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_distinctness.columns.extend(columns)
        c.has_distinctness.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    def hasUniqueValueRatio(
        self, columns: Sequence[str], assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_unique_value_ratio.columns.extend(columns)
        c.has_unique_value_ratio.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    # ========================================================================
    # Statistical
    # ========================================================================

    def _column_assertion_arm(
        self,
        arm_name: str,
        column: str,
        assertion: Predicate,
        hint: Optional[str],
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        spec = getattr(c, arm_name)
        spec.column = column
        spec.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    def hasMin(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_min", column, assertion, hint)

    def hasMax(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_max", column, assertion, hint)

    def hasMean(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_mean", column, assertion, hint)

    def hasSum(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_sum", column, assertion, hint)

    def hasStandardDeviation(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_standard_deviation", column, assertion, hint)

    # ========================================================================
    # Length
    # ========================================================================

    def hasMinLength(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_min_length", column, assertion, hint)

    def hasMaxLength(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_max_length", column, assertion, hint)

    # ========================================================================
    # Quantile
    # ========================================================================

    def hasApproxQuantile(
        self,
        column: str,
        quantile: float,
        assertion: Predicate,
        hint: Optional[str] = None,
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_approx_quantile.column = column
        c.has_approx_quantile.quantile = quantile
        c.has_approx_quantile.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    # ========================================================================
    # Information theory
    # ========================================================================

    def hasEntropy(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm("has_entropy", column, assertion, hint)

    # ========================================================================
    # Correlation
    # ========================================================================

    def hasCorrelation(
        self,
        column_a: str,
        column_b: str,
        assertion: Predicate,
        hint: Optional[str] = None,
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_correlation.column_a = column_a
        c.has_correlation.column_b = column_b
        c.has_correlation.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    # ========================================================================
    # Pattern matching
    # ========================================================================

    def hasPattern(
        self,
        column: str,
        pattern: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.has_pattern.column = column
        c.has_pattern.regex = pattern
        c.has_pattern.assertion.CopyFrom((assertion or is_one()).to_proto())
        return self._append(c)

    def containsCreditCardNumber(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._column_assertion_arm(
            "contains_credit_card_number", column, assertion or is_one(), hint
        )

    def containsEmail(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._column_assertion_arm(
            "contains_email", column, assertion or is_one(), hint
        )

    def containsURL(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._column_assertion_arm(
            "contains_url", column, assertion or is_one(), hint
        )

    def containsSocialSecurityNumber(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._column_assertion_arm(
            "contains_social_security_number", column, assertion or is_one(), hint
        )

    # ========================================================================
    # SQL condition
    # ========================================================================

    def satisfies(
        self,
        column_condition: str,
        constraint_name: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.satisfies.column_condition = column_condition
        c.satisfies.constraint_name = constraint_name
        c.satisfies.assertion.CopyFrom((assertion or is_one()).to_proto())
        return self._append(c)

    # ========================================================================
    # Membership
    # ========================================================================

    def isContainedIn(
        self,
        column: str,
        allowed_values: Sequence[str],
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        c.is_contained_in.column = column
        c.is_contained_in.allowed_values.extend(allowed_values)
        c.is_contained_in.assertion.CopyFrom((assertion or is_one()).to_proto())
        return self._append(c)

    # ========================================================================
    # Sign
    # ========================================================================

    def isNonNegative(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._column_assertion_arm(
            "is_non_negative", column, assertion or is_one(), hint
        )

    def isPositive(
        self,
        column: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._column_assertion_arm(
            "is_positive", column, assertion or is_one(), hint
        )

    # ========================================================================
    # Inter-column comparison
    # ========================================================================

    def _pair_columns_arm(
        self,
        arm_name: str,
        column_a: str,
        column_b: str,
        assertion: Predicate,
        hint: Optional[str],
    ) -> "Check":
        c = proto.Constraint()
        if hint:
            c.hint = hint
        spec = getattr(c, arm_name)
        spec.column_a = column_a
        spec.column_b = column_b
        spec.assertion.CopyFrom(assertion.to_proto())
        return self._append(c)

    def isLessThan(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._pair_columns_arm(
            "is_less_than", column_a, column_b, assertion or is_one(), hint
        )

    def isLessThanOrEqualTo(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._pair_columns_arm(
            "is_less_than_or_equal_to", column_a, column_b, assertion or is_one(), hint
        )

    def isGreaterThan(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._pair_columns_arm(
            "is_greater_than", column_a, column_b, assertion or is_one(), hint
        )

    def isGreaterThanOrEqualTo(
        self,
        column_a: str,
        column_b: str,
        assertion: Optional[Predicate] = None,
        hint: Optional[str] = None,
    ) -> "Check":
        return self._pair_columns_arm(
            "is_greater_than_or_equal_to", column_a, column_b, assertion or is_one(), hint
        )

    # ========================================================================
    # Cardinality estimation
    # ========================================================================

    def hasApproxCountDistinct(
        self, column: str, assertion: Predicate, hint: Optional[str] = None
    ) -> "Check":
        return self._column_assertion_arm(
            "has_approx_count_distinct", column, assertion, hint
        )

    # ========================================================================
    # Filter (WHERE clause)
    # ========================================================================

    def where(self, filter_condition: str) -> "Check":
        """Apply a SQL WHERE filter to the most recently added constraint."""
        if not self._constraints:
            raise ValueError("where() called before any constraint was added")
        self._constraints[-1].where = filter_condition
        return self

    # ========================================================================
    # Serialization
    # ========================================================================

    def to_proto(self) -> proto.Check:
        level = (
            proto.CheckLevel.CHECK_LEVEL_ERROR
            if self.level == CheckLevel.Error
            else proto.CheckLevel.CHECK_LEVEL_WARNING
        )
        check_msg = proto.Check(level=level, description=self.description)
        check_msg.constraints.extend(self._constraints)
        return check_msg

    def __repr__(self) -> str:
        return (
            f"Check(level={self.level.value}, description='{self.description}', "
            f"constraints={len(self._constraints)})"
        )


__all__ = ["Check", "CheckLevel"]
