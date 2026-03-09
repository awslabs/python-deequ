# -*- coding: utf-8 -*-
"""
Serializable predicates for Deequ Spark Connect.

These predicates replace Python lambda functions that were used in the Py4J-based
PyDeequ. Since lambdas cannot be serialized over Spark Connect's gRPC channel,
we use these predicate classes that serialize to protobuf messages.

Example usage:
    # Old (Py4J) - NOT serializable
    check.hasSize(lambda x: x >= 100)
    check.hasCompleteness("col", lambda x: x >= 0.95)

    # New (Spark Connect) - Serializable
    from pydeequ.v2.predicates import gte, eq, between

    check.hasSize(gte(100))
    check.hasCompleteness("col", gte(0.95))
    check.hasMean("amount", between(100, 200))
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Union

from pydeequ.v2.proto import deequ_connect_pb2 as proto


class Predicate(ABC):
    """Base class for serializable predicates."""

    @abstractmethod
    def to_proto(self) -> proto.PredicateMessage:
        """Convert predicate to protobuf message."""
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def to_callable(self):
        """
        Convert predicate to a callable function.

        Returns:
            A callable that takes a value and returns True/False

        Example:
            pred = gte(0.95)
            func = pred.to_callable()
            assert func(0.96) == True
            assert func(0.90) == False
        """
        raise NotImplementedError

    def __call__(self, value: float) -> bool:
        """Allow predicates to be called directly like functions."""
        return self.to_callable()(value)


@dataclass
class Comparison(Predicate):
    """Comparison predicate for single-value comparisons."""

    operator: proto.PredicateMessage.Operator
    value: float

    def to_proto(self) -> proto.PredicateMessage:
        return proto.PredicateMessage(operator=self.operator, value=self.value)

    def __repr__(self) -> str:
        op_map = {
            proto.PredicateMessage.Operator.EQ: "==",
            proto.PredicateMessage.Operator.NE: "!=",
            proto.PredicateMessage.Operator.GT: ">",
            proto.PredicateMessage.Operator.GE: ">=",
            proto.PredicateMessage.Operator.LT: "<",
            proto.PredicateMessage.Operator.LE: "<=",
        }
        return f"x {op_map.get(self.operator, '?')} {self.value}"

    def to_callable(self):
        """Convert to a callable function."""
        op = self.operator
        target = self.value

        if op == proto.PredicateMessage.Operator.EQ:
            return lambda x: abs(x - target) < 1e-9 if x is not None else False
        elif op == proto.PredicateMessage.Operator.NE:
            return lambda x: abs(x - target) >= 1e-9 if x is not None else False
        elif op == proto.PredicateMessage.Operator.GT:
            return lambda x: x > target if x is not None else False
        elif op == proto.PredicateMessage.Operator.GE:
            return lambda x: x >= target if x is not None else False
        elif op == proto.PredicateMessage.Operator.LT:
            return lambda x: x < target if x is not None else False
        elif op == proto.PredicateMessage.Operator.LE:
            return lambda x: x <= target if x is not None else False
        else:
            return lambda x: False


@dataclass
class Between(Predicate):
    """Between predicate for range checks (inclusive)."""

    lower: float
    upper: float

    def to_proto(self) -> proto.PredicateMessage:
        return proto.PredicateMessage(
            operator=proto.PredicateMessage.Operator.BETWEEN,
            lower_bound=self.lower,
            upper_bound=self.upper,
        )

    def __repr__(self) -> str:
        return f"{self.lower} <= x <= {self.upper}"

    def to_callable(self):
        """Convert to a callable function."""
        lower = self.lower
        upper = self.upper
        return lambda x: lower <= x <= upper if x is not None else False


# ============================================================================
# Factory Functions - Convenient way to create predicates
# ============================================================================


def eq(value: Union[int, float]) -> Predicate:
    """
    Create an equality predicate (x == value).

    Args:
        value: The value to compare against

    Returns:
        Predicate that checks if metric equals value

    Example:
        check.hasSize(eq(100))  # size must equal 100
    """
    return Comparison(proto.PredicateMessage.Operator.EQ, float(value))


def neq(value: Union[int, float]) -> Predicate:
    """
    Create a not-equal predicate (x != value).

    Args:
        value: The value to compare against

    Returns:
        Predicate that checks if metric does not equal value

    Example:
        check.hasSize(neq(0))  # size must not be zero
    """
    return Comparison(proto.PredicateMessage.Operator.NE, float(value))


def gt(value: Union[int, float]) -> Predicate:
    """
    Create a greater-than predicate (x > value).

    Args:
        value: The value to compare against

    Returns:
        Predicate that checks if metric is greater than value

    Example:
        check.hasSize(gt(0))  # size must be greater than 0
    """
    return Comparison(proto.PredicateMessage.Operator.GT, float(value))


def gte(value: Union[int, float]) -> Predicate:
    """
    Create a greater-than-or-equal predicate (x >= value).

    Args:
        value: The value to compare against

    Returns:
        Predicate that checks if metric is >= value

    Example:
        check.hasCompleteness("col", gte(0.95))  # at least 95% complete
    """
    return Comparison(proto.PredicateMessage.Operator.GE, float(value))


def lt(value: Union[int, float]) -> Predicate:
    """
    Create a less-than predicate (x < value).

    Args:
        value: The value to compare against

    Returns:
        Predicate that checks if metric is less than value

    Example:
        check.hasMean("errors", lt(10))  # mean errors less than 10
    """
    return Comparison(proto.PredicateMessage.Operator.LT, float(value))


def lte(value: Union[int, float]) -> Predicate:
    """
    Create a less-than-or-equal predicate (x <= value).

    Args:
        value: The value to compare against

    Returns:
        Predicate that checks if metric is <= value

    Example:
        check.hasMax("price", lte(1000))  # max price <= 1000
    """
    return Comparison(proto.PredicateMessage.Operator.LE, float(value))


def between(lower: Union[int, float], upper: Union[int, float]) -> Predicate:
    """
    Create a between predicate (lower <= x <= upper).

    Args:
        lower: Lower bound (inclusive)
        upper: Upper bound (inclusive)

    Returns:
        Predicate that checks if metric is within range

    Example:
        check.hasMean("age", between(18, 65))  # mean age between 18 and 65
    """
    return Between(float(lower), float(upper))


def is_one() -> Predicate:
    """
    Create a predicate that checks if value equals 1.0.

    This is the default assertion for many constraints like isComplete().

    Returns:
        Predicate that checks if metric equals 1.0

    Example:
        check.hasCompleteness("col", is_one())  # 100% complete
    """
    return eq(1.0)


def is_zero() -> Predicate:
    """
    Create a predicate that checks if value equals 0.0.

    Returns:
        Predicate that checks if metric equals 0.0

    Example:
        check.hasMean("null_count", is_zero())  # no nulls
    """
    return eq(0.0)


def is_positive() -> Predicate:
    """
    Create a predicate that checks if value is positive (> 0).

    Returns:
        Predicate that checks if metric is greater than 0

    Example:
        check.hasMin("quantity", is_positive())  # all quantities positive
    """
    return gt(0.0)


def is_non_negative() -> Predicate:
    """
    Create a predicate that checks if value is non-negative (>= 0).

    Returns:
        Predicate that checks if metric is >= 0

    Example:
        check.hasMin("balance", is_non_negative())  # no negative balances
    """
    return gte(0.0)


# Export all public symbols
__all__ = [
    # Base classes
    "Predicate",
    "Comparison",
    "Between",
    # Factory functions
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
    "is_one",
    "is_zero",
    "is_positive",
    "is_non_negative",
]
