# -*- coding: utf-8 -*-
"""
Serializable predicates for Deequ Spark Connect.

These predicates replace Python lambda functions that were used in the Py4J-based
PyDeequ. Since lambdas cannot be serialized over Spark Connect's gRPC channel,
we use these predicate classes that serialize to protobuf messages.

Example usage:
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
    def to_proto(self) -> proto.Predicate:
        """Convert predicate to protobuf message."""
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


@dataclass
class Comparison(Predicate):
    """Comparison predicate for single-value comparisons."""

    op: "proto.Predicate.CompareOp.ValueType"
    value: float

    def to_proto(self) -> proto.Predicate:
        return proto.Predicate(op=self.op, value=self.value)

    def __repr__(self) -> str:
        op_map = {
            proto.Predicate.CompareOp.COMPARE_OP_EQ: "==",
            proto.Predicate.CompareOp.COMPARE_OP_NE: "!=",
            proto.Predicate.CompareOp.COMPARE_OP_GT: ">",
            proto.Predicate.CompareOp.COMPARE_OP_GE: ">=",
            proto.Predicate.CompareOp.COMPARE_OP_LT: "<",
            proto.Predicate.CompareOp.COMPARE_OP_LE: "<=",
        }
        return f"x {op_map.get(self.op, '?')} {self.value}"


@dataclass
class Between(Predicate):
    """Between predicate for range checks (inclusive)."""

    lower: float
    upper: float

    def to_proto(self) -> proto.Predicate:
        return proto.Predicate(
            op=proto.Predicate.CompareOp.COMPARE_OP_BETWEEN,
            lower_bound=self.lower,
            upper_bound=self.upper,
        )

    def __repr__(self) -> str:
        return f"{self.lower} <= x <= {self.upper}"


# ============================================================================
# Factory Functions - Convenient way to create predicates
# ============================================================================


def eq(value: Union[int, float]) -> Predicate:
    """Create an equality predicate (x == value)."""
    return Comparison(proto.Predicate.CompareOp.COMPARE_OP_EQ, float(value))


def neq(value: Union[int, float]) -> Predicate:
    """Create a not-equal predicate (x != value)."""
    return Comparison(proto.Predicate.CompareOp.COMPARE_OP_NE, float(value))


def gt(value: Union[int, float]) -> Predicate:
    """Create a greater-than predicate (x > value)."""
    return Comparison(proto.Predicate.CompareOp.COMPARE_OP_GT, float(value))


def gte(value: Union[int, float]) -> Predicate:
    """Create a greater-than-or-equal predicate (x >= value)."""
    return Comparison(proto.Predicate.CompareOp.COMPARE_OP_GE, float(value))


def lt(value: Union[int, float]) -> Predicate:
    """Create a less-than predicate (x < value)."""
    return Comparison(proto.Predicate.CompareOp.COMPARE_OP_LT, float(value))


def lte(value: Union[int, float]) -> Predicate:
    """Create a less-than-or-equal predicate (x <= value)."""
    return Comparison(proto.Predicate.CompareOp.COMPARE_OP_LE, float(value))


def between(lower: Union[int, float], upper: Union[int, float]) -> Predicate:
    """Create a between predicate (lower <= x <= upper)."""
    return Between(float(lower), float(upper))


def is_one() -> Predicate:
    """Create a predicate that checks if value equals 1.0 (default for completeness checks)."""
    return eq(1.0)


def is_zero() -> Predicate:
    """Create a predicate that checks if value equals 0.0."""
    return eq(0.0)


def is_positive() -> Predicate:
    """Create a predicate that checks if value is positive (> 0)."""
    return gt(0.0)


def is_non_negative() -> Predicate:
    """Create a predicate that checks if value is non-negative (>= 0)."""
    return gte(0.0)


__all__ = [
    "Predicate",
    "Comparison",
    "Between",
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
