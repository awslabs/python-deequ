# -*- coding: utf-8 -*-
"""
Analyzer classes for Deequ Spark Connect.

Each analyzer's `to_proto()` builds a `proto.Analyzer` with the appropriate
`oneof` arm populated. See ADR-0001 in the deequ repo for the design.

Example usage:
    from pydeequ.v2.analyzers import Size, Completeness, Mean

    result = (AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("email"))
        .addAnalyzer(Mean("amount"))
        .run())
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Sequence, Union

from pydeequ.v2.proto import deequ_connect_pb2 as proto


class _ConnectAnalyzer(ABC):
    """Base class for Spark Connect compatible analyzers."""

    @abstractmethod
    def to_proto(self) -> proto.Analyzer:
        """Convert analyzer to protobuf message."""
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


def _set_where(msg: proto.Analyzer, where: Optional[str]) -> None:
    if where:
        msg.where = where


# ============================================================================
# Size — no parameters
# ============================================================================


@dataclass
class Size(_ConnectAnalyzer):
    """Number of rows in a DataFrame."""

    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        msg.size.SetInParent()  # populate the EmptySpec oneof arm
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"Size(where='{self.where}')" if self.where else "Size()"


# ============================================================================
# Single-column analyzers — share ColumnAnalyzerSpec
# ============================================================================


def _column_arm_factory(arm_name: str, deequ_name: str):
    """Return a dataclass-decorated analyzer class for column-only arms."""

    @dataclass
    class _SingleColumnAnalyzer(_ConnectAnalyzer):
        column: str
        where: Optional[str] = None

        def to_proto(self) -> proto.Analyzer:
            msg = proto.Analyzer()
            getattr(msg, arm_name).column = self.column
            _set_where(msg, self.where)
            return msg

        def __repr__(self) -> str:
            return f"{deequ_name}('{self.column}')"

    _SingleColumnAnalyzer.__name__ = deequ_name
    _SingleColumnAnalyzer.__qualname__ = deequ_name
    return _SingleColumnAnalyzer


Completeness = _column_arm_factory("completeness", "Completeness")
Mean = _column_arm_factory("mean", "Mean")
Sum = _column_arm_factory("sum", "Sum")
StandardDeviation = _column_arm_factory("standard_deviation", "StandardDeviation")
Minimum = _column_arm_factory("minimum", "Minimum")
Maximum = _column_arm_factory("maximum", "Maximum")
MinLength = _column_arm_factory("min_length", "MinLength")
MaxLength = _column_arm_factory("max_length", "MaxLength")
ApproxCountDistinct = _column_arm_factory("approx_count_distinct", "ApproxCountDistinct")
Entropy = _column_arm_factory("entropy", "Entropy")
DataType = _column_arm_factory("data_type", "DataType")


# ============================================================================
# Multi-column analyzers — share ColumnsAnalyzerSpec
# ============================================================================


def _columns_arm_factory(arm_name: str, deequ_name: str):
    @dataclass
    class _MultiColumnAnalyzer(_ConnectAnalyzer):
        columns: Union[str, Sequence[str]]
        where: Optional[str] = None

        def __post_init__(self):
            if isinstance(self.columns, str):
                self.columns = [self.columns]

        def to_proto(self) -> proto.Analyzer:
            msg = proto.Analyzer()
            getattr(msg, arm_name).columns.extend(self.columns)
            _set_where(msg, self.where)
            return msg

        def __repr__(self) -> str:
            return f"{deequ_name}({list(self.columns)})"

    _MultiColumnAnalyzer.__name__ = deequ_name
    _MultiColumnAnalyzer.__qualname__ = deequ_name
    return _MultiColumnAnalyzer


Uniqueness = _columns_arm_factory("uniqueness", "Uniqueness")
Distinctness = _columns_arm_factory("distinctness", "Distinctness")
UniqueValueRatio = _columns_arm_factory("unique_value_ratio", "UniqueValueRatio")
CountDistinct = _columns_arm_factory("count_distinct", "CountDistinct")
MutualInformation = _columns_arm_factory("mutual_information", "MutualInformation")


# ============================================================================
# Pair-of-columns analyzer
# ============================================================================


@dataclass
class Correlation(_ConnectAnalyzer):
    """Pearson correlation between two columns."""

    column_a: str
    column_b: str
    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        msg.correlation.column_a = self.column_a
        msg.correlation.column_b = self.column_b
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"Correlation('{self.column_a}', '{self.column_b}')"


# ============================================================================
# Approx quantile analyzers
# ============================================================================


@dataclass
class ApproxQuantile(_ConnectAnalyzer):
    """Approximate quantile of a numeric column."""

    column: str
    quantile: Optional[float] = None
    relative_error: Optional[float] = None
    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        spec = msg.approx_quantile
        spec.column = self.column
        if self.quantile is not None:
            spec.quantile = self.quantile
        if self.relative_error is not None:
            spec.relative_error = self.relative_error
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"ApproxQuantile('{self.column}', {self.quantile})"


@dataclass
class ApproxQuantiles(_ConnectAnalyzer):
    """Multiple approximate quantiles of a numeric column."""

    column: str
    quantiles: Sequence[float] = ()
    relative_error: Optional[float] = None
    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        spec = msg.approx_quantiles
        spec.column = self.column
        if self.quantiles:
            spec.quantiles.extend(self.quantiles)
        if self.relative_error is not None:
            spec.relative_error = self.relative_error
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"ApproxQuantiles('{self.column}', {list(self.quantiles)})"


# ============================================================================
# Histogram
# ============================================================================


@dataclass
class Histogram(_ConnectAnalyzer):
    """Histogram of values in a column."""

    column: str
    max_detail_bins: Optional[int] = None
    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        spec = msg.histogram
        spec.column = self.column
        if self.max_detail_bins is not None:
            spec.max_detail_bins = self.max_detail_bins
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"Histogram('{self.column}')"


# ============================================================================
# Compliance
# ============================================================================


@dataclass
class Compliance(_ConnectAnalyzer):
    """Fraction of rows satisfying a SQL predicate, named by `instance`."""

    instance: str
    predicate: str
    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        msg.compliance.instance = self.instance
        msg.compliance.predicate = self.predicate
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"Compliance('{self.instance}', '{self.predicate}')"


# ============================================================================
# PatternMatch
# ============================================================================


@dataclass
class PatternMatch(_ConnectAnalyzer):
    """Fraction of rows whose column value matches a regex."""

    column: str
    pattern: str
    where: Optional[str] = None

    def to_proto(self) -> proto.Analyzer:
        msg = proto.Analyzer()
        msg.pattern_match.column = self.column
        msg.pattern_match.pattern = self.pattern
        _set_where(msg, self.where)
        return msg

    def __repr__(self) -> str:
        return f"PatternMatch('{self.column}', '{self.pattern}')"


__all__ = [
    "_ConnectAnalyzer",
    # Single-column
    "Size",
    "Completeness",
    "Mean",
    "Sum",
    "StandardDeviation",
    "Minimum",
    "Maximum",
    "MinLength",
    "MaxLength",
    "ApproxCountDistinct",
    "Entropy",
    "DataType",
    # Multi-column
    "Uniqueness",
    "Distinctness",
    "UniqueValueRatio",
    "CountDistinct",
    "MutualInformation",
    # Pair
    "Correlation",
    # Quantile
    "ApproxQuantile",
    "ApproxQuantiles",
    # Specialised
    "Histogram",
    "Compliance",
    "PatternMatch",
]
