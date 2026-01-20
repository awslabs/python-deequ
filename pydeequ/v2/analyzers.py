# -*- coding: utf-8 -*-
"""
Analyzer classes for Deequ Spark Connect.

This module provides Spark Connect compatible analyzer classes that build
protobuf messages instead of using Py4J to call Scala code directly.

Example usage:
    from pydeequ.v2.analyzers import (
        AnalysisRunner, AnalyzerContext,
        Size, Completeness, Mean, Maximum, Minimum
    )

    result = (AnalysisRunner(spark)
        .onData(df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("email"))
        .addAnalyzer(Mean("amount"))
        .run())

    metrics = AnalyzerContext.successMetricsAsDataFrame(result)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Sequence, Union

from pydeequ.v2.proto import deequ_connect_pb2 as proto


class _ConnectAnalyzer(ABC):
    """Base class for Spark Connect compatible analyzers."""

    @abstractmethod
    def to_proto(self) -> proto.AnalyzerMessage:
        """Convert analyzer to protobuf message."""
        raise NotImplementedError

    @abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError


# ============================================================================
# Size Analyzer
# ============================================================================


@dataclass
class Size(_ConnectAnalyzer):
    """
    Computes the number of rows in a DataFrame.

    Args:
        where: Optional SQL WHERE clause to filter rows before counting

    Example:
        Size()  # Count all rows
        Size(where="status = 'active'")  # Count only active rows
    """

    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Size")
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"Size(where='{self.where}')"
        return "Size()"


# ============================================================================
# Completeness Analyzers
# ============================================================================


@dataclass
class Completeness(_ConnectAnalyzer):
    """
    Computes the fraction of non-null values in a column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows

    Example:
        Completeness("email")
        Completeness("email", where="status = 'active'")
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Completeness", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"Completeness('{self.column}', where='{self.where}')"
        return f"Completeness('{self.column}')"


# ============================================================================
# Statistical Analyzers
# ============================================================================


@dataclass
class Mean(_ConnectAnalyzer):
    """
    Computes the mean of a numeric column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Mean", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"Mean('{self.column}', where='{self.where}')"
        return f"Mean('{self.column}')"


@dataclass
class Sum(_ConnectAnalyzer):
    """
    Computes the sum of a numeric column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Sum", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"Sum('{self.column}', where='{self.where}')"
        return f"Sum('{self.column}')"


@dataclass
class Maximum(_ConnectAnalyzer):
    """
    Computes the maximum value of a numeric column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Maximum", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"Maximum('{self.column}', where='{self.where}')"
        return f"Maximum('{self.column}')"


@dataclass
class Minimum(_ConnectAnalyzer):
    """
    Computes the minimum value of a numeric column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Minimum", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"Minimum('{self.column}', where='{self.where}')"
        return f"Minimum('{self.column}')"


@dataclass
class StandardDeviation(_ConnectAnalyzer):
    """
    Computes the standard deviation of a numeric column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="StandardDeviation", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        if self.where:
            return f"StandardDeviation('{self.column}', where='{self.where}')"
        return f"StandardDeviation('{self.column}')"


# ============================================================================
# Uniqueness Analyzers
# ============================================================================


@dataclass
class Distinctness(_ConnectAnalyzer):
    """
    Computes the fraction of distinct values in column(s).

    Args:
        columns: Column name(s) to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    columns: Union[str, Sequence[str]]
    where: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.columns, str):
            self.columns = [self.columns]

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Distinctness")
        msg.columns.extend(self.columns)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"Distinctness({self.columns})"


@dataclass
class Uniqueness(_ConnectAnalyzer):
    """
    Computes the fraction of unique values (appearing exactly once) in column(s).

    Args:
        columns: Column name(s) to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    columns: Union[str, Sequence[str]]
    where: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.columns, str):
            self.columns = [self.columns]

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Uniqueness")
        msg.columns.extend(self.columns)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"Uniqueness({self.columns})"


@dataclass
class UniqueValueRatio(_ConnectAnalyzer):
    """
    Computes the ratio of unique values to total distinct values.

    Args:
        columns: Column name(s) to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    columns: Union[str, Sequence[str]]
    where: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.columns, str):
            self.columns = [self.columns]

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="UniqueValueRatio")
        msg.columns.extend(self.columns)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"UniqueValueRatio({self.columns})"


@dataclass
class CountDistinct(_ConnectAnalyzer):
    """
    Computes the count of distinct values in column(s).

    Args:
        columns: Column name(s) to analyze
    """

    columns: Union[str, Sequence[str]]

    def __post_init__(self):
        if isinstance(self.columns, str):
            self.columns = [self.columns]

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="CountDistinct")
        msg.columns.extend(self.columns)
        return msg

    def __repr__(self) -> str:
        return f"CountDistinct({self.columns})"


@dataclass
class ApproxCountDistinct(_ConnectAnalyzer):
    """
    Computes approximate count distinct using HyperLogLog.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="ApproxCountDistinct", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"ApproxCountDistinct('{self.column}')"


# ============================================================================
# Quantile Analyzers
# ============================================================================


@dataclass
class ApproxQuantile(_ConnectAnalyzer):
    """
    Computes an approximate quantile of a numeric column.

    Args:
        column: Column name to analyze
        quantile: Quantile to compute (0.0 to 1.0)
        relative_error: Relative error tolerance (default 0.01)
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    quantile: float
    relative_error: float = 0.01
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(
            type="ApproxQuantile",
            column=self.column,
            quantile=self.quantile,
            relative_error=self.relative_error,
        )
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"ApproxQuantile('{self.column}', {self.quantile})"


# ============================================================================
# Correlation Analyzers
# ============================================================================


@dataclass
class Correlation(_ConnectAnalyzer):
    """
    Computes Pearson correlation between two columns.

    Args:
        column1: First column name
        column2: Second column name
        where: Optional SQL WHERE clause to filter rows
    """

    column1: str
    column2: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Correlation")
        msg.columns.extend([self.column1, self.column2])
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"Correlation('{self.column1}', '{self.column2}')"


@dataclass
class MutualInformation(_ConnectAnalyzer):
    """
    Computes mutual information between columns.

    Args:
        columns: Column names to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    columns: Sequence[str]
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="MutualInformation")
        msg.columns.extend(self.columns)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"MutualInformation({self.columns})"


# ============================================================================
# String Analyzers
# ============================================================================


@dataclass
class MaxLength(_ConnectAnalyzer):
    """
    Computes the maximum string length in a column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="MaxLength", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"MaxLength('{self.column}')"


@dataclass
class MinLength(_ConnectAnalyzer):
    """
    Computes the minimum string length in a column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="MinLength", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"MinLength('{self.column}')"


# ============================================================================
# Pattern Analyzers
# ============================================================================


@dataclass
class PatternMatch(_ConnectAnalyzer):
    """
    Computes the fraction of values matching a regex pattern.

    Args:
        column: Column name to analyze
        pattern: Regex pattern to match
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    pattern: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(
            type="PatternMatch", column=self.column, pattern=self.pattern
        )
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"PatternMatch('{self.column}', '{self.pattern}')"


# ============================================================================
# Compliance Analyzer
# ============================================================================


@dataclass
class Compliance(_ConnectAnalyzer):
    """
    Computes the fraction of rows satisfying a SQL condition.

    Args:
        instance: Name for this compliance check
        predicate: SQL predicate (WHERE clause condition)
        where: Optional additional SQL WHERE clause to filter rows
    """

    instance: str
    predicate: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        # Use column for instance name and pattern for predicate
        msg = proto.AnalyzerMessage(
            type="Compliance", column=self.instance, pattern=self.predicate
        )
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"Compliance('{self.instance}', '{self.predicate}')"


# ============================================================================
# Entropy Analyzer
# ============================================================================


@dataclass
class Entropy(_ConnectAnalyzer):
    """
    Computes the entropy of a column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Entropy", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"Entropy('{self.column}')"


# ============================================================================
# Histogram Analyzer
# ============================================================================


@dataclass
class Histogram(_ConnectAnalyzer):
    """
    Computes histogram of values in a column.

    Args:
        column: Column name to analyze
        max_detail_bins: Maximum number of bins for detailed output
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    max_detail_bins: Optional[int] = None
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="Histogram", column=self.column)
        if self.max_detail_bins is not None:
            msg.max_detail_bins = self.max_detail_bins
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"Histogram('{self.column}')"


# ============================================================================
# DataType Analyzer
# ============================================================================


@dataclass
class DataType(_ConnectAnalyzer):
    """
    Analyzes the data types present in a column.

    Args:
        column: Column name to analyze
        where: Optional SQL WHERE clause to filter rows
    """

    column: str
    where: Optional[str] = None

    def to_proto(self) -> proto.AnalyzerMessage:
        msg = proto.AnalyzerMessage(type="DataType", column=self.column)
        if self.where:
            msg.where = self.where
        return msg

    def __repr__(self) -> str:
        return f"DataType('{self.column}')"


# Export all public symbols
__all__ = [
    # Base class
    "_ConnectAnalyzer",
    # Size
    "Size",
    # Completeness
    "Completeness",
    # Statistical
    "Mean",
    "Sum",
    "Maximum",
    "Minimum",
    "StandardDeviation",
    # Uniqueness
    "Distinctness",
    "Uniqueness",
    "UniqueValueRatio",
    "CountDistinct",
    "ApproxCountDistinct",
    # Quantile
    "ApproxQuantile",
    # Correlation
    "Correlation",
    "MutualInformation",
    # String
    "MaxLength",
    "MinLength",
    # Pattern
    "PatternMatch",
    # Compliance
    "Compliance",
    # Entropy
    "Entropy",
    # Histogram
    "Histogram",
    # DataType
    "DataType",
]
