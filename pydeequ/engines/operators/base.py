# -*- coding: utf-8 -*-
"""
Base classes for SQL operators.

This module provides the abstract base classes that combine protocols
and mixins to create the foundation for concrete operator implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

from pydeequ.engines.operators.mixins import (
    ColumnAliasMixin,
    SafeExtractMixin,
    WhereClauseMixin,
)

if TYPE_CHECKING:
    import pandas as pd
    from pydeequ.engines import MetricResult


class ScanOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin, ABC):
    """
    Base class for single-pass aggregation operators.

    Scan operators compute metrics via SQL aggregations that can be
    combined into a single SELECT statement. This enables efficient
    batch execution where multiple metrics are computed in one query.

    Subclasses must implement:
        - get_aggregations(): Return SQL aggregation expressions
        - extract_result(): Parse query results into MetricResult

    Attributes:
        column: Column name to analyze
        where: Optional SQL WHERE clause for filtering
    """

    def __init__(self, column: str, where: Optional[str] = None):
        """
        Initialize scan operator.

        Args:
            column: Column name to analyze
            where: Optional SQL WHERE clause for filtering
        """
        self.column = column
        self.where = where

    @abstractmethod
    def get_aggregations(self) -> List[str]:
        """
        Return SQL aggregation expressions.

        Returns:
            List of SQL aggregation expressions with AS alias clauses
        """
        raise NotImplementedError

    @abstractmethod
    def extract_result(self, df: "pd.DataFrame") -> "MetricResult":
        """
        Extract metric from query result DataFrame.

        Args:
            df: DataFrame containing query results

        Returns:
            MetricResult with extracted value
        """
        raise NotImplementedError

    @property
    def instance(self) -> str:
        """Return the instance identifier for this operator."""
        return self.column

    @property
    def entity(self) -> str:
        """Return the entity type for this operator."""
        return "Column"

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """Return the metric name for this operator."""
        raise NotImplementedError


class GroupingOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin, ABC):
    """
    Base class for operators requiring GROUP BY queries.

    Grouping operators need to compute intermediate aggregations
    via GROUP BY before computing the final metric. They cannot
    be batched with scan operators and require separate queries.

    Subclasses must implement:
        - get_grouping_columns(): Return columns to GROUP BY
        - build_query(): Build complete CTE-based query
        - extract_result(): Parse query results into MetricResult

    Attributes:
        columns: Column name(s) to analyze
        where: Optional SQL WHERE clause for filtering
    """

    def __init__(self, columns: List[str], where: Optional[str] = None):
        """
        Initialize grouping operator.

        Args:
            columns: Column name(s) to analyze
            where: Optional SQL WHERE clause for filtering
        """
        self.columns = columns
        self.where = where

    @abstractmethod
    def get_grouping_columns(self) -> List[str]:
        """
        Return columns to GROUP BY.

        Returns:
            List of column names for the GROUP BY clause
        """
        raise NotImplementedError

    @abstractmethod
    def build_query(self, table: str) -> str:
        """
        Build complete CTE-based query.

        Args:
            table: Name of the table to query

        Returns:
            Complete SQL query string
        """
        raise NotImplementedError

    @abstractmethod
    def extract_result(self, df: "pd.DataFrame") -> "MetricResult":
        """
        Extract metric from query result DataFrame.

        Args:
            df: DataFrame containing query results

        Returns:
            MetricResult with extracted value
        """
        raise NotImplementedError

    @property
    def instance(self) -> str:
        """Return the instance identifier for this operator."""
        return ",".join(self.columns)

    @property
    def entity(self) -> str:
        """Return the entity type for this operator."""
        return "Multicolumn" if len(self.columns) > 1 else "Column"

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """Return the metric name for this operator."""
        raise NotImplementedError


__all__ = [
    "ScanOperator",
    "GroupingOperator",
]
