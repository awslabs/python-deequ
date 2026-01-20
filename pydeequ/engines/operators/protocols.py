# -*- coding: utf-8 -*-
"""
Protocol definitions for SQL operators.

This module defines the structural typing contracts that operators must
implement. Using Protocol from typing allows for duck typing while still
providing IDE support and type checking.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Protocol, runtime_checkable

if TYPE_CHECKING:
    import pandas as pd
    from pydeequ.engines import MetricResult


@runtime_checkable
class ScanOperatorProtocol(Protocol):
    """
    Contract for single-pass aggregation operators.

    Scan operators compute metrics via SQL aggregations that can be
    combined into a single SELECT statement, enabling efficient
    batch execution.
    """

    def get_aggregations(self) -> List[str]:
        """
        Return SQL aggregation expressions.

        Returns:
            List of SQL aggregation expressions with AS alias clauses,
            e.g., ["AVG(col) AS mean_col", "COUNT(*) AS count_col"]
        """
        ...

    def extract_result(self, df: "pd.DataFrame") -> "MetricResult":
        """
        Extract metric from query result DataFrame.

        Args:
            df: DataFrame containing query results with columns
                matching the aliases from get_aggregations()

        Returns:
            MetricResult with extracted value
        """
        ...


@runtime_checkable
class GroupingOperatorProtocol(Protocol):
    """
    Contract for operators requiring GROUP BY queries.

    Grouping operators need to compute intermediate aggregations
    via GROUP BY before computing the final metric. They cannot
    be batched with scan operators and require separate queries.
    """

    def get_grouping_columns(self) -> List[str]:
        """
        Return columns to GROUP BY.

        Returns:
            List of column names for the GROUP BY clause
        """
        ...

    def build_query(self, table: str) -> str:
        """
        Build complete CTE-based query.

        Args:
            table: Name of the table to query

        Returns:
            Complete SQL query string with CTEs as needed
        """
        ...

    def extract_result(self, df: "pd.DataFrame") -> "MetricResult":
        """
        Extract metric from query result DataFrame.

        Args:
            df: DataFrame containing query results

        Returns:
            MetricResult with extracted value
        """
        ...


__all__ = [
    "ScanOperatorProtocol",
    "GroupingOperatorProtocol",
]
