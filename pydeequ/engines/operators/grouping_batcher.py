# -*- coding: utf-8 -*-
"""
Grouping operator batching for DuckDB performance optimization.

This module provides functionality to batch grouping operators that share
identical CTEs (same columns and where clause) into single queries.

Key insight: DistinctnessOperator, UniquenessOperator, and UniqueValueRatioOperator
all use the same frequency CTE:
    WITH freq AS (SELECT cols, COUNT(*) AS cnt FROM table GROUP BY cols)

By fusing operators with matching (columns, where_clause), we can:
- Compute all metrics in a single query
- Reduce the number of table scans
- Improve performance by 20-40% for checks with multiple grouping operators
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from pydeequ.engines import MetricResult
from pydeequ.engines.operators.grouping_operators import (
    DistinctnessOperator,
    UniquenessOperator,
    UniqueValueRatioOperator,
)

if TYPE_CHECKING:
    import pandas as pd
    from pydeequ.engines.operators.base import GroupingOperator


# Operators that can be batched together (share same freq CTE structure)
BATCHABLE_OPERATORS = (DistinctnessOperator, UniquenessOperator, UniqueValueRatioOperator)


class GroupingOperatorBatcher:
    """
    Batches grouping operators with matching (columns, where) into single queries.

    This class analyzes grouping operators and fuses compatible ones to reduce
    the number of SQL queries executed.
    """

    def __init__(self, operators: List["GroupingOperator"]):
        """
        Initialize the batcher with operators to analyze.

        Args:
            operators: List of grouping operators
        """
        self.operators = operators
        self._batched_groups: Dict[Tuple, List["GroupingOperator"]] = {}
        self._unbatchable: List["GroupingOperator"] = []
        self._analyze()

    def _get_batch_key(self, operator: "GroupingOperator") -> Optional[Tuple]:
        """
        Get the batch key for an operator (columns tuple + where clause).

        Returns None if the operator cannot be batched.
        """
        if not isinstance(operator, BATCHABLE_OPERATORS):
            return None

        # Create key from (columns tuple, where clause)
        cols = tuple(operator.columns)
        where = operator.where or ""
        return (cols, where)

    def _analyze(self) -> None:
        """Analyze operators and group batchable ones by key."""
        for operator in self.operators:
            key = self._get_batch_key(operator)
            if key is None:
                self._unbatchable.append(operator)
            else:
                if key not in self._batched_groups:
                    self._batched_groups[key] = []
                self._batched_groups[key].append(operator)

    def get_unbatchable_operators(self) -> List["GroupingOperator"]:
        """Return operators that cannot be batched."""
        return self._unbatchable

    def get_batch_count(self) -> int:
        """Return the number of batched query groups."""
        return len(self._batched_groups)

    def execute_batched(
        self,
        table: str,
        execute_fn,
    ) -> List[MetricResult]:
        """
        Execute batched queries and return results.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            List of MetricResult objects for all batched operators
        """
        results: List[MetricResult] = []

        for (cols, where), operators in self._batched_groups.items():
            # Build fused query
            query = self._build_fused_query(table, cols, where, operators)

            # Execute query
            df = execute_fn(query)

            # Extract results for each operator
            for operator in operators:
                result = self._extract_result(df, operator)
                results.append(result)

        return results

    def _build_fused_query(
        self,
        table: str,
        cols: Tuple[str, ...],
        where: str,
        operators: List["GroupingOperator"],
    ) -> str:
        """
        Build a fused query that computes metrics for all operators in a batch.

        Args:
            table: Name of the table to query
            cols: Tuple of column names
            where: WHERE clause (empty string if none)
            operators: List of operators to fuse

        Returns:
            SQL query string
        """
        cols_str = ", ".join(cols)
        where_clause = f"WHERE {where}" if where else ""

        # Determine which metrics we need to compute
        needs_distinct = any(isinstance(op, DistinctnessOperator) for op in operators)
        needs_unique = any(isinstance(op, UniquenessOperator) for op in operators)
        needs_unique_ratio = any(isinstance(op, UniqueValueRatioOperator) for op in operators)

        # Build SELECT clause
        select_parts = []

        # Always need total_count for Distinctness and Uniqueness
        if needs_distinct or needs_unique:
            select_parts.append("SUM(cnt) AS total_count")

        # distinct_count needed for Distinctness and UniqueValueRatio
        if needs_distinct or needs_unique_ratio:
            select_parts.append("COUNT(*) AS distinct_count")

        # unique_count needed for Uniqueness and UniqueValueRatio
        if needs_unique or needs_unique_ratio:
            select_parts.append("SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) AS unique_count")

        return f"""
            WITH freq AS (
                SELECT {cols_str}, COUNT(*) AS cnt
                FROM {table}
                {where_clause}
                GROUP BY {cols_str}
            )
            SELECT {', '.join(select_parts)}
            FROM freq
        """

    def _extract_result(
        self,
        df: "pd.DataFrame",
        operator: "GroupingOperator",
    ) -> MetricResult:
        """
        Extract the metric result for a specific operator from the fused query result.

        Args:
            df: DataFrame containing fused query results
            operator: The operator to extract result for

        Returns:
            MetricResult for the operator
        """
        if isinstance(operator, DistinctnessOperator):
            distinct = operator.safe_float(df, "distinct_count") or 0
            total = operator.safe_float(df, "total_count") or 0
            value = distinct / total if total > 0 else 0.0

        elif isinstance(operator, UniquenessOperator):
            unique = operator.safe_float(df, "unique_count") or 0
            total = operator.safe_float(df, "total_count") or 0
            value = unique / total if total > 0 else 0.0

        elif isinstance(operator, UniqueValueRatioOperator):
            distinct = operator.safe_float(df, "distinct_count") or 0
            unique = operator.safe_float(df, "unique_count") or 0
            value = unique / distinct if distinct > 0 else 0.0

        else:
            # Fallback (shouldn't happen for batchable operators)
            value = 0.0

        return MetricResult(
            name=operator.metric_name,
            instance=operator.instance,
            entity=operator.entity,
            value=value,
        )


__all__ = [
    "GroupingOperatorBatcher",
    "BATCHABLE_OPERATORS",
]
