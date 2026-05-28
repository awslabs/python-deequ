# -*- coding: utf-8 -*-
"""
Mixin classes providing shared behaviors for SQL operators.

These mixins provide reusable functionality that eliminates code duplication
across operator implementations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pandas as pd


class WhereClauseMixin:
    """
    Provides WHERE clause wrapping for conditional aggregations.

    This mixin eliminates the repeated if/else pattern for handling
    optional WHERE clauses in aggregations. Expects the class to
    have a `where` attribute.
    """

    where: Optional[str]

    def wrap_agg_with_where(self, agg_func: str, column: str) -> str:
        """
        Wrap an aggregation with optional WHERE filter using CASE WHEN.

        Args:
            agg_func: SQL aggregation function name (e.g., "AVG", "SUM", "MIN")
            column: Column name to aggregate

        Returns:
            SQL expression with conditional aggregation if where is set,
            otherwise standard aggregation

        Example:
            >>> op = SomeOperator(column="price", where="status='active'")
            >>> op.wrap_agg_with_where("AVG", "price")
            "AVG(CASE WHEN status='active' THEN price ELSE NULL END)"
        """
        if self.where:
            return f"{agg_func}(CASE WHEN {self.where} THEN {column} ELSE NULL END)"
        return f"{agg_func}({column})"

    def wrap_count_with_where(self, condition: str = "1") -> str:
        """
        Wrap COUNT with optional WHERE filter.

        Args:
            condition: SQL condition to count (default "1" counts all rows)

        Returns:
            SQL expression for conditional count

        Example:
            >>> op = SomeOperator(where="status='active'")
            >>> op.wrap_count_with_where()
            "SUM(CASE WHEN status='active' THEN 1 ELSE 0 END)"
            >>> op.wrap_count_with_where("price > 0")
            "SUM(CASE WHEN status='active' AND (price > 0) THEN 1 ELSE 0 END)"
        """
        if self.where:
            if condition == "1":
                return f"SUM(CASE WHEN {self.where} THEN 1 ELSE 0 END)"
            return f"SUM(CASE WHEN ({self.where}) AND ({condition}) THEN 1 ELSE 0 END)"
        if condition == "1":
            return "COUNT(*)"
        return f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END)"

    def get_where_clause(self) -> str:
        """
        Get WHERE clause for standalone queries.

        Returns:
            "WHERE {condition}" if where is set, otherwise empty string
        """
        if self.where:
            return f"WHERE {self.where}"
        return ""


class SafeExtractMixin:
    """
    Provides safe value extraction from DataFrames.

    This mixin standardizes the pattern of safely extracting values
    from query result DataFrames, handling NULL and NaN values.
    """

    def safe_float(self, df: "pd.DataFrame", column: str) -> Optional[float]:
        """
        Extract float value from DataFrame, handling NULL/NaN.

        Args:
            df: DataFrame containing query results
            column: Column name to extract

        Returns:
            Float value or None if not present/invalid
        """
        import pandas as pd

        if column not in df.columns:
            return None
        val = df[column].iloc[0]
        if val is not None and not pd.isna(val):
            return float(val)
        return None

    def safe_int(self, df: "pd.DataFrame", column: str) -> Optional[int]:
        """
        Extract int value from DataFrame, handling NULL/NaN.

        Args:
            df: DataFrame containing query results
            column: Column name to extract

        Returns:
            Integer value or None if not present/invalid
        """
        val = self.safe_float(df, column)
        return int(val) if val is not None else None

    def safe_string(self, df: "pd.DataFrame", column: str) -> Optional[str]:
        """
        Extract string value from DataFrame, handling NULL/NaN.

        Args:
            df: DataFrame containing query results
            column: Column name to extract

        Returns:
            String value or None if not present/invalid
        """
        import pandas as pd

        if column not in df.columns:
            return None
        val = df[column].iloc[0]
        if val is not None and not pd.isna(val):
            return str(val)
        return None


class ColumnAliasMixin:
    """
    Provides consistent column alias generation.

    This mixin ensures all operators generate unique and predictable
    column aliases for their SQL expressions.
    """

    def make_alias(self, prefix: str, *parts: str) -> str:
        """
        Generate unique column alias from prefix and parts.

        Args:
            prefix: Alias prefix (e.g., "mean", "count", "sum")
            *parts: Additional parts to include (e.g., column names)

        Returns:
            Underscore-separated alias with sanitized column names

        Example:
            >>> op = SomeOperator()
            >>> op.make_alias("mean", "price")
            "mean_price"
            >>> op.make_alias("corr", "price", "quantity")
            "corr_price_quantity"
        """
        # Sanitize parts: replace dots and other special chars
        sanitized = []
        for p in parts:
            if p:
                sanitized.append(p.replace(".", "_").replace(" ", "_"))
        suffix = "_".join(sanitized)
        return f"{prefix}_{suffix}" if suffix else prefix


__all__ = [
    "WhereClauseMixin",
    "SafeExtractMixin",
    "ColumnAliasMixin",
]
