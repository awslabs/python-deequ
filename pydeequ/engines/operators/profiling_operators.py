# -*- coding: utf-8 -*-
"""
Profiling operator implementations.

Profiling operators compute column profile statistics including completeness,
distinct values, min, max, mean, sum, stddev, percentiles, and histograms.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from pydeequ.engines import ColumnProfile
from pydeequ.engines.operators.mixins import (
    ColumnAliasMixin,
    SafeExtractMixin,
    WhereClauseMixin,
)

if TYPE_CHECKING:
    import pandas as pd


# SQL types that are considered numeric
NUMERIC_TYPES: Set[str] = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC",
    "INT", "INT1", "INT2", "INT4", "INT8",
    "FLOAT4", "FLOAT8",
}

# SQL types that are considered string
STRING_TYPES: Set[str] = {"VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING"}


class ColumnProfileOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin):
    """
    Computes all profile statistics for a column.

    This operator generates SQL queries to compute completeness, distinct values,
    and (for numeric columns) min, max, mean, sum, stddev, and percentiles.

    Attributes:
        column: Column name to profile
        column_type: SQL type of the column (e.g., "INTEGER", "VARCHAR")
        is_numeric: Whether the column is a numeric type
        compute_percentiles: Whether to compute percentile statistics
        compute_histogram: Whether to compute value histogram
        histogram_limit: Maximum number of histogram buckets
        where: Optional WHERE clause for filtering
    """

    def __init__(
        self,
        column: str,
        column_type: str,
        compute_percentiles: bool = True,
        compute_histogram: bool = False,
        histogram_limit: int = 100,
        where: Optional[str] = None,
    ):
        """
        Initialize ColumnProfileOperator.

        Args:
            column: Column name to profile
            column_type: SQL type of the column
            compute_percentiles: Whether to compute percentile statistics
            compute_histogram: Whether to compute value histogram
            histogram_limit: Maximum number of histogram buckets
            where: Optional WHERE clause for filtering
        """
        self.column = column
        self.column_type = column_type
        self.is_numeric = column_type in NUMERIC_TYPES
        self.compute_percentiles = compute_percentiles and self.is_numeric
        self.compute_histogram = compute_histogram
        self.histogram_limit = histogram_limit
        self.where = where

    def build_base_query(self, table: str) -> str:
        """
        Build query for basic statistics.

        Args:
            table: Table name to query

        Returns:
            SQL query string for base statistics
        """
        col = self.column
        if self.is_numeric:
            query = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count,
                    APPROX_COUNT_DISTINCT({col}) as distinct_count,
                    MIN({col}) as min_val,
                    MAX({col}) as max_val,
                    AVG({col}) as mean_val,
                    SUM({col}) as sum_val,
                    STDDEV_POP({col}) as stddev_val
                FROM {table}
            """
        else:
            query = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_count,
                    APPROX_COUNT_DISTINCT({col}) as distinct_count
                FROM {table}
            """
        return query.strip()

    def build_percentile_query(self, table: str) -> str:
        """
        Build query for percentiles (separate query).

        Args:
            table: Table name to query

        Returns:
            SQL query string for percentile statistics
        """
        col = self.column
        return f"""
            SELECT
                QUANTILE_CONT({col}, 0.25) as p25,
                QUANTILE_CONT({col}, 0.50) as p50,
                QUANTILE_CONT({col}, 0.75) as p75
            FROM {table}
        """.strip()

    def build_histogram_query(self, table: str) -> str:
        """
        Build query for histogram (separate query).

        Args:
            table: Table name to query

        Returns:
            SQL query string for histogram
        """
        col = self.column
        return f"""
            SELECT {col} as value, COUNT(*) as count
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY count DESC
            LIMIT {self.histogram_limit}
        """.strip()

    def extract_base_result(self, df: "pd.DataFrame") -> Dict:
        """
        Extract base statistics from query result.

        Args:
            df: DataFrame containing query results

        Returns:
            Dictionary of extracted statistics
        """
        import pandas as pd

        total = int(df["total"].iloc[0])

        # Handle NaN for empty datasets
        null_count_raw = df["null_count"].iloc[0]
        null_count = int(null_count_raw) if not pd.isna(null_count_raw) else 0

        distinct_count_raw = df["distinct_count"].iloc[0]
        distinct_count = int(distinct_count_raw) if not pd.isna(distinct_count_raw) else 0

        completeness = (total - null_count) / total if total > 0 else 1.0

        result = {
            "total": total,
            "null_count": null_count,
            "distinct_count": distinct_count,
            "completeness": completeness,
        }

        if self.is_numeric:
            result["minimum"] = self.safe_float(df, "min_val")
            result["maximum"] = self.safe_float(df, "max_val")
            result["mean"] = self.safe_float(df, "mean_val")
            result["sum"] = self.safe_float(df, "sum_val")
            result["std_dev"] = self.safe_float(df, "stddev_val")

        return result

    def extract_percentile_result(self, df: "pd.DataFrame") -> Optional[str]:
        """
        Extract percentile statistics from query result.

        Args:
            df: DataFrame containing percentile query results

        Returns:
            JSON string of percentile values or None
        """
        p25 = self.safe_float(df, "p25")
        p50 = self.safe_float(df, "p50")
        p75 = self.safe_float(df, "p75")

        return json.dumps({
            "0.25": p25,
            "0.50": p50,
            "0.75": p75,
        })

    def extract_histogram_result(self, df: "pd.DataFrame") -> Optional[str]:
        """
        Extract histogram from query result.

        Args:
            df: DataFrame containing histogram query results

        Returns:
            JSON string of histogram or None
        """
        histogram = {
            str(row["value"]): int(row["count"])
            for _, row in df.iterrows()
        }
        return json.dumps(histogram)

    def build_profile(
        self,
        base_stats: Dict,
        percentiles: Optional[str] = None,
        histogram: Optional[str] = None,
    ) -> ColumnProfile:
        """
        Build ColumnProfile from extracted statistics.

        Args:
            base_stats: Dictionary of base statistics
            percentiles: JSON string of percentile values
            histogram: JSON string of histogram

        Returns:
            ColumnProfile object
        """
        profile = ColumnProfile(
            column=self.column,
            completeness=base_stats["completeness"],
            approx_distinct_values=base_stats["distinct_count"],
            data_type=self.column_type,
            is_data_type_inferred=True,
        )

        if self.is_numeric:
            profile.minimum = base_stats.get("minimum")
            profile.maximum = base_stats.get("maximum")
            profile.mean = base_stats.get("mean")
            profile.sum = base_stats.get("sum")
            profile.std_dev = base_stats.get("std_dev")

            if percentiles:
                profile.approx_percentiles = percentiles

        if histogram:
            profile.histogram = histogram

        return profile


class MultiColumnProfileOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin):
    """
    Profiles multiple columns in minimal queries.

    This operator batches profile statistics for multiple columns to reduce
    the number of SQL queries needed for profiling.

    Attributes:
        columns: List of column names to profile
        schema: Dictionary mapping column names to SQL types
        numeric_columns: List of numeric column names
        string_columns: List of string column names
        where: Optional WHERE clause for filtering
    """

    def __init__(
        self,
        columns: List[str],
        schema: Dict[str, str],
        where: Optional[str] = None,
    ):
        """
        Initialize MultiColumnProfileOperator.

        Args:
            columns: List of column names to profile
            schema: Dictionary mapping column names to SQL types
            where: Optional WHERE clause for filtering
        """
        self.columns = columns
        self.schema = schema
        self.where = where

        # Categorize columns by type
        self.numeric_columns = [c for c in columns if schema.get(c) in NUMERIC_TYPES]
        self.string_columns = [c for c in columns if schema.get(c) in STRING_TYPES]
        self.other_columns = [
            c for c in columns
            if c not in self.numeric_columns and c not in self.string_columns
        ]

    def build_completeness_query(self, table: str) -> str:
        """
        Build query for completeness of all columns.

        Args:
            table: Table name to query

        Returns:
            SQL query string
        """
        aggregations = ["COUNT(*) as total"]
        for col in self.columns:
            aggregations.append(
                f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_{col}"
            )
            aggregations.append(f"APPROX_COUNT_DISTINCT({col}) as distinct_{col}")

        return f"SELECT {', '.join(aggregations)} FROM {table}"

    def build_numeric_stats_query(self, table: str) -> str:
        """
        Build query for numeric column statistics.

        Args:
            table: Table name to query

        Returns:
            SQL query string
        """
        if not self.numeric_columns:
            return ""

        aggregations = []
        for col in self.numeric_columns:
            aggregations.extend([
                f"MIN({col}) as min_{col}",
                f"MAX({col}) as max_{col}",
                f"AVG({col}) as mean_{col}",
                f"SUM({col}) as sum_{col}",
                f"STDDEV_POP({col}) as stddev_{col}",
            ])

        return f"SELECT {', '.join(aggregations)} FROM {table}"

    def extract_profiles(
        self,
        completeness_df: "pd.DataFrame",
        numeric_df: Optional["pd.DataFrame"] = None,
    ) -> List[ColumnProfile]:
        """
        Extract column profiles from query results.

        Args:
            completeness_df: DataFrame with completeness statistics
            numeric_df: DataFrame with numeric statistics (optional)

        Returns:
            List of ColumnProfile objects
        """
        import pandas as pd

        profiles = []
        total = int(completeness_df["total"].iloc[0])

        for col in self.columns:
            # Extract completeness stats
            null_count_raw = completeness_df[f"null_{col}"].iloc[0]
            null_count = int(null_count_raw) if not pd.isna(null_count_raw) else 0

            distinct_count_raw = completeness_df[f"distinct_{col}"].iloc[0]
            distinct_count = int(distinct_count_raw) if not pd.isna(distinct_count_raw) else 0

            completeness = (total - null_count) / total if total > 0 else 1.0

            profile = ColumnProfile(
                column=col,
                completeness=completeness,
                approx_distinct_values=distinct_count,
                data_type=self.schema.get(col, "Unknown"),
                is_data_type_inferred=True,
            )

            # Add numeric stats if applicable
            if col in self.numeric_columns and numeric_df is not None:
                profile.minimum = self.safe_float(numeric_df, f"min_{col}")
                profile.maximum = self.safe_float(numeric_df, f"max_{col}")
                profile.mean = self.safe_float(numeric_df, f"mean_{col}")
                profile.sum = self.safe_float(numeric_df, f"sum_{col}")
                profile.std_dev = self.safe_float(numeric_df, f"stddev_{col}")

            profiles.append(profile)

        return profiles


__all__ = [
    "ColumnProfileOperator",
    "MultiColumnProfileOperator",
    "NUMERIC_TYPES",
    "STRING_TYPES",
]
