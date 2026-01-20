# -*- coding: utf-8 -*-
"""
Grouping operator implementations.

Grouping operators require GROUP BY queries and cannot be batched with
scan operators. They require separate query execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from pydeequ.engines import MetricResult
from pydeequ.engines.operators.base import GroupingOperator

if TYPE_CHECKING:
    import pandas as pd


class DistinctnessOperator(GroupingOperator):
    """
    Computes distinctness = count_distinct / total_count.

    Distinctness measures what fraction of the total rows have
    unique value combinations in the specified columns.
    """

    def __init__(self, columns: List[str], where: Optional[str] = None):
        super().__init__(columns, where)

    @property
    def metric_name(self) -> str:
        return "Distinctness"

    def get_grouping_columns(self) -> List[str]:
        return self.columns

    def build_query(self, table: str) -> str:
        cols_str = ", ".join(self.columns)
        where_clause = self.get_where_clause()

        return f"""
            WITH freq AS (
                SELECT {cols_str}, COUNT(*) AS cnt
                FROM {table}
                {where_clause}
                GROUP BY {cols_str}
            )
            SELECT
                COUNT(*) AS distinct_count,
                SUM(cnt) AS total_count
            FROM freq
        """

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        distinct = self.safe_float(df, "distinct_count") or 0
        total = self.safe_float(df, "total_count") or 0
        value = distinct / total if total > 0 else 0.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class UniquenessOperator(GroupingOperator):
    """
    Computes uniqueness = count_unique (count=1) / total_count.

    Uniqueness measures what fraction of the total rows have
    value combinations that appear exactly once.
    """

    def __init__(self, columns: List[str], where: Optional[str] = None):
        super().__init__(columns, where)

    @property
    def metric_name(self) -> str:
        return "Uniqueness"

    def get_grouping_columns(self) -> List[str]:
        return self.columns

    def build_query(self, table: str) -> str:
        cols_str = ", ".join(self.columns)
        where_clause = self.get_where_clause()

        return f"""
            WITH freq AS (
                SELECT {cols_str}, COUNT(*) AS cnt
                FROM {table}
                {where_clause}
                GROUP BY {cols_str}
            )
            SELECT
                SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) AS unique_count,
                SUM(cnt) AS total_count
            FROM freq
        """

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        unique = self.safe_float(df, "unique_count") or 0
        total = self.safe_float(df, "total_count") or 0
        value = unique / total if total > 0 else 0.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class UniqueValueRatioOperator(GroupingOperator):
    """
    Computes unique value ratio = count_unique / count_distinct.

    This measures what fraction of distinct value combinations
    appear exactly once.
    """

    def __init__(self, columns: List[str], where: Optional[str] = None):
        super().__init__(columns, where)

    @property
    def metric_name(self) -> str:
        return "UniqueValueRatio"

    def get_grouping_columns(self) -> List[str]:
        return self.columns

    def build_query(self, table: str) -> str:
        cols_str = ", ".join(self.columns)
        where_clause = self.get_where_clause()

        return f"""
            WITH freq AS (
                SELECT {cols_str}, COUNT(*) AS cnt
                FROM {table}
                {where_clause}
                GROUP BY {cols_str}
            )
            SELECT
                COUNT(*) AS distinct_count,
                SUM(CASE WHEN cnt = 1 THEN 1 ELSE 0 END) AS unique_count
            FROM freq
        """

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        distinct = self.safe_float(df, "distinct_count") or 0
        unique = self.safe_float(df, "unique_count") or 0
        value = unique / distinct if distinct > 0 else 0.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class EntropyOperator(GroupingOperator):
    """
    Computes entropy = -SUM(p * log2(p)).

    Entropy measures the information content of a column's
    value distribution.
    """

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__([column], where)
        self.column = column

    @property
    def metric_name(self) -> str:
        return "Entropy"

    @property
    def instance(self) -> str:
        return self.column

    @property
    def entity(self) -> str:
        return "Column"

    def get_grouping_columns(self) -> List[str]:
        return [self.column]

    def build_query(self, table: str) -> str:
        where_clause = self.get_where_clause()

        return f"""
            WITH freq AS (
                SELECT {self.column}, COUNT(*) AS cnt
                FROM {table}
                {where_clause}
                GROUP BY {self.column}
            ),
            total AS (
                SELECT SUM(cnt) AS total_cnt FROM freq
            )
            SELECT
                -SUM((cnt * 1.0 / total_cnt) * LOG2(cnt * 1.0 / total_cnt)) AS entropy
            FROM freq, total
            WHERE cnt > 0
        """

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, "entropy")
        if value is None:
            value = 0.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class MutualInformationOperator(GroupingOperator):
    """Computes mutual information between two columns."""

    def __init__(self, columns: List[str], where: Optional[str] = None):
        if len(columns) != 2:
            raise ValueError("MutualInformation requires exactly 2 columns")
        super().__init__(columns, where)

    @property
    def metric_name(self) -> str:
        return "MutualInformation"

    def get_grouping_columns(self) -> List[str]:
        return self.columns

    def build_query(self, table: str) -> str:
        col1, col2 = self.columns
        where_clause = self.get_where_clause()

        return f"""
            WITH
            joint AS (
                SELECT {col1}, {col2}, COUNT(*) AS cnt
                FROM {table}
                {where_clause}
                GROUP BY {col1}, {col2}
            ),
            total AS (SELECT SUM(cnt) AS n FROM joint),
            marginal1 AS (
                SELECT {col1}, SUM(cnt) AS cnt1 FROM joint GROUP BY {col1}
            ),
            marginal2 AS (
                SELECT {col2}, SUM(cnt) AS cnt2 FROM joint GROUP BY {col2}
            )
            SELECT SUM(
                (j.cnt * 1.0 / t.n) *
                LOG2((j.cnt * 1.0 / t.n) / ((m1.cnt1 * 1.0 / t.n) * (m2.cnt2 * 1.0 / t.n)))
            ) AS mi
            FROM joint j, total t, marginal1 m1, marginal2 m2
            WHERE j.{col1} = m1.{col1} AND j.{col2} = m2.{col2} AND j.cnt > 0
        """

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, "mi")
        if value is None:
            value = 0.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class HistogramOperator(GroupingOperator):
    """
    Computes histogram of value distribution in a column.

    Returns a JSON-serialized dict mapping values to their counts.
    """

    def __init__(self, column: str, max_bins: int = 100, where: Optional[str] = None):
        super().__init__([column], where)
        self.column = column
        self.max_bins = max_bins

    @property
    def metric_name(self) -> str:
        return "Histogram"

    @property
    def instance(self) -> str:
        return self.column

    @property
    def entity(self) -> str:
        return "Column"

    def get_grouping_columns(self) -> List[str]:
        return [self.column]

    def build_query(self, table: str) -> str:
        where_clause = self.get_where_clause()
        if where_clause:
            where_clause += f" AND {self.column} IS NOT NULL"
        else:
            where_clause = f"WHERE {self.column} IS NOT NULL"

        return f"""
            SELECT {self.column} as value, COUNT(*) as count
            FROM {table}
            {where_clause}
            GROUP BY {self.column}
            ORDER BY count DESC
            LIMIT {self.max_bins}
        """

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        import json
        histogram = {str(row["value"]): int(row["count"]) for _, row in df.iterrows()}
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=json.dumps(histogram),
        )


__all__ = [
    "DistinctnessOperator",
    "UniquenessOperator",
    "UniqueValueRatioOperator",
    "EntropyOperator",
    "MutualInformationOperator",
    "HistogramOperator",
]
