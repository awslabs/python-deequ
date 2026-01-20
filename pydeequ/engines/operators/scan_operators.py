# -*- coding: utf-8 -*-
"""
Scan operator implementations.

Scan operators compute metrics via SQL aggregations that can be combined
into a single SELECT statement, enabling efficient batch execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional

from pydeequ.engines import MetricResult
from pydeequ.engines.operators.base import ScanOperator
from pydeequ.engines.operators.mixins import (
    ColumnAliasMixin,
    SafeExtractMixin,
    WhereClauseMixin,
)

if TYPE_CHECKING:
    import pandas as pd


class SizeOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin):
    """
    Computes the number of rows in a table.

    Unlike other scan operators, Size operates on the dataset level
    rather than a specific column.
    """

    def __init__(self, where: Optional[str] = None):
        self.where = where
        self.alias = "size_value"

    @property
    def metric_name(self) -> str:
        return "Size"

    @property
    def instance(self) -> str:
        return "*"

    @property
    def entity(self) -> str:
        return "Dataset"

    def get_aggregations(self) -> List[str]:
        if self.where:
            sql = f"SUM(CASE WHEN {self.where} THEN 1 ELSE 0 END) AS {self.alias}"
        else:
            sql = f"COUNT(*) AS {self.alias}"
        return [sql]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class CompletenessOperator(ScanOperator):
    """Computes the fraction of non-null values in a column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.count_alias = self.make_alias("count", column)
        self.null_alias = self.make_alias("null_count", column)

    @property
    def metric_name(self) -> str:
        return "Completeness"

    def get_aggregations(self) -> List[str]:
        count_sql = self.wrap_count_with_where("1")
        null_sql = self.wrap_count_with_where(f"{self.column} IS NULL")
        return [
            f"{count_sql} AS {self.count_alias}",
            f"{null_sql} AS {self.null_alias}",
        ]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        total = self.safe_float(df, self.count_alias) or 0
        nulls = self.safe_float(df, self.null_alias) or 0
        value = (total - nulls) / total if total > 0 else 1.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class MeanOperator(ScanOperator):
    """Computes the average of a numeric column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("mean", column)

    @property
    def metric_name(self) -> str:
        return "Mean"

    def get_aggregations(self) -> List[str]:
        agg = self.wrap_agg_with_where("AVG", self.column)
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class SumOperator(ScanOperator):
    """Computes the sum of a numeric column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("sum", column)

    @property
    def metric_name(self) -> str:
        return "Sum"

    def get_aggregations(self) -> List[str]:
        agg = self.wrap_agg_with_where("SUM", self.column)
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class MinimumOperator(ScanOperator):
    """Computes the minimum value of a numeric column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("min", column)

    @property
    def metric_name(self) -> str:
        return "Minimum"

    def get_aggregations(self) -> List[str]:
        agg = self.wrap_agg_with_where("MIN", self.column)
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class MaximumOperator(ScanOperator):
    """Computes the maximum value of a numeric column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("max", column)

    @property
    def metric_name(self) -> str:
        return "Maximum"

    def get_aggregations(self) -> List[str]:
        agg = self.wrap_agg_with_where("MAX", self.column)
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class StandardDeviationOperator(ScanOperator):
    """Computes the standard deviation of a numeric column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("stddev", column)

    @property
    def metric_name(self) -> str:
        return "StandardDeviation"

    def get_aggregations(self) -> List[str]:
        agg = self.wrap_agg_with_where("STDDEV_POP", self.column)
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class MaxLengthOperator(ScanOperator):
    """Computes the maximum string length in a column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("max_length", column)

    @property
    def metric_name(self) -> str:
        return "MaxLength"

    def get_aggregations(self) -> List[str]:
        if self.where:
            sql = f"MAX(CASE WHEN {self.where} THEN LENGTH({self.column}) ELSE NULL END)"
        else:
            sql = f"MAX(LENGTH({self.column}))"
        return [f"{sql} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class MinLengthOperator(ScanOperator):
    """Computes the minimum string length in a column."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("min_length", column)

    @property
    def metric_name(self) -> str:
        return "MinLength"

    def get_aggregations(self) -> List[str]:
        if self.where:
            sql = f"MIN(CASE WHEN {self.where} THEN LENGTH({self.column}) ELSE NULL END)"
        else:
            sql = f"MIN(LENGTH({self.column}))"
        return [f"{sql} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class PatternMatchOperator(ScanOperator):
    """Computes the fraction of values matching a regex pattern."""

    def __init__(self, column: str, pattern: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.pattern = pattern.replace("'", "''")  # Escape single quotes
        self.count_alias = self.make_alias("count", column)
        self.match_alias = self.make_alias("pattern_match", column)

    @property
    def metric_name(self) -> str:
        return "PatternMatch"

    def get_aggregations(self) -> List[str]:
        count_sql = self.wrap_count_with_where("1")
        match_cond = f"REGEXP_MATCHES({self.column}, '{self.pattern}')"
        match_sql = self.wrap_count_with_where(match_cond)
        return [
            f"{count_sql} AS {self.count_alias}",
            f"{match_sql} AS {self.match_alias}",
        ]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        total = self.safe_float(df, self.count_alias) or 0
        matches = self.safe_float(df, self.match_alias) or 0
        value = matches / total if total > 0 else 1.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class ComplianceOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin):
    """
    Computes the fraction of rows satisfying a SQL condition.

    Unlike other scan operators, Compliance operates on a predicate
    rather than a specific column.
    """

    def __init__(self, instance: str, predicate: str, where: Optional[str] = None):
        self.instance_name = instance
        self.predicate = predicate
        self.where = where
        self.count_alias = "compliance_count"
        self.match_alias = self.make_alias("compliance_match", instance)

    @property
    def metric_name(self) -> str:
        return "Compliance"

    @property
    def instance(self) -> str:
        return self.instance_name

    @property
    def entity(self) -> str:
        return "Dataset"

    def get_aggregations(self) -> List[str]:
        count_sql = self.wrap_count_with_where("1")
        match_sql = self.wrap_count_with_where(f"({self.predicate})")
        return [
            f"{count_sql} AS {self.count_alias}",
            f"{match_sql} AS {self.match_alias}",
        ]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        total = self.safe_float(df, self.count_alias) or 0
        matches = self.safe_float(df, self.match_alias) or 0
        value = matches / total if total > 0 else 1.0
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class CorrelationOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin):
    """Computes Pearson correlation between two columns."""

    def __init__(self, column1: str, column2: str, where: Optional[str] = None):
        self.column1 = column1
        self.column2 = column2
        self.where = where
        self.alias = self.make_alias("corr", column1, column2)

    @property
    def metric_name(self) -> str:
        return "Correlation"

    @property
    def instance(self) -> str:
        return f"{self.column1},{self.column2}"

    @property
    def entity(self) -> str:
        return "Multicolumn"

    def get_aggregations(self) -> List[str]:
        # Note: CORR doesn't support CASE WHEN wrapping in most DBs
        # For WHERE clause, the engine should apply it to the whole query
        sql = f"CORR({self.column1}, {self.column2})"
        return [f"{sql} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class CountDistinctOperator(WhereClauseMixin, SafeExtractMixin, ColumnAliasMixin):
    """Computes the count of distinct values in column(s)."""

    def __init__(self, columns: List[str], where: Optional[str] = None):
        self.columns = columns
        self.where = where
        self.alias = self.make_alias("count_distinct", *columns)

    @property
    def metric_name(self) -> str:
        return "CountDistinct"

    @property
    def instance(self) -> str:
        return ",".join(self.columns)

    @property
    def entity(self) -> str:
        return "Multicolumn" if len(self.columns) > 1 else "Column"

    def get_aggregations(self) -> List[str]:
        cols_str = ", ".join(self.columns)
        sql = f"COUNT(DISTINCT ({cols_str}))"
        return [f"{sql} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class ApproxCountDistinctOperator(ScanOperator):
    """Computes approximate count distinct using HyperLogLog."""

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__(column, where)
        self.alias = self.make_alias("approx_count_distinct", column)

    @property
    def metric_name(self) -> str:
        return "ApproxCountDistinct"

    def get_aggregations(self) -> List[str]:
        agg = self.wrap_agg_with_where("APPROX_COUNT_DISTINCT", self.column)
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


class ApproxQuantileOperator(ScanOperator):
    """Computes approximate quantile using QUANTILE_CONT."""

    def __init__(self, column: str, quantile: float = 0.5, where: Optional[str] = None):
        super().__init__(column, where)
        self.quantile = quantile
        self.alias = self.make_alias("approx_quantile", column)

    @property
    def metric_name(self) -> str:
        return "ApproxQuantile"

    def get_aggregations(self) -> List[str]:
        if self.where:
            agg = f"QUANTILE_CONT(CASE WHEN {self.where} THEN {self.column} ELSE NULL END, {self.quantile})"
        else:
            agg = f"QUANTILE_CONT({self.column}, {self.quantile})"
        return [f"{agg} AS {self.alias}"]

    def extract_result(self, df: "pd.DataFrame") -> MetricResult:
        value = self.safe_float(df, self.alias)
        return MetricResult(
            name=self.metric_name,
            instance=self.instance,
            entity=self.entity,
            value=value,
        )


__all__ = [
    "SizeOperator",
    "CompletenessOperator",
    "MeanOperator",
    "SumOperator",
    "MinimumOperator",
    "MaximumOperator",
    "StandardDeviationOperator",
    "MaxLengthOperator",
    "MinLengthOperator",
    "PatternMatchOperator",
    "ComplianceOperator",
    "CorrelationOperator",
    "CountDistinctOperator",
    "ApproxCountDistinctOperator",
    "ApproxQuantileOperator",
]
