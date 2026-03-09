# -*- coding: utf-8 -*-
"""
Constraint evaluator implementations.

This module contains all concrete evaluator classes that implement
specific constraint types.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from pydeequ.engines.constraints.base import (
    AnalyzerBasedEvaluator,
    BaseEvaluator,
    RatioCheckEvaluator,
)
from pydeequ.engines.operators import (
    ApproxCountDistinctOperator,
    ApproxQuantileOperator,
    CompletenessOperator,
    ComplianceOperator,
    CorrelationOperator,
    DistinctnessOperator,
    EntropyOperator,
    MaximumOperator,
    MaxLengthOperator,
    MeanOperator,
    MinimumOperator,
    MinLengthOperator,
    MutualInformationOperator,
    PatternMatchOperator,
    SizeOperator,
    StandardDeviationOperator,
    SumOperator,
    UniqueValueRatioOperator,
    UniquenessOperator,
)

if TYPE_CHECKING:
    import pandas as pd


# =============================================================================
# Analyzer-based evaluators
# =============================================================================


class SizeEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasSize constraint."""

    def get_operator(self):
        return SizeOperator(where=self.where)

    def to_string(self) -> str:
        if self.assertion:
            return f"hasSize(assertion)"
        return "hasSize()"


class CompletenessEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for isComplete and hasCompleteness constraints."""

    def get_operator(self):
        return CompletenessOperator(self.column, where=self.where)

    def to_string(self) -> str:
        if self.assertion:
            return f"hasCompleteness({self.column}, assertion)"
        return f"isComplete({self.column})"


class MeanEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasMean constraint."""

    def get_operator(self):
        return MeanOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasMean({self.column}, assertion)"


class MinimumEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasMin constraint."""

    def get_operator(self):
        return MinimumOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasMin({self.column}, assertion)"


class MaximumEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasMax constraint."""

    def get_operator(self):
        return MaximumOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasMax({self.column}, assertion)"


class SumEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasSum constraint."""

    def get_operator(self):
        return SumOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasSum({self.column}, assertion)"


class StandardDeviationEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasStandardDeviation constraint."""

    def get_operator(self):
        return StandardDeviationOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasStandardDeviation({self.column}, assertion)"


class UniquenessEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for isUnique and hasUniqueness constraints."""

    def get_operator(self):
        cols = self.columns if self.columns else [self.column]
        return UniquenessOperator(cols, where=self.where)

    def to_string(self) -> str:
        cols = self.columns if self.columns else [self.column]
        col_str = ", ".join(cols)
        if self.assertion:
            return f"hasUniqueness({col_str}, assertion)"
        return f"isUnique({col_str})"


class DistinctnessEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasDistinctness constraint."""

    def get_operator(self):
        cols = self.columns if self.columns else [self.column]
        return DistinctnessOperator(cols, where=self.where)

    def to_string(self) -> str:
        cols = self.columns if self.columns else [self.column]
        col_str = ", ".join(cols)
        return f"hasDistinctness({col_str}, assertion)"


class UniqueValueRatioEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasUniqueValueRatio constraint."""

    def get_operator(self):
        cols = self.columns if self.columns else [self.column]
        return UniqueValueRatioOperator(cols, where=self.where)

    def to_string(self) -> str:
        cols = self.columns if self.columns else [self.column]
        col_str = ", ".join(cols)
        return f"hasUniqueValueRatio({col_str}, assertion)"


class CorrelationEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasCorrelation constraint."""

    def get_operator(self):
        if len(self.columns) >= 2:
            return CorrelationOperator(self.columns[0], self.columns[1], where=self.where)
        return None

    def compute_value(
        self, table: str, execute_fn: Callable[[str], "pd.DataFrame"]
    ) -> Optional[float]:
        if len(self.columns) < 2:
            return None
        return super().compute_value(table, execute_fn)

    def to_string(self) -> str:
        if len(self.columns) >= 2:
            return f"hasCorrelation({self.columns[0]}, {self.columns[1]}, assertion)"
        return "hasCorrelation()"


class EntropyEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasEntropy constraint."""

    def get_operator(self):
        return EntropyOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasEntropy({self.column}, assertion)"


class MutualInformationEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasMutualInformation constraint."""

    def get_operator(self):
        if len(self.columns) >= 2:
            return MutualInformationOperator(self.columns, where=self.where)
        return None

    def compute_value(
        self, table: str, execute_fn: Callable[[str], "pd.DataFrame"]
    ) -> Optional[float]:
        if len(self.columns) < 2:
            return None
        return super().compute_value(table, execute_fn)

    def to_string(self) -> str:
        col_str = ", ".join(self.columns)
        return f"hasMutualInformation({col_str}, assertion)"


class PatternMatchEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasPattern constraint."""

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.pattern = constraint_proto.pattern if constraint_proto.pattern else ""

    def get_operator(self):
        return PatternMatchOperator(self.column, self.pattern, where=self.where)

    def to_string(self) -> str:
        return f"hasPattern({self.column}, '{self.pattern}')"


class MinLengthEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasMinLength constraint."""

    def get_operator(self):
        return MinLengthOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasMinLength({self.column}, assertion)"


class MaxLengthEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasMaxLength constraint."""

    def get_operator(self):
        return MaxLengthOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasMaxLength({self.column}, assertion)"


class ApproxCountDistinctEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasApproxCountDistinct constraint."""

    def get_operator(self):
        return ApproxCountDistinctOperator(self.column, where=self.where)

    def to_string(self) -> str:
        return f"hasApproxCountDistinct({self.column}, assertion)"


class ApproxQuantileEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for hasApproxQuantile constraint."""

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.quantile = constraint_proto.quantile if constraint_proto.quantile else 0.5

    def get_operator(self):
        return ApproxQuantileOperator(self.column, self.quantile, where=self.where)

    def to_string(self) -> str:
        return f"hasApproxQuantile({self.column}, {self.quantile}, assertion)"


class ComplianceEvaluator(AnalyzerBasedEvaluator):
    """Evaluator for satisfies constraint."""

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.predicate = constraint_proto.column_condition if constraint_proto.column_condition else ""
        self.name = constraint_proto.constraint_name if constraint_proto.constraint_name else "satisfies"

    def get_operator(self):
        return ComplianceOperator(self.name, self.predicate, where=self.where)

    def to_string(self) -> str:
        return f"satisfies({self.name}, '{self.predicate}')"


# =============================================================================
# Ratio-check evaluators
# =============================================================================


class IsPositiveEvaluator(RatioCheckEvaluator):
    """Evaluator for isPositive constraint."""

    def get_condition(self) -> str:
        return f"{self.column} > 0"

    def to_string(self) -> str:
        return f"isPositive({self.column})"


class IsNonNegativeEvaluator(RatioCheckEvaluator):
    """Evaluator for isNonNegative constraint."""

    def get_condition(self) -> str:
        return f"{self.column} >= 0"

    def to_string(self) -> str:
        return f"isNonNegative({self.column})"


class IsContainedInEvaluator(RatioCheckEvaluator):
    """Evaluator for isContainedIn constraint."""

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.allowed_values = list(constraint_proto.allowed_values) if constraint_proto.allowed_values else []

    def get_condition(self) -> str:
        # Escape single quotes in values
        escaped_values = [v.replace("'", "''") for v in self.allowed_values]
        values_str = ", ".join([f"'{v}'" for v in escaped_values])
        return f"{self.column} IN ({values_str})"

    def to_string(self) -> str:
        values_str = ", ".join([f"'{v}'" for v in self.allowed_values])
        return f"isContainedIn({self.column}, [{values_str}])"


class ContainsEmailEvaluator(RatioCheckEvaluator):
    """Evaluator for containsEmail constraint."""

    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    def get_condition(self) -> str:
        return f"REGEXP_MATCHES({self.column}, '{self.EMAIL_PATTERN}')"

    def to_string(self) -> str:
        return f"containsEmail({self.column})"


class ContainsURLEvaluator(RatioCheckEvaluator):
    """Evaluator for containsURL constraint."""

    URL_PATTERN = r"^https?://[^\s]+$"

    def get_condition(self) -> str:
        return f"REGEXP_MATCHES({self.column}, '{self.URL_PATTERN}')"

    def to_string(self) -> str:
        return f"containsURL({self.column})"


class ContainsCreditCardEvaluator(RatioCheckEvaluator):
    """Evaluator for containsCreditCardNumber constraint."""

    CC_PATTERN = r"^\d{13,19}$"

    def get_condition(self) -> str:
        return f"REGEXP_MATCHES({self.column}, '{self.CC_PATTERN}')"

    def to_string(self) -> str:
        return f"containsCreditCardNumber({self.column})"


class ContainsSSNEvaluator(RatioCheckEvaluator):
    """Evaluator for containsSocialSecurityNumber constraint."""

    SSN_PATTERN = r"^\d{3}-\d{2}-\d{4}$"

    def get_condition(self) -> str:
        return f"REGEXP_MATCHES({self.column}, '{self.SSN_PATTERN}')"

    def to_string(self) -> str:
        return f"containsSocialSecurityNumber({self.column})"


# =============================================================================
# Comparison evaluators
# =============================================================================


class ColumnComparisonEvaluator(RatioCheckEvaluator):
    """Evaluator for column comparison constraints."""

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self._comparison_type = constraint_proto.type

    def get_condition(self) -> str:
        if len(self.columns) < 2:
            return "1=0"  # Always false if not enough columns

        col_a, col_b = self.columns[0], self.columns[1]

        if self._comparison_type == "isLessThan":
            return f"{col_a} < {col_b}"
        elif self._comparison_type == "isLessThanOrEqualTo":
            return f"{col_a} <= {col_b}"
        elif self._comparison_type == "isGreaterThan":
            return f"{col_a} > {col_b}"
        elif self._comparison_type == "isGreaterThanOrEqualTo":
            return f"{col_a} >= {col_b}"

        return "1=0"

    def to_string(self) -> str:
        if len(self.columns) >= 2:
            return f"{self._comparison_type}({self.columns[0]}, {self.columns[1]})"
        return f"{self._comparison_type}()"


# =============================================================================
# Multi-column evaluators
# =============================================================================


class MultiColumnCompletenessEvaluator(BaseEvaluator):
    """Evaluator for areComplete and haveCompleteness constraints."""

    def compute_value(
        self, table: str, execute_fn: Callable[[str], "pd.DataFrame"]
    ) -> Optional[float]:
        if not self.columns:
            return 1.0

        # All columns must be non-null for a row to be "complete"
        null_conditions = " OR ".join([f"{col} IS NULL" for col in self.columns])

        if self.where:
            query = f"""
                SELECT
                    SUM(CASE WHEN {self.where} THEN 1 ELSE 0 END) as total,
                    SUM(CASE WHEN ({self.where}) AND ({null_conditions}) THEN 1 ELSE 0 END) as any_null
                FROM {table}
            """
        else:
            query = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {null_conditions} THEN 1 ELSE 0 END) as any_null
                FROM {table}
            """

        result = execute_fn(query)
        total = self.safe_float(result, "total") or 0
        any_null = self.safe_float(result, "any_null") or 0

        if total == 0:
            return 1.0
        return (total - any_null) / total

    def to_string(self) -> str:
        col_str = ", ".join(self.columns)
        if self.assertion:
            return f"haveCompleteness({col_str}, assertion)"
        return f"areComplete({col_str})"


__all__ = [
    # Analyzer-based evaluators
    "SizeEvaluator",
    "CompletenessEvaluator",
    "MeanEvaluator",
    "MinimumEvaluator",
    "MaximumEvaluator",
    "SumEvaluator",
    "StandardDeviationEvaluator",
    "UniquenessEvaluator",
    "DistinctnessEvaluator",
    "UniqueValueRatioEvaluator",
    "CorrelationEvaluator",
    "EntropyEvaluator",
    "MutualInformationEvaluator",
    "PatternMatchEvaluator",
    "MinLengthEvaluator",
    "MaxLengthEvaluator",
    "ApproxCountDistinctEvaluator",
    "ApproxQuantileEvaluator",
    "ComplianceEvaluator",
    # Ratio-check evaluators
    "IsPositiveEvaluator",
    "IsNonNegativeEvaluator",
    "IsContainedInEvaluator",
    "ContainsEmailEvaluator",
    "ContainsURLEvaluator",
    "ContainsCreditCardEvaluator",
    "ContainsSSNEvaluator",
    # Comparison evaluators
    "ColumnComparisonEvaluator",
    # Multi-column evaluators
    "MultiColumnCompletenessEvaluator",
]
