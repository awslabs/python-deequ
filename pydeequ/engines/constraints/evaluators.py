# -*- coding: utf-8 -*-
"""
Constraint evaluator implementations.

The bulk of analyzer-based evaluators are declarative: each subclass of
``AnalyzerEvaluator`` only declares which operator to build and how to
format its name. The shared logic — operator construction, column-list
handling, ``to_string`` rendering — lives once on the base class.

Evaluators with genuinely distinct behaviour (regex patterns, multi-column
correlation, custom SQL conditions) keep their own subclasses.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, ClassVar, List, Optional

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
#
# Each shallow subclass is now declarative config: an operator builder, a
# user-facing constraint name, and an optional alias used when no assertion
# was provided (e.g. ``isComplete`` instead of ``hasCompleteness``).
# =============================================================================


def _single_column_op(operator_cls: type) -> Callable[["AnalyzerEvaluator"], Any]:
    """Build an operator from a single-column analyzer evaluator."""
    return lambda ev: operator_cls(ev.column, where=ev.where)


def _column_list_op(operator_cls: type) -> Callable[["AnalyzerEvaluator"], Any]:
    """Build an operator from one-or-more column names.

    Falls back to a single-element list when only ``column`` is set, so the
    same evaluator handles ``isUnique("id")`` and ``hasUniqueness(["a","b"])``.
    """
    def build(ev: "AnalyzerEvaluator") -> Any:
        cols = ev.columns if ev.columns else [ev.column]
        return operator_cls(cols, where=ev.where)
    return build


class AnalyzerEvaluator(AnalyzerBasedEvaluator):
    """
    Generic analyzer-based evaluator driven by class-level config.

    Subclasses set:
        - ``OPERATOR_BUILDER``: callable taking the evaluator and returning an operator
        - ``CONSTRAINT_NAME``: the user-facing name (e.g. ``"hasMean"``)
        - ``BARE_NAME`` (optional): name used when there is no assertion
                       (e.g. ``"isComplete"`` for ``hasCompleteness``).
                       When None, the constraint always renders with an assertion suffix.
        - ``MULTI_COLUMN`` (optional): if True, ``to_string`` joins ``self.columns``
                       (falling back to ``[self.column]``); otherwise it renders ``self.column``.
    """

    OPERATOR_BUILDER: ClassVar[Optional[Callable[["AnalyzerEvaluator"], Any]]] = None
    CONSTRAINT_NAME: ClassVar[str] = ""
    BARE_NAME: ClassVar[Optional[str]] = None
    MULTI_COLUMN: ClassVar[bool] = False

    def get_operator(self):
        if self.OPERATOR_BUILDER is None:
            return None
        return self.OPERATOR_BUILDER(self)

    def _format_columns(self) -> str:
        if self.MULTI_COLUMN:
            cols = self.columns if self.columns else ([self.column] if self.column else [])
            return ", ".join(c for c in cols if c is not None)
        return self.column or ""

    def to_string(self) -> str:
        col_str = self._format_columns()
        if not self.assertion and self.BARE_NAME:
            return f"{self.BARE_NAME}({col_str})" if col_str else f"{self.BARE_NAME}()"
        return (
            f"{self.CONSTRAINT_NAME}({col_str}, assertion)"
            if col_str
            else f"{self.CONSTRAINT_NAME}(assertion)"
        )


# ---- Single-column analyzer evaluators -------------------------------------


class SizeEvaluator(AnalyzerEvaluator):
    """Evaluator for hasSize constraint."""

    OPERATOR_BUILDER = staticmethod(lambda ev: SizeOperator(where=ev.where))
    CONSTRAINT_NAME = "hasSize"
    BARE_NAME = "hasSize"  # rendered without column either way

    def to_string(self) -> str:
        # Size is dataset-level; never carries a column.
        return "hasSize(assertion)" if self.assertion else "hasSize()"


class CompletenessEvaluator(AnalyzerEvaluator):
    """Evaluator for isComplete and hasCompleteness constraints."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(CompletenessOperator))
    CONSTRAINT_NAME = "hasCompleteness"
    BARE_NAME = "isComplete"


class MeanEvaluator(AnalyzerEvaluator):
    """Evaluator for hasMean constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(MeanOperator))
    CONSTRAINT_NAME = "hasMean"


class MinimumEvaluator(AnalyzerEvaluator):
    """Evaluator for hasMin constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(MinimumOperator))
    CONSTRAINT_NAME = "hasMin"


class MaximumEvaluator(AnalyzerEvaluator):
    """Evaluator for hasMax constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(MaximumOperator))
    CONSTRAINT_NAME = "hasMax"


class SumEvaluator(AnalyzerEvaluator):
    """Evaluator for hasSum constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(SumOperator))
    CONSTRAINT_NAME = "hasSum"


class StandardDeviationEvaluator(AnalyzerEvaluator):
    """Evaluator for hasStandardDeviation constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(StandardDeviationOperator))
    CONSTRAINT_NAME = "hasStandardDeviation"


class EntropyEvaluator(AnalyzerEvaluator):
    """Evaluator for hasEntropy constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(EntropyOperator))
    CONSTRAINT_NAME = "hasEntropy"


class MinLengthEvaluator(AnalyzerEvaluator):
    """Evaluator for hasMinLength constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(MinLengthOperator))
    CONSTRAINT_NAME = "hasMinLength"


class MaxLengthEvaluator(AnalyzerEvaluator):
    """Evaluator for hasMaxLength constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(MaxLengthOperator))
    CONSTRAINT_NAME = "hasMaxLength"


class ApproxCountDistinctEvaluator(AnalyzerEvaluator):
    """Evaluator for hasApproxCountDistinct constraint."""

    OPERATOR_BUILDER = staticmethod(_single_column_op(ApproxCountDistinctOperator))
    CONSTRAINT_NAME = "hasApproxCountDistinct"


# ---- Column-list analyzer evaluators ---------------------------------------


class UniquenessEvaluator(AnalyzerEvaluator):
    """Evaluator for isUnique and hasUniqueness constraints."""

    OPERATOR_BUILDER = staticmethod(_column_list_op(UniquenessOperator))
    CONSTRAINT_NAME = "hasUniqueness"
    BARE_NAME = "isUnique"
    MULTI_COLUMN = True


class DistinctnessEvaluator(AnalyzerEvaluator):
    """Evaluator for hasDistinctness constraint."""

    OPERATOR_BUILDER = staticmethod(_column_list_op(DistinctnessOperator))
    CONSTRAINT_NAME = "hasDistinctness"
    MULTI_COLUMN = True


class UniqueValueRatioEvaluator(AnalyzerEvaluator):
    """Evaluator for hasUniqueValueRatio constraint."""

    OPERATOR_BUILDER = staticmethod(_column_list_op(UniqueValueRatioOperator))
    CONSTRAINT_NAME = "hasUniqueValueRatio"
    MULTI_COLUMN = True


# ---- Evaluators with custom logic (kept as full subclasses) ----------------


class CorrelationEvaluator(AnalyzerEvaluator):
    """Evaluator for hasCorrelation constraint.

    Requires exactly two columns; falls back to a no-op when the proto
    carries fewer than two.
    """

    CONSTRAINT_NAME = "hasCorrelation"

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


class MutualInformationEvaluator(AnalyzerEvaluator):
    """Evaluator for hasMutualInformation constraint.

    Requires at least two columns.
    """

    CONSTRAINT_NAME = "hasMutualInformation"

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


class PatternMatchEvaluator(AnalyzerEvaluator):
    """Evaluator for hasPattern constraint."""

    CONSTRAINT_NAME = "hasPattern"

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.pattern = constraint_proto.pattern if constraint_proto.pattern else ""

    def get_operator(self):
        return PatternMatchOperator(self.column, self.pattern, where=self.where)

    def to_string(self) -> str:
        return f"hasPattern({self.column}, '{self.pattern}')"


class ApproxQuantileEvaluator(AnalyzerEvaluator):
    """Evaluator for hasApproxQuantile constraint."""

    CONSTRAINT_NAME = "hasApproxQuantile"

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.quantile = constraint_proto.quantile if constraint_proto.quantile else 0.5

    def get_operator(self):
        return ApproxQuantileOperator(self.column, self.quantile, where=self.where)

    def to_string(self) -> str:
        return f"hasApproxQuantile({self.column}, {self.quantile}, assertion)"


class ComplianceEvaluator(AnalyzerEvaluator):
    """Evaluator for satisfies constraint."""

    CONSTRAINT_NAME = "satisfies"

    def __init__(self, constraint_proto):
        super().__init__(constraint_proto)
        self.predicate = (
            constraint_proto.column_condition if constraint_proto.column_condition else ""
        )
        self.name = (
            constraint_proto.constraint_name if constraint_proto.constraint_name else "satisfies"
        )

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
    # Generic base for analyzer-based evaluators
    "AnalyzerEvaluator",
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
