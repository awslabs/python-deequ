# -*- coding: utf-8 -*-
"""
Operator factory for creating operators from analyzers.

The registry maps an analyzer's class name to a *builder callable* that
takes the analyzer and returns the operator. This keeps the per-operator
constructor knowledge co-located with each registration entry, instead
of spreading it across an ``if/elif`` chain.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, Optional, Union

from pydeequ.engines.operators.grouping_operators import (
    DistinctnessOperator,
    EntropyOperator,
    HistogramOperator,
    MutualInformationOperator,
    UniqueValueRatioOperator,
    UniquenessOperator,
)
from pydeequ.engines.operators.metadata_operators import (
    DataTypeOperator,
)
from pydeequ.engines.operators.scan_operators import (
    ApproxCountDistinctOperator,
    ApproxQuantileOperator,
    ComplianceOperator,
    CompletenessOperator,
    CorrelationOperator,
    CountDistinctOperator,
    MaximumOperator,
    MaxLengthOperator,
    MeanOperator,
    MinimumOperator,
    MinLengthOperator,
    PatternMatchOperator,
    SizeOperator,
    StandardDeviationOperator,
    SumOperator,
)

if TYPE_CHECKING:
    from pydeequ.engines.operators.base import GroupingOperator, ScanOperator
    from pydeequ.v2.analyzers import _ConnectAnalyzer

OperatorType = Union["ScanOperator", "GroupingOperator", "DataTypeOperator"]
Builder = Callable[["_ConnectAnalyzer"], Optional[OperatorType]]


# ----------------------------------------------------------------------------
# Reusable builders
#
# Most operators want one of two construction shapes: a single column with an
# optional WHERE, or a column-list with an optional WHERE. The helpers below
# turn an operator class into a Builder so the registry stays declarative.
# ----------------------------------------------------------------------------


def _by_column(operator_cls):
    """Builder for operators taking ``(column, where=)``."""
    return lambda an: operator_cls(getattr(an, "column", None), where=getattr(an, "where", None))


def _by_columns(operator_cls):
    """Builder for operators taking ``(columns, where=)``.

    Accepts either an analyzer with a ``columns`` list/tuple or a string,
    and normalises to a list.
    """
    def build(an):
        cols = getattr(an, "columns", [])
        if isinstance(cols, str):
            cols = [cols]
        return operator_cls(list(cols), where=getattr(an, "where", None))
    return build


# ----------------------------------------------------------------------------
# Per-operator builders for shapes that don't fit the two helpers above
# ----------------------------------------------------------------------------


def _build_size(an):
    return SizeOperator(where=getattr(an, "where", None))


def _build_compliance(an):
    return ComplianceOperator(
        getattr(an, "instance", "compliance"),
        getattr(an, "predicate", ""),
        where=getattr(an, "where", None),
    )


def _build_pattern_match(an):
    return PatternMatchOperator(
        getattr(an, "column", None),
        getattr(an, "pattern", ""),
        where=getattr(an, "where", None),
    )


def _build_correlation(an):
    return CorrelationOperator(
        getattr(an, "column1", ""),
        getattr(an, "column2", ""),
        where=getattr(an, "where", None),
    )


def _build_approx_quantile(an):
    return ApproxQuantileOperator(
        getattr(an, "column", None),
        getattr(an, "quantile", 0.5),
        where=getattr(an, "where", None),
    )


def _build_histogram(an):
    return HistogramOperator(
        getattr(an, "column", ""),
        getattr(an, "max_detail_bins", 100) or 100,
        where=getattr(an, "where", None),
    )


class OperatorFactory:
    """
    Creates operators from analyzers using registries of builder callables.

    Each registry entry pairs an analyzer class name with a function that
    constructs the matching operator from that analyzer instance.
    """

    _scan_registry: Dict[str, Builder] = {}
    _grouping_registry: Dict[str, Builder] = {}
    _metadata_registry: Dict[str, Builder] = {}

    # ------------------------------------------------------------------
    # Registration decorators (for downstream extensions)
    # ------------------------------------------------------------------

    @classmethod
    def register_scan(cls, analyzer_name: str, builder: Builder) -> None:
        """Register a scan-operator builder for an analyzer class name."""
        cls._scan_registry[analyzer_name] = builder

    @classmethod
    def register_grouping(cls, analyzer_name: str, builder: Builder) -> None:
        """Register a grouping-operator builder for an analyzer class name."""
        cls._grouping_registry[analyzer_name] = builder

    @classmethod
    def register_metadata(cls, analyzer_name: str, builder: Builder) -> None:
        """Register a metadata-operator builder for an analyzer class name."""
        cls._metadata_registry[analyzer_name] = builder

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    @classmethod
    def create(cls, analyzer: "_ConnectAnalyzer") -> Optional[OperatorType]:
        """Create the operator for ``analyzer``, or None if not supported."""
        name = type(analyzer).__name__
        builder = (
            cls._scan_registry.get(name)
            or cls._grouping_registry.get(name)
            or cls._metadata_registry.get(name)
        )
        return builder(analyzer) if builder else None

    @classmethod
    def is_scan_operator(cls, analyzer: "_ConnectAnalyzer") -> bool:
        return type(analyzer).__name__ in cls._scan_registry

    @classmethod
    def is_grouping_operator(cls, analyzer: "_ConnectAnalyzer") -> bool:
        return type(analyzer).__name__ in cls._grouping_registry

    @classmethod
    def is_metadata_operator(cls, analyzer: "_ConnectAnalyzer") -> bool:
        return type(analyzer).__name__ in cls._metadata_registry

    @classmethod
    def is_supported(cls, analyzer: "_ConnectAnalyzer") -> bool:
        name = type(analyzer).__name__
        return (
            name in cls._scan_registry
            or name in cls._grouping_registry
            or name in cls._metadata_registry
        )


# ---------------------------------------------------------------------------
# Default registrations
# ---------------------------------------------------------------------------

OperatorFactory._scan_registry = {
    "Size": _build_size,
    "Completeness": _by_column(CompletenessOperator),
    "Mean": _by_column(MeanOperator),
    "Sum": _by_column(SumOperator),
    "Minimum": _by_column(MinimumOperator),
    "Maximum": _by_column(MaximumOperator),
    "StandardDeviation": _by_column(StandardDeviationOperator),
    "MaxLength": _by_column(MaxLengthOperator),
    "MinLength": _by_column(MinLengthOperator),
    "PatternMatch": _build_pattern_match,
    "Compliance": _build_compliance,
    "Correlation": _build_correlation,
    "CountDistinct": _by_columns(CountDistinctOperator),
    "ApproxCountDistinct": _by_column(ApproxCountDistinctOperator),
    "ApproxQuantile": _build_approx_quantile,
}

OperatorFactory._grouping_registry = {
    "Distinctness": _by_columns(DistinctnessOperator),
    "Uniqueness": _by_columns(UniquenessOperator),
    "UniqueValueRatio": _by_columns(UniqueValueRatioOperator),
    "Entropy": _by_column(EntropyOperator),
    "MutualInformation": _by_columns(MutualInformationOperator),
    "Histogram": _build_histogram,
}

OperatorFactory._metadata_registry = {
    "DataType": _by_column(DataTypeOperator),
}


__all__ = [
    "OperatorFactory",
]
