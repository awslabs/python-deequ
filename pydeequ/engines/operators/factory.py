# -*- coding: utf-8 -*-
"""
Operator factory for creating operators from analyzers.

This module provides a registry-based factory pattern that eliminates
isinstance() chains when creating operators from analyzers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional, Type, Union

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

# Type alias for operator types
OperatorType = Union["ScanOperator", "GroupingOperator", "DataTypeOperator"]


class OperatorFactory:
    """
    Creates operators from analyzers using registry pattern.

    This factory eliminates isinstance() chains by mapping analyzer
    types to their corresponding operator classes.
    """

    # Registry mapping analyzer type names to operator classes
    _scan_registry: Dict[str, Type] = {}
    _grouping_registry: Dict[str, Type] = {}
    _metadata_registry: Dict[str, Type] = {}

    @classmethod
    def register_scan(cls, analyzer_name: str):
        """
        Decorator to register a scan operator for an analyzer type.

        Args:
            analyzer_name: Name of the analyzer class (e.g., "Mean", "Sum")

        Returns:
            Decorator function
        """
        def decorator(operator_class: Type):
            cls._scan_registry[analyzer_name] = operator_class
            return operator_class
        return decorator

    @classmethod
    def register_grouping(cls, analyzer_name: str):
        """
        Decorator to register a grouping operator for an analyzer type.

        Args:
            analyzer_name: Name of the analyzer class

        Returns:
            Decorator function
        """
        def decorator(operator_class: Type):
            cls._grouping_registry[analyzer_name] = operator_class
            return operator_class
        return decorator

    @classmethod
    def register_metadata(cls, analyzer_name: str):
        """
        Decorator to register a metadata operator for an analyzer type.

        Metadata operators compute metrics from schema information rather
        than SQL queries. They are used for type inference and similar
        schema-based analysis.

        Args:
            analyzer_name: Name of the analyzer class

        Returns:
            Decorator function
        """
        def decorator(operator_class: Type):
            cls._metadata_registry[analyzer_name] = operator_class
            return operator_class
        return decorator

    @classmethod
    def create(cls, analyzer: "_ConnectAnalyzer") -> Optional[OperatorType]:
        """
        Create operator instance for given analyzer.

        Args:
            analyzer: Analyzer instance to create operator for

        Returns:
            Operator instance or None if analyzer type not supported
        """
        analyzer_name = type(analyzer).__name__

        # Try scan registry first
        if analyzer_name in cls._scan_registry:
            return cls._create_scan_operator(analyzer_name, analyzer)

        # Try grouping registry
        if analyzer_name in cls._grouping_registry:
            return cls._create_grouping_operator(analyzer_name, analyzer)

        # Try metadata registry
        if analyzer_name in cls._metadata_registry:
            return cls._create_metadata_operator(analyzer_name, analyzer)

        return None

    @classmethod
    def _create_scan_operator(
        cls, analyzer_name: str, analyzer: "_ConnectAnalyzer"
    ) -> "ScanOperator":
        """Create a scan operator from an analyzer."""
        operator_class = cls._scan_registry[analyzer_name]

        # Extract common attributes
        column = getattr(analyzer, "column", None)
        where = getattr(analyzer, "where", None)

        # Handle special cases
        if analyzer_name == "Size":
            return operator_class(where=where)
        elif analyzer_name == "Compliance":
            instance = getattr(analyzer, "instance", "compliance")
            predicate = getattr(analyzer, "predicate", "")
            return operator_class(instance, predicate, where=where)
        elif analyzer_name == "PatternMatch":
            pattern = getattr(analyzer, "pattern", "")
            return operator_class(column, pattern, where=where)
        elif analyzer_name == "Correlation":
            column1 = getattr(analyzer, "column1", "")
            column2 = getattr(analyzer, "column2", "")
            return operator_class(column1, column2, where=where)
        elif analyzer_name == "CountDistinct":
            columns = list(getattr(analyzer, "columns", []))
            return operator_class(columns, where=where)
        elif analyzer_name == "ApproxQuantile":
            quantile = getattr(analyzer, "quantile", 0.5)
            return operator_class(column, quantile, where=where)
        else:
            # Standard single-column operators
            return operator_class(column, where=where)

    @classmethod
    def _create_grouping_operator(
        cls, analyzer_name: str, analyzer: "_ConnectAnalyzer"
    ) -> "GroupingOperator":
        """Create a grouping operator from an analyzer."""
        operator_class = cls._grouping_registry[analyzer_name]

        where = getattr(analyzer, "where", None)

        if analyzer_name == "Entropy":
            column = getattr(analyzer, "column", "")
            return operator_class(column, where=where)
        elif analyzer_name == "Histogram":
            column = getattr(analyzer, "column", "")
            max_bins = getattr(analyzer, "max_detail_bins", 100) or 100
            return operator_class(column, max_bins, where=where)
        else:
            # Multi-column operators (Distinctness, Uniqueness, etc.)
            columns = getattr(analyzer, "columns", [])
            if isinstance(columns, str):
                columns = [columns]
            return operator_class(list(columns), where=where)

    @classmethod
    def _create_metadata_operator(
        cls, analyzer_name: str, analyzer: "_ConnectAnalyzer"
    ) -> "DataTypeOperator":
        """Create a metadata operator from an analyzer."""
        operator_class = cls._metadata_registry[analyzer_name]

        # Extract common attributes
        column = getattr(analyzer, "column", None)
        where = getattr(analyzer, "where", None)

        # Standard single-column metadata operators
        return operator_class(column, where=where)

    @classmethod
    def is_scan_operator(cls, analyzer: "_ConnectAnalyzer") -> bool:
        """Check if analyzer maps to a scan operator."""
        return type(analyzer).__name__ in cls._scan_registry

    @classmethod
    def is_grouping_operator(cls, analyzer: "_ConnectAnalyzer") -> bool:
        """Check if analyzer maps to a grouping operator."""
        return type(analyzer).__name__ in cls._grouping_registry

    @classmethod
    def is_metadata_operator(cls, analyzer: "_ConnectAnalyzer") -> bool:
        """Check if analyzer maps to a metadata operator."""
        return type(analyzer).__name__ in cls._metadata_registry

    @classmethod
    def is_supported(cls, analyzer: "_ConnectAnalyzer") -> bool:
        """Check if analyzer type is supported by the factory."""
        analyzer_name = type(analyzer).__name__
        return (
            analyzer_name in cls._scan_registry
            or analyzer_name in cls._grouping_registry
            or analyzer_name in cls._metadata_registry
        )


# Register all scan operators
OperatorFactory._scan_registry = {
    "Size": SizeOperator,
    "Completeness": CompletenessOperator,
    "Mean": MeanOperator,
    "Sum": SumOperator,
    "Minimum": MinimumOperator,
    "Maximum": MaximumOperator,
    "StandardDeviation": StandardDeviationOperator,
    "MaxLength": MaxLengthOperator,
    "MinLength": MinLengthOperator,
    "PatternMatch": PatternMatchOperator,
    "Compliance": ComplianceOperator,
    "Correlation": CorrelationOperator,
    "CountDistinct": CountDistinctOperator,
    "ApproxCountDistinct": ApproxCountDistinctOperator,
    "ApproxQuantile": ApproxQuantileOperator,
}

# Register all grouping operators
OperatorFactory._grouping_registry = {
    "Distinctness": DistinctnessOperator,
    "Uniqueness": UniquenessOperator,
    "UniqueValueRatio": UniqueValueRatioOperator,
    "Entropy": EntropyOperator,
    "MutualInformation": MutualInformationOperator,
    "Histogram": HistogramOperator,
}

# Register all metadata operators
OperatorFactory._metadata_registry = {
    "DataType": DataTypeOperator,
}


__all__ = [
    "OperatorFactory",
]
