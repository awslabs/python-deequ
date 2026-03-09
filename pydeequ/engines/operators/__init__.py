# -*- coding: utf-8 -*-
"""
SQL Operator abstractions for data quality metrics.

This module provides a hierarchical operator abstraction pattern that:
1. Eliminates code duplication across analyzer implementations
2. Separates SQL generation from result extraction
3. Enables efficient batch execution of scan operators
4. Provides consistent WHERE clause handling

Architecture:
    Protocols (Contracts)
    ├── ScanOperatorProtocol     - Single-pass aggregation operators
    └── GroupingOperatorProtocol - GROUP BY-based operators

    Mixins (Shared Behaviors)
    ├── WhereClauseMixin    - Conditional aggregation wrapping
    ├── SafeExtractMixin    - Safe value extraction from DataFrames
    └── ColumnAliasMixin    - Consistent alias generation

    Base Classes (Hierarchy)
    ├── ScanOperator        - Base for single-pass operators
    └── GroupingOperator    - Base for GROUP BY operators

    Factory
    └── OperatorFactory     - Creates operators from analyzers

Example usage:
    from pydeequ.engines.operators import OperatorFactory

    # Create operator from analyzer
    operator = OperatorFactory.create(Mean("price"))

    # Get SQL aggregations for scan operators
    aggregations = operator.get_aggregations()
    # ["AVG(price) AS mean_price"]

    # Execute query and extract result
    df = engine._execute_query(f"SELECT {', '.join(aggregations)} FROM table")
    result = operator.extract_result(df)
"""

from pydeequ.engines.operators.base import GroupingOperator, ScanOperator
from pydeequ.engines.operators.factory import OperatorFactory
from pydeequ.engines.operators.grouping_batcher import (
    GroupingOperatorBatcher,
    BATCHABLE_OPERATORS,
)
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
from pydeequ.engines.operators.profiling_operators import (
    ColumnProfileOperator,
    MultiColumnProfileOperator,
    NUMERIC_TYPES,
    STRING_TYPES,
)
from pydeequ.engines.operators.mixins import (
    ColumnAliasMixin,
    SafeExtractMixin,
    WhereClauseMixin,
)
from pydeequ.engines.operators.protocols import (
    GroupingOperatorProtocol,
    ScanOperatorProtocol,
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

__all__ = [
    # Protocols
    "ScanOperatorProtocol",
    "GroupingOperatorProtocol",
    # Mixins
    "WhereClauseMixin",
    "SafeExtractMixin",
    "ColumnAliasMixin",
    # Base classes
    "ScanOperator",
    "GroupingOperator",
    # Scan operators
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
    # Grouping operators
    "DistinctnessOperator",
    "UniquenessOperator",
    "UniqueValueRatioOperator",
    "EntropyOperator",
    "MutualInformationOperator",
    "HistogramOperator",
    # Grouping operator batching
    "GroupingOperatorBatcher",
    "BATCHABLE_OPERATORS",
    # Metadata operators
    "DataTypeOperator",
    # Profiling operators
    "ColumnProfileOperator",
    "MultiColumnProfileOperator",
    "NUMERIC_TYPES",
    "STRING_TYPES",
    # Factory
    "OperatorFactory",
]
