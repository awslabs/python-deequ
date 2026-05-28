# -*- coding: utf-8 -*-
"""
Constraint evaluator abstractions for data quality checks.

This module provides a constraint evaluator pattern that:
1. Encapsulates constraint evaluation logic in self-contained classes
2. Separates value computation from assertion evaluation
3. Provides consistent WHERE clause handling
4. Enables easy addition of new constraint types

Architecture:
    Protocols (Contracts)
    └── ConstraintEvaluatorProtocol - Defines evaluator contract

    Base Classes (Hierarchy)
    ├── BaseEvaluator         - Base with WHERE clause and assertion handling
    ├── RatioCheckEvaluator   - For match/total ratio constraints
    └── AnalyzerBasedEvaluator - Delegates to analyzer operators

    Evaluator Implementations
    ├── Analyzer-based (SizeEvaluator, CompletenessEvaluator, etc.)
    ├── Ratio-check (IsPositiveEvaluator, IsContainedInEvaluator, etc.)
    ├── Comparison (ColumnComparisonEvaluator)
    └── Multi-column (MultiColumnCompletenessEvaluator)

    Factory
    └── ConstraintEvaluatorFactory - Creates evaluators from protobufs

Example usage:
    from pydeequ.engines.constraints import ConstraintEvaluatorFactory

    # Create evaluator from constraint protobuf
    evaluator = ConstraintEvaluatorFactory.create(constraint_proto)

    if evaluator:
        # Compute the metric value
        value = evaluator.compute_value(table, execute_fn)

        # Evaluate the assertion
        passed = evaluator.evaluate(value)

        # Get human-readable description
        description = evaluator.to_string()
"""

from pydeequ.engines.constraints.base import (
    AnalyzerBasedEvaluator,
    BaseEvaluator,
    RatioCheckEvaluator,
)
from pydeequ.engines.constraints.batch_evaluator import (
    ConstraintBatchEvaluator,
    SCAN_BASED_EVALUATORS,
)
from pydeequ.engines.constraints.evaluators import (
    ApproxCountDistinctEvaluator,
    ApproxQuantileEvaluator,
    ColumnComparisonEvaluator,
    CompletenessEvaluator,
    ComplianceEvaluator,
    ContainsCreditCardEvaluator,
    ContainsEmailEvaluator,
    ContainsSSNEvaluator,
    ContainsURLEvaluator,
    CorrelationEvaluator,
    DistinctnessEvaluator,
    EntropyEvaluator,
    IsContainedInEvaluator,
    IsNonNegativeEvaluator,
    IsPositiveEvaluator,
    MaximumEvaluator,
    MaxLengthEvaluator,
    MeanEvaluator,
    MinimumEvaluator,
    MinLengthEvaluator,
    MultiColumnCompletenessEvaluator,
    MutualInformationEvaluator,
    PatternMatchEvaluator,
    SizeEvaluator,
    StandardDeviationEvaluator,
    SumEvaluator,
    UniquenessEvaluator,
    UniqueValueRatioEvaluator,
)
from pydeequ.engines.constraints.factory import ConstraintEvaluatorFactory
from pydeequ.engines.constraints.protocols import ConstraintEvaluatorProtocol

__all__ = [
    # Protocols
    "ConstraintEvaluatorProtocol",
    # Base classes
    "BaseEvaluator",
    "RatioCheckEvaluator",
    "AnalyzerBasedEvaluator",
    # Batch evaluator
    "ConstraintBatchEvaluator",
    "SCAN_BASED_EVALUATORS",
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
    # Factory
    "ConstraintEvaluatorFactory",
]
