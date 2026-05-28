# -*- coding: utf-8 -*-
"""
Factory for creating constraint evaluators.

This module provides a registry-based factory pattern for creating
evaluator instances from constraint protobufs.
"""

from __future__ import annotations

from typing import Dict, Optional, Type

from pydeequ.engines.constraints.base import BaseEvaluator
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

class ConstraintEvaluatorFactory:
    """
    Factory for creating constraint evaluators from protobufs.

    This factory uses a registry pattern to map constraint type strings
    to their corresponding evaluator classes.
    """

    _registry: Dict[str, Type[BaseEvaluator]] = {
        # Analyzer-based evaluators
        "hasSize": SizeEvaluator,
        "isComplete": CompletenessEvaluator,
        "hasCompleteness": CompletenessEvaluator,
        "hasMean": MeanEvaluator,
        "hasMin": MinimumEvaluator,
        "hasMax": MaximumEvaluator,
        "hasSum": SumEvaluator,
        "hasStandardDeviation": StandardDeviationEvaluator,
        "isUnique": UniquenessEvaluator,
        "hasUniqueness": UniquenessEvaluator,
        "hasDistinctness": DistinctnessEvaluator,
        "hasUniqueValueRatio": UniqueValueRatioEvaluator,
        "hasCorrelation": CorrelationEvaluator,
        "hasEntropy": EntropyEvaluator,
        "hasMutualInformation": MutualInformationEvaluator,
        "hasPattern": PatternMatchEvaluator,
        "hasMinLength": MinLengthEvaluator,
        "hasMaxLength": MaxLengthEvaluator,
        "hasApproxCountDistinct": ApproxCountDistinctEvaluator,
        "hasApproxQuantile": ApproxQuantileEvaluator,
        "satisfies": ComplianceEvaluator,
        # Ratio-check evaluators
        "isPositive": IsPositiveEvaluator,
        "isNonNegative": IsNonNegativeEvaluator,
        "isContainedIn": IsContainedInEvaluator,
        "containsEmail": ContainsEmailEvaluator,
        "containsURL": ContainsURLEvaluator,
        "containsCreditCardNumber": ContainsCreditCardEvaluator,
        "containsSocialSecurityNumber": ContainsSSNEvaluator,
        # Comparison evaluators
        "isLessThan": ColumnComparisonEvaluator,
        "isLessThanOrEqualTo": ColumnComparisonEvaluator,
        "isGreaterThan": ColumnComparisonEvaluator,
        "isGreaterThanOrEqualTo": ColumnComparisonEvaluator,
        # Multi-column evaluators
        "areComplete": MultiColumnCompletenessEvaluator,
        "haveCompleteness": MultiColumnCompletenessEvaluator,
    }

    @classmethod
    def create(cls, constraint_proto) -> Optional[BaseEvaluator]:
        """
        Create an evaluator instance from a constraint protobuf.

        Args:
            constraint_proto: Protobuf message containing constraint definition

        Returns:
            Evaluator instance or None if constraint type not supported
        """
        evaluator_class = cls._registry.get(constraint_proto.type)
        if evaluator_class:
            return evaluator_class(constraint_proto)
        return None

    @classmethod
    def is_supported(cls, constraint_type: str) -> bool:
        """
        Check if a constraint type is supported by the factory.

        Args:
            constraint_type: The constraint type string to check

        Returns:
            True if the constraint type is supported
        """
        return constraint_type in cls._registry

    @classmethod
    def supported_types(cls) -> list:
        """
        Get list of all supported constraint types.

        Returns:
            List of supported constraint type strings
        """
        return list(cls._registry.keys())


__all__ = [
    "ConstraintEvaluatorFactory",
]
