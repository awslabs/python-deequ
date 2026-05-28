# -*- coding: utf-8 -*-
"""
Unit tests for constraint evaluators.

These tests verify the constraint evaluator abstractions work correctly
in isolation, testing SQL generation and evaluation logic.
"""

import pandas as pd
import pytest

from pydeequ.engines.constraints import (
    ConstraintEvaluatorFactory,
    BaseEvaluator,
    RatioCheckEvaluator,
    AnalyzerBasedEvaluator,
    # Analyzer-based evaluators
    SizeEvaluator,
    CompletenessEvaluator,
    MeanEvaluator,
    MinimumEvaluator,
    MaximumEvaluator,
    SumEvaluator,
    StandardDeviationEvaluator,
    UniquenessEvaluator,
    DistinctnessEvaluator,
    UniqueValueRatioEvaluator,
    CorrelationEvaluator,
    EntropyEvaluator,
    MutualInformationEvaluator,
    PatternMatchEvaluator,
    MinLengthEvaluator,
    MaxLengthEvaluator,
    ApproxCountDistinctEvaluator,
    ApproxQuantileEvaluator,
    ComplianceEvaluator,
    # Ratio-check evaluators
    IsPositiveEvaluator,
    IsNonNegativeEvaluator,
    IsContainedInEvaluator,
    ContainsEmailEvaluator,
    ContainsURLEvaluator,
    ContainsCreditCardEvaluator,
    ContainsSSNEvaluator,
    # Comparison evaluators
    ColumnComparisonEvaluator,
    # Multi-column evaluators
    MultiColumnCompletenessEvaluator,
)


class MockConstraintProto:
    """Mock constraint protobuf for testing."""

    def __init__(
        self,
        type: str = "test",
        column: str = "",
        columns: list = None,
        where: str = "",
        pattern: str = "",
        column_condition: str = "",
        constraint_name: str = "",
        allowed_values: list = None,
        quantile: float = 0.5,
        assertion=None,
    ):
        self.type = type
        self.column = column
        self.columns = columns or []
        self.where = where
        self.pattern = pattern
        self.column_condition = column_condition
        self.constraint_name = constraint_name
        self.allowed_values = allowed_values or []
        self.quantile = quantile
        self._assertion = assertion

    def HasField(self, field_name):
        if field_name == "assertion":
            return self._assertion is not None
        return False

    @property
    def assertion(self):
        return self._assertion


class TestConstraintEvaluatorFactory:
    """Tests for ConstraintEvaluatorFactory."""

    def test_create_size_evaluator(self):
        """Test creating SizeEvaluator."""
        proto = MockConstraintProto(type="hasSize")
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, SizeEvaluator)

    def test_create_completeness_evaluator(self):
        """Test creating CompletenessEvaluator for isComplete."""
        proto = MockConstraintProto(type="isComplete", column="col1")
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, CompletenessEvaluator)

    def test_create_completeness_evaluator_has(self):
        """Test creating CompletenessEvaluator for hasCompleteness."""
        proto = MockConstraintProto(type="hasCompleteness", column="col1")
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, CompletenessEvaluator)

    def test_create_is_positive_evaluator(self):
        """Test creating IsPositiveEvaluator."""
        proto = MockConstraintProto(type="isPositive", column="col1")
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, IsPositiveEvaluator)

    def test_create_is_contained_in_evaluator(self):
        """Test creating IsContainedInEvaluator."""
        proto = MockConstraintProto(
            type="isContainedIn",
            column="col1",
            allowed_values=["a", "b", "c"]
        )
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, IsContainedInEvaluator)

    def test_create_column_comparison_evaluator(self):
        """Test creating ColumnComparisonEvaluator for isLessThan."""
        proto = MockConstraintProto(type="isLessThan", columns=["col1", "col2"])
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, ColumnComparisonEvaluator)

    def test_create_multi_column_completeness_evaluator(self):
        """Test creating MultiColumnCompletenessEvaluator."""
        proto = MockConstraintProto(type="areComplete", columns=["col1", "col2"])
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is not None
        assert isinstance(evaluator, MultiColumnCompletenessEvaluator)

    def test_create_unknown_type_returns_none(self):
        """Test that unknown constraint types return None."""
        proto = MockConstraintProto(type="unknownType")
        evaluator = ConstraintEvaluatorFactory.create(proto)
        assert evaluator is None

    def test_is_supported(self):
        """Test is_supported method."""
        assert ConstraintEvaluatorFactory.is_supported("hasSize")
        assert ConstraintEvaluatorFactory.is_supported("isComplete")
        assert ConstraintEvaluatorFactory.is_supported("isPositive")
        assert not ConstraintEvaluatorFactory.is_supported("unknownType")

    def test_supported_types(self):
        """Test supported_types method returns all registered types."""
        types = ConstraintEvaluatorFactory.supported_types()
        assert "hasSize" in types
        assert "isComplete" in types
        assert "hasCompleteness" in types
        assert "isPositive" in types
        assert "isNonNegative" in types
        assert "isContainedIn" in types
        assert "isLessThan" in types
        assert "areComplete" in types


class TestRatioCheckEvaluators:
    """Tests for ratio-check evaluator condition generation."""

    def test_is_positive_condition(self):
        """Test IsPositiveEvaluator generates correct condition."""
        proto = MockConstraintProto(type="isPositive", column="price")
        evaluator = IsPositiveEvaluator(proto)
        assert evaluator.get_condition() == "price > 0"

    def test_is_non_negative_condition(self):
        """Test IsNonNegativeEvaluator generates correct condition."""
        proto = MockConstraintProto(type="isNonNegative", column="count")
        evaluator = IsNonNegativeEvaluator(proto)
        assert evaluator.get_condition() == "count >= 0"

    def test_is_contained_in_condition(self):
        """Test IsContainedInEvaluator generates correct condition."""
        proto = MockConstraintProto(
            type="isContainedIn",
            column="status",
            allowed_values=["active", "pending"]
        )
        evaluator = IsContainedInEvaluator(proto)
        condition = evaluator.get_condition()
        assert "status IN" in condition
        assert "'active'" in condition
        assert "'pending'" in condition

    def test_is_contained_in_escapes_quotes(self):
        """Test IsContainedInEvaluator properly escapes single quotes."""
        proto = MockConstraintProto(
            type="isContainedIn",
            column="name",
            allowed_values=["O'Brien", "D'Angelo"]
        )
        evaluator = IsContainedInEvaluator(proto)
        condition = evaluator.get_condition()
        assert "O''Brien" in condition
        assert "D''Angelo" in condition

    def test_contains_email_pattern(self):
        """Test ContainsEmailEvaluator uses email regex pattern."""
        proto = MockConstraintProto(type="containsEmail", column="email")
        evaluator = ContainsEmailEvaluator(proto)
        condition = evaluator.get_condition()
        assert "REGEXP_MATCHES" in condition
        assert "email" in condition

    def test_contains_url_pattern(self):
        """Test ContainsURLEvaluator uses URL regex pattern."""
        proto = MockConstraintProto(type="containsURL", column="website")
        evaluator = ContainsURLEvaluator(proto)
        condition = evaluator.get_condition()
        assert "REGEXP_MATCHES" in condition
        assert "website" in condition

    def test_column_comparison_less_than(self):
        """Test ColumnComparisonEvaluator for isLessThan."""
        proto = MockConstraintProto(type="isLessThan", columns=["col_a", "col_b"])
        evaluator = ColumnComparisonEvaluator(proto)
        assert evaluator.get_condition() == "col_a < col_b"

    def test_column_comparison_greater_than(self):
        """Test ColumnComparisonEvaluator for isGreaterThan."""
        proto = MockConstraintProto(type="isGreaterThan", columns=["col_a", "col_b"])
        evaluator = ColumnComparisonEvaluator(proto)
        assert evaluator.get_condition() == "col_a > col_b"

    def test_column_comparison_less_than_or_equal(self):
        """Test ColumnComparisonEvaluator for isLessThanOrEqualTo."""
        proto = MockConstraintProto(type="isLessThanOrEqualTo", columns=["col_a", "col_b"])
        evaluator = ColumnComparisonEvaluator(proto)
        assert evaluator.get_condition() == "col_a <= col_b"

    def test_column_comparison_greater_than_or_equal(self):
        """Test ColumnComparisonEvaluator for isGreaterThanOrEqualTo."""
        proto = MockConstraintProto(type="isGreaterThanOrEqualTo", columns=["col_a", "col_b"])
        evaluator = ColumnComparisonEvaluator(proto)
        assert evaluator.get_condition() == "col_a >= col_b"


class TestEvaluatorToString:
    """Tests for evaluator to_string methods."""

    def test_size_evaluator_to_string(self):
        """Test SizeEvaluator to_string."""
        proto = MockConstraintProto(type="hasSize")
        evaluator = SizeEvaluator(proto)
        assert "hasSize" in evaluator.to_string()

    def test_completeness_evaluator_to_string_is_complete(self):
        """Test CompletenessEvaluator to_string for isComplete."""
        proto = MockConstraintProto(type="isComplete", column="col1")
        evaluator = CompletenessEvaluator(proto)
        result = evaluator.to_string()
        assert "Complete" in result
        assert "col1" in result

    def test_is_positive_evaluator_to_string(self):
        """Test IsPositiveEvaluator to_string."""
        proto = MockConstraintProto(type="isPositive", column="price")
        evaluator = IsPositiveEvaluator(proto)
        result = evaluator.to_string()
        assert "isPositive" in result
        assert "price" in result

    def test_is_contained_in_evaluator_to_string(self):
        """Test IsContainedInEvaluator to_string."""
        proto = MockConstraintProto(
            type="isContainedIn",
            column="status",
            allowed_values=["a", "b"]
        )
        evaluator = IsContainedInEvaluator(proto)
        result = evaluator.to_string()
        assert "isContainedIn" in result
        assert "status" in result

    def test_column_comparison_to_string(self):
        """Test ColumnComparisonEvaluator to_string."""
        proto = MockConstraintProto(type="isLessThan", columns=["a", "b"])
        evaluator = ColumnComparisonEvaluator(proto)
        result = evaluator.to_string()
        assert "isLessThan" in result
        assert "a" in result
        assert "b" in result

    def test_multi_column_completeness_to_string(self):
        """Test MultiColumnCompletenessEvaluator to_string."""
        proto = MockConstraintProto(type="areComplete", columns=["col1", "col2"])
        evaluator = MultiColumnCompletenessEvaluator(proto)
        result = evaluator.to_string()
        assert "Complete" in result
        assert "col1" in result
        assert "col2" in result


class TestEvaluatorEvaluation:
    """Tests for evaluator evaluation logic."""

    def test_evaluate_none_value_returns_false(self):
        """Test that evaluating None value returns False."""
        proto = MockConstraintProto(type="hasSize")
        evaluator = SizeEvaluator(proto)
        assert evaluator.evaluate(None) is False

    def test_evaluate_1_0_without_assertion_returns_true(self):
        """Test that evaluating 1.0 without assertion returns True."""
        proto = MockConstraintProto(type="isComplete", column="col1")
        evaluator = CompletenessEvaluator(proto)
        assert evaluator.evaluate(1.0) is True

    def test_evaluate_less_than_1_without_assertion_returns_false(self):
        """Test that evaluating < 1.0 without assertion returns False."""
        proto = MockConstraintProto(type="isComplete", column="col1")
        evaluator = CompletenessEvaluator(proto)
        assert evaluator.evaluate(0.5) is False


class TestAnalyzerBasedEvaluators:
    """Tests for analyzer-based evaluator operator generation."""

    def test_completeness_evaluator_get_operator(self):
        """Test CompletenessEvaluator creates correct operator."""
        from pydeequ.engines.operators import CompletenessOperator

        proto = MockConstraintProto(type="isComplete", column="col1")
        evaluator = CompletenessEvaluator(proto)
        operator = evaluator.get_operator()
        assert isinstance(operator, CompletenessOperator)
        assert operator.column == "col1"

    def test_mean_evaluator_get_operator(self):
        """Test MeanEvaluator creates correct operator."""
        from pydeequ.engines.operators import MeanOperator

        proto = MockConstraintProto(type="hasMean", column="value")
        evaluator = MeanEvaluator(proto)
        operator = evaluator.get_operator()
        assert isinstance(operator, MeanOperator)
        assert operator.column == "value"

    def test_uniqueness_evaluator_get_operator(self):
        """Test UniquenessEvaluator creates correct operator."""
        from pydeequ.engines.operators import UniquenessOperator

        proto = MockConstraintProto(type="isUnique", column="id")
        evaluator = UniquenessEvaluator(proto)
        operator = evaluator.get_operator()
        assert isinstance(operator, UniquenessOperator)

    def test_pattern_match_evaluator_get_operator(self):
        """Test PatternMatchEvaluator creates correct operator."""
        from pydeequ.engines.operators import PatternMatchOperator

        proto = MockConstraintProto(type="hasPattern", column="email", pattern="^.*@.*$")
        evaluator = PatternMatchEvaluator(proto)
        operator = evaluator.get_operator()
        assert isinstance(operator, PatternMatchOperator)
        assert operator.column == "email"

    def test_approx_quantile_evaluator_get_operator(self):
        """Test ApproxQuantileEvaluator creates correct operator."""
        from pydeequ.engines.operators import ApproxQuantileOperator

        proto = MockConstraintProto(type="hasApproxQuantile", column="value", quantile=0.75)
        evaluator = ApproxQuantileEvaluator(proto)
        operator = evaluator.get_operator()
        assert isinstance(operator, ApproxQuantileOperator)
        assert operator.quantile == 0.75


class TestWhereClauseHandling:
    """Tests for WHERE clause handling in evaluators."""

    def test_ratio_evaluator_with_where_clause(self):
        """Test ratio evaluator includes WHERE in query."""
        proto = MockConstraintProto(type="isPositive", column="price", where="status='active'")
        evaluator = IsPositiveEvaluator(proto)
        assert evaluator.where == "status='active'"

    def test_analyzer_evaluator_with_where_clause(self):
        """Test analyzer evaluator passes WHERE to operator."""
        proto = MockConstraintProto(type="hasMean", column="value", where="status='active'")
        evaluator = MeanEvaluator(proto)
        operator = evaluator.get_operator()
        assert operator.where == "status='active'"


class TestSpecialConstraintTypes:
    """Tests for special constraint types with extra parameters."""

    def test_compliance_evaluator(self):
        """Test ComplianceEvaluator with column_condition."""
        proto = MockConstraintProto(
            type="satisfies",
            column_condition="price > 0 AND quantity > 0",
            constraint_name="valid_order"
        )
        evaluator = ComplianceEvaluator(proto)
        assert evaluator.predicate == "price > 0 AND quantity > 0"
        assert evaluator.name == "valid_order"
        result = evaluator.to_string()
        assert "satisfies" in result

    def test_correlation_evaluator_requires_two_columns(self):
        """Test CorrelationEvaluator handles missing columns."""
        proto = MockConstraintProto(type="hasCorrelation", columns=["col1"])
        evaluator = CorrelationEvaluator(proto)
        # Should return None for operator when not enough columns
        result = evaluator.compute_value("test_table", lambda q: pd.DataFrame())
        assert result is None

    def test_mutual_information_evaluator_requires_two_columns(self):
        """Test MutualInformationEvaluator handles missing columns."""
        proto = MockConstraintProto(type="hasMutualInformation", columns=["col1"])
        evaluator = MutualInformationEvaluator(proto)
        result = evaluator.compute_value("test_table", lambda q: pd.DataFrame())
        assert result is None


class TestAllConstraintTypesSupported:
    """Verify all constraint types have evaluators."""

    @pytest.mark.parametrize("constraint_type", [
        "hasSize",
        "isComplete",
        "hasCompleteness",
        "hasMean",
        "hasMin",
        "hasMax",
        "hasSum",
        "hasStandardDeviation",
        "isUnique",
        "hasUniqueness",
        "hasDistinctness",
        "hasUniqueValueRatio",
        "hasCorrelation",
        "hasEntropy",
        "hasMutualInformation",
        "hasPattern",
        "hasMinLength",
        "hasMaxLength",
        "hasApproxCountDistinct",
        "hasApproxQuantile",
        "satisfies",
        "isPositive",
        "isNonNegative",
        "isContainedIn",
        "containsEmail",
        "containsURL",
        "containsCreditCardNumber",
        "containsSocialSecurityNumber",
        "isLessThan",
        "isLessThanOrEqualTo",
        "isGreaterThan",
        "isGreaterThanOrEqualTo",
        "areComplete",
        "haveCompleteness",
    ])
    def test_constraint_type_has_evaluator(self, constraint_type):
        """Verify each constraint type maps to an evaluator."""
        assert ConstraintEvaluatorFactory.is_supported(constraint_type)
