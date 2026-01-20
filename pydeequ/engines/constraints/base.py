# -*- coding: utf-8 -*-
"""
Base classes for constraint evaluators.

This module provides the abstract base classes that combine mixins
to create the foundation for concrete evaluator implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Callable, List, Optional

from pydeequ.engines.operators.mixins import SafeExtractMixin, WhereClauseMixin

if TYPE_CHECKING:
    import pandas as pd
    from pydeequ.v2.predicates import Predicate


class BaseEvaluator(WhereClauseMixin, SafeExtractMixin, ABC):
    """
    Base class for all constraint evaluators.

    Provides shared functionality for WHERE clause handling,
    assertion parsing, and predicate evaluation.

    Attributes:
        column: Optional column name for single-column constraints
        columns: List of column names for multi-column constraints
        where: Optional SQL WHERE clause for filtering
        assertion: Parsed predicate for evaluation
    """

    def __init__(self, constraint_proto):
        """
        Initialize evaluator from constraint protobuf.

        Args:
            constraint_proto: Protobuf message containing constraint definition
        """
        self.column = constraint_proto.column if constraint_proto.column else None
        self.columns = list(constraint_proto.columns) if constraint_proto.columns else []
        self.where = constraint_proto.where if constraint_proto.where else None
        self.assertion = self._parse_assertion(constraint_proto)
        self._constraint_type = constraint_proto.type

    @property
    def constraint_type(self) -> str:
        """Return the constraint type identifier."""
        return self._constraint_type

    def _parse_assertion(self, constraint_proto) -> Optional["Predicate"]:
        """
        Parse assertion predicate from constraint protobuf.

        Args:
            constraint_proto: Protobuf message containing constraint definition

        Returns:
            Parsed predicate or None if no assertion specified
        """
        from pydeequ.v2.proto import deequ_connect_pb2 as proto

        if not constraint_proto.HasField("assertion"):
            return None

        pred_msg = constraint_proto.assertion

        if pred_msg.operator == proto.PredicateMessage.Operator.BETWEEN:
            from pydeequ.v2.predicates import Between
            return Between(pred_msg.lower_bound, pred_msg.upper_bound)
        else:
            from pydeequ.v2.predicates import Comparison
            return Comparison(pred_msg.operator, pred_msg.value)

    def _evaluate_predicate(self, value: float, assertion: "Predicate") -> bool:
        """
        Evaluate a predicate against a value.

        Args:
            value: The value to check
            assertion: The predicate to evaluate

        Returns:
            True if the value satisfies the predicate
        """
        from pydeequ.v2.predicates import Between, Comparison
        from pydeequ.v2.proto import deequ_connect_pb2 as proto

        if isinstance(assertion, Comparison):
            op = assertion.operator
            target = assertion.value

            if op == proto.PredicateMessage.Operator.EQ:
                return abs(value - target) < 1e-9
            elif op == proto.PredicateMessage.Operator.NE:
                return abs(value - target) >= 1e-9
            elif op == proto.PredicateMessage.Operator.GT:
                return value > target
            elif op == proto.PredicateMessage.Operator.GE:
                return value >= target
            elif op == proto.PredicateMessage.Operator.LT:
                return value < target
            elif op == proto.PredicateMessage.Operator.LE:
                return value <= target

        elif isinstance(assertion, Between):
            return assertion.lower <= value <= assertion.upper

        return False

    def evaluate(self, value: Optional[float]) -> bool:
        """
        Evaluate whether the computed value satisfies the constraint.

        Args:
            value: The computed metric value

        Returns:
            True if the constraint is satisfied, False otherwise
        """
        if value is None:
            return False

        if self.assertion:
            return self._evaluate_predicate(value, self.assertion)

        # Default: value must equal 1.0 (for completeness-like constraints)
        return value == 1.0

    @abstractmethod
    def compute_value(
        self, table: str, execute_fn: Callable[[str], "pd.DataFrame"]
    ) -> Optional[float]:
        """
        Compute the metric value for this constraint.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            Computed metric value, or None if computation fails
        """
        raise NotImplementedError

    @abstractmethod
    def to_string(self) -> str:
        """
        Return a human-readable string representation of the constraint.

        Returns:
            Description of what the constraint checks
        """
        raise NotImplementedError


class RatioCheckEvaluator(BaseEvaluator):
    """
    Base class for constraints that compute matches/total ratio.

    These constraints check what fraction of rows satisfy some condition,
    such as isPositive, isNonNegative, isContainedIn, etc.
    """

    @abstractmethod
    def get_condition(self) -> str:
        """
        Get the SQL condition that defines a 'match'.

        Returns:
            SQL boolean expression for the match condition
        """
        raise NotImplementedError

    def compute_value(
        self, table: str, execute_fn: Callable[[str], "pd.DataFrame"]
    ) -> Optional[float]:
        """
        Compute the fraction of rows matching the condition.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            Ratio of matching rows to total rows
        """
        condition = self.get_condition()

        if self.where:
            query = f"""
                SELECT
                    SUM(CASE WHEN {self.where} THEN 1 ELSE 0 END) as total,
                    SUM(CASE WHEN ({self.where}) AND ({condition}) THEN 1 ELSE 0 END) as matches
                FROM {table}
            """
        else:
            query = f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) as matches
                FROM {table}
            """

        result = execute_fn(query)
        total = self.safe_float(result, "total") or 0
        matches = self.safe_float(result, "matches") or 0

        if total == 0:
            return 1.0
        return matches / total


class AnalyzerBasedEvaluator(BaseEvaluator):
    """
    Base class for constraints that delegate to an analyzer operator.

    These constraints compute their value by creating and running
    the corresponding analyzer operator.
    """

    @abstractmethod
    def get_operator(self):
        """
        Get the operator instance to compute the metric.

        Returns:
            Operator instance (ScanOperator or GroupingOperator)
        """
        raise NotImplementedError

    def compute_value(
        self, table: str, execute_fn: Callable[[str], "pd.DataFrame"]
    ) -> Optional[float]:
        """
        Compute the metric value using the analyzer operator.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            Computed metric value
        """
        operator = self.get_operator()

        # Check if it's a scan or grouping operator
        if hasattr(operator, "get_aggregations"):
            # Scan operator
            aggregations = operator.get_aggregations()
            query = f"SELECT {', '.join(aggregations)} FROM {table}"
            result = execute_fn(query)
            metric_result = operator.extract_result(result)
            return metric_result.value
        elif hasattr(operator, "build_query"):
            # Grouping operator
            query = operator.build_query(table)
            result = execute_fn(query)
            metric_result = operator.extract_result(result)
            return metric_result.value

        return None


__all__ = [
    "BaseEvaluator",
    "RatioCheckEvaluator",
    "AnalyzerBasedEvaluator",
]
