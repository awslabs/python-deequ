# -*- coding: utf-8 -*-
"""
Constraint batch evaluation for DuckDB performance optimization.

This module provides functionality to batch constraint evaluations that can
share SQL queries, reducing the number of queries executed.

Key optimizations:
1. Scan-based constraints (Size, Mean, Completeness, etc.) can be batched
   when they use scan operators with compatible aggregations.
2. Ratio-check constraints (isPositive, isNonNegative, isContainedIn, etc.)
   can be batched into a single query when they operate on the same table.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple, Type

from pydeequ.engines.constraints.base import (
    AnalyzerBasedEvaluator,
    BaseEvaluator,
    RatioCheckEvaluator,
)
from pydeequ.engines.constraints.evaluators import (
    CompletenessEvaluator,
    MaximumEvaluator,
    MeanEvaluator,
    MinimumEvaluator,
    SizeEvaluator,
    StandardDeviationEvaluator,
    SumEvaluator,
)

if TYPE_CHECKING:
    import pandas as pd


# Evaluators that use scan operators (can be batched via aggregations)
SCAN_BASED_EVALUATORS: Tuple[Type[AnalyzerBasedEvaluator], ...] = (
    SizeEvaluator,
    CompletenessEvaluator,
    MeanEvaluator,
    MinimumEvaluator,
    MaximumEvaluator,
    SumEvaluator,
    StandardDeviationEvaluator,
)


class ConstraintBatchEvaluator:
    """
    Batches constraint evaluations to minimize SQL queries.

    This class groups constraints by their evaluation pattern and executes
    them in batches where possible:
    - Scan-based evaluators: batched into single aggregation queries
    - Ratio-check evaluators: batched into single ratio queries
    - Other evaluators: executed individually
    """

    def __init__(self, evaluators: List[BaseEvaluator]):
        """
        Initialize the batch evaluator.

        Args:
            evaluators: List of constraint evaluators
        """
        self.evaluators = evaluators
        self._scan_based: List[AnalyzerBasedEvaluator] = []
        self._ratio_checks: List[RatioCheckEvaluator] = []
        self._other: List[BaseEvaluator] = []
        self._analyze()

    def _analyze(self) -> None:
        """Categorize evaluators by type for batching."""
        for evaluator in self.evaluators:
            if isinstance(evaluator, SCAN_BASED_EVALUATORS):
                self._scan_based.append(evaluator)
            elif isinstance(evaluator, RatioCheckEvaluator):
                self._ratio_checks.append(evaluator)
            else:
                self._other.append(evaluator)

    def get_batch_info(self) -> Dict[str, int]:
        """Return batch grouping information for debugging."""
        return {
            "scan_based": len(self._scan_based),
            "ratio_checks": len(self._ratio_checks),
            "other": len(self._other),
        }

    def execute(
        self,
        table: str,
        execute_fn: Callable[[str], "pd.DataFrame"],
    ) -> Dict[BaseEvaluator, Optional[float]]:
        """
        Execute all evaluators with batching optimization.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            Dictionary mapping evaluators to their computed values
        """
        results: Dict[BaseEvaluator, Optional[float]] = {}

        # Batch scan-based evaluators
        if self._scan_based:
            scan_results = self._execute_scan_batch(table, execute_fn)
            results.update(scan_results)

        # Batch ratio-check evaluators
        if self._ratio_checks:
            ratio_results = self._execute_ratio_batch(table, execute_fn)
            results.update(ratio_results)

        # Execute other evaluators individually
        for evaluator in self._other:
            try:
                value = evaluator.compute_value(table, execute_fn)
                results[evaluator] = value
            except Exception:
                results[evaluator] = None

        return results

    def _execute_scan_batch(
        self,
        table: str,
        execute_fn: Callable[[str], "pd.DataFrame"],
    ) -> Dict[BaseEvaluator, Optional[float]]:
        """
        Execute scan-based evaluators in a single batched query.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            Dictionary mapping evaluators to their computed values
        """
        results: Dict[BaseEvaluator, Optional[float]] = {}

        # Collect all aggregations from scan operators
        operators = []
        operator_to_evaluator = {}

        for evaluator in self._scan_based:
            operator = evaluator.get_operator()
            if operator and hasattr(operator, "get_aggregations"):
                operators.append(operator)
                operator_to_evaluator[id(operator)] = evaluator

        if not operators:
            # Fall back to individual execution
            for evaluator in self._scan_based:
                try:
                    value = evaluator.compute_value(table, execute_fn)
                    results[evaluator] = value
                except Exception:
                    results[evaluator] = None
            return results

        # Build batched query
        aggregations = []
        for operator in operators:
            aggregations.extend(operator.get_aggregations())

        query = f"SELECT {', '.join(aggregations)} FROM {table}"

        try:
            df = execute_fn(query)

            # Extract results for each operator
            for operator in operators:
                evaluator = operator_to_evaluator[id(operator)]
                try:
                    metric_result = operator.extract_result(df)
                    results[evaluator] = metric_result.value
                except Exception:
                    results[evaluator] = None

        except Exception:
            # Fall back to individual execution on batch failure
            for evaluator in self._scan_based:
                try:
                    value = evaluator.compute_value(table, execute_fn)
                    results[evaluator] = value
                except Exception:
                    results[evaluator] = None

        return results

    def _execute_ratio_batch(
        self,
        table: str,
        execute_fn: Callable[[str], "pd.DataFrame"],
    ) -> Dict[BaseEvaluator, Optional[float]]:
        """
        Execute ratio-check evaluators in a single batched query.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame

        Returns:
            Dictionary mapping evaluators to their computed values
        """
        results: Dict[BaseEvaluator, Optional[float]] = {}

        # Group evaluators by WHERE clause for proper batching
        where_groups: Dict[Optional[str], List[RatioCheckEvaluator]] = {}
        for evaluator in self._ratio_checks:
            where = getattr(evaluator, "where", None)
            if where not in where_groups:
                where_groups[where] = []
            where_groups[where].append(evaluator)

        # Execute each where-group as a batch
        for where, group_evaluators in where_groups.items():
            try:
                group_results = self._execute_ratio_group(
                    table, execute_fn, group_evaluators, where
                )
                results.update(group_results)
            except Exception:
                # Fall back to individual execution
                for evaluator in group_evaluators:
                    try:
                        value = evaluator.compute_value(table, execute_fn)
                        results[evaluator] = value
                    except Exception:
                        results[evaluator] = None

        return results

    def _execute_ratio_group(
        self,
        table: str,
        execute_fn: Callable[[str], "pd.DataFrame"],
        evaluators: List[RatioCheckEvaluator],
        where: Optional[str],
    ) -> Dict[BaseEvaluator, Optional[float]]:
        """
        Execute a group of ratio-check evaluators with the same WHERE clause.

        Args:
            table: Name of the table to query
            execute_fn: Function to execute SQL and return DataFrame
            evaluators: List of ratio-check evaluators
            where: WHERE clause (None if no filter)

        Returns:
            Dictionary mapping evaluators to their computed values
        """
        results: Dict[BaseEvaluator, Optional[float]] = {}

        # Build batched ratio query
        cases = []
        for i, evaluator in enumerate(evaluators):
            condition = evaluator.get_condition()
            cases.append(f"SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) as matches_{i}")

        # Add total count
        if where:
            query = f"""
                SELECT
                    SUM(CASE WHEN {where} THEN 1 ELSE 0 END) as total,
                    {', '.join([f"SUM(CASE WHEN ({where}) AND ({evaluators[i].get_condition()}) THEN 1 ELSE 0 END) as matches_{i}" for i in range(len(evaluators))])}
                FROM {table}
            """
        else:
            query = f"""
                SELECT
                    COUNT(*) as total,
                    {', '.join(cases)}
                FROM {table}
            """

        df = execute_fn(query)
        total = float(df["total"].iloc[0]) if df["total"].iloc[0] else 0

        for i, evaluator in enumerate(evaluators):
            matches = float(df[f"matches_{i}"].iloc[0]) if df[f"matches_{i}"].iloc[0] else 0
            if total == 0:
                results[evaluator] = 1.0
            else:
                results[evaluator] = matches / total

        return results


__all__ = [
    "ConstraintBatchEvaluator",
    "SCAN_BASED_EVALUATORS",
]
