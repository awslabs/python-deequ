# -*- coding: utf-8 -*-
"""
Protocol definitions for constraint evaluators.

This module defines the structural typing contracts that all constraint
evaluators must satisfy.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pydeequ.v2.predicates import Predicate


@runtime_checkable
class ConstraintEvaluatorProtocol(Protocol):
    """
    Contract for constraint evaluators.

    Constraint evaluators compute values from data and evaluate
    assertions against those values to determine pass/fail status.
    """

    @property
    def constraint_type(self) -> str:
        """Return the constraint type identifier."""
        ...

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
        ...

    def evaluate(
        self, value: Optional[float], assertion: Optional["Predicate"] = None
    ) -> bool:
        """
        Evaluate whether the computed value satisfies the constraint.

        Args:
            value: The computed metric value
            assertion: Optional predicate to evaluate against

        Returns:
            True if the constraint is satisfied, False otherwise
        """
        ...

    def to_string(self) -> str:
        """
        Return a human-readable string representation of the constraint.

        Returns:
            Description of what the constraint checks
        """
        ...


__all__ = [
    "ConstraintEvaluatorProtocol",
]
