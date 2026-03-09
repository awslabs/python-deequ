# -*- coding: utf-8 -*-
"""
Suggestion runner for executing rules against column profiles.

This module provides the SuggestionRunner class that orchestrates
running suggestion rules against column profiles.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, List, Optional

from pydeequ.engines.suggestions.registry import RuleRegistry
from pydeequ.engines.suggestions.rules import (
    HasMinLengthRule,
    HasMaxLengthRule,
    UniqueIfApproximatelyUniqueRule,
    STRING_TYPES,
)

if TYPE_CHECKING:
    import pandas as pd
    from pydeequ.engines import ColumnProfile, ConstraintSuggestion


class SuggestionRunner:
    """
    Runs suggestion rules against column profiles.

    The runner retrieves rules from the registry based on the specified
    rule sets and executes them against each column profile.

    Attributes:
        rule_sets: List of rule set names to apply
    """

    def __init__(self, rule_sets: Optional[List[str]] = None):
        """
        Initialize SuggestionRunner.

        Args:
            rule_sets: List of rule set names (e.g., ["DEFAULT", "NUMERICAL"]).
                      If None, defaults to ["DEFAULT"].
        """
        self.rule_sets = rule_sets or ["DEFAULT"]

    def run(
        self,
        profiles: List["ColumnProfile"],
        execute_fn: Optional[Callable[[str], "pd.DataFrame"]] = None,
        table: Optional[str] = None,
        row_count: Optional[int] = None,
    ) -> List["ConstraintSuggestion"]:
        """
        Run suggestion rules against column profiles.

        Args:
            profiles: List of column profiles to analyze
            execute_fn: Optional function to execute SQL queries (for rules
                       that need additional data like string lengths)
            table: Optional table name for queries
            row_count: Optional total row count for uniqueness checks

        Returns:
            List of constraint suggestions
        """
        rules = RuleRegistry.get_rules_for_sets(self.rule_sets)
        suggestions: List["ConstraintSuggestion"] = []

        for profile in profiles:
            for rule in rules:
                suggestion = self._apply_rule(
                    rule, profile, execute_fn, table, row_count
                )
                if suggestion:
                    suggestions.append(suggestion)

        return suggestions

    def _apply_rule(
        self,
        rule,
        profile: "ColumnProfile",
        execute_fn: Optional[Callable[[str], "pd.DataFrame"]],
        table: Optional[str],
        row_count: Optional[int],
    ) -> Optional["ConstraintSuggestion"]:
        """
        Apply a single rule to a profile.

        Some rules require special handling (e.g., string length rules need
        to query the database, uniqueness rules need row count).

        Args:
            rule: The rule to apply
            profile: Column profile to analyze
            execute_fn: Optional SQL execution function
            table: Optional table name
            row_count: Optional row count

        Returns:
            Constraint suggestion or None
        """
        # Handle HasMinLengthRule - needs string length from query
        if isinstance(rule, HasMinLengthRule):
            return self._handle_string_length_rule(
                rule, profile, execute_fn, table, is_min=True
            )

        # Handle HasMaxLengthRule - needs string length from query
        if isinstance(rule, HasMaxLengthRule):
            return self._handle_string_length_rule(
                rule, profile, execute_fn, table, is_min=False
            )

        # Handle UniqueIfApproximatelyUniqueRule - needs row count
        if isinstance(rule, UniqueIfApproximatelyUniqueRule):
            if rule.applies_to(profile):
                return rule.generate(profile, row_count=row_count)
            return None

        # Standard rule handling
        if rule.applies_to(profile):
            return rule.generate(profile)
        return None

    def _handle_string_length_rule(
        self,
        rule,
        profile: "ColumnProfile",
        execute_fn: Optional[Callable[[str], "pd.DataFrame"]],
        table: Optional[str],
        is_min: bool,
    ) -> Optional["ConstraintSuggestion"]:
        """
        Handle string length rules that need database queries.

        Args:
            rule: HasMinLengthRule or HasMaxLengthRule
            profile: Column profile
            execute_fn: SQL execution function
            table: Table name
            is_min: True for min length, False for max length

        Returns:
            Constraint suggestion or None
        """
        import pandas as pd

        if not rule.applies_to(profile):
            return None

        if execute_fn is None or table is None:
            return None

        col = profile.column
        agg_func = "MIN" if is_min else "MAX"
        query = f"SELECT {agg_func}(LENGTH({col})) as len FROM {table} WHERE {col} IS NOT NULL"

        try:
            result = execute_fn(query)
            length = result["len"].iloc[0]
            if length is not None and not pd.isna(length):
                length = int(length)
                if length > 0:
                    if is_min:
                        return rule.generate(profile, min_length=length)
                    else:
                        return rule.generate(profile, max_length=length)
        except Exception:
            pass

        return None


__all__ = [
    "SuggestionRunner",
]
