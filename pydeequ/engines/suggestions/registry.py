# -*- coding: utf-8 -*-
"""
Suggestion rule registry.

This module provides a registry for suggestion rules, allowing rules to be
organized by rule sets (DEFAULT, NUMERICAL, STRING, COMMON, EXTENDED).
"""

from __future__ import annotations

from typing import List

from pydeequ.engines.suggestions.rules import (
    SuggestionRule,
    CompleteIfCompleteRule,
    RetainCompletenessRule,
    NonNegativeNumbersRule,
    CategoricalRangeRule,
    HasMinRule,
    HasMaxRule,
    HasMeanRule,
    HasMinLengthRule,
    HasMaxLengthRule,
    UniqueIfApproximatelyUniqueRule,
)


class RuleRegistry:
    """
    Registry of suggestion rules by rule set.

    Provides centralized management of suggestion rules and retrieval
    by rule set names.
    """

    _rules: List[SuggestionRule] = []

    @classmethod
    def register(cls, rule: SuggestionRule) -> None:
        """
        Register a suggestion rule.

        Args:
            rule: SuggestionRule instance to register
        """
        cls._rules.append(rule)

    @classmethod
    def get_rules_for_sets(cls, rule_sets: List[str]) -> List[SuggestionRule]:
        """
        Get all rules that belong to any of the specified rule sets.

        Args:
            rule_sets: List of rule set names (e.g., ["DEFAULT", "NUMERICAL"])

        Returns:
            List of rules that belong to any of the specified sets
        """
        return [r for r in cls._rules if any(s in r.rule_sets for s in rule_sets)]

    @classmethod
    def get_all_rules(cls) -> List[SuggestionRule]:
        """
        Get all registered rules.

        Returns:
            List of all registered rules
        """
        return cls._rules.copy()

    @classmethod
    def clear(cls) -> None:
        """Clear all registered rules (mainly for testing)."""
        cls._rules = []


# Auto-register all default rules
def _register_default_rules() -> None:
    """Register all built-in suggestion rules."""
    RuleRegistry.register(CompleteIfCompleteRule())
    RuleRegistry.register(RetainCompletenessRule())
    RuleRegistry.register(NonNegativeNumbersRule())
    RuleRegistry.register(CategoricalRangeRule())
    RuleRegistry.register(HasMinRule())
    RuleRegistry.register(HasMaxRule())
    RuleRegistry.register(HasMeanRule())
    RuleRegistry.register(HasMinLengthRule())
    RuleRegistry.register(HasMaxLengthRule())
    RuleRegistry.register(UniqueIfApproximatelyUniqueRule())


# Register rules on module load
_register_default_rules()


__all__ = [
    "RuleRegistry",
]
