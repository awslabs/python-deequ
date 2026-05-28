# -*- coding: utf-8 -*-
"""
Constraint suggestion module.

This module provides a modular, rule-based system for suggesting data quality
constraints based on column profiles.

Architecture:
    rules.py      - SuggestionRule base class and 10 rule implementations
    registry.py   - RuleRegistry for organizing rules by rule set
    runner.py     - SuggestionRunner for orchestrating rule execution

Available Rule Sets:
    - DEFAULT: Basic rules (completeness, non-negative, categorical)
    - NUMERICAL: Rules for numeric columns (min, max, mean)
    - STRING: Rules for string columns (min/max length)
    - COMMON: General rules (uniqueness)
    - EXTENDED: All rules combined

Example usage:
    from pydeequ.engines.suggestions import SuggestionRunner

    # Run default rules
    runner = SuggestionRunner(rule_sets=["DEFAULT"])
    suggestions = runner.run(profiles, execute_fn=engine._execute_query, table="my_table")

    # Run multiple rule sets
    runner = SuggestionRunner(rule_sets=["DEFAULT", "NUMERICAL", "STRING"])
    suggestions = runner.run(profiles, execute_fn=engine._execute_query, table="my_table")
"""

from pydeequ.engines.suggestions.registry import RuleRegistry
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
from pydeequ.engines.suggestions.runner import SuggestionRunner


__all__ = [
    # Registry
    "RuleRegistry",
    # Runner
    "SuggestionRunner",
    # Base class
    "SuggestionRule",
    # Rules
    "CompleteIfCompleteRule",
    "RetainCompletenessRule",
    "NonNegativeNumbersRule",
    "CategoricalRangeRule",
    "HasMinRule",
    "HasMaxRule",
    "HasMeanRule",
    "HasMinLengthRule",
    "HasMaxLengthRule",
    "UniqueIfApproximatelyUniqueRule",
]
