# -*- coding: utf-8 -*-
"""
Suggestion rule implementations.

This module provides the base class and implementations for constraint
suggestion rules. Each rule analyzes column profiles and generates
appropriate constraint suggestions.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, Set

if TYPE_CHECKING:
    from pydeequ.engines import ColumnProfile, ConstraintSuggestion


# SQL types that are considered string
STRING_TYPES: Set[str] = {"VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING"}


class SuggestionRule(ABC):
    """
    Base class for constraint suggestion rules.

    Each rule examines column profiles and generates appropriate
    constraint suggestions based on data characteristics.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Rule name for identification."""
        pass

    @property
    @abstractmethod
    def rule_sets(self) -> List[str]:
        """Which rule sets this rule belongs to (DEFAULT, NUMERICAL, etc)."""
        pass

    @abstractmethod
    def applies_to(self, profile: "ColumnProfile") -> bool:
        """Whether this rule applies to the given column profile."""
        pass

    @abstractmethod
    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        """Generate a suggestion if applicable, or None."""
        pass


class CompleteIfCompleteRule(SuggestionRule):
    """Suggests isComplete() constraint for fully complete columns."""

    @property
    def name(self) -> str:
        return "CompleteIfComplete"

    @property
    def rule_sets(self) -> List[str]:
        return ["DEFAULT", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.completeness == 1.0

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="Completeness",
            current_value="1.0",
            description=f"'{profile.column}' is complete",
            suggesting_rule=self.name,
            code_for_constraint=f'.isComplete("{profile.column}")',
        )


class RetainCompletenessRule(SuggestionRule):
    """Suggests hasCompleteness() constraint for highly complete columns."""

    THRESHOLD = 0.9  # Minimum completeness to suggest retaining

    @property
    def name(self) -> str:
        return "RetainCompleteness"

    @property
    def rule_sets(self) -> List[str]:
        return ["DEFAULT", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        # Apply only if not fully complete but >= threshold
        return self.THRESHOLD <= profile.completeness < 1.0

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="Completeness",
            current_value=f"{profile.completeness:.4f}",
            description=f"'{profile.column}' has completeness {profile.completeness:.2%}",
            suggesting_rule=self.name,
            code_for_constraint=f'.hasCompleteness("{profile.column}", gte({profile.completeness:.2f}))',
        )


class NonNegativeNumbersRule(SuggestionRule):
    """Suggests isNonNegative() constraint for columns with no negative values."""

    @property
    def name(self) -> str:
        return "NonNegativeNumbers"

    @property
    def rule_sets(self) -> List[str]:
        return ["DEFAULT", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.minimum is not None and profile.minimum >= 0

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="NonNegative",
            current_value=f"{profile.minimum:.2f}",
            description=f"'{profile.column}' has no negative values",
            suggesting_rule=self.name,
            code_for_constraint=f'.isNonNegative("{profile.column}")',
        )


class CategoricalRangeRule(SuggestionRule):
    """Suggests isContainedIn() constraint for low cardinality categorical columns."""

    MAX_CATEGORIES = 10  # Maximum distinct values to suggest containment

    @property
    def name(self) -> str:
        return "CategoricalRangeRule"

    @property
    def rule_sets(self) -> List[str]:
        return ["DEFAULT", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        if not profile.histogram:
            return False
        hist = json.loads(profile.histogram)
        return len(hist) <= self.MAX_CATEGORIES

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        hist = json.loads(profile.histogram)
        values = list(hist.keys())
        values_str = ", ".join([f'"{v}"' for v in values])

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="Compliance",
            current_value=f"{len(values)} distinct values",
            description=f"'{profile.column}' has categorical values",
            suggesting_rule=self.name,
            code_for_constraint=f'.isContainedIn("{profile.column}", [{values_str}])',
        )


class HasMinRule(SuggestionRule):
    """Suggests hasMin() constraint for numeric columns."""

    @property
    def name(self) -> str:
        return "HasMin"

    @property
    def rule_sets(self) -> List[str]:
        return ["NUMERICAL", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.minimum is not None and profile.mean is not None

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="Minimum",
            current_value=f"{profile.minimum:.2f}",
            description=f"'{profile.column}' has minimum {profile.minimum:.2f}",
            suggesting_rule=self.name,
            code_for_constraint=f'.hasMin("{profile.column}", gte({profile.minimum:.2f}))',
        )


class HasMaxRule(SuggestionRule):
    """Suggests hasMax() constraint for numeric columns."""

    @property
    def name(self) -> str:
        return "HasMax"

    @property
    def rule_sets(self) -> List[str]:
        return ["NUMERICAL", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.maximum is not None and profile.mean is not None

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="Maximum",
            current_value=f"{profile.maximum:.2f}",
            description=f"'{profile.column}' has maximum {profile.maximum:.2f}",
            suggesting_rule=self.name,
            code_for_constraint=f'.hasMax("{profile.column}", lte({profile.maximum:.2f}))',
        )


class HasMeanRule(SuggestionRule):
    """Suggests hasMean() constraint for numeric columns."""

    @property
    def name(self) -> str:
        return "HasMean"

    @property
    def rule_sets(self) -> List[str]:
        return ["NUMERICAL", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.mean is not None

    def generate(self, profile: "ColumnProfile") -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        lower = profile.mean * 0.9
        upper = profile.mean * 1.1

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="Mean",
            current_value=f"{profile.mean:.2f}",
            description=f"'{profile.column}' has mean {profile.mean:.2f}",
            suggesting_rule=self.name,
            code_for_constraint=f'.hasMean("{profile.column}", between({lower:.2f}, {upper:.2f}))',
        )


class HasMinLengthRule(SuggestionRule):
    """Suggests hasMinLength() constraint for string columns."""

    @property
    def name(self) -> str:
        return "HasMinLength"

    @property
    def rule_sets(self) -> List[str]:
        return ["STRING", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.data_type in STRING_TYPES

    def generate(
        self,
        profile: "ColumnProfile",
        min_length: Optional[int] = None,
    ) -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        if min_length is None or min_length <= 0:
            return None

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="MinLength",
            current_value=str(min_length),
            description=f"'{profile.column}' has minimum length {min_length}",
            suggesting_rule=self.name,
            code_for_constraint=f'.hasMinLength("{profile.column}", gte({min_length}))',
        )


class HasMaxLengthRule(SuggestionRule):
    """Suggests hasMaxLength() constraint for string columns."""

    @property
    def name(self) -> str:
        return "HasMaxLength"

    @property
    def rule_sets(self) -> List[str]:
        return ["STRING", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        return profile.data_type in STRING_TYPES

    def generate(
        self,
        profile: "ColumnProfile",
        max_length: Optional[int] = None,
    ) -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        if max_length is None or max_length <= 0:
            return None

        return ConstraintSuggestion(
            column_name=profile.column,
            constraint_name="MaxLength",
            current_value=str(max_length),
            description=f"'{profile.column}' has maximum length {max_length}",
            suggesting_rule=self.name,
            code_for_constraint=f'.hasMaxLength("{profile.column}", lte({max_length}))',
        )


class UniqueIfApproximatelyUniqueRule(SuggestionRule):
    """Suggests isUnique() constraint for approximately unique columns."""

    UNIQUENESS_THRESHOLD = 0.99  # Minimum distinct ratio to consider unique

    @property
    def name(self) -> str:
        return "UniqueIfApproximatelyUnique"

    @property
    def rule_sets(self) -> List[str]:
        return ["COMMON", "EXTENDED"]

    def applies_to(self, profile: "ColumnProfile") -> bool:
        # Need total row count to determine uniqueness
        return True  # Check is done in generate with row_count

    def generate(
        self,
        profile: "ColumnProfile",
        row_count: Optional[int] = None,
    ) -> Optional["ConstraintSuggestion"]:
        from pydeequ.engines import ConstraintSuggestion

        if row_count is None or row_count <= 0:
            return None

        if profile.approx_distinct_values >= row_count * self.UNIQUENESS_THRESHOLD:
            return ConstraintSuggestion(
                column_name=profile.column,
                constraint_name="Uniqueness",
                current_value="~1.0",
                description=f"'{profile.column}' appears to be unique",
                suggesting_rule=self.name,
                code_for_constraint=f'.isUnique("{profile.column}")',
            )
        return None


# Export all rule classes
__all__ = [
    "SuggestionRule",
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
    "STRING_TYPES",
]
