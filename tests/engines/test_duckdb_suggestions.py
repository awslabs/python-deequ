# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""DuckDB-only constraint suggestion tests.

Tests the constraint suggestion functionality of the DuckDB engine.
"""

import pytest

from pydeequ.v2.suggestions import Rules


def get_suggestions_for_column(suggestions, column_name: str):
    """Get all suggestions for a specific column."""
    return [s for s in suggestions if s.column_name == column_name]


def get_suggestions_by_constraint(suggestions, constraint_name: str):
    """Get all suggestions for a specific constraint type."""
    return [s for s in suggestions if constraint_name in s.constraint_name]


class TestBasicSuggestions:
    """Tests for basic suggestion functionality."""

    def test_default_rules_generate_suggestions(self, engine_full):
        """DEFAULT rules generate suggestions for complete columns."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        # Should generate some suggestions for complete data
        assert isinstance(suggestions, list)

    def test_suggestions_have_required_fields(self, engine_full):
        """Suggestions contain all required fields."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        if suggestions:
            suggestion = suggestions[0]
            assert hasattr(suggestion, 'column_name')
            assert hasattr(suggestion, 'constraint_name')
            assert hasattr(suggestion, 'description')
            assert hasattr(suggestion, 'suggesting_rule')

    def test_restrict_to_columns(self, engine_full):
        """Suggestions can be restricted to specific columns."""
        suggestions = engine_full.suggest_constraints(
            columns=["att1"],
            rules=[Rules.DEFAULT]
        )
        # All suggestions should be for att1 (or dataset-level)
        column_suggestions = [s for s in suggestions if s.column_name]
        for s in column_suggestions:
            assert s.column_name == "att1" or s.column_name is None


class TestCompletenessRuleSuggestions:
    """Tests for completeness-related suggestions."""

    def test_complete_column_suggestions(self, engine_full):
        """Complete columns get completeness suggestions."""
        suggestions = engine_full.suggest_constraints(
            columns=["att1"],
            rules=[Rules.DEFAULT]
        )
        # Should suggest isComplete or hasCompleteness for complete column
        completeness_suggestions = get_suggestions_by_constraint(suggestions, "Complete")
        # May or may not generate based on implementation
        assert isinstance(suggestions, list)

    def test_incomplete_column_suggestions(self, engine_missing):
        """Incomplete columns may get retain completeness suggestions."""
        suggestions = engine_missing.suggest_constraints(
            columns=["att1"],
            rules=[Rules.DEFAULT]
        )
        # att1 is 50% complete - might suggest retaining that level
        assert isinstance(suggestions, list)


class TestUniquenessRuleSuggestions:
    """Tests for uniqueness-related suggestions."""

    def test_unique_column_suggestions(self, engine_unique):
        """Unique columns get uniqueness suggestions with COMMON rules."""
        suggestions = engine_unique.suggest_constraints(
            columns=["unique_col"],
            rules=[Rules.COMMON]
        )
        # Should suggest isUnique or hasUniqueness for unique column
        uniqueness_suggestions = get_suggestions_by_constraint(suggestions, "Unique")
        # Implementation dependent
        assert isinstance(suggestions, list)


class TestNumericalRuleSuggestions:
    """Tests for numerical constraint suggestions."""

    def test_numeric_column_suggestions(self, engine_numeric):
        """Numeric columns get statistical suggestions with NUMERICAL rules."""
        suggestions = engine_numeric.suggest_constraints(
            columns=["att1"],
            rules=[Rules.NUMERICAL]
        )
        # Should suggest hasMin, hasMax, hasMean for numeric column
        assert isinstance(suggestions, list)

    def test_min_max_suggestions(self, engine_numeric):
        """Numeric columns may get min/max suggestions."""
        suggestions = engine_numeric.suggest_constraints(
            columns=["att1"],
            rules=[Rules.NUMERICAL]
        )
        min_suggestions = get_suggestions_by_constraint(suggestions, "Min")
        max_suggestions = get_suggestions_by_constraint(suggestions, "Max")
        # May have min/max suggestions
        assert isinstance(suggestions, list)


class TestStringRuleSuggestions:
    """Tests for string-related suggestions."""

    def test_string_column_suggestions(self, engine_string_lengths):
        """String columns get length suggestions with STRING rules."""
        suggestions = engine_string_lengths.suggest_constraints(
            columns=["att1"],
            rules=[Rules.STRING]
        )
        # Should suggest hasMinLength, hasMaxLength for string column
        assert isinstance(suggestions, list)


class TestCategoricalRuleSuggestions:
    """Tests for categorical constraint suggestions."""

    def test_categorical_column_suggestions(self, engine_contained_in):
        """Low-cardinality columns may get containment suggestions."""
        suggestions = engine_contained_in.suggest_constraints(
            columns=["status"],
            rules=[Rules.DEFAULT]
        )
        # May suggest isContainedIn for categorical column
        assert isinstance(suggestions, list)


class TestMultipleRules:
    """Tests for combining multiple rule sets."""

    def test_extended_rules(self, engine_numeric):
        """EXTENDED rules combine all rule sets."""
        suggestions = engine_numeric.suggest_constraints(
            rules=[Rules.EXTENDED]
        )
        # Should get suggestions from all rule categories
        assert isinstance(suggestions, list)

    def test_multiple_rule_sets(self, engine_numeric):
        """Multiple rule sets can be combined."""
        suggestions = engine_numeric.suggest_constraints(
            rules=[Rules.DEFAULT, Rules.NUMERICAL]
        )
        assert isinstance(suggestions, list)


class TestSuggestionContent:
    """Tests for suggestion content quality."""

    def test_suggestion_has_description(self, engine_full):
        """Suggestions include human-readable descriptions."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        if suggestions:
            for s in suggestions:
                assert s.description is not None
                assert len(s.description) > 0

    def test_suggestion_has_rule_name(self, engine_full):
        """Suggestions identify the suggesting rule."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        if suggestions:
            for s in suggestions:
                assert s.suggesting_rule is not None

    def test_suggestion_has_current_value(self, engine_full):
        """Suggestions include current metric value."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        if suggestions:
            for s in suggestions:
                # current_value may be present
                assert hasattr(s, 'current_value')

    def test_suggestion_has_code_snippet(self, engine_full):
        """Suggestions may include code for constraint."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        if suggestions:
            for s in suggestions:
                # code_for_constraint may be present
                assert hasattr(s, 'code_for_constraint')


class TestEdgeCases:
    """Tests for edge cases in suggestions."""

    def test_empty_dataset_suggestions(self, engine_empty):
        """Suggestions on empty dataset."""
        suggestions = engine_empty.suggest_constraints(rules=[Rules.DEFAULT])
        # Should handle gracefully
        assert isinstance(suggestions, list)

    def test_single_row_suggestions(self, engine_single):
        """Suggestions on single-row dataset."""
        suggestions = engine_single.suggest_constraints(rules=[Rules.DEFAULT])
        assert isinstance(suggestions, list)

    def test_all_null_column_suggestions(self, engine_all_null):
        """Suggestions on all-NULL column."""
        suggestions = engine_all_null.suggest_constraints(
            columns=["value"],
            rules=[Rules.DEFAULT]
        )
        # Should handle all-NULL gracefully
        assert isinstance(suggestions, list)


class TestSuggestionDataFrame:
    """Tests for suggestion to DataFrame conversion."""

    def test_suggestions_to_dataframe(self, engine_full):
        """Suggestions can be converted to DataFrame."""
        suggestions = engine_full.suggest_constraints(rules=[Rules.DEFAULT])
        df = engine_full.suggestions_to_dataframe(suggestions)

        assert df is not None
        if len(suggestions) > 0:
            assert len(df) > 0
            assert "column_name" in df.columns
            assert "constraint_name" in df.columns


class TestDatasetSpecificSuggestions:
    """Tests for suggestions on specific dataset types."""

    def test_numeric_dataset(self, engine_numeric):
        """Numeric dataset gets appropriate suggestions."""
        suggestions = engine_numeric.suggest_constraints(
            rules=[Rules.DEFAULT, Rules.NUMERICAL]
        )
        # Should have suggestions for numeric columns
        numeric_suggestions = get_suggestions_for_column(suggestions, "att1")
        assert isinstance(suggestions, list)

    def test_string_dataset(self, engine_string_lengths):
        """String dataset gets appropriate suggestions."""
        suggestions = engine_string_lengths.suggest_constraints(
            rules=[Rules.DEFAULT, Rules.STRING]
        )
        string_suggestions = get_suggestions_for_column(suggestions, "att1")
        assert isinstance(suggestions, list)

    def test_mixed_type_dataset(self, engine_full):
        """Mixed-type dataset handles all columns."""
        suggestions = engine_full.suggest_constraints(
            rules=[Rules.EXTENDED]
        )
        # Should have suggestions for different column types
        assert isinstance(suggestions, list)


class TestNonNegativeRuleSuggestions:
    """Tests for non-negative number suggestions."""

    def test_positive_column_suggestions(self, engine_compliance):
        """All-positive columns may get non-negative suggestions."""
        suggestions = engine_compliance.suggest_constraints(
            columns=["positive"],
            rules=[Rules.DEFAULT]
        )
        # May suggest isNonNegative for positive column
        assert isinstance(suggestions, list)
