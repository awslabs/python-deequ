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

"""Unit tests for suggestion rules.

Tests the individual suggestion rules in isolation using mock column profiles.
"""

import json
import pytest

from pydeequ.engines import ColumnProfile
from pydeequ.engines.suggestions import (
    RuleRegistry,
    SuggestionRunner,
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


def make_profile(
    column: str = "test_col",
    completeness: float = 1.0,
    approx_distinct_values: int = 10,
    data_type: str = "INTEGER",
    minimum: float = None,
    maximum: float = None,
    mean: float = None,
    histogram: str = None,
) -> ColumnProfile:
    """Create a test column profile with specified attributes."""
    return ColumnProfile(
        column=column,
        completeness=completeness,
        approx_distinct_values=approx_distinct_values,
        data_type=data_type,
        minimum=minimum,
        maximum=maximum,
        mean=mean,
        histogram=histogram,
    )


class TestCompleteIfCompleteRule:
    """Tests for CompleteIfComplete rule."""

    def test_applies_when_fully_complete(self):
        """Rule applies when completeness is 1.0."""
        rule = CompleteIfCompleteRule()
        profile = make_profile(completeness=1.0)
        assert rule.applies_to(profile) is True

    def test_does_not_apply_when_not_complete(self):
        """Rule does not apply when completeness < 1.0."""
        rule = CompleteIfCompleteRule()
        profile = make_profile(completeness=0.95)
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates isComplete suggestion."""
        rule = CompleteIfCompleteRule()
        profile = make_profile(column="my_column", completeness=1.0)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "my_column"
        assert suggestion.constraint_name == "Completeness"
        assert suggestion.suggesting_rule == "CompleteIfComplete"
        assert ".isComplete" in suggestion.code_for_constraint

    def test_rule_sets(self):
        """Rule belongs to DEFAULT and EXTENDED sets."""
        rule = CompleteIfCompleteRule()
        assert "DEFAULT" in rule.rule_sets
        assert "EXTENDED" in rule.rule_sets


class TestRetainCompletenessRule:
    """Tests for RetainCompleteness rule."""

    def test_applies_when_high_completeness(self):
        """Rule applies when completeness >= 0.9 and < 1.0."""
        rule = RetainCompletenessRule()
        profile = make_profile(completeness=0.95)
        assert rule.applies_to(profile) is True

    def test_does_not_apply_when_fully_complete(self):
        """Rule does not apply when completeness is 1.0."""
        rule = RetainCompletenessRule()
        profile = make_profile(completeness=1.0)
        assert rule.applies_to(profile) is False

    def test_does_not_apply_when_low_completeness(self):
        """Rule does not apply when completeness < threshold."""
        rule = RetainCompletenessRule()
        profile = make_profile(completeness=0.85)
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates hasCompleteness suggestion."""
        rule = RetainCompletenessRule()
        profile = make_profile(column="my_column", completeness=0.95)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "my_column"
        assert suggestion.suggesting_rule == "RetainCompleteness"
        assert ".hasCompleteness" in suggestion.code_for_constraint


class TestNonNegativeNumbersRule:
    """Tests for NonNegativeNumbers rule."""

    def test_applies_when_minimum_non_negative(self):
        """Rule applies when minimum >= 0."""
        rule = NonNegativeNumbersRule()
        profile = make_profile(minimum=0.0)
        assert rule.applies_to(profile) is True

    def test_applies_when_minimum_positive(self):
        """Rule applies when minimum > 0."""
        rule = NonNegativeNumbersRule()
        profile = make_profile(minimum=5.0)
        assert rule.applies_to(profile) is True

    def test_does_not_apply_when_minimum_negative(self):
        """Rule does not apply when minimum < 0."""
        rule = NonNegativeNumbersRule()
        profile = make_profile(minimum=-1.0)
        assert rule.applies_to(profile) is False

    def test_does_not_apply_when_no_minimum(self):
        """Rule does not apply when minimum is None."""
        rule = NonNegativeNumbersRule()
        profile = make_profile(minimum=None)
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates isNonNegative suggestion."""
        rule = NonNegativeNumbersRule()
        profile = make_profile(column="amount", minimum=0.0)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "amount"
        assert suggestion.suggesting_rule == "NonNegativeNumbers"
        assert ".isNonNegative" in suggestion.code_for_constraint


class TestCategoricalRangeRule:
    """Tests for CategoricalRange rule."""

    def test_applies_when_low_cardinality_histogram(self):
        """Rule applies when histogram has <= 10 values."""
        rule = CategoricalRangeRule()
        histogram = json.dumps({"A": 10, "B": 20, "C": 30})
        profile = make_profile(histogram=histogram)
        assert rule.applies_to(profile) is True

    def test_does_not_apply_when_no_histogram(self):
        """Rule does not apply when no histogram."""
        rule = CategoricalRangeRule()
        profile = make_profile(histogram=None)
        assert rule.applies_to(profile) is False

    def test_does_not_apply_when_high_cardinality(self):
        """Rule does not apply when histogram has > 10 values."""
        rule = CategoricalRangeRule()
        histogram = json.dumps({f"val_{i}": i for i in range(20)})
        profile = make_profile(histogram=histogram)
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates isContainedIn suggestion."""
        rule = CategoricalRangeRule()
        histogram = json.dumps({"A": 10, "B": 20})
        profile = make_profile(column="status", histogram=histogram)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "status"
        assert suggestion.suggesting_rule == "CategoricalRangeRule"
        assert ".isContainedIn" in suggestion.code_for_constraint


class TestHasMinRule:
    """Tests for HasMin rule."""

    def test_applies_when_numeric_with_stats(self):
        """Rule applies when minimum and mean are present."""
        rule = HasMinRule()
        profile = make_profile(minimum=0.0, mean=5.0)
        assert rule.applies_to(profile) is True

    def test_does_not_apply_when_no_minimum(self):
        """Rule does not apply when minimum is None."""
        rule = HasMinRule()
        profile = make_profile(minimum=None, mean=5.0)
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates hasMin suggestion."""
        rule = HasMinRule()
        profile = make_profile(column="value", minimum=1.0, mean=5.0)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "value"
        assert suggestion.suggesting_rule == "HasMin"
        assert ".hasMin" in suggestion.code_for_constraint

    def test_rule_sets(self):
        """Rule belongs to NUMERICAL and EXTENDED sets."""
        rule = HasMinRule()
        assert "NUMERICAL" in rule.rule_sets
        assert "EXTENDED" in rule.rule_sets


class TestHasMaxRule:
    """Tests for HasMax rule."""

    def test_applies_when_numeric_with_stats(self):
        """Rule applies when maximum and mean are present."""
        rule = HasMaxRule()
        profile = make_profile(maximum=10.0, mean=5.0)
        assert rule.applies_to(profile) is True

    def test_generates_correct_suggestion(self):
        """Rule generates hasMax suggestion."""
        rule = HasMaxRule()
        profile = make_profile(column="value", maximum=10.0, mean=5.0)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "value"
        assert suggestion.suggesting_rule == "HasMax"
        assert ".hasMax" in suggestion.code_for_constraint


class TestHasMeanRule:
    """Tests for HasMean rule."""

    def test_applies_when_mean_present(self):
        """Rule applies when mean is present."""
        rule = HasMeanRule()
        profile = make_profile(mean=5.0)
        assert rule.applies_to(profile) is True

    def test_does_not_apply_when_no_mean(self):
        """Rule does not apply when mean is None."""
        rule = HasMeanRule()
        profile = make_profile(mean=None)
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates hasMean suggestion with range."""
        rule = HasMeanRule()
        profile = make_profile(column="value", mean=100.0)
        suggestion = rule.generate(profile)

        assert suggestion.column_name == "value"
        assert suggestion.suggesting_rule == "HasMean"
        assert ".hasMean" in suggestion.code_for_constraint
        assert "between" in suggestion.code_for_constraint


class TestHasMinLengthRule:
    """Tests for HasMinLength rule."""

    def test_applies_to_string_columns(self):
        """Rule applies to string data types."""
        rule = HasMinLengthRule()
        profile = make_profile(data_type="VARCHAR")
        assert rule.applies_to(profile) is True

    def test_does_not_apply_to_numeric_columns(self):
        """Rule does not apply to numeric data types."""
        rule = HasMinLengthRule()
        profile = make_profile(data_type="INTEGER")
        assert rule.applies_to(profile) is False

    def test_generates_correct_suggestion(self):
        """Rule generates hasMinLength suggestion."""
        rule = HasMinLengthRule()
        profile = make_profile(column="name", data_type="VARCHAR")
        suggestion = rule.generate(profile, min_length=3)

        assert suggestion.column_name == "name"
        assert suggestion.suggesting_rule == "HasMinLength"
        assert ".hasMinLength" in suggestion.code_for_constraint

    def test_returns_none_when_no_length(self):
        """Rule returns None when no min_length provided."""
        rule = HasMinLengthRule()
        profile = make_profile(data_type="VARCHAR")
        suggestion = rule.generate(profile, min_length=None)
        assert suggestion is None

    def test_rule_sets(self):
        """Rule belongs to STRING and EXTENDED sets."""
        rule = HasMinLengthRule()
        assert "STRING" in rule.rule_sets
        assert "EXTENDED" in rule.rule_sets


class TestHasMaxLengthRule:
    """Tests for HasMaxLength rule."""

    def test_applies_to_string_columns(self):
        """Rule applies to string data types."""
        rule = HasMaxLengthRule()
        profile = make_profile(data_type="TEXT")
        assert rule.applies_to(profile) is True

    def test_generates_correct_suggestion(self):
        """Rule generates hasMaxLength suggestion."""
        rule = HasMaxLengthRule()
        profile = make_profile(column="name", data_type="VARCHAR")
        suggestion = rule.generate(profile, max_length=50)

        assert suggestion.column_name == "name"
        assert suggestion.suggesting_rule == "HasMaxLength"
        assert ".hasMaxLength" in suggestion.code_for_constraint


class TestUniqueIfApproximatelyUniqueRule:
    """Tests for UniqueIfApproximatelyUnique rule."""

    def test_generates_suggestion_when_unique(self):
        """Rule generates isUnique when distinct values >= 99% of rows."""
        rule = UniqueIfApproximatelyUniqueRule()
        profile = make_profile(column="id", approx_distinct_values=100)
        suggestion = rule.generate(profile, row_count=100)

        assert suggestion is not None
        assert suggestion.column_name == "id"
        assert suggestion.suggesting_rule == "UniqueIfApproximatelyUnique"
        assert ".isUnique" in suggestion.code_for_constraint

    def test_does_not_generate_when_not_unique(self):
        """Rule returns None when distinct values < 99% of rows."""
        rule = UniqueIfApproximatelyUniqueRule()
        profile = make_profile(approx_distinct_values=50)
        suggestion = rule.generate(profile, row_count=100)
        assert suggestion is None

    def test_returns_none_when_no_row_count(self):
        """Rule returns None when row_count is not provided."""
        rule = UniqueIfApproximatelyUniqueRule()
        profile = make_profile(approx_distinct_values=100)
        suggestion = rule.generate(profile, row_count=None)
        assert suggestion is None

    def test_rule_sets(self):
        """Rule belongs to COMMON and EXTENDED sets."""
        rule = UniqueIfApproximatelyUniqueRule()
        assert "COMMON" in rule.rule_sets
        assert "EXTENDED" in rule.rule_sets


class TestRuleRegistry:
    """Tests for RuleRegistry."""

    def test_registry_has_default_rules(self):
        """Registry has rules registered by default."""
        rules = RuleRegistry.get_all_rules()
        assert len(rules) > 0

    def test_get_rules_for_sets_default(self):
        """Can retrieve DEFAULT rules."""
        rules = RuleRegistry.get_rules_for_sets(["DEFAULT"])
        rule_names = [r.name for r in rules]
        assert "CompleteIfComplete" in rule_names
        assert "NonNegativeNumbers" in rule_names

    def test_get_rules_for_sets_numerical(self):
        """Can retrieve NUMERICAL rules."""
        rules = RuleRegistry.get_rules_for_sets(["NUMERICAL"])
        rule_names = [r.name for r in rules]
        assert "HasMin" in rule_names
        assert "HasMax" in rule_names
        assert "HasMean" in rule_names

    def test_get_rules_for_sets_string(self):
        """Can retrieve STRING rules."""
        rules = RuleRegistry.get_rules_for_sets(["STRING"])
        rule_names = [r.name for r in rules]
        assert "HasMinLength" in rule_names
        assert "HasMaxLength" in rule_names

    def test_get_rules_for_multiple_sets(self):
        """Can retrieve rules from multiple sets."""
        rules = RuleRegistry.get_rules_for_sets(["DEFAULT", "NUMERICAL"])
        rule_names = [r.name for r in rules]
        assert "CompleteIfComplete" in rule_names
        assert "HasMin" in rule_names


class TestSuggestionRunner:
    """Tests for SuggestionRunner."""

    def test_runner_default_rules(self):
        """Runner uses DEFAULT rules by default."""
        runner = SuggestionRunner()
        assert runner.rule_sets == ["DEFAULT"]

    def test_runner_custom_rules(self):
        """Runner can use custom rule sets."""
        runner = SuggestionRunner(rule_sets=["NUMERICAL", "STRING"])
        assert "NUMERICAL" in runner.rule_sets
        assert "STRING" in runner.rule_sets

    def test_run_generates_suggestions(self):
        """Runner generates suggestions from profiles."""
        runner = SuggestionRunner(rule_sets=["DEFAULT"])
        profiles = [
            make_profile(column="complete_col", completeness=1.0),
            make_profile(column="partial_col", completeness=0.95),
        ]
        suggestions = runner.run(profiles)

        # Should have suggestions for both columns
        column_names = [s.column_name for s in suggestions]
        assert "complete_col" in column_names
        assert "partial_col" in column_names

    def test_run_with_numeric_profiles(self):
        """Runner generates numeric suggestions."""
        runner = SuggestionRunner(rule_sets=["NUMERICAL"])
        profiles = [
            make_profile(column="value", minimum=0.0, maximum=100.0, mean=50.0),
        ]
        suggestions = runner.run(profiles)

        rule_names = [s.suggesting_rule for s in suggestions]
        assert "HasMin" in rule_names
        assert "HasMax" in rule_names
        assert "HasMean" in rule_names

    def test_run_with_row_count_for_uniqueness(self):
        """Runner uses row_count for uniqueness checks."""
        runner = SuggestionRunner(rule_sets=["COMMON"])
        profiles = [
            make_profile(column="id", approx_distinct_values=100),
        ]
        suggestions = runner.run(profiles, row_count=100)

        rule_names = [s.suggesting_rule for s in suggestions]
        assert "UniqueIfApproximatelyUnique" in rule_names
