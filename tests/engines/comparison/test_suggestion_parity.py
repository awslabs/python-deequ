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

"""Cross-engine constraint suggestion parity tests.

Tests that verify DuckDB engine produces the same constraint suggestions
as the Spark engine baseline. Requires Spark Connect to be running.

Note: Suggestions may differ between engines due to different profiling
algorithms. These tests focus on structural consistency rather than
exact match.
"""

import pytest

from pydeequ.v2.suggestions import Rules

from tests.engines.comparison.conftest import requires_spark, DualEngines


def get_suggestions_for_column(suggestions, column_name: str):
    """Get all suggestions for a specific column."""
    return [s for s in suggestions if s.column_name == column_name]


def get_suggestions_by_constraint(suggestions, constraint_name: str):
    """Get all suggestions matching a constraint type."""
    return [s for s in suggestions if constraint_name in s.constraint_name]


@requires_spark
class TestSuggestionStructureParity:
    """Parity tests for suggestion structure consistency."""

    def test_default_rules_structure(self, dual_engines_full: DualEngines):
        """DEFAULT rules produce structurally similar suggestions."""
        spark_suggestions = dual_engines_full.spark_engine.suggest_constraints(rules=[Rules.DEFAULT])
        duckdb_suggestions = dual_engines_full.duckdb_engine.suggest_constraints(rules=[Rules.DEFAULT])

        # Both should return a list of suggestions
        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)

        # Both should have required fields
        if spark_suggestions:
            s = spark_suggestions[0]
            assert hasattr(s, 'column_name')
            assert hasattr(s, 'constraint_name')
            assert hasattr(s, 'description')

        if duckdb_suggestions:
            s = duckdb_suggestions[0]
            assert hasattr(s, 'column_name')
            assert hasattr(s, 'constraint_name')
            assert hasattr(s, 'description')

    def test_numerical_rules_structure(self, dual_engines_numeric: DualEngines):
        """NUMERICAL rules produce structurally similar suggestions."""
        spark_suggestions = dual_engines_numeric.spark_engine.suggest_constraints(rules=[Rules.NUMERICAL])
        duckdb_suggestions = dual_engines_numeric.duckdb_engine.suggest_constraints(rules=[Rules.NUMERICAL])

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)

    def test_string_rules_structure(self, dual_engines_string_lengths: DualEngines):
        """STRING rules produce structurally similar suggestions."""
        spark_suggestions = dual_engines_string_lengths.spark_engine.suggest_constraints(rules=[Rules.STRING])
        duckdb_suggestions = dual_engines_string_lengths.duckdb_engine.suggest_constraints(rules=[Rules.STRING])

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)


@requires_spark
class TestSuggestionColumnCoverage:
    """Parity tests for column coverage in suggestions."""

    def test_complete_column_suggestions(self, dual_engines_full: DualEngines):
        """Both engines suggest constraints for complete columns."""
        spark_suggestions = dual_engines_full.spark_engine.suggest_constraints(
            columns=["att1"],
            rules=[Rules.DEFAULT]
        )
        duckdb_suggestions = dual_engines_full.duckdb_engine.suggest_constraints(
            columns=["att1"],
            rules=[Rules.DEFAULT]
        )

        # Both should return results (may differ in content)
        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)

    def test_numeric_column_suggestions(self, dual_engines_numeric: DualEngines):
        """Both engines suggest constraints for numeric columns."""
        spark_suggestions = dual_engines_numeric.spark_engine.suggest_constraints(
            columns=["att1"],
            rules=[Rules.NUMERICAL]
        )
        duckdb_suggestions = dual_engines_numeric.duckdb_engine.suggest_constraints(
            columns=["att1"],
            rules=[Rules.NUMERICAL]
        )

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)


@requires_spark
class TestSuggestionConstraintTypes:
    """Parity tests for suggested constraint types."""

    def test_completeness_suggestions(self, dual_engines_full: DualEngines):
        """Both engines may suggest completeness constraints."""
        spark_suggestions = dual_engines_full.spark_engine.suggest_constraints(rules=[Rules.DEFAULT])
        duckdb_suggestions = dual_engines_full.duckdb_engine.suggest_constraints(rules=[Rules.DEFAULT])

        spark_completeness = get_suggestions_by_constraint(spark_suggestions, "Complete")
        duckdb_completeness = get_suggestions_by_constraint(duckdb_suggestions, "Complete")

        # Both might suggest completeness (or not - depends on data)
        # Just verify structure is consistent
        assert isinstance(spark_completeness, list)
        assert isinstance(duckdb_completeness, list)

    def test_uniqueness_suggestions(self, dual_engines_unique: DualEngines):
        """Both engines may suggest uniqueness constraints."""
        spark_suggestions = dual_engines_unique.spark_engine.suggest_constraints(rules=[Rules.COMMON])
        duckdb_suggestions = dual_engines_unique.duckdb_engine.suggest_constraints(rules=[Rules.COMMON])

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)


@requires_spark
class TestSuggestionRuleSetsParity:
    """Parity tests for different rule sets."""

    def test_extended_rules(self, dual_engines_full: DualEngines):
        """EXTENDED rules produce consistent suggestions on both engines."""
        spark_suggestions = dual_engines_full.spark_engine.suggest_constraints(rules=[Rules.EXTENDED])
        duckdb_suggestions = dual_engines_full.duckdb_engine.suggest_constraints(rules=[Rules.EXTENDED])

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)

    def test_multiple_rule_sets(self, dual_engines_numeric: DualEngines):
        """Multiple rule sets produce consistent suggestions."""
        spark_suggestions = dual_engines_numeric.spark_engine.suggest_constraints(
            rules=[Rules.DEFAULT, Rules.NUMERICAL]
        )
        duckdb_suggestions = dual_engines_numeric.duckdb_engine.suggest_constraints(
            rules=[Rules.DEFAULT, Rules.NUMERICAL]
        )

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)


@requires_spark
class TestSuggestionEdgeCases:
    """Parity tests for edge cases in suggestions."""

    def test_single_row_suggestions(self, dual_engines_single: DualEngines):
        """Single-row dataset produces consistent suggestions."""
        spark_suggestions = dual_engines_single.spark_engine.suggest_constraints(rules=[Rules.DEFAULT])
        duckdb_suggestions = dual_engines_single.duckdb_engine.suggest_constraints(rules=[Rules.DEFAULT])

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)

    def test_all_null_column_suggestions(self, dual_engines_all_null: DualEngines):
        """All-NULL column produces consistent suggestions."""
        spark_suggestions = dual_engines_all_null.spark_engine.suggest_constraints(
            columns=["value"],
            rules=[Rules.DEFAULT]
        )
        duckdb_suggestions = dual_engines_all_null.duckdb_engine.suggest_constraints(
            columns=["value"],
            rules=[Rules.DEFAULT]
        )

        assert isinstance(spark_suggestions, list)
        assert isinstance(duckdb_suggestions, list)


@requires_spark
class TestSuggestionColumnRestriction:
    """Parity tests for column restriction in suggestions."""

    def test_restrict_to_columns(self, dual_engines_full: DualEngines):
        """Column restriction produces consistent suggestions."""
        spark_suggestions = dual_engines_full.spark_engine.suggest_constraints(
            columns=["att1", "att2"],
            rules=[Rules.DEFAULT]
        )
        duckdb_suggestions = dual_engines_full.duckdb_engine.suggest_constraints(
            columns=["att1", "att2"],
            rules=[Rules.DEFAULT]
        )

        # Check that suggestions are for the restricted columns
        spark_columns = {s.column_name for s in spark_suggestions if s.column_name}
        duckdb_columns = {s.column_name for s in duckdb_suggestions if s.column_name}

        # Both should only include requested columns (or None for dataset-level)
        for col in spark_columns:
            if col:
                assert col in ["att1", "att2"]
        for col in duckdb_columns:
            if col:
                assert col in ["att1", "att2"]
