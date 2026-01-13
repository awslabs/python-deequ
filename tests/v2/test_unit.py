# -*- coding: utf-8 -*-
"""
Unit tests for PyDeequ V2 Spark Connect module.

These tests verify the Python client API works correctly without
requiring a Spark session. They test protobuf serialization of
predicates, checks, and analyzers.
"""

import unittest

from pydeequ.v2 import (
    # Checks
    Check,
    CheckLevel,
    Completeness,
    Mean,
    # Analyzers
    Size,
    between,
    # Predicates
    eq,
    gte,
    is_one,
)
class TestPredicates(unittest.TestCase):
    """Test predicate serialization.

    These tests use hardcoded numeric values for operator enums to detect
    proto sync issues between deequ (source of truth) and python-deequ.

    Expected values from deequ_connect.proto:
        UNSPECIFIED = 0, EQ = 1, NE = 2, GT = 3, GE = 4, LT = 5, LE = 6, BETWEEN = 7
    """

    def test_eq_predicate(self):
        p = eq(100)
        proto = p.to_proto()
        self.assertEqual(proto.operator, 1)  # EQ
        self.assertEqual(proto.value, 100.0)

    def test_gte_predicate(self):
        p = gte(0.95)
        proto = p.to_proto()
        self.assertEqual(proto.operator, 4)  # GE
        self.assertEqual(proto.value, 0.95)

    def test_between_predicate(self):
        p = between(10, 20)
        proto = p.to_proto()
        self.assertEqual(proto.operator, 7)  # BETWEEN
        self.assertEqual(proto.lower_bound, 10.0)
        self.assertEqual(proto.upper_bound, 20.0)

    def test_is_one_predicate(self):
        p = is_one()
        proto = p.to_proto()
        self.assertEqual(proto.operator, 1)  # EQ
        self.assertEqual(proto.value, 1.0)


class TestCheckBuilder(unittest.TestCase):
    """Test Check class protobuf building."""

    def test_check_with_constraints(self):
        check = (
            Check(CheckLevel.Error, "Test check")
            .isComplete("id")
            .hasCompleteness("email", gte(0.95))
            .hasSize(eq(100))
        )

        proto = check.to_proto()

        self.assertEqual(proto.level, 0)  # ERROR
        self.assertEqual(proto.description, "Test check")
        self.assertEqual(len(proto.constraints), 3)

        # Check constraint types
        self.assertEqual(proto.constraints[0].type, "isComplete")
        self.assertEqual(proto.constraints[0].column, "id")

        self.assertEqual(proto.constraints[1].type, "hasCompleteness")
        self.assertEqual(proto.constraints[1].column, "email")

        self.assertEqual(proto.constraints[2].type, "hasSize")

    def test_check_warning_level(self):
        check = Check(CheckLevel.Warning, "Warning check")
        proto = check.to_proto()
        self.assertEqual(proto.level, 1)  # WARNING


class TestAnalyzerBuilder(unittest.TestCase):
    """Test Analyzer classes protobuf building."""

    def test_size_analyzer(self):
        analyzer = Size()
        proto = analyzer.to_proto()
        self.assertEqual(proto.type, "Size")

    def test_completeness_analyzer(self):
        analyzer = Completeness("email")
        proto = analyzer.to_proto()
        self.assertEqual(proto.type, "Completeness")
        self.assertEqual(proto.column, "email")

    def test_mean_analyzer(self):
        analyzer = Mean("amount")
        proto = analyzer.to_proto()
        self.assertEqual(proto.type, "Mean")
        self.assertEqual(proto.column, "amount")

    def test_analyzer_with_where(self):
        analyzer = Size(where="status = 'active'")
        proto = analyzer.to_proto()
        self.assertEqual(proto.type, "Size")
        self.assertEqual(proto.where, "status = 'active'")


if __name__ == "__main__":
    unittest.main()
