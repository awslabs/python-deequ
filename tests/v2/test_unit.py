# -*- coding: utf-8 -*-
"""
Unit tests for PyDeequ V2 Spark Connect module.

These tests verify the Python client API works correctly without requiring a
Spark session. They cover protobuf serialization of predicates, checks, and
analyzers under the Stage 1 schema (oneof arms per builder method).
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

    Hardcoded numeric values detect proto sync issues between deequ (source of
    truth) and python-deequ.

    Expected values from deequ_connect.proto's CompareOp enum:
        COMPARE_OP_UNSPECIFIED = 0, EQ = 1, NE = 2, GT = 3, GE = 4,
        LT = 5, LE = 6, BETWEEN = 7
    """

    def test_eq_predicate(self):
        p = eq(100)
        msg = p.to_proto()
        self.assertEqual(msg.op, 1)  # COMPARE_OP_EQ
        self.assertEqual(msg.value, 100.0)

    def test_gte_predicate(self):
        p = gte(0.95)
        msg = p.to_proto()
        self.assertEqual(msg.op, 4)  # COMPARE_OP_GE
        self.assertEqual(msg.value, 0.95)

    def test_between_predicate(self):
        p = between(10, 20)
        msg = p.to_proto()
        self.assertEqual(msg.op, 7)  # COMPARE_OP_BETWEEN
        self.assertEqual(msg.lower_bound, 10.0)
        self.assertEqual(msg.upper_bound, 20.0)

    def test_is_one_predicate(self):
        p = is_one()
        msg = p.to_proto()
        self.assertEqual(msg.op, 1)  # COMPARE_OP_EQ
        self.assertEqual(msg.value, 1.0)


class TestCheckBuilder(unittest.TestCase):
    """Test Check class protobuf building."""

    def test_check_with_constraints(self):
        check = (
            Check(CheckLevel.Error, "Test check")
            .isComplete("id")
            .hasCompleteness("email", gte(0.95))
            .hasSize(eq(100))
        )

        msg = check.to_proto()

        # CHECK_LEVEL_ERROR = 1
        self.assertEqual(msg.level, 1)
        self.assertEqual(msg.description, "Test check")
        self.assertEqual(len(msg.constraints), 3)

        # Each constraint sets exactly one oneof arm.
        self.assertEqual(msg.constraints[0].WhichOneof("body"), "is_complete")
        self.assertEqual(msg.constraints[0].is_complete.column, "id")

        self.assertEqual(msg.constraints[1].WhichOneof("body"), "has_completeness")
        self.assertEqual(msg.constraints[1].has_completeness.column, "email")
        self.assertEqual(msg.constraints[1].has_completeness.assertion.op, 4)  # GE

        self.assertEqual(msg.constraints[2].WhichOneof("body"), "has_size")
        self.assertEqual(msg.constraints[2].has_size.assertion.op, 1)  # EQ

    def test_check_warning_level(self):
        check = Check(CheckLevel.Warning, "Warning check")
        msg = check.to_proto()
        self.assertEqual(msg.level, 2)  # CHECK_LEVEL_WARNING

    def test_where_raises_without_constraints(self):
        with self.assertRaises(ValueError):
            Check(CheckLevel.Error, "x").where("y = 1")


class TestAnalyzerBuilder(unittest.TestCase):
    """Test Analyzer classes protobuf building."""

    def test_size_analyzer(self):
        analyzer = Size()
        msg = analyzer.to_proto()
        self.assertEqual(msg.WhichOneof("body"), "size")

    def test_completeness_analyzer(self):
        analyzer = Completeness("email")
        msg = analyzer.to_proto()
        self.assertEqual(msg.WhichOneof("body"), "completeness")
        self.assertEqual(msg.completeness.column, "email")

    def test_mean_analyzer(self):
        analyzer = Mean("amount")
        msg = analyzer.to_proto()
        self.assertEqual(msg.WhichOneof("body"), "mean")
        self.assertEqual(msg.mean.column, "amount")

    def test_analyzer_with_where(self):
        analyzer = Size(where="status = 'active'")
        msg = analyzer.to_proto()
        self.assertEqual(msg.WhichOneof("body"), "size")
        self.assertEqual(msg.where, "status = 'active'")


if __name__ == "__main__":
    unittest.main()
