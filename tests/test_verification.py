# -*- coding: utf-8 -*-
import unittest

import pandas as pd
from pyspark.sql import Row
from pyspark.sql.types import BooleanType

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite
from tests.conftest import setup_pyspark


class TestRowLevelResults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-row-level-results-local").getOrCreate()
        cls.sc = cls.spark.sparkContext
        cls.df = cls.sc.parallelize(
            [
                Row(a="foo", b=1, c=5),
                Row(a="bar", b=2, c=6),
                Row(a="baz", b=3, c=None),
            ]
        ).toDF()

    @classmethod
    def tearDownClass(cls):
        # Must shutdown callback for tests to stop
        # TODO Document this call to users or encapsulate in PyDeequSession
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_row_level_results_with_completeness(self):
        """Test that isComplete produces a Boolean column with correct per-row values."""
        check = Check(self.spark, CheckLevel.Error, "completeness_check")
        check = check.isComplete("c")

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        row_level_df = VerificationResult.rowLevelResultsAsDataFrame(self.spark, result, self.df)

        # Should have same row count as original DataFrame
        self.assertEqual(row_level_df.count(), self.df.count())

        # Should have original columns (a, b, c) plus one Boolean column for the check
        self.assertIn("completeness_check", row_level_df.columns)
        self.assertTrue(isinstance(row_level_df.schema["completeness_check"].dataType, BooleanType))

        # Order by b to ensure deterministic row ordering
        # b=1: c=5 (complete), b=2: c=6 (complete), b=3: c=None (incomplete)
        results = row_level_df.orderBy("b").select("completeness_check").collect()
        values = [row["completeness_check"] for row in results]
        self.assertEqual(values, [True, True, False])

    def test_row_level_results_with_contained_in(self):
        """Test that isContainedIn produces correct row-level results."""
        check = Check(self.spark, CheckLevel.Error, "contained_check")
        check = check.isContainedIn("a", ["foo", "bar"])

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        row_level_df = VerificationResult.rowLevelResultsAsDataFrame(self.spark, result, self.df)

        self.assertIn("contained_check", row_level_df.columns)

        # Order by a to ensure deterministic row ordering
        # a="bar" (contained), a="baz" (not contained), a="foo" (contained)
        results = row_level_df.orderBy("a").select("contained_check").collect()
        values = [row["contained_check"] for row in results]
        self.assertEqual(values, [True, False, True])

    def test_row_level_results_multiple_constraints_anded(self):
        """Test that multiple constraints in one Check are ANDed into a single column."""
        check = Check(self.spark, CheckLevel.Error, "multi_check")
        check = check.isComplete("a").isComplete("c")

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        row_level_df = VerificationResult.rowLevelResultsAsDataFrame(self.spark, result, self.df)

        self.assertIn("multi_check", row_level_df.columns)

        # Order by b to ensure deterministic row ordering
        # b=1: a,c complete -> True, b=2: a,c complete -> True, b=3: c=None -> False
        results = row_level_df.orderBy("b").select("multi_check").collect()
        values = [row["multi_check"] for row in results]
        self.assertEqual(values, [True, True, False])

    def test_row_level_results_aggregate_only_check(self):
        """Test that aggregate-only checks (hasSize) don't add columns."""
        check = Check(self.spark, CheckLevel.Warning, "size_check")
        check = check.hasSize(lambda x: x >= 3)

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        row_level_df = VerificationResult.rowLevelResultsAsDataFrame(self.spark, result, self.df)

        # hasSize is aggregate-only, so no new column should be added
        self.assertEqual(sorted(row_level_df.columns), sorted(self.df.columns))

    def test_row_level_results_preserves_original_columns(self):
        """Test that the original DataFrame columns are preserved."""
        check = Check(self.spark, CheckLevel.Error, "preserve_check")
        check = check.isComplete("c")

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        row_level_df = VerificationResult.rowLevelResultsAsDataFrame(self.spark, result, self.df)

        for col in self.df.columns:
            self.assertIn(col, row_level_df.columns)

        # Verify original data is unchanged (ordered for deterministic comparison)
        original_values = self.df.orderBy("b").select("a", "b").collect()
        result_values = row_level_df.orderBy("b").select("a", "b").collect()
        self.assertEqual(original_values, result_values)

    def test_row_level_results_as_pandas(self):
        """Test the pandas=True option returns a Pandas DataFrame."""
        check = Check(self.spark, CheckLevel.Error, "pandas_check")
        check = check.isComplete("c")

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        row_level_df = VerificationResult.rowLevelResultsAsDataFrame(
            self.spark, result, self.df, pandas=True
        )

        self.assertIsInstance(row_level_df, pd.DataFrame)
        self.assertIn("pandas_check", row_level_df.columns)


if __name__ == "__main__":
    unittest.main()
