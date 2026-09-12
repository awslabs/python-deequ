# -*- coding: utf-8 -*-
import unittest
from datetime import datetime, timedelta

from pyspark.sql import Row

from pydeequ.dqdl import EvaluateDataQuality
from tests.conftest import setup_pyspark


class TestDQDL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-dqdl-local").getOrCreate()
        cls.sc = cls.spark.sparkContext
        now = datetime.now()
        cls.df = cls.sc.parallelize(
            [
                Row(id="1", name="foo", updated_at=now - timedelta(hours=1)),
                Row(id="2", name="bar", updated_at=now - timedelta(hours=2)),
                Row(id="3", name=None, updated_at=now - timedelta(hours=50)),
            ]
        ).toDF()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def outcomes(self, ruleset, **kwargs):
        result = EvaluateDataQuality.process(self.spark, self.df, ruleset, **kwargs)
        return {row.Rule: row for row in result.collect()}

    def test_process_returns_one_row_per_rule(self):
        result = EvaluateDataQuality.process(self.spark, self.df, 'Rules=[RowCount = 3, IsComplete "name"]')
        self.assertEqual(
            result.columns, ["Rule", "Outcome", "FailureReason", "EvaluatedMetrics", "EvaluatedRule"]
        )
        outcomes = {row.Rule: row.Outcome for row in result.collect()}
        self.assertEqual(outcomes, {"RowCount = 3": "Passed", 'IsComplete "name"': "Failed"})

    def test_data_freshness(self):
        outcomes = self.outcomes(
            'Rules=[DataFreshness "updated_at" <= 72 hours, DataFreshness "updated_at" <= 24 hours]'
        )
        fresh = outcomes['DataFreshness "updated_at" <= 72 hours']
        stale = outcomes['DataFreshness "updated_at" <= 24 hours']
        self.assertEqual(fresh.Outcome, "Passed")
        self.assertEqual(stale.Outcome, "Failed")
        self.assertAlmostEqual(stale.EvaluatedMetrics["Column.updated_at.DataFreshness.Compliance"], 2 / 3)

    def test_data_freshness_units(self):
        outcomes = self.outcomes(
            'Rules=[DataFreshness "updated_at" <= 3 days, DataFreshness "updated_at" > 30 minutes]'
        )
        self.assertEqual({row.Outcome for row in outcomes.values()}, {"Passed"})

    def test_additional_data_sources(self):
        reference = self.sc.parallelize([Row(id="1"), Row(id="2"), Row(id="3")]).toDF()
        outcomes = self.outcomes(
            'Rules=[RowCountMatch "reference" = 1.0]', additionalDataSources={"reference": reference}
        )
        self.assertEqual(outcomes['RowCountMatch "reference" = 1.0'].Outcome, "Passed")

    def test_process_pandas(self):
        result = EvaluateDataQuality.process(self.spark, self.df, "Rules=[RowCount > 0]", pandas=True)
        self.assertEqual(result["Outcome"].tolist(), ["Passed"])

    def test_process_rows(self):
        results = EvaluateDataQuality.processRows(self.spark, self.df, 'Rules=[IsComplete "name"]')
        self.assertEqual(set(results), {"originalData", "ruleOutcomes", "rowLevelOutcomes"})
        self.assertEqual(results["originalData"].count(), 3)
        self.assertEqual(results["ruleOutcomes"].first().Outcome, "Failed")
        row_results = {
            row.id: row.DataQualityEvaluationResult for row in results["rowLevelOutcomes"].collect()
        }
        self.assertEqual(row_results, {"1": "Passed", "2": "Passed", "3": "Failed"})

    def test_process_rows_pandas(self):
        results = EvaluateDataQuality.processRows(self.spark, self.df, 'Rules=[IsComplete "id"]', pandas=True)
        self.assertEqual(len(results["rowLevelOutcomes"]), 3)

    def test_ruleset_must_be_str(self):
        with self.assertRaises(TypeError):
            EvaluateDataQuality.process(self.spark, self.df, ["RowCount > 0"])
