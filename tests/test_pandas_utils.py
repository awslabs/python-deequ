# -*- coding: utf-8 -*-
import json
import unittest

import numpy as np
from pandas import DataFrame as pandasDF

from pydeequ import PyDeequSession
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import *
from pydeequ.verification import *
from tests.conftest import setup_pyspark


class TestPandasUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-analyzers-local").getOrCreate()
        cls.pydeequ_session = PyDeequSession(cls.spark)
        cls.AnalysisRunner = cls.pydeequ_session.createAnalysisRunner()
        cls.ColumnProfilerRunner = ColumnProfilerRunner(cls.spark)
        cls.ConstraintSuggestionRunner = ConstraintSuggestionRunner(cls.spark)
        cls.sc = cls.spark.sparkContext
        data = [("foo", 1, True, 1.0, float("nan")), ("bar", 2, False, 2.0, float("nan"))]
        cls.pyspark_df = cls.spark.createDataFrame(data, schema=["strings", "ints", "bools", "floats", "nans"])
        cls.pandas_df = pandasDF(
            {
                "strings": ["foo", "bar"],
                "ints": [1, 2],
                "bools": [True, False],
                "floats": [1.0, 2.0],
                "nans": [np.nan, np.nan],
            }
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_p2s_analyzer(self):
        pd_result = self.AnalysisRunner.onData(self.pandas_df).addAnalyzer(Completeness("strings")).run()
        pd_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, pd_result)

        sp_result = self.AnalysisRunner.onData(self.pyspark_df).addAnalyzer(Completeness("strings")).run()
        sp_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, sp_result)

        self.assertEqual(sp_result_df.select("value").collect(), pd_result_df.select("value").collect())

    def test_p2s_profiles(self):
        pd_result = self.ColumnProfilerRunner.onData(self.pandas_df).run()

        sp_result = self.ColumnProfilerRunner.onData(self.pyspark_df).run()

        for col in self.pandas_df.columns.values:
            self.assertEqual(pd_result.profiles[col].completeness, sp_result.profiles[col].completeness)

    def test_p2s_verification(self):
        sp_check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        sp_result = (
            VerificationSuite(self.spark)
            .onData(self.pyspark_df)
            .addCheck(sp_check.hasSize(lambda x: x == 2, "size of dataframe should be 2"))
            .run()
        )
        sp_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, sp_result)

        pd_check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        pd_result = (
            VerificationSuite(self.spark)
            .onData(self.pandas_df)
            .addCheck(pd_check.hasSize(lambda x: x == 2, "size of dataframe should be 2"))
            .run()
        )
        pd_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, pd_result)

        self.assertEqual(
            pd_result_df.select("constraint_status").collect(), sp_result_df.select("constraint_status").collect()
        )

    def test_p2s_suggestion(self):
        sp_result = self.ConstraintSuggestionRunner.onData(self.pyspark_df).addConstraintRule(DEFAULT()).run()
        print(json.dumps(sp_result, indent=1))

        pd_result = self.ConstraintSuggestionRunner.onData(self.pandas_df).addConstraintRule(DEFAULT()).run()
        print(json.dumps(pd_result, indent=1))

        self.assertEqual(len(sp_result), len(pd_result))

    def test_s2p_analyzers(self):
        pd_result = self.AnalysisRunner.onData(self.pandas_df).addAnalyzer(Completeness("strings")).run()
        pd_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, pd_result, pandas=True)
        print(pd_result_df.head())
        self.assertIsInstance(pd_result_df, pandasDF)

    def test_s2p_verification(self):
        pd_check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        pd_result = (
            VerificationSuite(self.spark)
            .onData(self.pandas_df)
            .addCheck(pd_check.hasSize(lambda x: x == 2, "size of dataframe should be 2"))
            .run()
        )
        pd_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, pd_result, pandas=True)
        print(pd_result_df.head())
        self.assertIsInstance(pd_result_df, pandasDF)
