from pydeequ.suggestions import *
import numpy as np
from pandas import DataFrame as pandasDF
import pytest

from pydeequ import PyDeequSession
from pydeequ.checks import *
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import *
from pydeequ.verification import *


class TestPandasUtils():

    @pytest.fixture(autouse=True)
    def _initialize(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext
        self.pydeequ_session = PyDeequSession(self.spark)
        self.AnalysisRunner = self.pydeequ_session.createAnalysisRunner()
        self.ColumnProfilerRunner = ColumnProfilerRunner(self.spark)
        self.ConstraintSuggestionRunner = ConstraintSuggestionRunner(self.spark)
        data = [
            ('foo', 1, True, 1.0, float('nan')),
            ('bar', 2, False, 2.0, float('nan'))
        ]
        self.pyspark_df = self.spark.createDataFrame(data, schema=['strings', 'ints', 'bools','floats','nans'])
        self.pandas_df = pandasDF({
            'strings': ['foo','bar'],
            'ints': [1,2],
            'bools': [True, False],
            'floats': [1.0,2.0],
            'nans': [np.nan, np.nan]
        })

    def assertEqual(self, expected, actual):
        assert expected == actual

    def assertIsInstance(self, expected, actual):
        assert isinstance(expected, actual)

    def test_p2s_analyzer(self):
        pd_result = self.AnalysisRunner.onData(self.pandas_df) \
            .addAnalyzer(Completeness('strings')) \
            .run()
        pd_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, pd_result)

        sp_result = self.AnalysisRunner.onData(self.pyspark_df) \
            .addAnalyzer(Completeness('strings')) \
            .run()
        sp_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, sp_result)

        self.assertEqual(sp_result_df.select('value').collect(),
                         pd_result_df.select('value').collect())

    def test_p2s_profiles(self):
        pd_result = self.ColumnProfilerRunner \
            .onData(self.pandas_df) \
            .run()

        sp_result = self.ColumnProfilerRunner \
            .onData(self.pyspark_df) \
            .run()

        for col in self.pandas_df.columns.values:
            self.assertEqual(pd_result.profiles[col].completeness, sp_result.profiles[col].completeness)

    def test_p2s_verification(self):
        sp_check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        sp_result = VerificationSuite(self.spark).onData(self.pyspark_df) \
            .addCheck(sp_check.hasSize(lambda x: x == 2, "size of dataframe should be 2")) \
            .run()
        sp_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, sp_result)

        pd_check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        pd_result = VerificationSuite(self.spark).onData(self.pandas_df) \
            .addCheck(pd_check.hasSize(lambda x: x == 2, "size of dataframe should be 2")) \
            .run()
        pd_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, pd_result)

        self.assertEqual(pd_result_df.select('constraint_status').collect(),
                         sp_result_df.select('constraint_status').collect())

    def test_p2s_suggestion(self):
        sp_result = self.ConstraintSuggestionRunner \
            .onData(self.pyspark_df) \
            .addConstraintRule(DEFAULT()) \
            .run()
        print(json.dumps(sp_result, indent=1))

        pd_result = self.ConstraintSuggestionRunner \
            .onData(self.pandas_df) \
            .addConstraintRule(DEFAULT()) \
            .run()
        print(json.dumps(pd_result, indent=1))

        self.assertEqual(len(sp_result), len(pd_result))

    def test_s2p_analyzers(self):
        pd_result = self.AnalysisRunner.onData(self.pandas_df) \
            .addAnalyzer(Completeness('strings')) \
            .run()
        pd_result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, pd_result, pandas=True)
        print(pd_result_df.head())
        self.assertIsInstance(pd_result_df, pandasDF)

    def test_s2p_verification(self):
        pd_check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        pd_result = VerificationSuite(self.spark).onData(self.pandas_df) \
            .addCheck(pd_check.hasSize(lambda x: x == 2, "size of dataframe should be 2")) \
            .run()
        pd_result_df = VerificationResult.checkResultsAsDataFrame(self.spark, pd_result, pandas=True)
        print(pd_result_df.head())
        self.assertIsInstance(pd_result_df, pandasDF)
