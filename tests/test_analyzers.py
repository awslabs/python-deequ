# -*- coding: utf-8 -*-
import unittest

import pytest
from pyspark.sql import Row

from pydeequ import PyDeequSession
from pydeequ.analyzers import (
    AnalyzerContext,
    ApproxCountDistinct,
    ApproxQuantile,
    ApproxQuantiles,
    Completeness,
    Compliance,
    Correlation,
    CountDistinct,
    DataType,
    Distinctness,
    Entropy,
    Histogram,
    KLLParameters,
    KLLSketch,
    Maximum,
    MaxLength,
    Mean,
    Minimum,
    MinLength,
    MutualInformation,
    PatternMatch,
    Size,
    StandardDeviation,
    Sum,
    Uniqueness,
    UniqueValueRatio,
)
from tests.conftest import setup_pyspark


class TestAnalyzers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-analyzers-local").getOrCreate()
        # cls.AnalysisRunner = AnalysisRunner(cls.spark)
        cls.pydeequ_session = PyDeequSession(cls.spark)
        cls.AnalysisRunner = cls.pydeequ_session.createAnalysisRunner()
        cls.sc = cls.spark.sparkContext
        cls.df = cls.sc.parallelize(
            [Row(a="foo", b=1, c=5, d=1), Row(a="bar", b=2, c=6, d=3), Row(a="baz", b=3, c=None, d=1)]
        ).toDF()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def ApproxCountDistinct(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(ApproxCountDistinct(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def ApproxQuantile(self, column, quantile, where=None):
        relativeError: float = 0.01
        result = (
            self.AnalysisRunner.onData(self.df)
            .addAnalyzer(ApproxQuantile(column, quantile, relativeError, where))
            .run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def ApproxQuantiles(self, column, quantiles):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(ApproxQuantiles(column, quantiles)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Completeness(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Completeness(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Compliance(self, instance, predicate, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Compliance(instance, predicate, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Correlation(self, column1, column2, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Correlation(column1, column2, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        AnalyzerContext.successMetricsAsJson(self.spark, result)
        return result_df.select("value").collect()

    def CountDistinct(self, columns):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(CountDistinct(columns)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Datatype(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(DataType(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Distinctness(self, columns, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Distinctness(columns, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Entropy(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Entropy(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Histogram(self, column, binningUdf=None, maxDetailBins: int = None, where: str = None):
        result = (
            self.AnalysisRunner.onData(self.df).addAnalyzer(Histogram(column, binningUdf, maxDetailBins, where)).run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def test_KLLSketch(self):
        result = (
            self.AnalysisRunner.onData(self.df).addAnalyzer(KLLSketch("b", KLLParameters(self.spark, 2, 0.64, 2))).run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_df.show()
        return result_df.select("value").collect()

    def Histogram_maxBins(self, column, binningUdf=None, maxDetailBins: int = None, where: str = None):
        result = (
            self.AnalysisRunner.onData(self.df).addAnalyzer(Histogram(column, binningUdf, maxDetailBins, where)).run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Maximum(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Maximum(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def MaxLength(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(MaxLength(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Mean(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Mean(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Minimum(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Minimum(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def MinLength(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(MinLength(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def MutualInformation(self, columns, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(MutualInformation(columns, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def StandardDeviation(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(StandardDeviation(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def Sum(self, column, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Sum(column, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def test_ApproxCountDistinct(self):
        self.assertEqual(self.ApproxCountDistinct("b"), [Row(value=3)])
        self.assertEqual(self.ApproxCountDistinct("c"), [Row(value=2)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_approxCountDistinct(self):
        self.assertEqual(self.ApproxCountDistinct("b"), [Row(value=2)])

    def test_ApproxQuantile(self):
        self.assertEqual(self.ApproxQuantile("b", 0.5), [Row(value=2.0)])
        self.assertEqual(self.ApproxQuantile("c", 0.5), [Row(value=5.0)])
        self.assertEqual(self.ApproxQuantile("b", 0.25), [Row(value=1.0)])

    def Uniqueness(self, columns, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Uniqueness(columns, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    def UniqueValueRatio(self, columns, where=None):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(UniqueValueRatio(columns, where)).run()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        return result_df.select("value").collect()

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_approxQuantiles(self):
        self.assertEqual(self.ApproxQuantiles("b", [0.2, 0.5, 0.73]), [Row(value=1.5), Row(value=2.0), Row(value=3.0)])

    def test_ApproxQuantiles(self):
        self.assertEqual(self.ApproxQuantiles("b", [0.25, 0.5, 0.75]), [Row(value=1.0), Row(value=2.0), Row(value=3.0)])
        self.assertEqual(self.ApproxQuantiles("c", [0.25, 0.5, 0.75]), [Row(value=5.0), Row(value=5.0), Row(value=6.0)])

    def test_Completeness(self):
        self.assertEqual(self.Completeness("b"), [Row(value=1.0)])
        self.assertEqual(self.Completeness("c"), [Row(value=2 / 3)])
        self.assertEqual(self.Completeness("a"), [Row(value=1)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Completeness(self):
        self.assertEqual(self.Completeness("c"), [Row(value=1.0)])

    def test_Compliance(self):
        self.assertEqual(self.Compliance("top b", "b >= 2"), [Row(value=2 / 3)])
        self.assertEqual(self.Compliance("c", "c >= 2"), [Row(value=2 / 3)])
        self.assertEqual(self.Compliance("b, e value", "b >=2 AND d >= 2"), [Row(value=1 / 3)])
        self.assertEqual(self.Compliance("find a", 'a = "foo"'), [Row(value=1 / 3)])

    def test_Correlation(self):
        self.assertEqual(self.Correlation("b", "c"), [Row(value=1.0)])
        self.assertEqual(self.Correlation("b", "d"), [Row(value=0.0)])
        self.assertEqual(self.Correlation("b", "a"), [])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Correlation(self):
        self.assertEqual(self.Correlation("b", "c"), [Row(value=-1.0)])

    def test_CountDistinct(self):
        self.assertEqual(self.CountDistinct("b"), [Row(value=3.0)])
        self.assertEqual(self.CountDistinct(["b", "c"]), [Row(value=3.0)])
        self.assertEqual(self.CountDistinct(["b", "d"]), [Row(value=3.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_CountDistinct(self):
        self.assertEqual(self.CountDistinct("b"), [Row(value=1.0)])

    def test_DataType(self):
        self.assertEqual(
            self.Datatype("b"),
            [
                Row(value=5.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=3.0),
                Row(value=1.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
            ],
        )
        self.assertEqual(
            self.Datatype("c"),
            [
                Row(value=5.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=2.0),
                Row(value=0.6666666666666666),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=0.0),
                Row(value=0.0),
            ],
        )

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Datatype(self):
        self.assertEqual(
            self.Datatype("c"),
            [
                Row(value=3.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=0.0),
                Row(value=2.0),
                Row(value=0.6666666666666666),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=0.0),
                Row(value=0.0),
            ],
        )

    def test_Distinctness(self):
        self.assertEqual(self.Distinctness("b"), [Row(value=1.0)])
        self.assertEqual(self.Distinctness(["b", "c"]), [Row(value=1.0)])
        self.assertEqual(self.Distinctness(["b", "d"]), [Row(value=1.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Distinctness(self):
        self.assertEqual(self.Distinctness("b"), [Row(value=0)])

    def test_Entropy(self):
        self.assertEqual(self.Entropy("b"), [Row(value=1.0986122886681096)])
        self.assertEqual(self.Entropy("a"), [Row(value=1.0986122886681096)])
        self.assertEqual(self.Entropy("c"), [Row(value=0.6931471805599453)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Entropy(self):
        self.assertEqual(self.Entropy("b"), [Row(value=0)])

    def test_Histogram(self):
        self.assertEqual(
            self.Histogram("b"),
            [
                Row(value=3.0),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
            ],
        )
        self.assertEqual(
            self.Histogram("c"),
            [
                Row(value=3.0),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
            ],
        )

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Histogram(self):
        self.assertEqual(
            self.Histogram("b"),
            [
                Row(value=2.0),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
            ],
        )

    def test_Histogram_maxBins(self):
        self.assertEqual(
            self.Histogram_maxBins("b", maxDetailBins=2),
            [
                Row(value=3.0),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
            ],
        )
        self.assertEqual(
            self.Histogram_maxBins("c", maxDetailBins=2),
            [
                Row(value=3.0),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
            ],
        )

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Histogram_maxBins(self):
        self.assertEqual(
            self.Histogram_maxBins("b"),
            [
                Row(value=2.0),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
                Row(value=1.0),
                Row(value=0.3333333333333333),
            ],
        )

    def test_Maximum(self):
        self.assertEqual(self.Maximum("b"), [Row(value=3.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Maximum(self):
        self.assertEqual(self.Maximum("c"), [Row(value=3.0)])

    def test_MaxLength(self):
        self.assertEqual(self.MaxLength("a"), [Row(value=3.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_MaxLength(self):
        self.assertEqual(self.MaxLength("b"), [Row(value=3.0)])

    def test_Mean(self):
        self.assertEqual(self.Mean("b"), [Row(value=2.0)])
        self.assertEqual(self.Mean("c"), [Row(value=11 / 2)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Mean(self):
        self.assertEqual(self.Mean("b"), [Row(value=3.0)])

    def test_Minimum(self):
        self.assertEqual(self.Minimum("b"), [Row(value=1.0)])
        self.assertEqual(self.Minimum("c"), [Row(value=5.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Minimum(self):
        self.assertEqual(self.Minimum("a"), [Row(value=3.0)])
        self.assertEqual(self.Minimum("b"), [Row(value=3.0)])

    def test_MinLength(self):
        self.assertEqual(self.MinLength("a"), [Row(value=3.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_MinLength(self):
        self.assertEqual(self.MinLength("a"), [])

    def test_MutualInformation(self):
        self.assertEqual(self.MutualInformation(["b", "c"]), [Row(value=0.7324081924454064)])
        self.assertEqual(self.MutualInformation(["b", "d"]), [Row(value=0.6365141682948128)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_MutualInformation(self):
        self.assertEqual(self.MutualInformation(["b", "d"]), [])

    # TODO: Revisit when PatternMatch class is sorted out
    def test_PatternMatch(self):
        result = (
            self.AnalysisRunner.onData(self.df).addAnalyzer(PatternMatch(column="a", pattern_regex="ba(r|z)")).run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_json = AnalyzerContext.successMetricsAsJson(self.spark, result)
        df_from_json = self.spark.read.json(self.sc.parallelize([result_json]))
        self.assertEqual(df_from_json.select("value").collect(), result_df.select("value").collect())
        self.assertEqual(result_df.select("value").collect(), [Row(value=0.0)])

    def test_Size(self):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Size()).run()
        # result_df = result.select('value').collect()
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_df_row = result_df.select("value").collect()
        self.assertEqual(result_df_row, [Row(value=3.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Size(self):
        result = self.AnalysisRunner.onData(self.df).addAnalyzer(Size()).run()
        result_df = result.select("value").collect()
        self.assertEqual(result_df, [Row(value=4.0)])

    def test_StandardDeviation(self):
        self.assertEqual(self.StandardDeviation("b"), [Row(value=0.816496580927726)])
        self.assertEqual(self.StandardDeviation("c"), [Row(value=0.5)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_StandardDeviation(self):
        self.assertEqual(self.StandardDeviation("c"), [Row(value=0.8)])

    def test_Sum(self):
        self.assertEqual(self.Sum("b"), [Row(value=6.0)])
        self.assertEqual(self.Sum("c"), [Row(value=11.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Sum(self):
        self.assertEqual(self.Sum("b"), [Row(value=3.0)])

    def test_Uniqueness(self):
        self.assertEqual(self.Uniqueness(["b", "c"]), [Row(value=1.0)])
        self.assertEqual(self.Uniqueness(["b", "d"]), [Row(value=1.0)])
        self.assertEqual(self.Uniqueness(["a", "a"]), [Row(value=1.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_Uniqueness(self):
        self.assertEqual(self.Uniqueness(["a", "a"]), [])

    def test_UniqueValueRatio(self):
        self.assertEqual(self.UniqueValueRatio(["b", "d"]), [Row(value=1.0)])
        self.assertEqual(self.UniqueValueRatio(["b"]), [Row(value=1.0)])

    @pytest.mark.skip(reason="@unittest.expectedFailure")
    def test_fail_UniqueValueRatio(self):
        self.assertEqual(self.UniqueValueRatio(["a", "a"]), [])


if __name__ == "__main__":
    unittest.main()
