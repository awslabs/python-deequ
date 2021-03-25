import pytest

from pydeequ.profiles import ColumnProfilerRunBuilder, ColumnProfilerRunner
from pydeequ.analyzers import KLLParameters
from pyspark.sql import SparkSession, Row


class TestProfiles():
    @pytest.fixture(autouse=True)
    def _initialize(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext
        self.df = self.sc.parallelize([
            Row(a="foo", b=1, c=5),
            Row(a="bar", b=2, c=6),
            Row(a="baz", b=3, c=None)]).toDF()

    def assertEqual(self, expected, actual):
        assert expected == actual

    def assertIn(self, expected, actual):
        assert expected in actual

    def test_profile_run(self):
        result = ColumnProfilerRunner(self.spark) \
            .onData(self.df) \
            .run()
        for col, profile in result.profiles.items():
            print(profile)

        print(result.profiles['a'].column, result.profiles['a'].completeness)

    def test_kll_and_approxPercentiles(self):
        result = ColumnProfilerRunner(self.spark) \
            .onData(self.df) \
            .withKLLProfiling() \
            .setKLLParameters(KLLParameters(self.spark, 2, 0.64, 2)) \
            .run()
        for col, profile in result.profiles.items():
            print(profile)

        self.assertEqual(result.profiles['b'].kll.apply(1).lowValue, 2.0)
        self.assertEqual(result.profiles['b'].kll.apply(1).highValue, 3.0)
        self.assertEqual(result.profiles['b'].kll.apply(1).count, 2)
        self.assertEqual(result.profiles['b'].kll.argmax, 1)
        self.assertIn(1.0, result.profiles['b'].approxPercentiles)
        self.assertIn(2.0, result.profiles['b'].approxPercentiles)
        self.assertIn(3.0, result.profiles['b'].approxPercentiles)

    def test_spark_session_type_exception(self):
        try:
            ColumnProfilerRunner("foo")
            raise Exception("Did not raise TypeError")
        except TypeError:
            pass
        try:
            ColumnProfilerRunBuilder(5, self.df)
            raise Exception("Did not raise TypeError")
        except TypeError:
            pass
        try:
            ColumnProfilerRunBuilder(self.spark, "fail")
            raise Exception("Did not raise TypeError")
        except TypeError:
            pass
