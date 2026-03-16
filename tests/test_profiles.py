# -*- coding: utf-8 -*-
import unittest
from pyspark.sql import Row
from pydeequ.profiles import ColumnProfilerRunBuilder, ColumnProfilerRunner, DistributionValue, StringColumnProfile
from pydeequ.analyzers import KLLParameters, DataTypeInstances
from tests.conftest import setup_pyspark

class TestProfiles(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-profiles-local").getOrCreate()
        cls.sc = cls.spark.sparkContext
        cls.df = cls.sc.parallelize([Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="bazz", b=3, c=None)]).toDF()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_setPredefinedTypes(self):
        result = ColumnProfilerRunner(self.spark) \
            .onData(self.df) \
            .setPredefinedTypes({'a': DataTypeInstances.Unknown, 'b': DataTypeInstances.String, 'c': DataTypeInstances.Fractional}) \
            .run()
        print(result)
        for col, profile in result.profiles.items():
            print("Profiles:", profile)

    def test_profile_run(self):
        result = ColumnProfilerRunner(self.spark).onData(self.df).run()
        for col, profile in result.profiles.items():
            print(profile)
            print(f"col: {col} -> profile: {profile}")

        print("Results: ", result)
        print(result.profiles["a"].column, result.profiles["a"].completeness)

    def test_kll_and_approxPercentiles(self):
        result = (
            ColumnProfilerRunner(self.spark)
            .onData(self.df)
            .withKLLProfiling()
            .setKLLParameters(KLLParameters(self.spark, 2, 0.64, 2))
            .run()
        )
        for col, profile in result.profiles.items():
            print(f"col: {col} -> profile: {profile}")

        self.assertEqual(result.profiles["b"].kll.apply(1).lowValue, 2.0)
        self.assertEqual(result.profiles["b"].kll.apply(1).highValue, 3.0)
        self.assertEqual(result.profiles["b"].kll.apply(1).count, 2)
        self.assertEqual(result.profiles["b"].kll.argmax, 1)
        self.assertIn(1.0, result.profiles["b"].approxPercentiles)
        self.assertIn(2.0, result.profiles["b"].approxPercentiles)
        self.assertIn(3.0, result.profiles["b"].approxPercentiles)

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

    def test_StringColumnProfile(self):
        result = ColumnProfilerRunner(self.spark).onData(self.df).run()
        column_profile = result.profiles["a"]
        self.assertIsInstance(column_profile, StringColumnProfile)
        self.assertEqual(column_profile.minLength, 3)
        self.assertEqual(column_profile.maxLength, 4)

        self.assertEqual(column_profile.completeness, 1.0)
        self.assertEqual(column_profile.approximateNumDistinctValues, 3)
        self.assertEqual(column_profile.typeCounts["String"], 3)
        self.assertEqual(column_profile.isDataTypeInferred, False)
        self.assertListEqual(
            sorted(column_profile.histogram),
            [
                DistributionValue("bar", 1, 1/3),
                DistributionValue("bazz", 1, 1/3),
                DistributionValue("foo", 1, 1/3),
            ]
        )


if __name__ == "__main__":
    unittest.main()
