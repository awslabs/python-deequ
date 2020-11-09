import unittest
from pydeequ.scala_utils import ScalaFunction1, ScalaFunction2
from pyspark.sql import SparkSession


class TestScalaUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # TODO share spark context between test cases?
        deequ_maven_coord = "com.amazon.deequ:deequ:1.0.3-rc2" # TODO get Maven Coord from Configs
        f2j_maven_coord = "net.sourceforge.f2j:arpack_combined_all" # This package is excluded because it causes an error in the SparkSession fig
        cls.spark = (SparkSession
                 .builder
                 .master('local[*]')
                 .config("spark.executor.memory", "2g")
                 .config("spark.jars.packages",
                         deequ_maven_coord).config("spark.jars.excludes", f2j_maven_coord)
                 .appName('test-scala-utils-local')
                 .getOrCreate())
        cls.sc = cls.spark.sparkContext

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_scala_function1(self):
        greaterThan10 = ScalaFunction1(self.sc._gateway, lambda x: x > 10)
        self.assertFalse(greaterThan10.apply(9))
        self.assertTrue(greaterThan10.apply(11))

        notNoneTest = ScalaFunction1(self.sc._gateway, lambda x: x is not None)
        self.assertFalse(notNoneTest.apply(None))
        self.assertTrue(notNoneTest.apply('foo'))

        appendTest = ScalaFunction1(self.sc._gateway, lambda x: "{}test".format(x))
        self.assertEqual("xtest", appendTest.apply('x'))

    def test_scala_function2(self):
        concatFunction = ScalaFunction2(self.sc._gateway, lambda x, y: x + y)
        self.assertEqual("ab", concatFunction.apply("a", "b"))


if __name__ == '__main__':
    unittest.main()
