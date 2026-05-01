# -*- coding: utf-8 -*-
import unittest

from pydeequ.scala_utils import ScalaFunction1, ScalaFunction2
from tests.conftest import setup_pyspark


class TestScalaUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-scala-utils-local").getOrCreate()
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
        self.assertTrue(notNoneTest.apply("foo"))

        # Test hashCode()
        self.assertNotEqual(greaterThan10.hashCode(), notNoneTest.hashCode())
        self.assertTrue(isinstance(greaterThan10.hashCode(), int))

        appendTest = ScalaFunction1(self.sc._gateway, "{}test".format)
        self.assertEqual("xtest", appendTest.apply("x"))

    def test_scala_function2(self):
        lambda_func = lambda x, y: x + y
        concatFunction = ScalaFunction2(self.sc._gateway, lambda_func)
        self.assertEqual("ab", concatFunction.apply("a", "b"))

        anotherConcatFunction = ScalaFunction2(self.sc._gateway, lambda_func)

        # Test hashCode()
        self.assertEqual(concatFunction.hashCode(), anotherConcatFunction.hashCode())
        self.assertTrue(isinstance(concatFunction.hashCode(), int))


if __name__ == "__main__":
    unittest.main()
