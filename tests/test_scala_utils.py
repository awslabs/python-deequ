import pytest

from pydeequ.scala_utils import ScalaFunction1, ScalaFunction2
from pyspark.sql import SparkSession


class TestScalaUtils():

    @pytest.fixture(autouse=True)
    def _initialize(self, spark_session):
        self.spark = spark_session
        self.sc = self.spark.sparkContext


    def test_scala_function1(self):
        greaterThan10 = ScalaFunction1(self.sc._gateway, lambda x: x > 10)
        assert greaterThan10.apply(9) is False
        assert greaterThan10.apply(11) is True

        notNoneTest = ScalaFunction1(self.sc._gateway, lambda x: x is not None)
        assert notNoneTest.apply(None) is False
        assert notNoneTest.apply('foo') is True

        appendTest = ScalaFunction1(self.sc._gateway, lambda x: "{}test".format(x))
        assert "xtest" == appendTest.apply('x')

    def test_scala_function2(self):
        concatFunction = ScalaFunction2(self.sc._gateway, lambda x, y: x + y)
        assert "ab" == concatFunction.apply("a", "b")


