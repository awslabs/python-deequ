# -*- coding: utf-8 -*-
import json
import unittest

from pyspark.sql import Row

from pydeequ.suggestions import (
    DEFAULT,
    CategoricalRangeRule,
    CompleteIfCompleteRule,
    ConstraintSuggestionRunner,
    FractionalCategoricalRangeRule,
    NonNegativeNumbersRule,
    RetainCompletenessRule,
    RetainTypeRule,
    UniqueIfApproximatelyUniqueRule,
)
from tests.conftest import setup_pyspark


def _create_spark_dataframe(spark_context):
    column_a = ["foo"] * 10 + ["bar"] * 10 + ["baz"] * 10
    column_b = [1] * 5 + [2] * 15 + [3] * 10
    column_c = [5] * 15 + [6] * 10 + [None] * 5
    column_d = list(range(30))
    column_e = [True] * 10 + [False] * 20
    rows = [
        Row(a=a, b=b, c=c, d=d, e=e)
        for a, b, c, d, e in zip(column_a, column_b, column_c, column_d, column_e)
    ]
    return spark_context.parallelize(rows).toDF()


class TestSuggestions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-analyzers-local").getOrCreate()
        cls.ConstraintSuggestionRunner = ConstraintSuggestionRunner(cls.spark)
        cls.sc = cls.spark.sparkContext
        cls.df = _create_spark_dataframe(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_CategoricalRangeRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(CategoricalRangeRule()).run()
        )
        assert len(result["constraint_suggestions"]) > 0

    def test_CompleteIfCompleteRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(CompleteIfCompleteRule()).run()
        )
        assert len(result["constraint_suggestions"]) > 0

    def test_FractionalCategoricalRangeRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df)
            .addConstraintRule(FractionalCategoricalRangeRule())
            .run()
        )
        assert len(result["constraint_suggestions"]) > 0

    def test_NonNegativeNumbersRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(NonNegativeNumbersRule()).run()
        )
        assert len(result["constraint_suggestions"]) > 0

    def test_RetainCompletenessRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(RetainCompletenessRule()).run()
        )
        assert len(result["constraint_suggestions"]) > 0

    def test_RetainTypeRule(self):
        self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(RetainTypeRule()).run()

    def test_UniqueIfApproximatelyUniqueRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df)
            .addConstraintRule(UniqueIfApproximatelyUniqueRule())
            .run()
        )
        assert len(result["constraint_suggestions"]) > 0

    def test_default(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(DEFAULT()).run()
        assert len(result["constraint_suggestions"]) > 0
