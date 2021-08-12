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


class TestSuggestions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-analyzers-local").getOrCreate()
        cls.ConstraintSuggestionRunner = ConstraintSuggestionRunner(cls.spark)
        cls.sc = cls.spark.sparkContext
        cls.df = cls.sc.parallelize([Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]).toDF()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_CategoricalRangeRule(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(CategoricalRangeRule()).run()
        print(json.dumps(result, indent=1))

    def test_CompleteIfCompleteRule(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(CompleteIfCompleteRule()).run()
        print(json.dumps(result, indent=1))

    def test_FractionalCategoricalRangeRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(FractionalCategoricalRangeRule()).run()
        )
        print(json.dumps(result, indent=1))

    def test_NonNegativeNumbersRule(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(NonNegativeNumbersRule()).run()
        print(json.dumps(result, indent=1))

    def test_RetainCompletenessRule(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(RetainCompletenessRule()).run()
        print(json.dumps(result, indent=1))

    def test_RetainTypeRule(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(RetainTypeRule()).run()
        print(json.dumps(result, indent=1))

    def test_UniqueIfApproximatelyUniqueRule(self):
        result = (
            self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(UniqueIfApproximatelyUniqueRule()).run()
        )
        print(json.dumps(result, indent=1))

    def test_default(self):
        result = self.ConstraintSuggestionRunner.onData(self.df).addConstraintRule(DEFAULT()).run()
        print(json.dumps(result, indent=1))
