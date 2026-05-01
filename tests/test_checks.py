# -*- coding: utf-8 -*-
import unittest

import pytest
from pyspark.sql import DataFrame, Row

from pydeequ.analyzers import KLLParameters
from pydeequ.checks import *
from pydeequ.verification import *
from tests.conftest import setup_pyspark


class TestChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-checkss-local").getOrCreate()
        cls.sc = cls.spark.sparkContext
        cls.df = cls.sc.parallelize(
            [
                Row(
                    a="foo",
                    b=1,
                    c=5,
                    d=5,
                    e=3,
                    f=1,
                    g="a",
                    h=0,
                    creditCard="5130566665286573",
                    email="foo@example.com",
                    ssn="123-45-6789",
                    URL="http://userid@example.com:8080",
                    boolean="true",
                ),
                Row(
                    a="bar",
                    b=2,
                    c=6,
                    d=5,
                    e=2,
                    f=2,
                    g="b",
                    h=-1,
                    creditCard="4532677117740914",
                    email="bar@example.com",
                    ssn="123456789",
                    URL="http://foo.com/(something)?after=parens",
                    boolean="false",
                ),
                Row(
                    a="baz",
                    b=3,
                    c=None,
                    d=5,
                    e=1,
                    f=1,
                    g=None,
                    h=2,
                    creditCard="340145324521741",
                    email="yourusername@example.com",
                    ssn="000-00-0000",
                    URL="http://userid@example.com:8080",
                    boolean="true",
                ),
            ]
        ).toDF()

    @classmethod
    def tearDownClass(cls):
        # Must shutdown callback for tests to stop
        # TODO Document this call to users or encapsulate in PyDeequSession
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_initializer(self):
        # TODO verify that it does more than run
        check = Check(self.spark, CheckLevel.Warning, "test initializer")

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()

        result_df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        result_df.show()

    def hasSize(self, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasSize")

        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasSize(assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def containsCreditCardNumber(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsCreditCardNumber")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.containsCreditCardNumber(column, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def containsEmail(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsEmail")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.containsEmail(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def containsURL(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsURL")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.containsURL(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def containsSocialSecurityNumber(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsSocialSecurityNumber")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.containsSocialSecurityNumber(column, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_message").collect()

    def hasDataType(self, column, datatype, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasDataType")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasDataType(column, datatype, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isComplete(self, column, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isComplete")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.isComplete(column, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isUnique(self, column, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isUnique")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.isUnique(column, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasUniqueness(self, columns, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasUniqueness")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.hasUniqueness(columns, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def areComplete(self, columns, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test areComplete")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.areComplete(columns, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def haveCompleteness(self, columns, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test haveCompleteness")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.haveCompleteness(columns, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def areAnyComplete(self, columns, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test areAnyComplete")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.areAnyComplete(columns, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasCompleteness(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasCompleteness")

        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.hasCompleteness(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def haveAnyCompleteness(self, columns, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test haveAnyCompleteness")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.haveAnyCompleteness(columns, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasDistinctness(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasDistinctness")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.hasDistinctness(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasUniqueValueRatio(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasUniqueValueRatio")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasUniqueValueRatio(column, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasEntropy(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasEntropy")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasEntropy(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasMutualInformation(self, columnA, columnB, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMutualInformation")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasMutualInformation(columnA, columnB, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasApproxQuantile(self, column, quantile, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasApproxQuantile")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasApproxQuantile(column, quantile, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasMinLength(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMinLength")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.hasMinLength(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasMaxLength(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMaxLength")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.hasMaxLength(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasMin(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMin")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasMin(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasMax(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMax")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasMax(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasMean(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMean")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasMean(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasSum(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasSum")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasSum(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasStandardDeviation(self, column, assertion, hint=None):
        """lambda has to be the exact standard deviation"""
        check = Check(self.spark, CheckLevel.Warning, "test hasStandardDeviation")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(
                check.hasStandardDeviation(
                    column,
                    assertion,
                    hint,
                )
            )
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasApproxCountDistinct(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasApproxCountDistinct")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasApproxCountDistinct(column, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasCorrelation(self, columnA, columnB, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasCorrelation")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasCorrelation(columnA, columnB, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isNonNegative(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isNonNegative")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(check.isNonNegative(column, assertion, hint)).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isPositive(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isPositive")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.isPositive(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isLessThan(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isLessThan")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.isLessThan(columnA, columnB, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def satisfies(self, columnCondition, constraintName, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test satisfies")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.satisfies(columnCondition, constraintName, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasPattern(self, column, pattern, assertion=None, name=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasPattern")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasPattern(column, pattern, assertion, name, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isLessThanOrEqualTo(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isLessThanOrEqualTo")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.isLessThanOrEqualTo(columnA, columnB, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isGreaterThan(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isGreaterThan")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.isGreaterThan(columnA, columnB, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isContainedIn(self, column, allowed_values, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isContainedIn")
        result = (
            VerificationSuite(self.spark).onData(self.df).addCheck(
                check.isContainedIn(column, allowed_values, assertion=assertion, hint=hint)
            ).run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def isGreaterThanOrEqualTo(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isGreaterThanOrEqualTo")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.isGreaterThanOrEqualTo(columnA, columnB, assertion, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def hasNumberOfDistinctValues(self, column, assertion, binningUdf, maxBins, hint=None):
        # Need to test binningUDF,  maxBins have to be int?
        check = Check(self.spark, CheckLevel.Warning, "test hasNumberOfDistinctValues")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(check.hasNumberOfDistinctValues(column, assertion, binningUdf, maxBins, hint))
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def where(self, assertion, filter, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test where")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasSize(assertion, hint).where(filter)).run()
        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def test_hasSize(self):
        self.assertEqual(self.hasSize(lambda x: x == 3.0), [Row(constraint_status="Success")])
        self.assertEqual(
            self.hasSize(lambda x: x >= 2.0 and x < 5.0, "size of dataframe should be 3"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasSize(lambda x: (x >= 2.0 and x < 5.0) and (x != 4.0), "size of dataframe should be 3"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasSize(lambda x: (x < 2.0 or x == 5.0), "size of dataframe should be 3"),
            [Row(constraint_status="Failure")],
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasSize(self):
        self.assertEqual(self.hasSize(lambda x: x >= 5.0, "size of dataframe should be 3"), [Row(constraint="Success")])
        self.assertEqual(
            self.hasSize(lambda x: (x > 2.0), "size of dataframe should be 3"), [Row(constraint_status="Failure")]
        )

    def test_containsCreditCardNumber(self):
        self.assertEqual(self.containsCreditCardNumber("creditCard"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 1.0, "All rows contain a credit card number"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 0), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.containsCreditCardNumber("ssn", lambda x: x == 1), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_containsCreditCardNumber(self):
        self.assertEqual(
            self.containsCreditCardNumber("5130566665286573", "Not a column"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 0.5), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 1), [Row(constraint_status="Failure")]
        )

    def test_containsEmail(self):
        self.assertEqual(self.containsEmail("email"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.containsEmail("email", lambda x: x == 1.0, "All rows contain an email"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.containsEmail("a", lambda x: x == 1, "No rows contain an email"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.containsEmail("email", lambda x: x == 0, "All rows contain an email"),
            [Row(constraint_status="Failure")],
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_containsEmail(self):
        self.assertEqual(
            self.containsEmail("a", lambda x: x == 0.5, "No rows contain an email"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.containsEmail("email", lambda x: x == 1, "All rows contain an email"),
            [Row(constraint_status="Failure")],
        )

    def test_containsURL(self):
        self.assertEqual(self.containsURL("URL"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.containsURL("URL", lambda x: x == 1.0, "All rows contain a URL"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.containsURL("email", lambda x: x == 1, "No rows contain an email"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.containsURL("URL", lambda x: x == 0, "All rows contain an email"), [Row(constraint_status="Failure")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_containsURL(self):
        self.assertEqual(
            self.containsURL("email", lambda x: x == 0.5, "No rows contain an email"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.containsEmail("http://userid@example.com:8080", lambda x: x == 0, "No rows contain an email"),
            [Row(constraint_status="Failure")],
        )

    def test_containsSocialSecurityNumber(self):
        self.assertEqual(
            self.containsSocialSecurityNumber("ssn"),
            [Row(constraint_message="Value: 0.6666666666666666 does not meet the constraint requirement!")],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("ssn", lambda x: x == 2 / 3, "2/3 has a value "),
            [Row(constraint_message="")],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("a", lambda x: x == 0, "No rows contain a ssn"),
            [Row(constraint_message="")],
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_containsSocialSecurityNumber(self):
        self.assertEqual(
            self.containsSocialSecurityNumber("a", lambda x: x == 0.5, "No rows contain a ssn"),
            [Row(constraint_message="")],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("123-45-6789", lambda x: x == 0, "No rows contain an ssn"),
            [Row(constraint_message="")],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("a"),
            [Row(constraint_message="Value: 0.6666666666666666 does not meet the constraint requirement!")],
        )

    def test_hasDataType(self):
        self.assertEqual(self.hasDataType("a", ConstrainableDataTypes.String), [Row(constraint_status="Success")])
        self.assertEqual(self.hasDataType("b", ConstrainableDataTypes.Numeric), [Row(constraint_status="Success")])
        self.assertEqual(
            self.hasDataType("boolean", ConstrainableDataTypes.Boolean), [Row(constraint_status="Success")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasDataType(self):
        self.assertEqual(
            self.hasDataType("nonexistent_row", ConstrainableDataTypes.Null), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.hasDataType("b", ConstrainableDataTypes.String), [Row(constraint_status="Success")])
        self.assertEqual(self.hasDataType("a", ConstrainableDataTypes.Numeric), [Row(constraint_status="Success")])

    def test_isComplete(self):
        self.assertEqual(
            self.isComplete("creditCard", "All rows contain a credit card number"), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.isComplete("c", "Is incomplete"), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isComplete(self):
        self.assertEqual(self.isComplete("c", "Is incomplete"), [Row(constraint_status="Success")])
        self.assertEqual(self.isComplete("a", "Is complete"), [Row(constraint_status="Failure")])

    def test_isUnique(self):
        self.assertEqual(self.isUnique("b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isUnique("b", "All rows are unique"), [Row(constraint_status="Success")])

        self.assertEqual(self.isUnique("d", "Is not unique"), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isUnique(self):
        self.assertEqual(self.isUnique("d"), [Row(constraint_status="Success")])
        self.assertEqual(self.isUnique("email" "All rows are unique"), [Row(constraint_status="Failure")])

    def test_hasUniqueness(self):
        self.assertEqual(
            self.hasUniqueness(["d", "a"], lambda x: x == 1, "There should be 1 uniqueness"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueness(["d"], lambda x: x == 1, "All rows are not unique"), [Row(constraint_status="Failure")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasUniqueness(self):
        self.assertEqual(
            self.hasUniqueness(["b", "e"], lambda x: x == 0, "There is 1 Uniqueness"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueness(["d", "d"], lambda x: x == 0, "There is 0 Uniqueness"),
            [Row(constraint_status="Failure")],
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_chained_call(self):
        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(
                check.hasUniqueness(["a"], lambda x: x == 1, "a should be unique").hasSize(
                    lambda x: x == 3, "size of dataframe should be 3"
                )
            )
            .run()
        )

        df = self.spark._jvm.com.amazon.deequ.VerificationResult.checkResultsAsDataFrame(
            self.spark._jsparkSession,
            result,
            getattr(self.spark._jvm.com.amazon.deequ.VerificationResult, "checkResultsAsDataFrame$default$3")(),
        )
        self.assertEqual(
            DataFrame(df, self.spark).select("constraint_status").collect(),
            [Row(constraint_status="Failure"), Row(constraint_status="Success")],
        )

        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(
                check.hasUniqueness(["h"], lambda x: x == 1, "h should be unique").hasSize(
                    lambda x: x == 5, "size of dataframe should be 3"
                )
            )
            .run()
        )

        df = self.spark._jvm.com.amazon.deequ.VerificationResult.checkResultsAsDataFrame(
            self.spark._jsparkSession,
            result,
            getattr(self.spark._jvm.com.amazon.deequ.VerificationResult, "checkResultsAsDataFrame$default$3")(),
        )
        self.assertEqual(
            DataFrame(df, self.spark).select("constraint_status").collect(),
            [Row(constraint_status="Success"), Row(constraint_status="Success")],
        )

    def test_chained_call(self):

        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(
                check.hasUniqueness(["b"], lambda x: x == 1, "b should be unique").hasSize(
                    lambda x: x == 3, "size of dataframe should be 3"
                )
            )
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        self.assertEqual(
            df.select("constraint_status").collect(),
            [Row(constraint_status="Success"), Row(constraint_status="Success")],
        )
        # Case #2
        check2 = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result2 = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(
                check2.hasUniqueness(["d"], lambda x: x == 1, "d is not ").hasSize(
                    lambda x: x < 3, "size of dataframe should be 3"
                )
            )
            .run()
        )

        df2 = VerificationResult.checkResultsAsDataFrame(self.spark, result2)
        self.assertEqual(
            df2.select("constraint_status").collect(),
            [Row(constraint_status="Failure"), Row(constraint_status="Failure")],
        )

    def test_areComplete(self):
        self.assertEqual(self.areComplete(["a", "b"], "Both Complete"), [Row(constraint_status="Success")])
        self.assertEqual(self.areComplete(["c"], "c is not complete"), [Row(constraint_status="Failure")])
        self.assertEqual(self.areComplete(["c", "b"], "c is not complete"), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_areComplete(self):
        self.assertEqual(self.areComplete(["email", "ssn"], "both complete"), [Row(constraint_status="Failure")])
        self.assertEqual(self.areComplete(["URL", "a"], "Both Complete"), [Row(constraint_status="Failure")])

    def test_haveCompleteness(self):
        self.assertEqual(
            self.haveCompleteness(["a", "c"], lambda x: x == 2 / 3, "C is incomplete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveCompleteness(["c"], lambda x: x == 1, "c is not complete"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.haveCompleteness(["a", "b"], lambda x: x == 1, "both are complete"), [Row(constraint_status="Success")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_haveCompleteness(self):
        self.assertEqual(
            self.haveCompleteness(["a", "ssn"], lambda x: x == 2 / 3, "both are complete"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.haveCompleteness(["URL", "email"], lambda x: x == 2 / 3, "Both are Complete"),
            [Row(constraint_status="Success")],
        )

    def test_areAnyComplete(self):
        self.assertEqual(
            self.areAnyComplete(["a", "b"], "both columns are complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.areAnyComplete(["c"], "c is not complete"), [Row(constraint_status="Failure")])
        self.assertEqual(self.areAnyComplete(["a", "c"], "Column a is complete"), [Row(constraint_status="Success")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_areComplete(self):
        self.assertEqual(self.areAnyComplete(["email", "ssn"], "both complete"), [Row(constraint_status="Failure")])
        self.assertEqual(self.areAnyComplete(["URL", "a"], "Both Complete"), [Row(constraint_status="Failure")])

    def test_hasCompleteness(self):
        self.assertEqual(
            self.hasCompleteness("a", lambda x: x == 1, "Column A is complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasCompleteness("c", lambda x: x == 1, "c is not complete"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasCompleteness("c", lambda x: x == 2 / 3, "c is not complete"), [Row(constraint_status="Success")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasCompleteness(self):
        self.assertEqual(
            self.hasCompleteness("URL", lambda x: x == 1, "URL is complete"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasCompleteness("c", lambda x: x == 1, "Both are Complete"), [Row(constraint_status="Success")]
        )

    def test_haveAnyCompleteness(self):
        self.assertEqual(
            self.haveAnyCompleteness(["c"], lambda x: x == 2 / 3, "Column c is incomplete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveAnyCompleteness(["a", "c"], lambda x: float(x) == 1, "Column A is complete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveAnyCompleteness(["a"], lambda x: x == 1, "a is complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.haveAnyCompleteness(["c"], lambda x: x == 1, "c is incomplete"), [Row(constraint_status="Failure")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_haveAnyCompleteness(self):
        self.assertEqual(
            self.haveAnyCompleteness(["c"], lambda x: x == 2 / 3, "Column c is incomplete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveAnyCompleteness(["a", "c"], lambda x: float(x) == 1, "Column A is complete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveAnyCompleteness(["a"], lambda x: x == 1, "a is complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.haveAnyCompleteness(["c"], lambda x: x == 1, "c is incomplete"), [Row(constraint_status="Success")]
        )

    def test_hasDistinctness(self):
        self.assertEqual(
            self.hasDistinctness(["c"], lambda x: x == 1, "Column c is distinct"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDistinctness(["d"], lambda x: x == 1 / 3, "Column d is not distinct"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasDistinctness(["b", "e"], lambda x: float(x) == 1, " distinct"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDistinctness(["b", "f"], lambda x: float(x) == 1, " distinct"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDistinctness(["f", "b"], lambda x: float(x) == 1, " distinct"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDistinctness(["d", "d"], lambda x: x == 0, "Column d is not distinct"),
            [Row(constraint_status="Failure")],
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasDistinctness(self):
        self.assertEqual(
            self.hasDistinctness("d", lambda x: x == 1, "URL is complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDistinctness("c", lambda x: x == 1, "Both are Complete"), [Row(constraint_status="Failure")]
        )

    def test_hasUniqueValueRatio(self):
        # 1, is unique
        # .5 is not completely unique
        self.assertEqual(
            self.hasUniqueValueRatio(["a", "b"], lambda x: float(x) == 1, "Both columns have unique values"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueValueRatio(["a"], lambda x: float(x) == 1, "Column A is unique"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueValueRatio(["f"], lambda x: x == 0.5, "f is not completely unique"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueValueRatio(["f", "d"], lambda x: x == 0.5, "f and d are not completely unique"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueValueRatio(["d"], lambda x: x == 1, "d is not unique"), [Row(constraint_status="Failure")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasUniqueValueRatio(self):
        self.assertEqual(
            self.hasUniqueValueRatio("d", lambda x: x == 1, "d is not unique"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasUniqueValueRatio("a", lambda x: x == 1, " a is Complete"), [Row(constraint_status="Failure")]
        )

    def hasEntropy(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasEntropy")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.hasEntropy(column, assertion, hint)).run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select("constraint_status").collect()

    def test_hasEntropy(self):
        self.assertEqual(
            self.hasEntropy("a", lambda x: x == 1.0986122886681096, "Has an entropy of 1.0986122886681096"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasEntropy("b", lambda x: x == 1.0986122886681096, "Column b is unique"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.hasEntropy("c", lambda x: x == 0.6931471805599453), [Row(constraint_status="Success")])
        self.assertEqual(self.hasEntropy("f", lambda x: x == 0.5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasEntropy(self):
        self.assertEqual(
            self.hasEntropy("a", lambda x: float(x) == 1.0986122886681096, "Has an entropy of 1.0986122886681096"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(self.hasEntropy("f", lambda x: x == 0.5), [Row(constraint_status="Success")])

    def test_hasMutualInformation(self):
        self.assertEqual(
            self.hasMutualInformation(
                "b", "e", lambda x: x == 1.0986122886681096, "Has an entropy of 1.0986122886681096"
            ),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasMutualInformation("a", "b", lambda x: x == 1.0986122886681096, "Column b is unique"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasMutualInformation("c", "b", lambda x: x == 0.7324081924454064), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.hasMutualInformation("f", "b", lambda x: x == 0.5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMutualInformation(self):
        self.assertEqual(
            self.hasMutualInformation(
                "b", "e", lambda x: x == 1.0986122886681096, "Has an entropy of 1.0986122886681096"
            ),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasMutualInformation("a", "b", lambda x: x == 1, "Column b is unique"),
            [Row(constraint_status="Success")],
        )

    def test_hasApproxQuantile(self):
        self.assertEqual(
            self.hasApproxQuantile("b", ".25", lambda x: x == 1, "Quantile of 1"), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.hasApproxQuantile("f", ".75", lambda x: x == 2), [Row(constraint_status="Success")])
        self.assertEqual(self.hasApproxQuantile("c", ".75", lambda x: x == 2), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMutualInformation(self):
        self.assertEqual(
            self.hasApproxQuantile("a", ".25", lambda x: x == 0, "Non string values only"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasApproxQuantile("b", ".25", lambda x: x == 1, "Quantile of 1"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasApproxQuantile("c", ".75", lambda x: x == 2), [Row(constraint_status="Success")])

    def test_hasMinLength(self):
        self.assertEqual(
            self.hasMinLength("email", lambda x: float(x) == 15, "Column email has 15 minimum characters"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasMinLength("creditCard", lambda x: x == 50, "does not meet criteria"),
            [Row(constraint_status="Failure")],
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMinLength(self):
        self.assertEqual(
            self.hasMinLength("b", lambda x: x == 0, "string values only"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasMinLength("g", lambda x: x == 1), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMinLength("g", lambda x: x == 5), [Row(constraint_status="Success")])

    def test_hasMaxLength(self):
        self.assertEqual(
            self.hasMaxLength("email", lambda x: x == 24, "Column email has 24 characters max"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasMaxLength("email", lambda x: x == 25, "does not meet criteria"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasMaxLength("email", lambda x: x > 20, "does not meet criteria"), [Row(constraint_status="Success")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMaxLength(self):
        self.assertEqual(
            self.hasMaxLength("b", lambda x: x == 0, "string values only"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasMaxLength("g", lambda x: x == 1), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMaxLength("g", lambda x: x == 5), [Row(constraint_status="Success")])

    def test_hasMin(self):
        self.assertEqual(
            self.hasMin("b", lambda x: x == 1, "Column b has a minimum of 1 "), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasMin("d", lambda x: x == 25, "does not meet criteria"), [Row(constraint_status="Failure")]
        )
        # See if it works with a None
        self.assertEqual(self.hasMin("c", lambda x: x == 5), [Row(constraint_status="Success")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMin(self):
        self.assertEqual(self.hasMin("b", lambda x: x == 0, "string values only"), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMin("c", lambda x: x == 6), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMin("g", lambda x: x == 5), [Row(constraint_status="Success")])

    def test_hasMax(self):
        self.assertEqual(
            self.hasMax("b", lambda x: x == 3, "Column b has a max of 3"), [Row(constraint_status="Success")]
        )
        # See if it works with a None
        self.assertEqual(self.hasMax("c", lambda x: x == 6), [Row(constraint_status="Success")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMax(self):
        self.assertEqual(self.hasMin("b", lambda x: x == 0), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMin("c", lambda x: x == 6), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMin("g", lambda x: x == 5), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMin("email", lambda x: x == 5), [Row(constraint_status="Success")])

    def test_hasMean(self):
        self.assertEqual(
            self.hasMean("b", lambda x: x == 2.0, "The mean of the column should be 2"),
            [Row(constraint_status="Success")],
        )
        # The none value is not included in the Number of rows
        self.assertEqual(self.hasMean("c", lambda x: x == 11 / 2), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMean("f", lambda x: x == 4 / 3), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMean("f", lambda x: x == 5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasMean(self):
        self.assertEqual(self.hasMean("b", lambda x: x == 0), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMean("c", lambda x: x == 11 / 2), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMean("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasSum(self):
        self.assertEqual(
            self.hasSum("b", lambda x: float(x) == 6.0, "The Sum of the column should be 6.0"),
            [Row(constraint_status="Success")],
        )
        # The none value is not included in the Number of rows
        self.assertEqual(self.hasSum("c", lambda x: x == 11), [Row(constraint_status="Success")])
        self.assertEqual(self.hasSum("f", lambda x: x > 3), [Row(constraint_status="Success")])
        self.assertEqual(self.hasSum("f", lambda x: x == 5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasSum(self):
        self.assertEqual(self.hasSum("b", lambda x: x == 6), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasSum("c", lambda x: x == 12), [Row(constraint_status="Success")])
        self.assertEqual(self.hasSum("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasStandardDeviation(self):
        self.assertEqual(
            self.hasStandardDeviation(
                "b", lambda x: x == 0.816496580927726, "Standard deviation should be .816496580927726"
            ),
            [Row(constraint_status="Success")],
        )
        # The none value is not included in Number of rows
        self.assertEqual(self.hasStandardDeviation("c", lambda x: x == 0.5), [Row(constraint_status="Success")])
        self.assertEqual(self.hasStandardDeviation("f", lambda x: x > 0.2), [Row(constraint_status="Success")])
        self.assertEqual(self.hasStandardDeviation("f", lambda x: x == 5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasStandardDeviation(self):
        self.assertEqual(
            self.hasStandardDeviation("b", lambda x: x == 0.816496580927726), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasStandardDeviation("c", lambda x: x == 12), [Row(constraint_status="Success")])
        self.assertEqual(self.hasStandardDeviation("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasApproxCountDistinct(self):
        self.assertEqual(
            self.hasApproxCountDistinct("b", lambda x: x == 3.0, "There should be 3 distinct values"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.hasApproxCountDistinct("c", lambda x: x == 2), [Row(constraint_status="Success")])
        self.assertEqual(self.hasApproxCountDistinct("f", lambda x: x > 1), [Row(constraint_status="Success")])
        self.assertEqual(self.hasApproxCountDistinct("f", lambda x: x == 5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasApproxCountDistinct(self):
        self.assertEqual(self.hasApproxCountDistinct("b", lambda x: x == 3), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasApproxCountDistinct("c", lambda x: x == 12), [Row(constraint_status="Success")])
        self.assertEqual(self.hasApproxCountDistinct("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasCorrelation(self):
        # Has to be non string / bool
        self.assertEqual(
            self.hasCorrelation("b", "e", lambda x: x == -1, "correlation should be -1"),
            [Row(constraint_status="Success")],
        )
        # can not have a None
        self.assertEqual(self.hasCorrelation("c", "d", lambda x: x == 2), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasCorrelation("f", "c", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(self.hasCorrelation("f", "c", lambda x: x == 5), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasCorrelation(self):
        self.assertEqual(self.hasCorrelation("b", "e", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(self.hasCorrelation("b", "e", lambda x: x == -1), [Row(constraint_status="Failure")])

    def test_satisfies(self):
        self.assertEqual(self.satisfies('b >=2', 'b greater than or equal to 2'),
                         [Row(constraint_status='Failure')])
        self.assertEqual(self.satisfies("b >=2", "b", lambda x: x == 2 / 3), [Row(constraint_status="Success")])
        self.assertEqual(
            self.satisfies("b >=2 AND d >= 2", "b and d", lambda x: x == 2 / 3), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.satisfies('a = "foo"', "find a", lambda x: x == 2 / 3), [Row(constraint_status="Failure")]
        )

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_satisfies(self):
        self.assertEqual(
            self.satisfies("b >=2 AND d >= 2", "b and d", lambda x: x == 5 / 6), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.satisfies('a = "zoo"', "find a", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_hasPattern(self):
        self.assertEqual(self.hasPattern("ssn", "\d{3}\-\d{2}\-\d{4}", lambda x: x == 2 / 3), [Row(constraint_status="Success")])
        # Default assertion is 1, thus failure
        self.assertEqual(self.hasPattern("ssn", "\d{3}\-\d{2}\-\d{4}"), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.hasPattern("ssn", "\d{3}\-\d{2}\-\d{4}", lambda x: x == 2 / 3, hint="it be should be above 0.66"),
            [Row(constraint_status="Success")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_hasPattern(self):
        self.assertEqual(self.hasPattern("ssn", r"\d{3}\-\d{2}\-\d{4}", lambda x: x == 2 / 3), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasPattern("ssn", r"\d{3}\d{2}\d{4}", lambda x: x == 2 / 3), [Row(constraint_status="Failure")])
        
    def test_isNonNegative(self):
        self.assertEqual(self.isNonNegative("b"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.isNonNegative("h", lambda x: x == 2 / 3, "All values of the column is non-negative"),
            [Row(constraint_status="Success")],
        )
        # Asserts a null value
        self.assertEqual(self.isNonNegative("c", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(self.isNonNegative("f", lambda x: x == 0), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isNonNegative(self):
        self.assertEqual(self.isNonNegative("b", lambda x: x == 1), [Row(constraint_status="Failure")])
        self.assertEqual(self.isNonNegative("e", lambda x: x == 0), [Row(constraint_status="Success")])

    def test_isPositive(self):
        self.assertEqual(self.isPositive("b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isPositive("b", lambda x: x == 1), [Row(constraint_status="Success")])
        # Asserts a null value
        self.assertEqual(self.isPositive("c", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(self.isPositive("f", lambda x: x == 0), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isPositive(self):
        self.assertEqual(self.isPositive("b", lambda x: x == 1), [Row(constraint_status="Failure")])
        self.assertEqual(self.isPositive("e", lambda x: x == 0), [Row(constraint_status="Success")])

    def test_isLessThan(self):
        self.assertEqual(
            self.isLessThan("b", "d", lambda x: x == 1, "Column B < Column D"), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.isLessThan("b", "d"), [Row(constraint_status="Success")])
        # Asserts a null value
        self.assertEqual(self.isLessThan("b", "c", lambda x: x == 2 / 3), [Row(constraint_status="Success")])
        self.assertEqual(self.isLessThan("d", "b", lambda x: x == 1), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isLessThan(self):
        self.assertEqual(
            self.isLessThan("b", "d", lambda x: x == 1, "Column B < Column D"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.isLessThan("e", "f", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_isLessThanOrEqualTo(self):
        self.assertEqual(
            self.isLessThanOrEqualTo("b", "d", lambda x: x == 1, "Column B < Column D"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isLessThanOrEqualTo("b", "d"), [Row(constraint_status="Success")])
        self.assertEqual(self.isLessThanOrEqualTo("b", "c"), [Row(constraint_status="Failure")])

    def test_isGreaterThan(self):
        self.assertEqual(
            self.isGreaterThan("d", "b", lambda x: x == 1, "Column B < Column D"), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.isGreaterThan("d", "b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isGreaterThan("c", "b"), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isGreaterThan(self):
        self.assertEqual(
            self.isGreaterThan("d", "b", lambda x: x == 1, "Column B < Column D"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.isGreaterThan("h", "f", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_isContainedIn(self):
        # test all variants for assertion and hint
        self.assertEqual(
            self.isContainedIn("a", ["foo", "bar", "baz"], lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(
            self.isContainedIn("a", ["foo", "bar", "baz"], lambda x: x == 1, hint="it should be 1"),
            [Row(constraint_status="Success")])
        self.assertEqual(self.isContainedIn("a", ["foo", "bar", "baz"]), [Row(constraint_status="Success")])
        # A none value makes the test still pass
        self.assertEqual(self.isContainedIn("c", ["5", "6"]), [Row(constraint_status="Success")])
        self.assertEqual(self.isContainedIn("b", ["15", "2", "3"]), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isContainedIn(self):
        self.assertEqual(self.isContainedIn("a", [1, 2, 3]), [Row(constraint_status="Success")])
        self.assertEqual(self.isContainedIn("f", [1, 2]), [Row(constraint_status="Failure")])

    def test_isGreaterThanOrEqualTo(self):
        self.assertEqual(
            self.isGreaterThanOrEqualTo("d", "b", lambda x: x == 1, "All values in column D is greater than column B"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isGreaterThanOrEqualTo("d", "b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isGreaterThanOrEqualTo("c", "b"), [Row(constraint_status="Failure")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_isGreaterThanOrEqualTo(self):
        self.assertEqual(
            self.isGreaterThanOrEqualTo("d", "b", lambda x: x == 1, "Column B < Column D"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(self.isGreaterThanOrEqualTo("h", "f", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_where(self):
        self.assertEqual(self.where(lambda x: x == 2.0, "boolean='true'", "column 'boolean' has two values true"),
                         [Row(constraint_status="Success")])
        self.assertEqual(
            self.where(lambda x: x == 3.0, "d=5", "column 'd' has three values 3"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.where(lambda x: x == 2.0, "ssn='000-00-0000'", "column 'ssn' has one value 000-00-0000"),
            [Row(constraint_status="Failure")],
        )
        check = Check(self.spark, CheckLevel.Warning, "test where").hasMin("f", lambda x: x == 2, "The f has min value 2 becasue of the additional filter").where('f>=2')
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check.isGreaterThan("e", "h", lambda x: x == 1, "Column H is not smaller than Column E")).run()
        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        self.assertEqual(
            df.select("constraint_status").collect(),
            [Row(constraint_status="Success"), Row(constraint_status="Failure")],
        )
        with self.assertRaises(TypeError):
            Check(self.spark, CheckLevel.Warning, "test where").kllSketchSatisfies(
                "b", lambda x: x.parameters().apply(0) == 1.0, KLLParameters(self.spark, 2, 0.64, 2)
            ).where("d=5")

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_where(self):
        self.assertEqual(self.where(lambda x: x == 2.0, "boolean='false'", "column 'boolean' has one value false"),
                         [Row(constraint_status="Success")])
        self.assertEqual(
            self.where(lambda x: x == 3.0, "a='bar'", "column 'a' has one value 'bar'"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.where(lambda x: x == 1.0, "f=1", "column 'f' has one value 1"),
            [Row(constraint_status="Failure")],
        )
    # def test_hasNumberOfDistinctValues(self):
    #     #Todo: test binningUDf
    #     self.assertEqual(self.hasNumberOfDistinctValues('b', lambda x: x == 3, None, 3, "Column B has 3 distinct values"),
    #         [Row(constraint_status='Success')])
    #

    # def test_isPrimaryKey(self):
    #     # TODO: figure out String*
    #     # Python tuple => varArgs
    #     check = Check(self.spark, CheckLevel.Warning, "test isPrimaryKey")
    #     result = VerificationSuite(self.spark).onData(self.df) \
    #         .addCheck(check.isPrimaryKey('b', ('a', 'b'))) \
    #         .run()
    #
    #     df = self.spark._jvm.com.amazon.deequ.VerificationResult.checkResultsAsDataFrame(
    #         self.spark._jsparkSession,
    #         result,
    #         getattr(self.spark._jvm.com.amazon.deequ.VerificationResult, "checkResultsAsDataFrame$default$3")()
    #     )
    #     print(DataFrame(df, self.spark).collect())
    #     print(result.toString())
    #
    # def test_hasHistogramValues(self):
    #     # Not sure what to put as an assertion
    #     check = Check(self.spark, CheckLevel.Warning, "test hasHistogramValues")
    #     result = VerificationSuite(self.spark).onData(self.df) \
    #         .addCheck(check.hasHistogramValues('b', lambda x: x == 1, None, 3)) \
    #         .run()
    #
    #     df = self.spark._jvm.com.amazon.deequ.VerificationResult.checkResultsAsDataFrame(
    #         self.spark._jsparkSession,
    #         result,
    #         getattr(self.spark._jvm.com.amazon.deequ.VerificationResult, "checkResultsAsDataFrame$default$3")()
    #     )
    #     print(DataFrame(df, self.spark).collect())
    #     print(result.toString())
    #
    def test_kllSketchSatisfies(self):
        # Not sure what to put as an assertion, Check with KLL values
        check = Check(self.spark, CheckLevel.Warning, "test kllSketchSatisfies")
        result = (
            VerificationSuite(self.spark)
            .onData(self.df)
            .addCheck(
                check.kllSketchSatisfies(
                    "b", lambda x: x.parameters().apply(0) == 1.0, KLLParameters(self.spark, 2, 0.64, 2)
                )
            )
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        df.show()
        print(df.select("constraint_message").collect())

    def test_list_of_constraints(self):
        check = Check(self.spark, CheckLevel.Warning, "test list of constraints")

        check.addConstraints([check.isComplete('b'),
                              check.containsEmail('email')])

        result = VerificationSuite(self.spark) \
                .onData(self.df).addCheck(check) \
                .run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        self.assertEqual(df.select("constraint_status").collect(), [Row(constraint_status="Success"), Row(constraint_status="Success")])

    @pytest.mark.xfail(reason="@unittest.expectedFailure")
    def test_fail_list_of_constraints(self):
        check = Check(self.spark, CheckLevel.Warning, "test list of constraints")

        check.addConstraints([check.isComplete('c'),
                              check.containsEmail('email')])

        result = VerificationSuite(self.spark) \
                .onData(self.df).addCheck(check) \
                .run()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        self.assertEqual(df.select("constraint_status").collect(), [Row(constraint_status="Success"), Row(constraint_status="Success")])

    def test_hash_code(self):
        """
        Lack of Exception is passing.  Previously this test would fail with:
        AttributeError: 'ScalaFunction1' object has no attribute 'hashCode'
        """
        vrb = VerificationSuite(self.spark) \
            .onData(self.df)
        check = Check(self.spark, CheckLevel.Error, "Enough checks to trigger a hashCode not an attribute of ScalaFunction1")
        check.isComplete('b')
        vrb.addCheck(check)
        check.containsEmail('email')
        vrb.addCheck(check)
        check.isGreaterThanOrEqualTo("d", "b")
        vrb.addCheck(check)
        check.isLessThanOrEqualTo("b", "d")
        vrb.addCheck(check)
        check.hasDataType("d", ConstrainableDataTypes.String, lambda x: x >= 1)
        vrb.addCheck(check)

        result = vrb.run()
