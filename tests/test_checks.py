# -*- coding: utf-8 -*-
import unittest
from typing import Union

from py4j.protocol import Py4JError
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

from pydeequ.analyzers import KLLParameters
from pydeequ.checks import Check, CheckLevel, ConstrainableDataTypes
from pydeequ.scala_utils import scala_map_to_dict
from pydeequ.verification import VerificationResult, VerificationSuite
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
        check = Check(self.spark, CheckLevel.Warning, "test initializer")
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        result_df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        self.assertEqual(result_df.collect(), [])
        self.assertEqual(
            result_df.schema,
            StructType(
                [
                    StructField("check", StringType()),
                    StructField("check_level", StringType()),
                    StructField("check_status", StringType()),
                    StructField("constraint", StringType()),
                    StructField("constraint_status", StringType()),
                    StructField("constraint_message", StringType()),
                ]
            ),
        )

    def run_check(self, check: Check, columns: Union[str, list[str]] = "constraint_status") -> list[Row]:
        columns = [columns] if isinstance(columns, str) else columns
        result = VerificationSuite(self.spark).onData(self.df).addCheck(check).run()
        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        return df.select(*columns).collect()

    def hasSize(self, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        return self.run_check(check.hasSize(assertion, hint))

    def containsCreditCardNumber(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsCreditCardNumber")
        return self.run_check(check.containsCreditCardNumber(column, assertion, hint))

    def containsEmail(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsEmail")
        return self.run_check(check.containsEmail(column, assertion, hint))

    def containsURL(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsURL")
        return self.run_check(check.containsURL(column, assertion, hint))

    def containsSocialSecurityNumber(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test containsSocialSecurityNumber")
        return self.run_check(
            check.containsSocialSecurityNumber(column, assertion, hint),
            ["constraint_status", "constraint_message"]
        )

    def hasDataType(self, column, datatype, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasDataType")
        return self.run_check(check.hasDataType(column, datatype, assertion, hint))

    def isComplete(self, column, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isComplete")
        return self.run_check(check.isComplete(column, hint))

    def isUnique(self, column, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isUnique")
        return self.run_check(check.isUnique(column, hint))

    def hasUniqueness(self, columns, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasUniqueness")
        return self.run_check(check.hasUniqueness(columns, assertion, hint))

    def areComplete(self, columns, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test areComplete")
        return self.run_check(check.areComplete(columns, hint))

    def haveCompleteness(self, columns, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test haveCompleteness")
        return self.run_check(check.haveCompleteness(columns, assertion, hint))

    def areAnyComplete(self, columns, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test areAnyComplete")
        return self.run_check(check.areAnyComplete(columns, hint))

    def hasCompleteness(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasCompleteness")
        return self.run_check(check.hasCompleteness(column, assertion, hint))

    def haveAnyCompleteness(self, columns, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test haveAnyCompleteness")
        return self.run_check(check.haveAnyCompleteness(columns, assertion, hint))

    def hasDistinctness(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasDistinctness")
        return self.run_check(check.hasDistinctness(column, assertion, hint))

    def hasUniqueValueRatio(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasUniqueValueRatio")
        return self.run_check(check.hasUniqueValueRatio(column, assertion, hint))

    def hasEntropy(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasEntropy")
        return self.run_check(check.hasEntropy(column, assertion, hint))

    def hasMutualInformation(self, columnA, columnB, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMutualInformation")
        return self.run_check(check.hasMutualInformation(columnA, columnB, assertion, hint))

    def hasApproxQuantile(self, column, quantile, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasApproxQuantile")
        return self.run_check(check.hasApproxQuantile(column, quantile, assertion, hint))

    def hasMinLength(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMinLength")
        return self.run_check(check.hasMinLength(column, assertion, hint))

    def hasMaxLength(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMaxLength")
        return self.run_check(check.hasMaxLength(column, assertion, hint))

    def hasMin(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMin")
        return self.run_check(check.hasMin(column, assertion, hint))

    def hasMax(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMax")
        return self.run_check(check.hasMax(column, assertion, hint))

    def hasMean(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasMean")
        return self.run_check(check.hasMean(column, assertion, hint))

    def hasSum(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasSum")
        return self.run_check(check.hasSum(column, assertion, hint))

    def hasStandardDeviation(self, column, assertion, hint=None):
        """lambda has to be the exact standard deviation"""
        check = Check(self.spark, CheckLevel.Warning, "test hasStandardDeviation")
        return self.run_check(check.hasStandardDeviation(column, assertion, hint))

    def hasApproxCountDistinct(self, column, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasApproxCountDistinct")
        return self.run_check(check.hasApproxCountDistinct(column, assertion, hint))

    def hasCorrelation(self, columnA, columnB, assertion, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasCorrelation")
        return self.run_check(check.hasCorrelation(columnA, columnB, assertion, hint))

    def isNonNegative(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isNonNegative")
        return self.run_check(check.isNonNegative(column, assertion, hint))

    def isPositive(self, column, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isPositive")
        return self.run_check(check.isPositive(column, assertion, hint))

    def isLessThan(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isLessThan")
        return self.run_check(check.isLessThan(columnA, columnB, assertion, hint))

    def satisfies(self, columnCondition, constraintName, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test satisfies")
        return self.run_check(check.satisfies(columnCondition, constraintName, assertion, hint))

    def hasPattern(self, column, pattern, assertion=None, name=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test hasPattern")
        return self.run_check(check.hasPattern(column, pattern, assertion, name, hint))

    def isLessThanOrEqualTo(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isLessThanOrEqualTo")
        return self.run_check(check.isLessThanOrEqualTo(columnA, columnB, assertion, hint))

    def isGreaterThan(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isGreaterThan")
        return self.run_check(check.isGreaterThan(columnA, columnB, assertion, hint))

    def isContainedIn(self, column, allowed_values, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isContainedIn")
        return self.run_check(check.isContainedIn(column, allowed_values, assertion=assertion, hint=hint))

    def isGreaterThanOrEqualTo(self, columnA, columnB, assertion=None, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test isGreaterThanOrEqualTo")
        return self.run_check(check.isGreaterThanOrEqualTo(columnA, columnB, assertion, hint))

    def hasNumberOfDistinctValues(self, column, assertion, binningUdf, maxBins, hint=None):
        # TODO: Need to test binningUdf, maxBins have to be int?
        check = Check(self.spark, CheckLevel.Warning, "test hasNumberOfDistinctValues")
        return self.run_check(check.hasNumberOfDistinctValues(column, assertion, binningUdf, maxBins, hint))

    def where(self, assertion, filter, hint=None):
        check = Check(self.spark, CheckLevel.Warning, "test where")
        return self.run_check(check.hasSize(assertion, hint).where(filter))

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
            self.hasSize(lambda x: (x > 2.0), "size of dataframe should be 3"),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasSize(self):
        self.assertEqual(
            self.hasSize(lambda x: (x < 2.0 or x == 5.0), "size of dataframe should be 3"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasSize(lambda x: x >= 5.0, "size of dataframe should be 3"),
            [Row(constraint_status="Failure")],
        )

    def test_containsCreditCardNumber(self):
        self.assertEqual(self.containsCreditCardNumber("creditCard"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.containsCreditCardNumber(
                "creditCard", lambda x: x == 1.0, "All rows contain a credit card number"
            ),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 1), [Row(constraint_status="Success")]
        )

    def test_fail_containsCreditCardNumber(self):
        self.assertEqual(
            self.containsCreditCardNumber("5130566665286573", "Not a column"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 0), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.containsCreditCardNumber("creditCard", lambda x: x == 0.5),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsCreditCardNumber("ssn", lambda x: x == 1), [Row(constraint_status="Failure")]
        )

    def test_containsEmail(self):
        self.assertEqual(self.containsEmail("email"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.containsEmail("email", lambda x: x == 1.0, "All rows contain an email"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.containsEmail("email", lambda x: x == 1, "All rows contain an email"),
            [Row(constraint_status="Success")],
        )

    def test_fail_containsEmail(self):
        self.assertEqual(
            self.containsEmail("email", lambda x: x == 0, "All rows contain an email"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsEmail("a", lambda x: x == 0.5, "No rows contain an email"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsEmail("a", lambda x: x == 1, "No rows contain an email"),
            [Row(constraint_status="Failure")],
        )

    def test_containsURL(self):
        self.assertEqual(self.containsURL("URL"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.containsURL("URL", lambda x: x == 1.0, "All rows contain a URL"),
            [Row(constraint_status="Success")],
        )

    def test_fail_containsURL(self):
        self.assertEqual(
            self.containsURL("email", lambda x: x == 0.5, "No rows contain an email"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsEmail(
                "http://userid@example.com:8080", lambda x: x == 0, "No rows contain an email"
            ),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsURL("email", lambda x: x == 1, "No rows contain an email"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.containsURL("URL", lambda x: x == 0, "All rows contain an email"),
            [Row(constraint_status="Failure")],
        )

    def test_containsSocialSecurityNumber(self):
        self.assertEqual(
            self.containsSocialSecurityNumber("ssn", lambda x: x == 2 / 3, "2/3 has a value "),
            [Row(constraint_status="Success", constraint_message="")],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("a", lambda x: x == 0, "No rows contain a ssn"),
            [Row(constraint_status="Success", constraint_message="")],
        )

    def test_fail_containsSocialSecurityNumber(self):
        self.assertEqual(
            self.containsSocialSecurityNumber("ssn"),
            [
                Row(
                    constraint_status="Failure",
                    constraint_message="Value: 0.6666666666666666 does not meet the constraint requirement!"
                )
            ],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("a", lambda x: x == 0.5, "No rows contain a ssn"),
            [
                Row(
                    constraint_status="Failure",
                    constraint_message="Value: 0.0 does not meet the constraint requirement! No rows contain a ssn"
                )
            ],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("123-45-6789", lambda x: x == 0, "No rows contain an ssn"),
            [
                Row(
                    constraint_status="Failure",
                    constraint_message="Input data does not include column 123-45-6789!"
                )
            ],
        )
        self.assertEqual(
            self.containsSocialSecurityNumber("a"),
            [
                Row(
                    constraint_status="Failure",
                    constraint_message="Value: 0.0 does not meet the constraint requirement!"
                )
            ],
        )

    def test_hasDataType(self):
        self.assertEqual(
            self.hasDataType("a", ConstrainableDataTypes.String), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDataType("b", ConstrainableDataTypes.Numeric), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasDataType("boolean", ConstrainableDataTypes.Boolean), [Row(constraint_status="Success")]
        )

    def test_fail_hasDataType(self):
        self.assertEqual(
            self.hasDataType("nonexistent_row", ConstrainableDataTypes.Null),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasDataType("b", ConstrainableDataTypes.String), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasDataType("a", ConstrainableDataTypes.Numeric), [Row(constraint_status="Failure")]
        )

    def test_isComplete(self):
        self.assertEqual(
            self.isComplete("creditCard", "All rows contain a credit card number"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isComplete("a", "Is complete"), [Row(constraint_status="Success")])

    def test_fail_isComplete(self):
        self.assertEqual(self.isComplete("c", "Is incomplete"), [Row(constraint_status="Failure")])

    def test_isUnique(self):
        self.assertEqual(self.isUnique("b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isUnique("b", "All rows are unique"), [Row(constraint_status="Success")])

    def test_fail_isUnique(self):
        self.assertEqual(self.isUnique("d"), [Row(constraint_status="Failure")])
        self.assertEqual(self.isUnique("email" "All rows are unique"), [Row(constraint_status="Failure")])

    def test_hasUniqueness(self):
        self.assertEqual(
            self.hasUniqueness(["d", "a"], lambda x: x == 1, "There should be 1 uniqueness"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueness(["d", "d"], lambda x: x == 0, "There is 0 Uniqueness"),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasUniqueness(self):
        self.assertEqual(
            self.hasUniqueness(["d"], lambda x: x == 1, "All rows are not unique"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasUniqueness(["b", "e"], lambda x: x == 0, "There is 1 Uniqueness"),
            [Row(constraint_status="Failure")],
        )

    def test_chained_call(self):
        # Case #1
        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = self.run_check(
            check.hasUniqueness(["b"], lambda x: x == 1, "b should be unique")
            .hasSize(lambda x: x == 3, "size of dataframe should be 3")
        )
        self.assertEqual(result, [Row(constraint_status="Success"), Row(constraint_status="Success")])

        # Case #2
        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = self.run_check(
            check.hasUniqueness(["a"], lambda x: x == 1, "a should be unique")
            .hasSize(lambda x: x == 3, "size of dataframe should be 3")
        )
        self.assertEqual(result, [Row(constraint_status="Success"), Row(constraint_status="Success")])

    def test_fail_chained_call(self):
        # Case #1
        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = self.run_check(
            check.hasUniqueness(["d"], lambda x: x == 1, "d is not ")
            .hasSize(lambda x: x < 3, "size of dataframe should be 3")
        )
        self.assertEqual(result, [Row(constraint_status="Failure"), Row(constraint_status="Failure")])

        # Case #2
        check = Check(self.spark, CheckLevel.Warning, "test chained_call")
        result = self.run_check(
            check.hasUniqueness(["h"], lambda x: x == 1, "h should be unique")
            .hasSize(lambda x: x == 5, "size of dataframe should be 3")
        )
        self.assertEqual(result, [Row(constraint_status="Success"), Row(constraint_status="Failure")])

    def test_areComplete(self):
        self.assertEqual(self.areComplete(["a", "b"], "Both Complete"), [Row(constraint_status="Success")])
        self.assertEqual(
            self.areComplete(["email", "ssn"], "both complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(self.areComplete(["URL", "a"], "Both Complete"), [Row(constraint_status="Success")])

    def test_fail_areComplete(self):
        self.assertEqual(self.areComplete(["c"], "c is not complete"), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.areComplete(["c", "b"], "c is not complete"), [Row(constraint_status="Failure")]
        )

    def test_haveCompleteness(self):
        self.assertEqual(
            self.haveCompleteness(["a", "c"], lambda x: x == 2 / 3, "C is incomplete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveCompleteness(["a", "b"], lambda x: x == 1, "both are complete"),
            [Row(constraint_status="Success")],
        )

    def test_fail_haveCompleteness(self):
        self.assertEqual(
            self.haveCompleteness(["a", "ssn"], lambda x: x == 2 / 3, "both are complete"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.haveCompleteness(["c"], lambda x: x == 1, "c is not complete"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.haveCompleteness(["URL", "email"], lambda x: x == 2 / 3, "Both are Complete"),
            [Row(constraint_status="Failure")],
        )

    def test_areAnyComplete(self):
        self.assertEqual(
            self.areAnyComplete(["a", "b"], "both columns are complete"), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.areAnyComplete(["a", "c"], "Column a is complete"), [Row(constraint_status="Success")]
        )

    def test_fail_areAnyComplete(self):
        self.assertEqual(self.areAnyComplete(["c"], "c is not complete"), [Row(constraint_status="Failure")])

    def test_hasCompleteness(self):
        self.assertEqual(
            self.hasCompleteness("a", lambda x: x == 1, "Column A is complete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasCompleteness("c", lambda x: x == 2 / 3, "c is not complete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasCompleteness("URL", lambda x: x == 1, "URL is complete"),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasCompleteness(self):
        self.assertEqual(
            self.hasCompleteness("c", lambda x: x == 1, "c is not complete"),
            [Row(constraint_status="Failure")],
        )

    def test_haveAnyCompleteness(self):
        self.assertEqual(
            self.haveAnyCompleteness(["a"], lambda x: x == 1, "a is complete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveAnyCompleteness(["c"], lambda x: x == 2 / 3, "Column c is incomplete"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.haveAnyCompleteness(["a", "c"], lambda x: float(x) == 1, "Column A is complete"),
            [Row(constraint_status="Success")],
        )

    def test_fail_haveAnyCompleteness(self):
        self.assertEqual(
            self.haveAnyCompleteness(["c"], lambda x: x == 1, "c is incomplete"),
            [Row(constraint_status="Failure")],
        )

    def test_hasDistinctness(self):
        self.assertEqual(
            self.hasDistinctness(["c"], lambda x: x == 1, "Column c is distinct"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasDistinctness(["d"], lambda x: x == 1 / 3, "Column d is not distinct"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasDistinctness(["b", "e"], lambda x: float(x) == 1, " distinct"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasDistinctness(["b", "f"], lambda x: float(x) == 1, " distinct"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasDistinctness(["f", "b"], lambda x: float(x) == 1, " distinct"),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasDistinctness(self):
        self.assertEqual(
            self.hasDistinctness(["d"], lambda x: x == 1, "d is complete"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasDistinctness(["d", "d"], lambda x: x == 0, "Column d is not distinct"),
            [Row(constraint_status="Failure")],
        )

    def test_hasUniqueValueRatio(self):
        # 1, is unique
        # .5 is not completely unique
        self.assertEqual(
            self.hasUniqueValueRatio(["a", "b"], lambda x: float(x) == 1, "Both columns have unique values"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasUniqueValueRatio(["a"], lambda x: x == 1, "a is unique"),
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

    def test_fail_hasUniqueValueRatio(self):
        self.assertEqual(
            self.hasUniqueValueRatio(["d"], lambda x: x == 1, "d is not unique"),
            [Row(constraint_status="Failure")],
        )

    def test_hasEntropy(self):
        self.assertEqual(
            self.hasEntropy("a", lambda x: x == 1.0986122886681096, "Has an entropy of 1.0986122886681096"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasEntropy("b", lambda x: x == 1.0986122886681096, "Column b is unique"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasEntropy("c", lambda x: x == 0.6931471805599453), [Row(constraint_status="Success")]
        )

    def test_fail_hasEntropy(self):
        self.assertEqual(self.hasEntropy("f", lambda x: x == 0.5), [Row(constraint_status="Failure")])

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
            self.hasMutualInformation("c", "b", lambda x: x == 0.7324081924454064),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasMutualInformation(self):
        self.assertEqual(
            self.hasMutualInformation("a", "b", lambda x: x == 1, "Column b is unique"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasMutualInformation("f", "b", lambda x: x == 0.5), [Row(constraint_status="Failure")]
        )

    def test_hasApproxQuantile(self):
        self.assertEqual(
            self.hasApproxQuantile("b", ".25", lambda x: x == 1, "Quantile of 1"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasApproxQuantile("f", ".75", lambda x: x == 2), [Row(constraint_status="Success")]
        )

    def test_fail_hasApproxQuantile(self):
        self.assertEqual(
            self.hasApproxQuantile("a", ".25", lambda x: x == 0, "Non string values only"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.hasApproxQuantile("c", ".75", lambda x: x == 2), [Row(constraint_status="Failure")]
        )

    def test_hasMinLength(self):
        self.assertEqual(self.hasMinLength("g", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(
            self.hasMinLength("email", lambda x: float(x) == 15, "Column email has 15 minimum characters"),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasMinLength(self):
        self.assertEqual(
            self.hasMinLength("b", lambda x: x == 0, "string values only"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasMinLength("g", lambda x: x == 5), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.hasMinLength("creditCard", lambda x: x == 50, "does not meet criteria"),
            [Row(constraint_status="Failure")],
        )

    def test_hasMaxLength(self):
        self.assertEqual(self.hasMaxLength("g", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(
            self.hasMaxLength("email", lambda x: x == 24, "Column email has 24 characters max"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasMaxLength("email", lambda x: x > 20, "does not meet criteria"),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasMaxLength(self):
        self.assertEqual(
            self.hasMaxLength("b", lambda x: x == 0, "string values only"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasMaxLength("g", lambda x: x == 5), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.hasMaxLength("email", lambda x: x == 25, "does not meet criteria"),
            [Row(constraint_status="Failure")],
        )

    def test_hasMin(self):
        self.assertEqual(
            self.hasMin("b", lambda x: x == 1, "Column b has a minimum of 1 "),
            [Row(constraint_status="Success")],
        )
        # See if it works with a None
        self.assertEqual(self.hasMin("c", lambda x: x == 5), [Row(constraint_status="Success")])

    def test_fail_hasMin(self):
        self.assertEqual(
            self.hasMin("b", lambda x: x == 0, "string values only"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasMin("c", lambda x: x == 6), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.hasMin("d", lambda x: x == 25, "does not meet criteria"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasMin("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasMax(self):
        self.assertEqual(
            self.hasMax("b", lambda x: x == 3, "Column b has a max of 3"), [Row(constraint_status="Success")]
        )
        # See if it works with a None
        self.assertEqual(self.hasMax("c", lambda x: x == 6), [Row(constraint_status="Success")])

    def test_fail_hasMax(self):
        self.assertEqual(self.hasMin("b", lambda x: x == 0), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMin("c", lambda x: x == 6), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMin("g", lambda x: x == 5), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMin("email", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasMean(self):
        self.assertEqual(
            self.hasMean("b", lambda x: x == 2.0, "The mean of the column should be 2"),
            [Row(constraint_status="Success")],
        )
        # The none value is not included in the Number of rows
        self.assertEqual(self.hasMean("c", lambda x: x == 11 / 2), [Row(constraint_status="Success")])
        self.assertEqual(self.hasMean("f", lambda x: x == 4 / 3), [Row(constraint_status="Success")])

    def test_fail_hasMean(self):
        self.assertEqual(self.hasMean("b", lambda x: x == 0), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMean("f", lambda x: x == 5), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasMean("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasSum(self):
        self.assertEqual(
            self.hasSum("b", lambda x: float(x) == 6.0, "The Sum of the column should be 6.0"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.hasSum("b", lambda x: x == 6), [Row(constraint_status="Success")])
        self.assertEqual(self.hasSum("c", lambda x: x == 11), [Row(constraint_status="Success")])
        self.assertEqual(self.hasSum("f", lambda x: x > 3), [Row(constraint_status="Success")])

    def test_fail_hasSum(self):
        self.assertEqual(self.hasSum("c", lambda x: x == 12), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasSum("f", lambda x: x == 5), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasSum("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasStandardDeviation(self):
        self.assertEqual(
            self.hasStandardDeviation(
                "b", lambda x: x == 0.816496580927726, "Standard deviation should be .816496580927726"
            ),
            [Row(constraint_status="Success")],
        )
        # The none value is not included in Number of rows
        self.assertEqual(
            self.hasStandardDeviation("b", lambda x: x == 0.816496580927726),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasStandardDeviation("c", lambda x: x == 0.5), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasStandardDeviation("f", lambda x: x > 0.2), [Row(constraint_status="Success")]
        )

    def test_fail_hasStandardDeviation(self):
        self.assertEqual(
            self.hasStandardDeviation("c", lambda x: x == 12), [Row(constraint_status="Failure")]
        )
        self.assertEqual(self.hasStandardDeviation("f", lambda x: x == 5), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasStandardDeviation("g", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_hasApproxCountDistinct(self):
        self.assertEqual(
            self.hasApproxCountDistinct("b", lambda x: x == 3.0, "There should be 3 distinct values"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasApproxCountDistinct("b", lambda x: x == 3), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasApproxCountDistinct("c", lambda x: x == 2), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.hasApproxCountDistinct("f", lambda x: x > 1), [Row(constraint_status="Success")]
        )

    def test_fail_hasApproxCountDistinct(self):
        self.assertEqual(
            self.hasApproxCountDistinct("c", lambda x: x == 12), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasApproxCountDistinct("f", lambda x: x == 5), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.hasApproxCountDistinct("g", lambda x: x == 5), [Row(constraint_status="Failure")]
        )

    def test_hasCorrelation(self):
        # Has to be non-string / bool
        self.assertEqual(
            self.hasCorrelation("b", "e", lambda x: x == -1, "correlation should be -1"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.hasCorrelation("f", "c", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_fail_hasCorrelation(self):
        self.assertEqual(self.hasCorrelation("b", "e", lambda x: x == 1), [Row(constraint_status="Failure")])
        # cannot have a None
        self.assertEqual(self.hasCorrelation("c", "d", lambda x: x == 2), [Row(constraint_status="Failure")])
        self.assertEqual(self.hasCorrelation("f", "c", lambda x: x == 5), [Row(constraint_status="Failure")])

    def test_satisfies(self):
        self.assertEqual(
            self.satisfies("b >=2", "b", lambda x: x == 2 / 3), [Row(constraint_status="Success")]
        )
        self.assertEqual(
            self.satisfies("b >=2 AND d >= 2", "b and d", lambda x: x == 2 / 3),
            [Row(constraint_status="Success")],
        )

    def test_fail_satisfies(self):
        self.assertEqual(
            self.satisfies('a = "foo"', "find a", lambda x: x == 2 / 3), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.satisfies("b >=2", "b greater than or equal to 2"), [Row(constraint_status="Failure")]
        )
        self.assertEqual(
            self.satisfies("b >=2 AND d >= 2", "b and d", lambda x: x == 5 / 6),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.satisfies('a = "zoo"', "find a", lambda x: x == 1), [Row(constraint_status="Failure")]
        )

    def test_hasPattern(self):
        self.assertEqual(
            self.hasPattern("ssn", "\d{3}\-\d{2}\-\d{4}", lambda x: x == 2 / 3),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasPattern(
                "ssn", "\d{3}\-\d{2}\-\d{4}", lambda x: x == 2 / 3, hint="it be should be above 0.66"
            ),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.hasPattern("ssn", r"\d{3}\-\d{2}\-\d{4}", lambda x: x == 2 / 3),
            [Row(constraint_status="Success")],
        )

    def test_fail_hasPattern(self):
        # Default assertion is 1, thus failure
        self.assertEqual(self.hasPattern("ssn", "\d{3}\-\d{2}\-\d{4}"), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.hasPattern("ssn", r"\d{3}\d{2}\d{4}", lambda x: x == 2 / 3),
            [Row(constraint_status="Failure")],
        )

    def test_isNonNegative(self):
        self.assertEqual(self.isNonNegative("b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isNonNegative("b", lambda x: x == 1), [Row(constraint_status="Success")])
        self.assertEqual(
            self.isNonNegative("h", lambda x: x == 2 / 3, "All values of the column is non-negative"),
            [Row(constraint_status="Success")],
        )
        # Asserts a null value
        self.assertEqual(self.isNonNegative("c", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_fail_isNonNegative(self):
        self.assertEqual(self.isNonNegative("e", lambda x: x == 0), [Row(constraint_status="Failure")])
        self.assertEqual(self.isNonNegative("f", lambda x: x == 0), [Row(constraint_status="Failure")])

    def test_isPositive(self):
        self.assertEqual(self.isPositive("b"), [Row(constraint_status="Success")])
        self.assertEqual(self.isPositive("b", lambda x: x == 1), [Row(constraint_status="Success")])
        # Asserts a null value
        self.assertEqual(self.isPositive("c", lambda x: x == 1), [Row(constraint_status="Success")])

    def test_fail_isPositive(self):
        self.assertEqual(self.isPositive("e", lambda x: x == 0), [Row(constraint_status="Failure")])
        self.assertEqual(self.isPositive("f", lambda x: x == 0), [Row(constraint_status="Failure")])

    def test_isLessThan(self):
        self.assertEqual(
            self.isLessThan("b", "d", lambda x: x == 1, "Column B < Column D"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isLessThan("b", "d"), [Row(constraint_status="Success")])
        # Asserts a null value
        self.assertEqual(self.isLessThan("b", "c", lambda x: x == 2 / 3), [Row(constraint_status="Success")])

    def test_fail_isLessThan(self):
        self.assertEqual(self.isLessThan("d", "b", lambda x: x == 1), [Row(constraint_status="Failure")])
        self.assertEqual(self.isLessThan("e", "f", lambda x: x == 1), [Row(constraint_status="Failure")])

    def test_isLessThanOrEqualTo(self):
        self.assertEqual(
            self.isLessThanOrEqualTo("b", "d", lambda x: x == 1, "Column B < Column D"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isLessThanOrEqualTo("b", "d"), [Row(constraint_status="Success")])

    def test_fail_isLessThanOrEqualTo(self):
        self.assertEqual(self.isLessThanOrEqualTo("b", "c"), [Row(constraint_status="Failure")])

    def test_isGreaterThan(self):
        self.assertEqual(
            self.isGreaterThan("d", "b", lambda x: x == 1, "Column B < Column D"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isGreaterThan("d", "b"), [Row(constraint_status="Success")])

    def test_fail_isGreaterThan(self):
        self.assertEqual(self.isGreaterThan("h", "f", lambda x: x == 1), [Row(constraint_status="Failure")])
        self.assertEqual(self.isGreaterThan("c", "b"), [Row(constraint_status="Failure")])

    def test_isContainedIn(self):
        # test all variants for assertion and hint
        self.assertEqual(
            self.isContainedIn("a", ["foo", "bar", "baz"], lambda x: x == 1),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.isContainedIn("a", ["foo", "bar", "baz"], lambda x: x == 1, hint="it should be 1"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isContainedIn("a", ["foo", "bar", "baz"]), [Row(constraint_status="Success")])
        # A none value makes the test still pass
        self.assertEqual(self.isContainedIn("c", ["5", "6"]), [Row(constraint_status="Success")])

    def test_fail_isContainedIn(self):
        self.assertEqual(self.isContainedIn("b", ["15", "2", "3"]), [Row(constraint_status="Failure")])

        with self.assertRaises(Py4JError) as err:
            # Only strings are supported as allowed_values list
            self.isContainedIn("a", [1, 2, 3])
        self.assertIn("Cannot convert java.lang.Integer to java.lang.String", str(err.exception))

    def test_isGreaterThanOrEqualTo(self):
        self.assertEqual(
            self.isGreaterThanOrEqualTo(
                "d", "b", lambda x: x == 1, "All values in column D is greater than column B"
            ),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(self.isGreaterThanOrEqualTo("d", "b"), [Row(constraint_status="Success")])

    def test_fail_isGreaterThanOrEqualTo(self):
        self.assertEqual(self.isGreaterThanOrEqualTo("c", "b"), [Row(constraint_status="Failure")])
        self.assertEqual(
            self.isGreaterThanOrEqualTo("h", "f", lambda x: x == 1), [Row(constraint_status="Failure")]
        )

    def test_where(self):
        self.assertEqual(
            self.where(lambda x: x == 2.0, "boolean='true'", "column 'boolean' has two values true"),
            [Row(constraint_status="Success")],
        )
        self.assertEqual(
            self.where(lambda x: x == 3.0, "d=5", "column 'd' has three values 3"),
            [Row(constraint_status="Success")],
        )

        check = (
            Check(self.spark, CheckLevel.Warning, "test where")
            .hasMin("f", lambda x: x == 2, "The f has min value 2 becasue of the additional filter")
            .where("f>=2")
        )
        check = check.isGreaterThan("e", "h", lambda x: x == 1, "Column H is not smaller than Column E")
        result = self.run_check(check)
        self.assertEqual(
            result,
            [Row(constraint_status="Success"), Row(constraint_status="Failure")],
        )

    def test_fail_where(self):
        self.assertEqual(
            self.where(lambda x: x == 2.0, "boolean='false'", "column 'boolean' has one value false"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.where(lambda x: x == 3.0, "a='bar'", "column 'a' has one value 'bar'"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.where(lambda x: x == 1.0, "f=1", "column 'f' has one value 1"),
            [Row(constraint_status="Failure")],
        )
        self.assertEqual(
            self.where(lambda x: x == 2.0, "ssn='000-00-0000'", "column 'ssn' has one value 000-00-0000"),
            [Row(constraint_status="Failure")],
        )

        with self.assertRaises(TypeError):
            Check(self.spark, CheckLevel.Warning, "test where").kllSketchSatisfies(
                "b", lambda x: x.parameters().apply(0) == 1.0, KLLParameters(self.spark, 2, 0.64, 2)
            ).where("d=5")

    def test_hasNumberOfDistinctValues(self):
        # TODO: test binningUdf
        self.assertEqual(
            self.hasNumberOfDistinctValues("b", lambda x: x == 3, None, 3, "Column B has 3 distinct values"),
            [Row(constraint_status="Success")],
        )

    def test_isPrimaryKey(self):
        check = Check(self.spark, CheckLevel.Warning, "test isPrimaryKey")
        check = (
            check.isPrimaryKey("b")  # this column is unique
            .isPrimaryKey("d", "f", "b")  # this column combination is unique
        )
        result = self.run_check(check)
        self.assertEqual(result, [Row(constraint_status="Success"), Row(constraint_status="Success")])

    def test_fail_isPrimaryKey(self):
        check = Check(self.spark, CheckLevel.Warning, "test isPrimaryKey")
        result = self.run_check(check.isPrimaryKey("d", "f"))  # these columns are not unique
        self.assertEqual(result, [Row(constraint_status="Failure")])

    def test_hasHistogramValues(self):
        # TODO: test binningUdf
        def assertion_func(x):
            def _parse_dv(dv):
                return dv.absolute(), dv.ratio()

            distribution_values = scala_map_to_dict(self.spark._jvm, x.values())
            dv1 = _parse_dv(distribution_values["1"])
            dv2 = _parse_dv(distribution_values["2"])
            dv3 = _parse_dv(distribution_values["3"])
            return (
                len(distribution_values) == 3
                and dv1 == (1, 1 / 3)
                and dv2 == (1, 1 / 3)
                and dv3 == (1, 1 / 3)
            )

        check = Check(self.spark, CheckLevel.Warning, "test hasHistogramValues")
        result = self.run_check(check.hasHistogramValues("b", assertion_func, None, 3))
        self.assertEqual(result, [Row(constraint_status="Success")])

    def test_kllSketchSatisfies(self):
        def assertion_func(x):
            bucket0 = x.apply(0)
            bucket1 = x.apply(1)
            v0 = (bucket0.lowValue(), bucket0.highValue(), bucket0.count())
            v1 = (bucket1.lowValue(), bucket1.highValue(), bucket1.count())
            return v0 == (1.0, 2.0, 1) and v1 == (2.0, 3.0, 2)

        check = Check(self.spark, CheckLevel.Warning, "test kllSketchSatisfies")
        result = self.run_check(
            check.kllSketchSatisfies("b", assertion_func, KLLParameters(self.spark, 2, 0.64, 2))
        )
        self.assertEqual(result, [Row(constraint_status="Success")])

    def test_list_of_constraints(self):
        check = Check(self.spark, CheckLevel.Warning, "test list of constraints")
        check.addConstraints([check.isComplete("b"), check.containsEmail("email")])
        result = self.run_check(check)
        self.assertEqual(result, [Row(constraint_status="Success"), Row(constraint_status="Success")])

    def test_fail_list_of_constraints(self):
        check = Check(self.spark, CheckLevel.Warning, "test list of constraints")
        check.addConstraints([check.isComplete("c"), check.containsEmail("d")])
        result = self.run_check(check)
        self.assertEqual(result, [Row(constraint_status="Failure"), Row(constraint_status="Failure")])
