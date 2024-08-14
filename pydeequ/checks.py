# -*- coding: utf-8 -*-
from enum import Enum

from py4j.protocol import Py4JError
from pyspark.sql import SparkSession

from pydeequ.check_functions import is_one
from pydeequ.scala_utils import ScalaFunction1, to_scala_seq
from pydeequ.configs import SPARK_VERSION

# TODO implement custom assertions
# TODO implement all methods without outside class dependencies
# TODO Integration with Constraints


class CheckLevel(Enum):
    Error = "Error"
    Warning = "Warning"

    def _get_java_object(self, jvm):
        if self == CheckLevel.Error:
            return jvm.com.amazon.deequ.checks.CheckLevel.Error()
        if self == CheckLevel.Warning:
            return jvm.com.amazon.deequ.checks.CheckLevel.Warning()
        raise ValueError("Invalid value for CheckLevel Enum")


class CheckStatus(Enum):
    Success = "Success"
    Warning = "Warning"
    Error = "Error"

    @classmethod
    def _create_from_java_object(cls, java_object):
        check_status_class = java_object._jvm.com.amazon.deequ.checks.CheckStatus
        if java_object.equals(check_status_class.Success()):
            return CheckStatus.Success
        if java_object.equals(check_status_class.Warning()):
            return CheckStatus.Warning
        if java_object.equals(check_status_class.Error()):
            return CheckStatus.Error
        raise ValueError(f"{java_object} is not a valid CheckStatus Object")


class ConstrainableDataTypes(Enum):
    Null = "Null"
    Fractional = "Fractional"
    Integral = "Integral"
    Boolean = "Boolean"
    String = "String"
    Numeric = "Numeric"

    def _get_java_object(self, jvm):
        if self == ConstrainableDataTypes.Null:
            return jvm.com.amazon.deequ.constraints.ConstrainableDataTypes.Null()
        if self == ConstrainableDataTypes.Fractional:
            return jvm.com.amazon.deequ.constraints.ConstrainableDataTypes.Fractional()
        if self == ConstrainableDataTypes.Integral:
            return jvm.com.amazon.deequ.constraints.ConstrainableDataTypes.Integral()
        if self == ConstrainableDataTypes.Boolean:
            return jvm.com.amazon.deequ.constraints.ConstrainableDataTypes.Boolean()
        if self == ConstrainableDataTypes.String:
            return jvm.com.amazon.deequ.constraints.ConstrainableDataTypes.String()
        if self == ConstrainableDataTypes.Numeric:
            return jvm.com.amazon.deequ.constraints.ConstrainableDataTypes.Numeric()
        raise ValueError("Invalid value for ConstrainableDataType Enum")


class CheckResult:
    pass


class Check:
    """
    A class representing a list of constraints that can be applied to a given
     [[org.apache.spark.sql.DataFrame]]. In order to run the checks, use the `run` method. You can
     also use VerificationSuite.run to run your checks along with other Checks and Analysis objects.
     When run with VerificationSuite, Analyzers required by multiple checks/analysis blocks is
     optimized to run once.
    """

    def __init__(self, spark_session: SparkSession, level: CheckLevel, description: str, constraints: list = None):
        """ "

        :param SparkSession spark_session: SparkSession
        :param CheckLevel level: Assertion level of the check group. If any of the constraints fail this level is used for
                the status of the check.
        :param str description: The name describes the check block. Generally it will be used to show in the logs.
        :param list constraints: The constraints to apply when this check is run. New ones can be added
                and will return a new object
        """
        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm
        self.level = level
        self._java_level = self.level._get_java_object(self._jvm)
        self._check_java_class = self._jvm.com.amazon.deequ.checks.Check
        self.description = description
        self._Check = self._check_java_class(
            self._java_level, self.description, getattr(self._check_java_class, "apply$default$3")()
        )
        self.constraints = constraints or []
        for constraint in self.constraints:
            self.addConstraint(constraint)

    def addConstraints(self, constraints: list):
        self.constraints.extend(constraints)
        for constraint in constraints:
            self._Check = constraint._Check

    def addConstraint(self, constraint):
        """
        Returns a new Check object with the given constraints added to the constraints list.
        :param Constraint constraint: new constraint to be added.
        :return: new Check object
        """
        self.constraints.append(constraint)
        self._Check = constraint._Check

    def where(self, filter: str):
        try:
            self._Check = self._Check.where(filter)
        except Py4JError:
            raise TypeError(f"Method doesn't exist in {self._Check.getClass()}, class has to be filterable")
        return self

    def addFilterableContstraint(self, creationFunc):
        """Adds a constraint that can subsequently be replaced with a filtered version
        :param creationFunc:
        :return:
        """
        raise NotImplementedError("Private factory method for other check methods")

    def hasSize(self, assertion, hint=None):
        """
        Creates a constraint that calculates the data frame size and runs the assertion on it.

        :param lambda assertion: Refers to a data frame size. The given function can include comparisons
                and conjunction or disjunction statements.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasSize self: A Check.scala object that implements the assertion on the column.

        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasSize(assertion_func, hint)  # getattr(self._Check, "hasSize$default$2"))
        return self

    def isComplete(self, column, hint=None):
        """Creates a constraint that asserts on a column completion.

        :param str column: Column in Data Frame to run the assertion on.
        :param str hint: A hint that discerns why a constraint could have failed.
        :return: isComplete self:A Check.scala object that asserts on a column completion.
        """
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isComplete(column, hint, self._jvm.scala.Option.apply(None))
        return self

    def hasCompleteness(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts column completion.
        Uses the given history selection strategy to retrieve historical completeness values on this
        column from the history provider.

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasCompleteness self: A Check object that implements the assertion.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasCompleteness(column, assertion_func, hint, self._jvm.scala.Option.apply(None))
        return self

    def areComplete(self, columns, hint=None):
        """Creates a constraint that asserts completion in combined set of columns.

        :param list[str] columns: Columns in Data Frame to run the assertion on.
        :param str hint: A hint that states why a constraint could have failed.
        :return: areComplete self: A Check.scala object that asserts completion in the columns.
        """
        hint = self._jvm.scala.Option.apply(hint)
        columns_seq = to_scala_seq(self._jvm, columns)
        self._Check = self._Check.areComplete(columns_seq, hint)
        return self

    def haveCompleteness(self, columns, assertion, hint=None):
        """Creates a constraint that asserts on completed rows in a combined set of columns.

        :param list[str] columns: Columns in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: haveCompleteness self: A Check.scala object that implements the assertion on the columns.
        """
        columns_seq = to_scala_seq(self._jvm, columns)
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.haveCompleteness(columns_seq, assertion_func, hint)
        return self

    def areAnyComplete(self, columns, hint=None):
        """Creates a constraint that asserts any completion in the combined set of columns.

        :param list[str] columns: Columns in Data Frame to run the assertion on.
        :param str hint: A hint that states why a constraint could have failed.
        :return: areAnyComplete self: A Check.scala object that asserts completion in the columns.
        """
        hint = self._jvm.scala.Option.apply(hint)
        columns_seq = to_scala_seq(self._jvm, columns)
        self._Check = self._Check.areAnyComplete(columns_seq, hint)
        return self

    def haveAnyCompleteness(self, columns, assertion, hint=None):
        """Creates a constraint that asserts on any completion in the combined set of columns.

        :param list[str] columns: Columns in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: haveAnyCompleteness self: A Check.scala object that asserts completion in the columns.
        """
        columns_seq = to_scala_seq(self._jvm, columns)
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.haveAnyCompleteness(columns_seq, assertion_func, hint)
        return self

    def isUnique(self, column, hint=None):
        """
        Creates a constraint that asserts on a column uniqueness

        :param list[str] columns: Columns in Data Frame to run the assertion on.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isUnique self: A Check.scala object that asserts uniqueness in the column.
        """
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isUnique(column, hint, self._jvm.scala.Option.apply(None))
        return self

    def isPrimaryKey(self, column, *columns, hint=None):
        """
        Creates a constraint that asserts on a column(s) primary key characteristics.
        Currently only checks uniqueness, but reserved for primary key checks if there is another
        assertion to run on primary key columns.

        #  how does column and columns differ
        :param str column: Column in Data Frame to run the assertion on.
        :param str hint: A hint that states why a constraint could have failed.
        :param list[str] columns: Columns to run the assertion on.
        :return: isPrimaryKey self: A Check.scala object that asserts completion in the columns.
        """
        hint = self._jvm.scala.Option.apply(hint)
        print(f"Unsolved integration: {hint}")
        raise NotImplementedError("Unsolved integration of Python tuple => varArgs")

    def hasUniqueness(self, columns, assertion, hint=None):
        """
        Creates a constraint that asserts any uniqueness in a single or combined set of key columns.
        Uniqueness is the fraction of unique values of a column(s)  values that occur exactly once.

        :param list[str] columns: Column(s) in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasUniqueness self: A Check object that asserts uniqueness in the columns.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        columns_seq = to_scala_seq(self._jvm, columns)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasUniqueness(columns_seq, assertion_func, hint)
        return self

    def hasDistinctness(self, columns, assertion, hint=None):
        """
        Creates a constraint on the distinctness in a single or combined set of key columns.
        Distinctness is the fraction of distinct values of a column(s).

        :param list[str] columns: Column(s) in Data Frame to run the assertion on.
        :param lambda assertion : A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasDistinctness self: A Check object that asserts distinctness in the columns.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        columns_seq = to_scala_seq(self._jvm, columns)
        self._Check = self._Check.hasDistinctness(columns_seq, assertion_func, hint)
        return self

    def hasUniqueValueRatio(self, columns, assertion, hint=None):
        """
        Creates a constraint on the unique value ratio in a single or combined set of key columns.

        :param list[str] columns: Column(s) in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasUniqueValueRatio self: A Check object that asserts the unique value ratio in the columns.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        columns_seq = to_scala_seq(self._jvm, columns)
        self._Check = self._Check.hasUniqueValueRatio(columns_seq, assertion_func, hint, self._jvm.scala.Option.apply(None))
        return self

    def hasNumberOfDistinctValues(self, column, assertion, binningUdf, maxBins, hint=None):
        """Creates a constraint that asserts on the number of distinct values a column has.

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param lambda binningUDF: An optional binning function.
        :param int maxBins: Histogram details is only provided for N column values with top counts. MaxBins sets the N.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasNumberOfDistinctValues self: A Check object that asserts distinctness in the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasNumberOfDistinctValues(column, assertion_func, binningUdf, maxBins, hint)
        return self

    def hasHistogramValues(self, column, assertion, binningUdf, maxBins, hint=None):
        """Creates a constraint that asserts on column's value distribution.

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter as a distribution input parameter.
        :param str binningUDF: An optional binning function.
        :param str maxBins: Histogram details is only provided for N column values with top counts. MaxBins sets the N.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasHistogramValues self: A Check object that asserts the column's value distribution in the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasHistogramValues(column, assertion_func, binningUdf, maxBins, hint)
        return self

    def kllSketchSatisfies(self, column, assertion, kllParameters=None, hint=None):
        """Creates a constraint that asserts on column's sketch size.

        :param str column: Column in Data Frame to run the assertion on.
        :param Lambda(BucketDistribution) assertion: A function that accepts an int or float
                parameter as a distribution input parameter.
        :param KLLParameters kllParameters: Parameters of KLL sketch
        :param str hint: A hint that states why a constraint could have failed.
        :return: kllSketchSatisfies self: A Check object that asserts the column's sketch size in the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        params = self._jvm.scala.Option.apply(kllParameters._param if kllParameters else None)
        self._Check = self._Check.kllSketchSatisfies(column, assertion_func, params, hint)
        return self

    def _isNewestPointNonAnomalous(self):
        """Creates a constraint that runs AnomalyDetection on the new value

        :param MetricsRepostiroy metricsRepository  (MetricsRepository): A metrics repository to get previous results.
        :param AnomalyDetectionStrategy anomalyDetectionStrategy (AnomalyDetectionStrategy ): The anomaly detection strategy.
        :param Analzyer analyzer: The analyzer for the metric to run anomaly detection on.
        :param withTagValues: Can contain a Map with tag names and the corresponding values to filter for.
        :param beforeDate: The maximum dateTime of previous AnalysisResults to use for the Anomaly Detection.
        :param afterDate: The minimum dateTime of previous AnalysisResults to use for the Anomaly Detection.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isNewestPointAnomalous self: A Check object that asserts the column's sketch size in the columns.
        """
        raise NotImplementedError("private method in scala, will need anomaly detection implemented beforehand")

    def hasEntropy(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on a column entropy. Entropy is a measure of the level of information
        contained in a message.

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasEntropy self: A Check object that asserts the entropy in the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasEntropy(column, assertion_func, hint)
        return self

    def hasMutualInformation(self, columnA, columnB, assertion, hint=None):
        """
        Creates a constraint that asserts on a mutual information between two columns. Mutual Information describes how
        much information about one column can be inferred from another.

        :param str columnA: First column in Data Frame which calculates the mutual information.
        :param str columnB: Second column in Data Frame which calculates the mutual information.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMutualInformation self: A Check object that asserts the mutual information in the columns.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasMutualInformation(columnA, columnB, assertion_func, hint)
        return self

    def hasApproxQuantile(self, column, quantile, assertion, hint=None):
        """
        Creates a constraint that asserts on an approximated quantile

        :param str column: Column in Data Frame to run the assertion on
        :param float quantile: Quantile to run the assertion on.
        :param lambda assertion: A function that accepts the computed quantile as an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasApproxQuantile self: A Check object that asserts the approximated quantile in the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasApproxQuantile(column, float(quantile), assertion_func, hint)
        return self

    def hasMinLength(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the minimum length of a string datatype column.

        :param str column: Column in Data Frame to run the assertion on. The column is expected to be a string type.
        :param lambda assertion: A function which accepts the int or float parameter
                that discerns the minimum length of the string.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMinLength self: A Check object that asserts minLength of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasMinLength(column, assertion_func, hint, self._jvm.scala.Option.apply(None))
        return self

    def hasMaxLength(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the maximum length of a string datatype column

        :param str column: Column in Data Frame to run the assertion on. The column is expected to be a string type.
        :param lanmbda assertion: A function which accepts an int or float parameter
                that discerns the maximum length of the string.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMaxLength self : A Check object that asserts maxLength of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasMaxLength(column, assertion_func, hint, self._jvm.scala.Option.apply(None))
        return self

    def hasMin(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the minimum of a column. The column is contains either a long,
        int or float datatype.

        :param str column: Column in Data Frame to run the assertion on. The column is expected to be an int, long or float type.
        :param lambda assertion: A function which accepts an int or float parameter
                that discerns the minumum value of the column.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMaxLength self: A Check object that asserts the minumum of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasMin(column, assertion_func, hint, self._jvm.scala.Option.apply(None))
        return self

    def hasMax(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the maximum of the column. The column contains either a long,
        int or float datatype.

        :param str column: Column in Data Frame to run the assertion on. The column is expected to be an int, long or float type.
        :param lambda assertion: A function which accepts an int or float parameter
                that discerns the maximum value of the column.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMax self: A Check object that asserts maximum of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasMax(column, assertion_func, hint, self._jvm.scala.Option.apply(None))
        return self

    def hasMean(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the mean of the column

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function with an int or float parameter. The parameter is the mean.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMean self: A Check object that asserts the mean of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasMean(column, assertion_func, hint)
        return self

    def hasSum(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the sum of the column

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function with an int or float parameter. The parameter is the sum.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMean self: A Check object that asserts the sum of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasSum(column, assertion_func, hint)
        return self

    def hasStandardDeviation(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the standard deviation of the column

        :param str column: Column in Data Frame to run the assertion on.
        :param lambda assertion: A function with an int or float parameter. The parameter is the standard deviation.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasMean self: A Check object that asserts the std deviation of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasStandardDeviation(column, assertion_func, hint)
        return self

    def hasApproxCountDistinct(self, column, assertion, hint=None):
        """
        Creates a constraint that asserts on the approximate count distinct of the given column

        :param str column: Column in DataFrame to run the assertion on.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasApproxCountDistinct self: A Check object that asserts the  count distinct of the column.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasApproxCountDistinct(column, assertion_func, hint)
        return self

    def hasCorrelation(self, columnA, columnB, assertion, hint=None):
        """
        Creates a constraint that asserts on the pearson correlation between two columns.

        :param str columnA: First column in Data Frame which calculates the correlation.
        :param str columnB: Second column in Data Frame which calculates the correlation.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasCorrelation self: A Check object that asserts the correlation calculation in the columns.
        """
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasCorrelation(columnA, columnB, assertion_func, hint)
        return self

    def satisfies(self, columnCondition, constraintName, assertion=None, hint=None):
        """
        Creates a constraint that runs the given condition on the data frame.

        :param str columnCondition: Data frame column which is a combination of expression and the column name.
                It has to comply with Spark SQL syntax. Can be written in an exact same way with conditions inside the
                `WHERE` clause.
        :param str constraintName: A name that summarizes the check being made. This name is being used to name the
                metrics for the analysis being done.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: satisfies self: A Check object that runs the condition on the data frame.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "satisfies$default$3")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.satisfies(
            columnCondition,
            constraintName,
            assertion_func,
            hint,
            self._jvm.scala.collection.Seq.empty(),
            self._jvm.scala.Option.apply(None)
        )
        return self

    def hasPattern(self, column, pattern, assertion=None, name=None, hint=None):
        """
        Checks for pattern compliance. Given a column name and a regular expression, defines a
        Check on the average compliance of the column's values to the regular expression.

        :param str column: Column in DataFrame to be checked
        :param Regex pattern: A name that summarizes the current check and the
                metrics for the analysis being done.
        :param lambda assertion: A function with an int or float parameter.
        :param str name: A name for the pattern constraint.
        :param str hint: A hint that states why a constraint could have failed.
        :return: hasPattern self: A Check object that runs the condition on the column.
        """
        if not assertion:
            assertion = is_one

        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        name = self._jvm.scala.Option.apply(name)
        hint = self._jvm.scala.Option.apply(hint)
        pattern_regex = self._jvm.scala.util.matching.Regex(pattern, None)
        self._Check = self._Check.hasPattern(
            column, pattern_regex, assertion_func, name, hint, self._jvm.scala.Option.apply(None)
        )
        return self

    def containsCreditCardNumber(self, column, assertion=None, hint=None):
        """
        Check to run against the compliance of a column against a Credit Card pattern.

        :param str column: Column in DataFrame to be checked. The column is expected to be a string type.
        :param lambda assertion: A function with an int or float parameter.
        :param hint hint: A hint that states why a constraint could have failed.
        :return: containsCreditCardNumber self: A Check object that runs the compliance on the column.
        """
        assertion = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "containsCreditCardNumber$default$2")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.containsCreditCardNumber(column, assertion, hint)
        return self

    def containsEmail(self, column, assertion=None, hint=None):
        """
        Check to run against the compliance of a column against an e-mail pattern.

        :param str column: The Column in DataFrame to be checked. The column is expected to be a string datatype.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: containsCreditCardNumber self: A Check object that runs the compliance on the column.
        """
        assertion = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "containsEmail$default$2")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.containsEmail(column, assertion, hint)
        return self

    def containsURL(self, column, assertion=None, hint=None):
        """
        Check to run against the compliance of a column against an e-mail pattern.

        :param str column: The Column in DataFrame to be checked. The column is expected to be a string datatype.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: containsURL self: A Check object that runs the compliance on the column.
        """
        assertion = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "containsURL$default$2")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.containsURL(column, assertion, hint)
        return self

    def containsSocialSecurityNumber(self, column, assertion=None, hint=None):
        """
        Check to run against the compliance of a column against the Social security number pattern
        for the US.

        :param str column: The Column in DataFrame to be checked. The column is expected to be a string datatype.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: containsSocialSecurityNumber self: A Check object that runs the compliance on the column.
        """
        assertion = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "containsSocialSecurityNumber$default$2")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.containsSocialSecurityNumber(column, assertion, hint)
        return self

    def hasDataType(self, column, datatype: ConstrainableDataTypes, assertion=None, hint=None):
        """
        Check to run against the fraction of rows that conform to the given data type.

        :param str column: The Column in DataFrame to be checked.
        :param ConstrainableDataTypes datatype: Data type that the columns should be compared against
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.

        :return: hasDataType self: A Check object that runs the compliance on the column.
        """
        datatype_jvm = datatype._get_java_object(self._jvm)
        assertion = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "hasDataType$default$3")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.hasDataType(column, datatype_jvm, assertion, hint)
        return self

    def isNonNegative(self, column, assertion=None, hint=None):
        """
        Creates a constraint which asserts that a column contains no negative values.

        :param str column: The Column in DataFrame to run the assertion on.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.

        :return: self (isNonNegative): A Check object that runs the compliance on the column.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "isNonNegative$default$2")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isNonNegative(column, assertion_func, hint)
        return self

    def isPositive(self, column, assertion=None, hint=None):
        """
        Creates a constraint which asserts that a column contains no negative values and is greater than 0.

        :param str column: The Column in DataFrame to run the assertion on.
        :param lambda assertion: A function with an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isNonNegative self: A Check object that runs the assertion on the column.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "isPositive$default$2")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isPositive(column, assertion_func, hint)
        return self

    def isLessThan(self, columnA, columnB, assertion=None, hint=None):
        """
        Asserts that, in each row, the value of columnA is less than the value of columnB

        :param str columnA: Column in DataFrame to run the assertion on.
        :param str columnB: Column in DataFrame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isLessThan self : A Check object that checks the assertion on the columns.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "isLessThan$default$3")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isLessThan(columnA, columnB, assertion_func, hint)
        return self

    def isLessThanOrEqualTo(self, columnA, columnB, assertion=None, hint=None):
        """
        Asserts that, in each row, the value of columnA is less than or equal to the value of columnB.

        :param str columnA: Column in DataFrame to run the assertion on.
        :param str columnB: Column in DataFrame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isLessThanOrEqualTo self (isLessThanOrEqualTo): A Check object that checks the assertion on the columns.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "isLessThanOrEqualTo$default$3")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isLessThanOrEqualTo(columnA, columnB, assertion_func, hint)
        return self

    def isGreaterThan(self, columnA, columnB, assertion=None, hint=None):
        """
        Asserts that, in each row, the value of columnA is greater than the value of columnB

        :param str columnA: Column in DataFrame to run the assertion on.
        :param str columnB: Column in DataFrame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isGreaterThan self: A Check object that runs the assertion on the columns.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "isGreaterThan$default$3")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isGreaterThan(columnA, columnB, assertion_func, hint)
        return self

    def isGreaterThanOrEqualTo(self, columnA, columnB, assertion=None, hint=None):
        """
        Asserts that, in each row, the value of columnA is greather than or equal to the value of columnB

        :param str columnA: Column in DataFrame to run the assertion on.
        :param str columnB: Column in DataFrame to run the assertion on.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isGreaterThanOrEqualTo self: A Check object that runs the assertion on the columns.
        """
        assertion_func = (
            ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
            if assertion
            else getattr(self._Check, "isGreaterThanOrEqualTo$default$3")()
        )
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isGreaterThanOrEqualTo(columnA, columnB, assertion_func, hint)
        return self

    def isContainedIn(self, column, allowed_values, assertion=None, hint=None):
        """
        Asserts that every non-null value in a column is contained in a set of predefined values

        :param str column: Column in DataFrame to run the assertion on.
        :param list[str] allowed_values: A function that accepts allowed values for the column.
        :param lambda assertion: A function that accepts an int or float parameter.
        :param str hint: A hint that states why a constraint could have failed.
        :return: isContainedIn self: A Check object that runs the assertion on the columns.
        """
        arr = self._spark_session.sparkContext._gateway.new_array(self._jvm.java.lang.String, len(allowed_values))
        for i in range(len(allowed_values)):
            arr[i] = allowed_values[i]

        if not assertion:
            assertion = is_one
        assertion_func = ScalaFunction1(self._spark_session.sparkContext._gateway, assertion)
        hint = self._jvm.scala.Option.apply(hint)
        self._Check = self._Check.isContainedIn(column, arr, assertion_func, hint)
        return self

    def evaluate(self, context):
        """
        Evaluate this check on computed metrics.

        :param context: result of the metrics computation
        :return: evaluate self: A Check object that evaluates the check.
        """
        raise NotImplementedError("We need an AnalyzerContext wrapper class before implementing")

    def requiredAnalyzers(self):
        # TODO: documentation
        raise NotImplementedError("Need to evaluate usage first, seems to be called internally by Checks")
