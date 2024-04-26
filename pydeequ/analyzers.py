# -*- coding: utf-8 -*-
"""
Analyzers file for all the different analyzers classes in Deequ
"""
import json

from pyspark.sql import DataFrame, SparkSession, SQLContext

from pydeequ.pandas_utils import ensure_pyspark_df
from pydeequ.repository import MetricsRepository, ResultKey
from enum import Enum
from pydeequ.scala_utils import to_scala_seq
from pydeequ.configs import SPARK_VERSION

class _AnalyzerObject:
    """
    Analyzer base object to pass and accumulate the analyzers of the run with respect to the JVM
    """

    def _set_jvm(self, jvm):
        self._jvm = jvm
        return self

    @property
    def _deequAnalyzers(self):
        if self._jvm:
            return self._jvm.com.amazon.deequ.analyzers
        raise AttributeError(
            "JVM not set, please run _set_jvm() method first."
        )  # TODO: Test that this exception gets raised


class AnalysisRunner:
    """
    Runs a set of analyzers on the data at hand and optimizes the resulting computations to minimize
    the number of scans over the data. Additionally, the internal states of the computation can be
    stored and aggregated with existing states to enable incremental computations.

    :param spark_session SparkSession: SparkSession
    """

    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session

    def onData(self, df):
        """
        Starting point to construct an AnalysisRun.
        :param dataFrame df: tabular data on which the checks should be verified
        :return: new AnalysisRunBuilder object
        """
        df = ensure_pyspark_df(self._spark_session, df)
        return AnalysisRunBuilder(self._spark_session, df)


class AnalyzerContext:
    """
    The result returned from AnalysisRunner and Analysis.
    """

    @classmethod
    def successMetricsAsDataFrame(
        cls, spark_session: SparkSession, analyzerContext, forAnalyzers: list = None, pandas: bool = False
    ):
        """
        Get the Analysis Run as a DataFrame.

        :param SparkSession spark_session: SparkSession
        :param AnalyzerContext analyzerContext: Analysis Run
        :param list forAnalyzers: Subset of Analyzers from the Analysis Run
        :return DataFrame: DataFrame of Analysis Run
        """
        if forAnalyzers:
            raise NotImplementedError("forAnalyzers have not been implemented yet.")
        forAnalyzers = getattr(
            spark_session._jvm.com.amazon.deequ.analyzers.runners.AnalyzerContext, "successMetricsAsDataFrame$default$3"
        )()
        analysis_result = (
            spark_session._jvm.com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame(
                spark_session._jsparkSession, analyzerContext, forAnalyzers
            )
        )
        sql_ctx = SQLContext(
            sparkContext=spark_session._sc,
            sparkSession=spark_session,
            jsqlContext=spark_session._jsparkSession.sqlContext(),
        )
        return DataFrame(analysis_result, sql_ctx).toPandas() if pandas else DataFrame(analysis_result, sql_ctx)

    @classmethod
    def successMetricsAsJson(cls, spark_session: SparkSession, analyzerContext, forAnalyzers: list = None):
        """
        Get the Analysis Run as a JSON.

        :param SparkSession spark_session: SparkSession
        :param AnalyzerContext analyzerContext: Analysis Run
        :param list forAnalyzers: Subset of Analyzers from the Analysis Run
        :return JSON : JSON output of Analysis Run
        """
        if forAnalyzers:
            raise NotImplementedError("forAnalyzers have not been implemented yet.")
        forAnalyzers = getattr(
            spark_session._jvm.com.amazon.deequ.analyzers.runners.AnalyzerContext, "successMetricsAsJson$default$2"
        )()
        analysis_result = spark_session._jvm.com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsJson(
            analyzerContext, forAnalyzers
        )
        return json.loads(analysis_result)


class AnalysisRunBuilder:
    """
    Low level class for running analyzers module. This is meant to be called by AnalysisRunner.

    :param spark_session SparkSession: SparkSession
    :param DataFrame  df: DataFrame to run the Analysis on.
    """

    def __init__(self, spark_session: SparkSession, df: DataFrame):

        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        self._df = df
        self._AnalysisRunBuilder = self._jvm.com.amazon.deequ.analyzers.runners.AnalysisRunBuilder(df._jdf)

    def addAnalyzer(self, analyzer: _AnalyzerObject):
        """
        Adds a single analyzer to the current Analyzer run.

        :param analyzer: Adds an analyzer strategy to the run.
        :return self: for further chained method calls.
        """
        analyzer._set_jvm(self._jvm)
        _analyzer_jvm = analyzer._analyzer_jvm
        self._AnalysisRunBuilder.addAnalyzer(_analyzer_jvm)
        return self

    def run(self):
        """
        Run the Analysis.

        :return: self: Runs the AnalysisRunBuilder.
        """
        return self._AnalysisRunBuilder.run()

    def useRepository(self, repository: MetricsRepository):
        """
        Set a metrics repository associated with the current data to enable features
        like reusing previously computed results and storing the results of the current run.

        :param MetricsRepository repository: A metrics repository to store and
            load results associated with the run
        :return: self
        """
        self._AnalysisRunBuilder = self._AnalysisRunBuilder.useRepository(repository.repository)
        return self

    def saveOrAppendResult(self, resultKey: ResultKey):
        """
        A shortcut to save the results of the run or append them to existing results
        in the metrics repository.

        :param ResultKey resultKey: The result key to identify the current run
        :return: self
        """
        self._AnalysisRunBuilder.saveOrAppendResult(resultKey.resultKey)
        return self


class ApproxCountDistinct(_AnalyzerObject):
    """
    Computes the approximate count distinctness of a column with HyperLogLogPlusPlus.

    :param str column: Column to compute this aggregation on.
    :param str where: Additional filter to apply before the analyzer is run.
    """

    def __init__(self, column: str, where: str = None):
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed aggregation

        :return self
        """
        return self._deequAnalyzers.ApproxCountDistinct(self.column, self._jvm.scala.Option.apply(self.where))


class ApproxQuantile(_AnalyzerObject):
    """
    Computes the Approximate Quantile of a column. The allowed relative error compared
    to the exact quantile can be configured with the `relativeError` parameter.

    :param str column: The column in the DataFrame for which the approximate quantile is analyzed.
    :param float [0,1] quantile: The computed quantile. It must be within the
            interval [0, 1], where 0.5 would be the median.
    :param float [0,1] relativeError: Relative target precision to achieve in the
            quantile computation. A `relativeError` = 0.0 would yield the exact
            quantile while increasing the computational load.
    :param str where: Additional filter to apply before the analyzer is run.
    """

    def __init__(self, column: str, quantile: float, relativeError: float = 0.01, where=None):
        self.column = column
        self.quantile = quantile
        self.relativeError = relativeError
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed aggregation

        :return self
        """
        return self._deequAnalyzers.ApproxQuantile(
            self.column, self.quantile, self.relativeError, self._jvm.scala.Option.apply(self.where)
        )


class ApproxQuantiles(_AnalyzerObject):
    """
     Computes the approximate quantiles of a column. The allowed relative
     error compared to the exact quantile can be configured with
     `relativeError` parameter.

    :param str column: Column in DataFrame for which the approximate
            quantile is analyzed.
    :param List[float[0,1]]) quantiles: Computed Quantiles. Must be in
            the interval [0, 1], where 0.5 would be the median.
    :param float [0,1] relativeError: Relative target precision to achieve
            in the quantile computation. A `relativeError` = 0.0 would
            yield the exact quantile while increasing the computational load.
    """

    def __init__(self, column, quantiles, relativeError=0.01):
        self.column = column
        self.quantiles = quantiles
        self.relativeError = relativeError

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed aggregation

        :return self
        """
        return self._deequAnalyzers.ApproxQuantiles(
            self.column, to_scala_seq(self._jvm, self.quantiles), self.relativeError
        )


class Completeness(_AnalyzerObject):
    """Completeness is the fraction of non-null values in a column.

    :param str column: Column in DataFrame for which Completeness is analyzed.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where=None):
        """
        :param column: Column in DataFrame for which Completeness is analyzed.
        :param str where: additional filter to apply before the analyzer is run.
        """
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed completeness

        :return self: access the value of the Completeness analyzer.
        """
        return self._deequAnalyzers.Completeness(
            self.column,
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.Option.apply(None)
        )


class Compliance(_AnalyzerObject):
    """
    Compliance measures the fraction of rows that complies with the given
    column constraint. E.g if the constraint is "att1>3" and data frame
    has 5 rows with att1 column value greater than 3 and 10 rows under
    3; a DoubleMetric would be returned with 0.33 value.

    :param str instance: Unlike other column analyzers (e.g completeness)
        this analyzer can not infer to the metric instance name from
        column name. Also the constraint given here can be referring
        to multiple columns, so metric instance name should be
        provided,describing what the analysis  being done for.
    :param str predicate: SQL-predicate to apply per row
    :param str where: additional filter to apply before
        the analyzer is run.
    """

    def __init__(self, instance, predicate, where=None):

        self.instance = instance
        self.predicate = predicate
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed compliance

        :return self
        """
        return self._deequAnalyzers.Compliance(
            self.instance,
            self.predicate,
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.collection.Seq.empty(),
            self._jvm.scala.Option.apply(None)
        )


class Correlation(_AnalyzerObject):
    """
    Computes the pearson correlation coefficient between the two given columns.

    :param str column1: First column in the DataFrame for which the Correlation is analyzed.
    :param str column2: Second column in the DataFrame for which the Correlation is analyzed.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column1, column2, where=None):
        self.column1 = column1
        self.column2 = column2
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed correlation

        :return self
        """
        return self._deequAnalyzers.Correlation(self.column1, self.column2, self._jvm.scala.Option.apply(self.where))


class CountDistinct(_AnalyzerObject):
    """
    Counts the distinct elements in the column(s).

    :param List[str] columns: Column(s) in the DataFrame for which distinctness is analyzed.
    """

    def __init__(self, columns):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns

    @property
    def _analyzer_jvm(self):
        """Returns the value of the computed distinctness

        :return self
        """
        return self._deequAnalyzers.CountDistinct(to_scala_seq(self._jvm, self.columns))


class DataType(_AnalyzerObject):
    """
    Data Type Analyzer. Returns the datatypes of column

    :param str column: Column in the DataFrame for which data type is analyzed.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where=None):
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the datatype of the column(s)

        :return self
        """
        return self._deequAnalyzers.DataType(self.column, self._jvm.scala.Option.apply(self.where))


class Distinctness(_AnalyzerObject):
    """
    Count the distinctness of elements in column(s).
    Distinctness is the fraction of distinct values of a column(s).

    :param str OR list[str] columns: Column(s) in the DataFrame for which data
        type is to be analyzed. The column is expected to be a str for single
        column or list[str] for multiple columns.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, columns, where: str = None):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the distinctness of the column(s)

        :return self: access the value of the distincness analyzer.
        """
        return self._deequAnalyzers.Distinctness(
            to_scala_seq(self._jvm, self.columns), self._jvm.scala.Option.apply(self.where)
        )


class Entropy(_AnalyzerObject):
    """
    Entropy is a measure of the level of information contained in a message.
    Given the probability distribution over values in a column, it describes
    how many bits are required to identify a value.

    :param str column: Column in DataFrame for which entropy is calculated.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the entropy of the column

        :return self
        """
        return self._deequAnalyzers.Entropy(self.column, self._jvm.scala.Option.apply(self.where))


class Histogram(_AnalyzerObject):
    """
    Histogram is the summary of values in a column of a DataFrame.
    It groups the column's values then calculates the number of rows with
    that specific value and the fraction of the value.

    :param str column: Column in DataFrame to do histogram analysis.
    :param lambda expr binningUdf: Optional binning function to run before
        grouping to re-categorize the column values.For example to turn a
        numerical value to a categorical value binning functions might be used.
    :param int maxDetailBins: Histogram details is only provided for N column
        values with top counts. MaxBins sets the N. This limit does not affect
        what is being returned as number of bins.It always returns the distinct
        value count.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, binningUdf=None, maxDetailBins: int = None, where: str = None):

        self.column = column
        self.binningUdf = binningUdf
        self.maxDetailBins = maxDetailBins
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the histogram summary of values in a column.

        :return self
        """
        if not self.maxDetailBins:
            self.maxDetailBins = getattr(self._jvm.com.amazon.deequ.analyzers.Histogram, "apply$default$3")()
        return self._deequAnalyzers.Histogram(
            self.column,
            self._jvm.scala.Option.apply(self.binningUdf),
            self.maxDetailBins,
            self._jvm.scala.Option.apply(self.where),
            getattr(self._jvm.com.amazon.deequ.analyzers.Histogram, "apply$default$5")(),
            getattr(self._jvm.com.amazon.deequ.analyzers.Histogram, "apply$default$6")()
        )


class KLLParameters:
    """
    Parameter definition for KLL Sketches.

    :param int sketchSize: size of kll sketch.
    :param float shrinkingFactor: shrinking factor of kll sketch.
    :param int numberOfBuckets:  number of buckets.
    """

    def __init__(self, spark_session: SparkSession, sketchSize: int, shrinkingFactor: float, numberOfBuckets: int):
        self._spark_session = spark_session
        self.sketchSize = sketchSize
        self.shrinkingFactor = shrinkingFactor
        self.numberOfBuckets = numberOfBuckets

    @property
    def _param(self):
        """
        Return the JVM KLLParameter object
        """
        return self._spark_session._jvm.com.amazon.deequ.analyzers.KLLParameters(
            self.sketchSize, self.shrinkingFactor, self.numberOfBuckets
        )


class KLLSketch(_AnalyzerObject):
    """
    The KLL Sketch analyzer.

    :param str column: Column in DataFrame to do histogram analysis.
    :param KLLParameters kllParameters: parameters of KLL Sketch
    """

    def __init__(self, column: str, kllParameters: KLLParameters):
        self.column = column
        self.kllParameters = kllParameters

    @property
    def _analyzer_jvm(self):
        """
        Returns the histogram summary of values in a column.

        :return self
        """
        if not self.kllParameters:
            self.kllParameters = getattr(self._jvm.com.amazon.deequ.analyzers.KLLSketch, "apply$default$2")()
        return self._deequAnalyzers.KLLSketch(self.column, self._jvm.scala.Option.apply(self.kllParameters._param))


class Maximum(_AnalyzerObject):
    """Get the maximum of a numeric column."""

    def __init__(self, column, where: str = None):
        """
        :param str column: column to find the maximum.
        :param str where: additional filter to apply before the analyzer is run.
        """
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the maximum value in a column.

        :return self
        """
        return self._deequAnalyzers.Maximum(
            self.column, self._jvm.scala.Option.apply(self.where), self._jvm.scala.Option.apply(None)
        )


class MaxLength(_AnalyzerObject):
    """MaxLength Analyzer. Get Max length of a str type column.

    :param str column: column in DataFrame to find the maximum length.
            Column is expected to be a str type.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):

        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the maximum length in a column.

        :return self
        """
        return self._deequAnalyzers.MaxLength(
            self.column,
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.Option.apply(None)
        )


class Mean(_AnalyzerObject):
    """
    Mean Analyzer. Get mean of a column

    :param str column: Column in DataFrame to find the mean.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):

        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the mean of a column.

        :return self
        """
        return self._deequAnalyzers.Mean(self.column, self._jvm.scala.Option.apply(self.where))


class Minimum(_AnalyzerObject):
    """Count the distinct elements in a single or multiple columns

    :param str column: Column in DataFrame to find the minimum value.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """Returns the minimum of a column.

        :return self
        """
        return self._deequAnalyzers.Minimum(
            self.column, self._jvm.scala.Option.apply(self.where), self._jvm.scala.Option.apply(None)
        )


class MinLength(_AnalyzerObject):
    """
    Get the minimum length of a column

    :param str column: Column in DataFrame to find the minimum Length.
        Column is expected to be a str type.
    :param str where : additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the minimum length of column.

        :return self
        """
        return self._deequAnalyzers.MinLength(
            self.column,
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.Option.apply(None)
        )


class MutualInformation(_AnalyzerObject):
    """
    Describes how much information about one column can be inferred from another column.

    :param list[str] columns: Columns in DataFrame for mutual information analysis.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, columns, where: str = None):
        self.columns = columns
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the mutual information of columns.

        :return self
        """
        return self._deequAnalyzers.MutualInformation(
            to_scala_seq(self._jvm, self.columns), self._jvm.scala.Option.apply(self.where)
        )


class PatternMatch(_AnalyzerObject):
    """
    PatternMatch is a measure of the fraction of rows that complies with a
    given column regex constraint.

    E.g A sample dataFrame column has five rows that contain a credit card
    number and 10 rows that do not. According to regex, using the
    constraint Patterns.CREDITCARD returns a doubleMetric .33 value.


    :param str column: Column in DataFrame to check pattern.
    :param str pattern_regex: pattern regex
    :param str pattern_groupNames: groupNames for pattern regex
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, pattern_regex: str, *pattern_groupNames, where: str = None):
        self.column = column
        self.pattern_regex = pattern_regex
        if pattern_groupNames:
            raise NotImplementedError("pattern_groupNames have not been implemented yet.")
        self.pattern_groupNames = None
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the pattern match of column.

        :return self
        """
        return self._deequAnalyzers.PatternMatch(
            self.column,
            self._jvm.scala.util.matching.Regex(self.pattern_regex, None),
            # TODO: revisit bc scala constructor does some weird implicit type casting from python str -> java list
            #  if we don't cast it to str()
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.Option.apply(None)
        )


class Size(_AnalyzerObject):
    """
    Size is the number of rows in a DataFrame.

    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, where: str = None):
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the size of DataFrame.

        :return self
        """
        return self._deequAnalyzers.Size(self._jvm.scala.Option.apply(self.where))


class StandardDeviation(_AnalyzerObject):
    """
    Calculates the Standard Deviation of column

    :param str column: Column in DataFrame for standard deviation calculation.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):
        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the standard deviation of column.

        :return self
        """
        return self._deequAnalyzers.StandardDeviation(self.column, self._jvm.scala.Option.apply(self.where))


class Sum(_AnalyzerObject):
    """
    Calculates the sum of a column

    :param str column: Column in DataFrame to calculate the sum.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, column, where: str = None):

        self.column = column
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the sum of column.

        :return self
        """
        return self._deequAnalyzers.Sum(self.column, self._jvm.scala.Option.apply(self.where))


class Uniqueness(_AnalyzerObject):
    """
    Uniqueness is the fraction of unique values of column(s),
    values that occur exactly once.

    :param list[str] columns: Columns in DataFrame to find uniqueness.
    :param str where: additional filter to apply before the analyzer is run.
    """

    def __init__(self, columns, where: str = None):
        self.columns = columns
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the uniqueness in columns.

        :return self
        """
        return self._deequAnalyzers.Uniqueness(
            to_scala_seq(self._jvm, self.columns),
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.Option.apply(None)
        )


class UniqueValueRatio(_AnalyzerObject):
    """
    Calculates the ratio of uniqueness.

    :param list[str] columns: Columns in DataFrame to find unique value ratio.
    :param str where: additional filter to apply before the analyzer is run.

    """

    def __init__(self, columns, where: str = None):
        self.columns = columns
        self.where = where

    @property
    def _analyzer_jvm(self):
        """
        Returns the unique value ratio in columns.

        :return self
        """
        return self._deequAnalyzers.UniqueValueRatio(
            to_scala_seq(self._jvm, self.columns),
            self._jvm.scala.Option.apply(self.where),
            self._jvm.scala.Option.apply(None)
        )

class DataTypeInstances(Enum):
    """
    An enum class that types columns to scala datatypes
    """
    Boolean = "Boolean"
    Unknown = "Unknown"
    Fractional = "Fractional"
    Integral = "Integral"
    String = "String"

    def _create_java_object(self, jvm):
        dataType_analyzers_class = jvm.com.amazon.deequ.analyzers.DataTypeInstances
        if self == DataTypeInstances.String:
            return dataType_analyzers_class.String()
        elif self == DataTypeInstances.Boolean:
            return dataType_analyzers_class.Boolean()
        elif self == DataTypeInstances.Unknown:
            return dataType_analyzers_class.Unknown()
        elif self == DataTypeInstances.Integral:
            return dataType_analyzers_class.Integral()
        elif self == DataTypeInstances.Fractional:
            return dataType_analyzers_class.Fractional()
        else:
            raise ValueError(f"{jvm} is not a valid datatype Object")