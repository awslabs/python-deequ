# -*- coding: utf-8 -*-
""" Profiles file for all the Profiles classes in Deequ"""
import json
from collections import namedtuple

from pyspark.sql import DataFrame, SparkSession
from pydeequ.analyzers import KLLParameters
from pydeequ.metrics import BucketDistribution
from pydeequ.pandas_utils import ensure_pyspark_df
from enum import Enum
from pydeequ.scala_utils import (
    get_or_else_none,
    java_list_to_python_list,
    scala_map_to_dict,
    scala_map_to_java_map,
    to_scala_map,
    to_scala_seq,
)

# ColumnProfilerRunner Classes
# TODO refactor documentation (https://devguide.python.org/documenting/)

DistributionValue = namedtuple("DistributionValue", "value count ratio")  # TODO: Revisit with Chris


class ColumnProfilerRunner:
    """
    Primary class for interacting with the profiles module.

    :param SparkSession spark_session: sparkSession
    """

    def __init__(self, spark_session: SparkSession):
        if not isinstance(spark_session, SparkSession):
            raise TypeError(f"Expected SparkSession object for spark_session, not {type(spark_session)}")

        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm
        self._ColumnProfilerRunner = self._jvm.com.amazon.deequ.profiles.ColumnProfilerRunner()

    def onData(self, df):
        """
        Starting point to construct a profile

        :param df: Tabular data on which the profiles module will use
        :return: The starting point to construct a profile
        """
        df = ensure_pyspark_df(self._spark_session, df)
        return ColumnProfilerRunBuilder(self._spark_session, df)

    def run(
        self,
        data,
        restrictToColumns,
        lowCardinalityHistogramThreshold,
        printStatusUpdates,
        cacheInputs,
        fileOutputOptions,
        metricsRepositoryOptions,
        kllParameters,
        predefinedTypes,
    ):
        """

        :param data:
        :param restrictToColumns:
        :param lowCardinalityHistogramThreshold:
        :param printStatusUpdates:
        :param cacheInputs:
        :param fileOutputOptions:
        :param metricsRepositoryOptions:
        :param kllParameters:
        :param predefinedTypes:
        :return:
        """
        raise NotImplementedError("Encapsulated")
        # TODO: preprocess parameters
        # return self._ColumnProfilerRunner.run(
        #     data,
        #     restrictToColumns,
        #     lowCardinalityHistogramThreshold,
        #     printStatusUpdates,
        #     cacheInputs,
        #     fileOutputOptions,
        #     metricsRepositoryOptions,
        #     kllParameters,
        #     predefinedTypes
        # )


# ColumnProfilerRunBuilder Classes


class ColumnProfilerRunBuilder:
    """
    Low level class for running profiling module

    :param SparkSession spark_session: sparkSession
    :param data: Tabular data which will be used to construct a profile.
    """

    def __init__(self, spark_session: SparkSession, data: DataFrame):

        if not isinstance(spark_session, SparkSession):
            raise TypeError(f"Expected SparkSession object for spark_session, not {type(spark_session)}")
        if not isinstance(data, DataFrame):
            raise TypeError(f"Expected DataFrame object for data, not {type(data)}")

        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm
        self._ColumnProfilerRunBuilder = self._jvm.com.amazon.deequ.profiles.ColumnProfilerRunBuilder(data._jdf)

    def run(self):
        """
        A method that runs a profile check on the data to obtain a ColumnProfiles class

        :return: A ColumnProfiles result
        """
        run = self._ColumnProfilerRunBuilder.run()
        return ColumnProfilesBuilder(self._spark_session)._columnProfilesFromColumnRunBuilderRun(run)

    def printStatusUpdates(self, print_status_updates: bool):
        """
        Print status updates between passes

        :param bool print_status_updates: Whether to print status updates
        :return: Printed status
        """
        self._ColumnProfilerRunBuilder.printStatusUpdates(print_status_updates)
        return self

    def cacheInputs(self, cache_inputs: bool):
        """
        Cache the inputs

        :param bool cache_inputs: Whether to print status updates
        :return: Cache inputs
        """
        self._ColumnProfilerRunBuilder.cacheInputs(cache_inputs)
        return self

    def withLowCardinalityHistogramThreshold(self, low_cardinality_histogram_threshold: int):
        """
        Set the thresholds of value until it is expensive to calculate the histograms

        :param int low_cardinality_histogram_threshold: The designated threshold
        :return: a set threshold
        """
        self._ColumnProfilerRunBuilder.withLowCardinalityHistogramThreshold(low_cardinality_histogram_threshold)
        return self

    def restrictToColumns(self, restrict_to_columns: list):
        """
        Can be used to specify a subset of columns to look out

        :param list restrict_to_columns: Specified columns
        :return: A subset of columns to look at
        """
        self._ColumnProfilerRunBuilder.restrictToColumns(to_scala_seq(self._jvm, restrict_to_columns))
        return self

    def withKLLProfiling(self):
        """
        Enable KLL Sketches profiling on Numerical columns, disabled by default.

        :return: Enable KLL Sketches profiling on Numerical columns, disabled by default.
        """
        self._ColumnProfilerRunBuilder.withKLLProfiling()
        return self

    def setKLLParameters(self, kllParameters: KLLParameters):
        """
        Set kllParameters

        :param KLLParameters kllParameters: kllParameters(sketchSize, shrinkingFactor, numberOfBuckets)
        """
        self._ColumnProfilerRunBuilder.setKLLParameters(self._jvm.scala.Option.apply(kllParameters._param))
        return self

    def setPredefinedTypes(self, dataTypesDict: dict):
        """
        Set predefined data types for each column (e.g. baseline)

        :param dict{"columnName": DataTypeInstance} dataTypes: dataType map for baseline columns.
        :return: Baseline for each column. I.E. returns the dataType label to the desired DataTypeInstance
        """
        dataType_scala_map = {}
        for key, value in dataTypesDict.items():
            val = value._create_java_object(self._jvm)
            dataType_scala_map[key] = val
        self._ColumnProfilerRunBuilder.setPredefinedTypes(to_scala_map(self._spark_session, dataType_scala_map))
        return self

    def useRepository(self, repository):
        """
        Set a metrics repository associated with the current data to enable features like reusing
        previously computed results and storing the results of the current run.

        :param repository:A metrics repository to store and load results associated with the run
        :return: Sets a metrics repository with the current data to use features on
        """
        self._ColumnProfilerRunBuilder = self._ColumnProfilerRunBuilder.useRepository(repository.repository)
        return self

    def saveOrAppendResult(self, resultKey):
        """
        A shortcut to save the results of the run or append them to existing results in the metrics repository

        :param resultKey: The result key to identify the current run
        :return: A saved results of the run in the metrics repository
        """

        self._ColumnProfilerRunBuilder.saveOrAppendResult(resultKey.resultKey)
        return self

    def useSparkSession(self, sparkSession):
        """
        Use a sparkSession to conveniently create output files

        :param SparkSession sparkSession: sparkSession
        :return: A sparksession to create output files
        """
        # TODO


class ColumnProfilesBuilder:
    def __init__(self, spark_session: SparkSession):
        """
        The results returned from the columnProfilerRunner

        :param SparkSession spark_session: sparkSession
        """
        if not isinstance(spark_session, SparkSession):
            raise TypeError(f"Expected SparkSession object, not {type(spark_session)}")

        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm
        self._profiles = []
        self.columnProfileClasses = {
            "StandardColumnProfile": StandardColumnProfile,
            "StringColumnProfile": StandardColumnProfile,
            "NumericColumnProfile": NumericColumnProfile,

        }

    def _columnProfilesFromColumnRunBuilderRun(self, run):
        """
        Produces a Java profile based on the designated column

        :param run: columnProfilerRunner result
        :return: a setter for columnProfilerRunner result
        """
        self._run_result = run
        profile_map = self._jvm.scala.collection.JavaConversions.mapAsJavaMap(run.profiles())  # TODO from ScalaUtils
        self._profiles = {column: self._columnProfileBuilder(column, profile_map[column]) for column in profile_map}
        return self

    @property
    def profiles(self):
        """
        A getter for profiles

        :return: a getter for profiles
        """
        return self._profiles

    def _columnProfileBuilder(self, column, java_column_profile):
        """Factory function for ColumnProfile
        Returns a Java profile based on the designated column

        :param column: The column to run a profile on
        :param java_column_profile: The profile mapped as a Java map
        """
        return self.columnProfileClasses[java_column_profile.getClass().getSimpleName()](
            self._spark_session, column, java_column_profile
        )


class ColumnProfile:
    """Factory class for Standard and Numeric Column Profiles
    The class for getting the Standard and Numeric Column profiles of the data.

    :param SparkSession spark_session: sparkSession
    :param column: designated column to run a profile on
    :param java_column_profile: The profile mapped as a Java map
    """

    def __init__(self, spark_session: SparkSession, column, java_column_profile):
        if not isinstance(spark_session, SparkSession):
            raise TypeError(f"Expected SparkSession object for spark_session, not {type(spark_session)}")
        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm
        self._java_column_profile = java_column_profile
        self._column = column
        self._completeness = java_column_profile.completeness()
        self._approximateNumDistinctValues = java_column_profile.approximateNumDistinctValues()
        self._dataType = java_column_profile.dataType()
        self._typeCounts = scala_map_to_dict(self._jvm, java_column_profile.typeCounts())
        self._isDataTypeInferred = java_column_profile.isDataTypeInferred() == "true"
        if get_or_else_none(self._java_column_profile.histogram()):
            self._histogram = [
                DistributionValue(k, v.absolute(), v.ratio())
                for k, v in scala_map_to_java_map(
                    self._jvm, self._java_column_profile.histogram().get().values()
                ).items()
            ]
        else:
            self._histogram = None

    @property
    def column(self):
        """
        Getter for the current column name in ColumnProfile

        :return:  gets the column name in the Column Profile
        """
        return self._column

    @property
    def completeness(self):
        """ "
        Getter that returns the completeness of data in the column

        :return:  gets the calculated completeness of data in the column
        """
        return self._completeness

    @property
    def approximateNumDistinctValues(self):
        """
        Getter that returns the amount of distinct values in the column

        :return: gets the number of distinct values in the column
        """
        return self._approximateNumDistinctValues

    @property
    def dataType(self):
        """
        Getter that returns the datatype of the column

        :return: gets the datatype of the column
        """
        return str(self._dataType)

    @property
    def isDataTypeInferred(self):
        """
        Getter that returns a boolean of whether the Data Type of the column was inferred

        :return: gets the isDataTypeInferred of the column
        """
        return self._isDataTypeInferred

    @property
    def typeCounts(self):
        """
        A getter for the number of values for each datatype in the column

        :return: gets the number of values for each datatype
        """
        return self._typeCounts

    @property
    def histogram(self):
        """
        A getter for the full value distribution of the column

        :return: gets the histogram of a column
        """
        return self._histogram


class StandardColumnProfile(ColumnProfile):
    """
    Standard Column Profile class

    :param SparkSession spark_session: sparkSession
    :param column: the designated column of which the profile is run on
    :param java_column_profile: The profile mapped as a Java map
    """

    def __init__(self, spark_session: SparkSession, column, java_column_profile):
        super().__init__(spark_session, column, java_column_profile)
        self.all = {
            "completeness": self.completeness,
            "approximateNumDistinctValues": self.approximateNumDistinctValues,
            "dataType": self.dataType,
            "isDataTypeInferred": self.isDataTypeInferred,
            "typeCounts": self.typeCounts,
            "histogram": self.histogram,
        }

    def __str__(self):
        """
        A JSON of the standard profiles for each column

        :return: A JSON of the standard profiles
        """
        return f"StandardProfiles for column: {self.column}: {json.dumps(self.all, indent=4)}"


class NumericColumnProfile(ColumnProfile):
    """
    Numeric Column Profile class

    :param SparkSession spark_session: sparkSession
    :param column: the designated column of which the profile is run on
    :param java_column_profile: The profile mapped as a Java map
    """

    def __init__(self, spark_session: SparkSession, column, java_column_profile):
        super().__init__(spark_session, column, java_column_profile)
        # TODO: self.numRecords = java_column_profile.numRecords()
        self._kll = (
            BucketDistribution(spark_session, java_column_profile.kll().get())
            if get_or_else_none(java_column_profile.kll())
            else None
        )
        self._mean = get_or_else_none(java_column_profile.mean())
        self._maximum = get_or_else_none(java_column_profile.maximum())
        self._minimum = get_or_else_none(java_column_profile.minimum())
        self._sum = get_or_else_none(java_column_profile.sum())
        self._stdDev = get_or_else_none(java_column_profile.stdDev())
        self._approxPercentiles = (
            java_list_to_python_list(str(get_or_else_none(java_column_profile.approxPercentiles())), float)
            if get_or_else_none(java_column_profile.approxPercentiles())
            else []
        )
        self.all = {
            "completeness": self.completeness,
            "approximateNumDistinctValues": self.approximateNumDistinctValues,
            "dataType": self.dataType,
            "isDataTypeInferred": self.isDataTypeInferred,
            "typeCounts": self.typeCounts,
            "histogram": self.histogram,
            "kll": str(self._kll),
            "mean": self._mean,
            "maximum": self._maximum,
            "minimum": self._minimum,
            "sum": self._sum,
            "stdDev": self._stdDev,
            "approxPercentiles": self._approxPercentiles,
        }

    def __str__(self):
        """
        A JSON of the numerical profiles for each column

        :return: A JSON of the numerical profiles

        """
        return f"NumericProfiles for column: {self.column}: {json.dumps(self.all, indent=4)}"

    @property
    def kll(self):
        """
        A getter for the kll value of a numeric column
        :return: gets the kll value of a numeric column
        """
        return self._kll

    @property
    def mean(self):
        """
        A getter for the calculated mean of the numeric column

        :return: gets the mean of the column
        """
        return self._mean

    @property
    def maximum(self):
        """
        A getter for the maximum value of the numeric column

        :return: gets the maximum value of the column
        """
        return self._maximum

    @property
    def minimum(self):
        """
        A getter for the minimum value of the numeric column

        :return: gets the minimum value of the column
        """
        return self._minimum

    @property
    def sum(self):
        """
        A getter for the sum of the numeric column

        :return: gets the sum value of the column
        """
        return self._sum

    @property
    def stdDev(self):
        """
        A getter for the standard deviation  of the numeric column

        :return: gets the standard deviation of the column
        """
        return self._stdDev

    @property
    def approxPercentiles(self):
        """
        A getter for the approximate percentiles  of the numeric column

        :return: gets the approximate percentiles of the column
        """
        return self._approxPercentiles

