# -*- coding: utf-8 -*-
"""
Repository file for all the different metrics repository classes in Deequ

Author: Calvin Wang
"""
import json
import time

from pyspark.sql import DataFrame, SparkSession

from pydeequ.scala_utils import to_scala_map, to_scala_seq


class MetricsRepository:
    """
    Base class for Metrics Repository
    """

    @classmethod
    def helper_metrics_file(cls, spark_session: SparkSession, filename: str = "metrics.json"):
        """
        Helper method to create the metrics file for storage
        """
        tempDir = spark_session._jvm.com.google.common.io.Files.createTempDir()
        f = spark_session._jvm.java.io.File(tempDir, filename)
        return f.getAbsolutePath()

    def _check_RepositoryLoader(self):
        """
        Private method for checking if our repository loader has been initialized yet.
        Repository loader is the object that will handle the loaded metrics repository runs
        from the past and prompt for further filtering of the below methods.
        """
        if not self.RepositoryLoader:
            raise AttributeError("No data loaded yet, did you run repository.load() yet?")

    def load(self):
        """
        Get a builder class to construct a loading query to get AnalysisResults
        """
        self.RepositoryLoader = self.repository.load()
        return self

    def withTagValues(self, tagValues: dict):
        """
        Filter out results that don't have specific values for specific tags
        :param tagValues: Dict with tag names and the corresponding values to filter for
        """
        self._check_RepositoryLoader()
        self.RepositoryLoader.withTagValues(to_scala_map(self._spark_session, tagValues))
        return self

    def forAnalyzers(self, analyzers: list):
        """
        Choose all metrics that you want to load
        :param analyzers: List of analyers who's resulting metrics you want to load
        """
        analyzers_jvm = []
        for analyzer in analyzers:
            analyzer._set_jvm(self._jvm)
            analyzers_jvm.append(analyzer._analyzer_jvm)

        self.RepositoryLoader.forAnalyzers(to_scala_seq(self._jvm, analyzers_jvm))
        return self

    def before(self, dateTime: int):
        """
        Only look at AnalysisResults with a result key with a smaller value
        :param dateTime: The maximum dateTime of AnalysisResults to look at
        """
        self._check_RepositoryLoader()
        self.RepositoryLoader.before(dateTime)
        return self

    def after(self, dateTime: int):
        """
        Only look at AnalysisResults with a result key with a greater value
        :param dateTime: The minimum dateTime of AnalysisResults to look at
        """
        self._check_RepositoryLoader()
        self.RepositoryLoader.after(dateTime)
        return self

    def getSuccessMetricsAsJson(self, withTags: list = None):
        """
        Get the AnalysisResult as JSON
        :param withTags: List of tags to filter previous Metrics Repository runs with
        """
        self._check_RepositoryLoader()
        if not withTags:
            withTags = getattr(self.repository.load(), "getSuccessMetricsAsJson$default$1")()  # empty sequence
        else:
            withTags = to_scala_seq(self._jvm, withTags)
        return json.loads(self.RepositoryLoader.getSuccessMetricsAsJson(withTags))

    def getSuccessMetricsAsDataFrame(self, withTags: list = None, pandas: bool = False):
        """
        Get the AnalysisResult as DataFrame
        :param withTags: List of tags to filter previous Metrics Repository runs with
        """
        self._check_RepositoryLoader()
        if not withTags:
            withTags = getattr(self.repository.load(), "getSuccessMetricsAsDataFrame$default$2")()  # empty sequence
        else:
            withTags = to_scala_seq(self._jvm, withTags)
        success = self.RepositoryLoader.getSuccessMetricsAsDataFrame(self._jspark_session, withTags)
        return DataFrame(success, self._spark_session).toPandas() if pandas else DataFrame(success, self._spark_session)


class InMemoryMetricsRepository(MetricsRepository):
    """
    High level InMemoryMetricsRepository Interface
    """

    def __init__(self, spark_session: SparkSession):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        self.deequIMmetRep = spark_session._jvm.com.amazon.deequ.repository.memory.InMemoryMetricsRepository
        self.repository = self.deequIMmetRep()


class FileSystemMetricsRepository(MetricsRepository):
    """
    High level FileSystemMetricsRepository Interface

    :param spark_session: SparkSession
    :param path: The location of the file metrics repository.
    """

    def __init__(self, spark_session: SparkSession, path: str = None):

        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jspark_session = spark_session._jsparkSession
        if not path:
            path = self.helper_metrics_file(self._spark_session)
        self.path = path
        self.deequFSmetRep = spark_session._jvm.com.amazon.deequ.repository.fs.FileSystemMetricsRepository
        self.repository = self.deequFSmetRep(self._jspark_session, path)


class ResultKey:
    """
    Information that uniquely identifies a AnalysisResult

    :param spark_session: SparkSession
    :param dataSetDate: Date of the result key
    :param tags: A map with additional annotations

    """

    def __init__(self, spark_session: SparkSession, dataSetDate: int = None, tags: dict = None):

        self.resultKey = self.__key(spark_session, dataSetDate, tags)

    def __key(self, spark_session: SparkSession, dataSetDate: int = None, tags: dict = None):
        """
        Private method for key construction.

        :param spark_session: Spark Session
        :param dataSetDate: A date related to the AnalysisResult
        :param tags: A map with additional annotations
        """
        if not dataSetDate:
            dataSetDate = self.current_milli_time()
        if not tags:
            tags = getattr(
                spark_session._jvm.com.amazon.deequ.repository.ResultKey, "apply$default$2"
            )()  # empty scala map
        else:
            tags = to_scala_map(spark_session, tags)

        return spark_session._jvm.com.amazon.deequ.repository.ResultKey(dataSetDate, tags)

    @staticmethod
    def current_milli_time():
        """
        Get current time in milliseconds
        # TODO: Consider putting this into scala_utils? Or general utils?
        """
        return int(round(time.time() * 1000))
