from pydeequ.analyzers import _AnalyzerObject
import json

from pyspark import SQLContext
from pyspark.sql import DataFrame, SparkSession

from pydeequ.analyzers import AnalysisRunBuilder
from pydeequ.checks import Check
from pydeequ.pandas_utils import ensure_pyspark_df

# TODO integrate Analyzer context


class AnomalyCheckConfig:
    pass


class VerificationResult:
    """The results returned from the VerificationSuite
    :param verificationRunBuilder verificationRun:  verification result run()
    """

    def __init__(self, spark_session: SparkSession, verificationRun):
        self._spark_session = spark_session
        self.verificationRun = verificationRun

    @property
    def status(self):
        """
        Getter for the overall status of the verification Run()
        :return: gets the overall status of the verification Run()
        """
        return str(self.verificationRun.status())

    @property
    def checkResults(self):
        """
        Getter for the checks and checksResults of the verification Run()
        :return: JSON with the checks and the checkResults of the verification Run()
        """
        return self.checkResultsAsJson(self._spark_session, self)

    @property
    def metrics(self):
        """
        Getter for the analyzers and their metric results of the verification Run()
        :return: gets the Analyzers and their metric results of the verification Run()
        """
        return self.verificationRun.metrics()

    @classmethod
    def successMetricsAsDataFrame(
        cls, spark_session: SparkSession, verificationResult, forAnalyzers: list = None, pandas: bool = False
    ):
        """
        The results returned in a Data Frame

        :param spark_session: Sparksession
        :param verificationResult: Result of the verification Run()
        :param forAnalyzers: Subset of Analyzers from the Analysis Run
        :return: Data frame of the verification Run()
        """
        if forAnalyzers:
            raise NotImplementedError("forAnalyzers have not been implemented yet.")
        forAnalyzers = getattr(
            spark_session._jvm.com.amazon.deequ.VerificationResult, "successMetricsAsDataFrame$default$3"
        )()

        df = spark_session._jvm.com.amazon.deequ.VerificationResult.successMetricsAsDataFrame(
            spark_session._jsparkSession, verificationResult.verificationRun, forAnalyzers
        )

        sql_ctx = SQLContext(
            sparkContext=spark_session._sc,
            sparkSession=spark_session,
            jsqlContext=spark_session._jsparkSession.sqlContext(),
        )
        return DataFrame(df, sql_ctx).toPandas() if pandas else DataFrame(df, sql_ctx)

    @classmethod
    def successMetricsAsJson(cls, spark_session: SparkSession, verificationResult, forAnalyzers: list = None):
        """
        The results returned in a JSON

        :param SparkSession spark_session: Sparksession
        :param verificationResult: Result of the verification Run()
        :param forAnalyzers: Subset of Analyzers from the Analysis Run
        :return: JSON of the verification Run()
        """
        if forAnalyzers:
            raise NotImplementedError("forAnalyzers have not been implemented yet.")
        forAnalyzers = getattr(
            spark_session._jvm.com.amazon.deequ.VerificationResult, "successMetricsAsJson$default$2"
        )()
        df = spark_session._jvm.com.amazon.deequ.VerificationResult.successMetricsAsJson(
            verificationResult.verificationRun, forAnalyzers
        )
        return json.loads(df)

    @classmethod
    def checkResultsAsJson(cls, spark_session: SparkSession, verificationResult, forChecks=None):
        """
         Returns the check verification Results as a JSON

        :param SparkSession spark_session: SparkSession
        :param verificationResult: The results of the verification run
        :param forChecks: Subset of Checks
        :return: returns a JSON
        """
        if forChecks:
            raise NotImplementedError("forChecks have not been implemented yet.")
        forChecks = getattr(spark_session._jvm.com.amazon.deequ.VerificationResult, "checkResultsAsJson$default$2")()
        df = spark_session._jvm.com.amazon.deequ.VerificationResult.checkResultsAsJson(
            verificationResult.verificationRun, forChecks
        )
        return json.loads(df)

    @classmethod
    def checkResultsAsDataFrame(
        cls, spark_session: SparkSession, verificationResult, forChecks=None, pandas: bool = False
    ):
        """
        Returns the verificaton Results as a Data Frame

        :param SparkSession spark_session: SparkSession
        :param verificationResult: The results of the verification run
        :param forChecks: Subset of Checks
        :return: returns a Data Frame of the results
        """
        if forChecks:
            raise NotImplementedError("forChecks have not been implemented yet.")
        forChecks = getattr(
            spark_session._jvm.com.amazon.deequ.VerificationResult, "checkResultsAsDataFrame$default$3"
        )()

        df = spark_session._jvm.com.amazon.deequ.VerificationResult.checkResultsAsDataFrame(
            spark_session._jsparkSession, verificationResult.verificationRun, forChecks
        )
        sql_ctx = SQLContext(
            sparkContext=spark_session._sc,
            sparkSession=spark_session,
            jsqlContext=spark_session._jsparkSession.sqlContext(),
        )
        return DataFrame(df, sql_ctx).toPandas() if pandas else DataFrame(df, sql_ctx)


class VerificationRunBuilder:
    # TODO Remaining Methods

    """A class to build a Verification Run"""

    def __init__(self, spark_session: SparkSession, data: DataFrame):
        """
        :param SparkSession spark_session: SparkSession
        :param DataFrame data: DataFrame of the data
        """
        if not isinstance(spark_session, SparkSession):
            raise TypeError(f"Expected SparkSession object for spark_session, not {type(spark_session)}")
        if not isinstance(data, DataFrame):
            raise TypeError(f"Expected DataFrame object for data, not {type(data)}")

        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm
        self._VerificationRunBuilder = self._jvm.com.amazon.deequ.VerificationRunBuilder(data._jdf)
        self.checks = []

    def addCheck(self, check: Check):
        """
        Adds checks to the run using checks method.

        :param Check check: A check object to be executed during the run
        :return: Adds checks to the run
        """
        # TODO Support Multiple checks
        self.checks.append(check)
        self._VerificationRunBuilder.addCheck(check._Check)
        return self

    def addAnomalyCheck(self, anomaly, analyzer: _AnalyzerObject, anomalyCheckConfig=None):
        """
        Add a check using anomaly_detection methods. The Anomaly Detection Strategy only checks
        if the new value is an Anomaly.

        :param anomaly:The anomaly detection strategy
        :param AnalysisRunBuilder analyzer: The analyzer for the metric to run anomaly detection on
        :param anomalyCheckConfig: Some configuration settings for the Check
        :return: Adds an anomaly strategy to the run
        """
        if anomalyCheckConfig:
            raise NotImplementedError("anomalyCheckConfigs have not been implemented yet, using default value")

        AnomalyCheckConfig = self._jvm.scala.Option.apply(anomalyCheckConfig)

        anomaly._set_jvm(self._jvm)
        anomaly_jvm = anomaly._anomaly_jvm

        analyzer._set_jvm(self._jvm)
        analyzer_jvm = analyzer._analyzer_jvm

        self._VerificationRunBuilder.addAnomalyCheck(anomaly_jvm, analyzer_jvm, AnomalyCheckConfig)
        return self

    def run(self):
        """
        A method that runs the desired VerificationRunBuilder functions on the data to obtain a Verification Result
        :return:a verificationResult object
        """
        return VerificationResult(self._spark_session, self._VerificationRunBuilder.run())

    def useRepository(self, repository):
        """
        This method reassigns our AnalysisRunBuilder because useRepository returns back a different
        class: AnalysisRunBuilderWithRepository

        Sets a metrics repository associated with the current data to enable features like reusing previously computed
        results and storing the results of the current run.

        :param repository: a metrics repository to store and load results associated with the run
        """
        self._VerificationRunBuilder = self._VerificationRunBuilder.useRepository(repository.repository)
        return self

    def saveOrAppendResult(self, resultKey):
        """
        A shortcut to save the results of the run or append them to the existing results in the metrics repository.

        :param resultKey: The result key to identify the current run
        :return: :A VerificationRunBuilder.scala object that saves or appends a result
        """
        self._VerificationRunBuilder.saveOrAppendResult(resultKey.resultKey)
        return self


class VerificationRunBuilderWithSparkSession(VerificationRunBuilder):
    pass


class VerificationSuite:
    # TODO Remaining Methods
    """Responsible for running checks, the required analysis and return the results"""

    def __init__(self, spark_session):
        """

        :param SparkSession spark_session: The SparkSession
        """
        if not isinstance(spark_session, SparkSession):
            raise TypeError(f"Expected SparkSession object for spark_session, not {type(spark_session)}")

        self._spark_session = spark_session
        self._sc = spark_session.sparkContext
        self._jvm = spark_session._jvm

    def onData(self, df):
        """
        Starting point to construct a VerificationRun.
        :param data:  Tabular data on which the checks should be verified
        :return: The starting point to construct a verificationRun
        """
        df = ensure_pyspark_df(self._spark_session, df)
        return VerificationRunBuilder(self._spark_session, df)
