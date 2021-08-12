# -*- coding: utf-8 -*-
import unittest

import pytest
from pyspark.sql import Row

from pydeequ.analyzers import *
from pydeequ.anomaly_detection import *
from pydeequ.repository import *
from pydeequ.verification import *
from tests.conftest import setup_pyspark


class TestAnomalies(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-anomalydetection-local").getOrCreate()
        cls.sc = cls.spark.sparkContext

        cls.df_1 = cls.sc.parallelize(
            [
                Row(
                    a=3,
                    b=0,
                    c="colder",
                ),
                Row(
                    a=3,
                    b=5,
                    c="bolder",
                ),
            ]
        ).toDF()

        cls.df_2 = cls.sc.parallelize(
            [
                Row(
                    a=3,
                    b=0,
                    c="foo",
                ),
                Row(
                    a=3,
                    b=5,
                    c="zoo",
                ),
                Row(
                    a=100,
                    b=5,
                    c="who",
                ),
                Row(
                    a=2,
                    b=30,
                    c="email",
                ),
                Row(
                    a=10,
                    b=5,
                    c="cards",
                ),
            ]
        ).toDF()

        cls.df_3 = cls.sc.parallelize(
            [
                Row(
                    a=1,
                    b=23,
                    c="pool",
                )
            ]
        ).toDF()

        cls.df_4 = cls.sc.parallelize(
            [
                Row(
                    a=1,
                    b=23,
                    c="pool",
                )
            ]
        ).toDF()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def RelativeRateOfChangeStrategy(self, df_prev, df_curr, analyzer_func, maxRateDecrease=None, maxRateIncrease=None):
        metricsRepository = InMemoryMetricsRepository(self.spark)
        previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60)

        VerificationSuite(self.spark).onData(df_prev).useRepository(metricsRepository).saveOrAppendResult(
            previousKey
        ).addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateDecrease, maxRateIncrease, order=1), analyzer_func).run()

        currKey = ResultKey(self.spark, ResultKey.current_milli_time())

        currResult = (
            VerificationSuite(self.spark)
            .onData(df_curr)
            .useRepository(metricsRepository)
            .saveOrAppendResult(currKey)
            .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateDecrease, maxRateIncrease, order=1), analyzer_func)
            .run()
        )

        print(VerificationResult.successMetricsAsJson(self.spark, currResult))
        df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)

        print(df.collect())
        return df.select("check_status").collect()

    def AbsoluteChangeStrategy(self, df_prev, df_curr, analyzer_func, maxRateDecrease=None, maxRateIncrease=None):
        metricsRepository = InMemoryMetricsRepository(self.spark)
        previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60)

        VerificationSuite(self.spark).onData(df_prev).useRepository(metricsRepository).saveOrAppendResult(
            previousKey
        ).addAnomalyCheck(AbsoluteChangeStrategy(maxRateDecrease, maxRateIncrease, order=1), analyzer_func).run()

        currKey = ResultKey(self.spark, ResultKey.current_milli_time())

        currResult = (
            VerificationSuite(self.spark)
            .onData(df_curr)
            .useRepository(metricsRepository)
            .saveOrAppendResult(currKey)
            .addAnomalyCheck(AbsoluteChangeStrategy(maxRateDecrease, maxRateIncrease, order=1), analyzer_func)
            .run()
        )

        print(VerificationResult.successMetricsAsJson(self.spark, currResult))

        df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)

        print(df.collect())
        return df.select("check_status").collect()

    def OnlineNormalStrategy(
        self,
        df_prev,
        df_curr,
        analyzer_func,
        lowerDeviationFactor=3.0,
        upperDeviationFactor=3.0,
        ignoreStartPercentage=0.1,
        ignoreAnomalies=True,
    ):
        metricsRepository = InMemoryMetricsRepository(self.spark)
        previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60)

        VerificationSuite(self.spark).onData(df_prev).useRepository(metricsRepository).saveOrAppendResult(
            previousKey
        ).addAnomalyCheck(
            OnlineNormalStrategy(lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies),
            analyzer_func,
        ).run()

        currKey = ResultKey(self.spark, ResultKey.current_milli_time())

        currResult = (
            VerificationSuite(self.spark)
            .onData(df_curr)
            .useRepository(metricsRepository)
            .saveOrAppendResult(currKey)
            .addAnomalyCheck(
                OnlineNormalStrategy(
                    lowerDeviationFactor, upperDeviationFactor, ignoreStartPercentage, ignoreAnomalies
                ),
                analyzer_func,
            )
            .run()
        )

        print(VerificationResult.successMetricsAsJson(self.spark, currResult))

        df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)

        print(df.collect())
        return df.select("check_status").collect()

    def SimpleThresholdStrategy(self, df_prev, df_curr, analyzer_func, lowerBound, upperBound,
                                anomalyCheckConfig: AnomalyCheckConfig = None):
        metricsRepository = InMemoryMetricsRepository(self.spark)
        previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60)

        VerificationSuite(self.spark).onData(df_prev).useRepository(metricsRepository).saveOrAppendResult(
            previousKey
        ).addAnomalyCheck(SimpleThresholdStrategy(lowerBound, upperBound), analyzer_func).run()

        currKey = ResultKey(self.spark, ResultKey.current_milli_time())

        currResult = (
            VerificationSuite(self.spark)
            .onData(df_curr)
            .useRepository(metricsRepository)
            .saveOrAppendResult(currKey)
            .addAnomalyCheck(SimpleThresholdStrategy(lowerBound, upperBound), analyzer_func, anomalyCheckConfig)
            .run()
        )

        df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)

        print(df.collect())
        return df.select("check_status").collect()

    def BatchNormalStrategy(
        self,
        df_prev,
        df_prev_2,
        df_curr,
        analyzer_func,
        lowerDeviationFactor=3.0,
        upperDeviationFactor=3.0,
        includeInterval=False,
    ):
        metricsRepository = InMemoryMetricsRepository(self.spark)

        previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60 * 1)

        VerificationSuite(self.spark).onData(df_prev).useRepository(metricsRepository).saveOrAppendResult(
            previousKey
        ).addAnomalyCheck(
            BatchNormalStrategy(lowerDeviationFactor, upperDeviationFactor, includeInterval), analyzer_func
        ).run()

        previousKey_2 = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60 * 2)

        VerificationSuite(self.spark).onData(df_prev_2).useRepository(metricsRepository).saveOrAppendResult(
            previousKey_2
        ).addAnomalyCheck(
            BatchNormalStrategy(lowerDeviationFactor, upperDeviationFactor, includeInterval), analyzer_func
        ).run()

        currKey = ResultKey(self.spark, ResultKey.current_milli_time())

        currResult = (
            VerificationSuite(self.spark)
            .onData(df_curr)
            .useRepository(metricsRepository)
            .saveOrAppendResult(currKey)
            .addAnomalyCheck(
                BatchNormalStrategy(lowerDeviationFactor, upperDeviationFactor, includeInterval), analyzer_func
            )
            .run()
        )

        print(VerificationResult.successMetricsAsJson(self.spark, currResult))

        df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)

        print(df.collect())
        return df.select("check_status").collect()

    def HoltWinters(self, analyzer_func, test, df_prev, df_curr=None):
        if test == 1:
            metricsRepository = InMemoryMetricsRepository(self.spark)

            for x in range(25):
                previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60 * x)
                prevResult = (
                    VerificationSuite(self.spark)
                    .onData(df_prev)
                    .useRepository(metricsRepository)
                    .saveOrAppendResult(previousKey)
                    .addAnomalyCheck(HoltWinters(MetricInterval.Monthly, SeriesSeasonality.Yearly), analyzer_func)
                    .run()
                )

            print(VerificationResult.successMetricsAsJson(self.spark, prevResult))
            df = VerificationResult.checkResultsAsDataFrame(self.spark, prevResult)

            print(df.collect())
            return df.select("check_status").collect()

        if test in [2, 3]:
            metricsRepository = InMemoryMetricsRepository(self.spark)

            for x in range(14):
                previousKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 1000 * 60 * x)
                VerificationSuite(self.spark).onData(df_prev).useRepository(metricsRepository).saveOrAppendResult(
                    previousKey
                ).addAnomalyCheck(HoltWinters(MetricInterval.Daily, SeriesSeasonality.Weekly), analyzer_func).run()

            currKey = ResultKey(self.spark, ResultKey.current_milli_time())
            currResult = (
                VerificationSuite(self.spark)
                .onData(df_curr)
                .useRepository(metricsRepository)
                .saveOrAppendResult(currKey)
                .addAnomalyCheck(HoltWinters(MetricInterval.Daily, SeriesSeasonality.Weekly), analyzer_func)
                .run()
            )

            print(VerificationResult.successMetricsAsJson(self.spark, currResult))
            df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)

            print(df.collect())
            return df.select("check_status").collect()

    # TODO - Failing bcoz of
    # can not implement breeze.stats.DescriptiveStats, because it is not an interface
    # (breeze.stats.DescriptiveStats is in unnamed module of loader 'app')
    @pytest.mark.xfail(reason="TODO: breeze.stats.DescriptiveStats is in unnamed module of loader 'app'")
    def test_BatchNormalStrategy(self):

        # Interval is inclusive, so meet the requirements upper value is up to 9
        self.assertEqual(
            self.BatchNormalStrategy(self.df_1, self.df_2, self.df_3, Size(), includeInterval=True),
            [Row(check_status="Success")],
        )

        self.assertEqual(
            self.BatchNormalStrategy(self.df_1, self.df_2, self.df_3, Size()), [Row(check_status="Success")]
        )

        self.assertEqual(
            self.BatchNormalStrategy(
                self.df_1, self.df_2, self.df_3, Size(), lowerDeviationFactor=0.5, upperDeviationFactor=0.5
            ),
            [Row(check_status="Warning")],
        )

    def test_OnlineNormalStrategy(self):

        # Sees if table 1 and 2 are within range
        self.assertEqual(self.OnlineNormalStrategy(self.df_1, self.df_2, Size()), [Row(check_status="Success")])

        # df 2 does not meet the requirement
        self.assertEqual(
            self.OnlineNormalStrategy(self.df_3, self.df_2, Size(), lowerDeviationFactor=1.0, upperDeviationFactor=0.5),
            [Row(check_status="Warning")],
        )

        # df 3 does not meet the requirement
        self.assertEqual(
            self.OnlineNormalStrategy(self.df_2, self.df_3, Size(), lowerDeviationFactor=0.5, upperDeviationFactor=1.0),
            [Row(check_status="Warning")],
        )

        # df 3 does not meet the requirement
        self.assertEqual(
            self.OnlineNormalStrategy(
                self.df_2, self.df_3, Size(), lowerDeviationFactor=0.5, upperDeviationFactor=1.0, ignoreAnomalies=False
            ),
            [Row(check_status="Warning")],
        )

        # test with another analyzer
        self.assertEqual(
            self.OnlineNormalStrategy(
                self.df_2,
                self.df_1,
                MinLength("c"),
                lowerDeviationFactor=1.0,
                upperDeviationFactor=1.0,
                ignoreAnomalies=False,
            ),
            [Row(check_status="Success")],
        )

    # TODO - Fix in deequ - Failing bcoz of
    # can not implement breeze.stats.DescriptiveStats, because it is not an interface
    # (breeze.stats.DescriptiveStats is in unnamed module of loader 'app')
    @pytest.mark.xfail(reason="TODO: breeze.stats.DescriptiveStats is in unnamed module of loader 'app'")
    def test_holtWinters(self):

        # must have 15 points of data
        self.assertEqual(self.HoltWinters(Size(), 1, self.df_1), [Row(check_status="Success")])

        self.assertEqual(self.HoltWinters(Size(), 2, self.df_1, self.df_2), [Row(check_status="Warning")])

        self.assertEqual(self.HoltWinters(Size(), 2, self.df_3, self.df_4), [Row(check_status="Success")])

    def test_SimpleThresholdStrategy(self):
        # Lower bound is 1 upper bound is 6 (Range: 1-6 rows)
        self.assertEqual(
            self.SimpleThresholdStrategy(self.df_1, self.df_2, Size(), 1.0, 6.0), [Row(check_status="Success")]
        )

        # Lower bound is 1.0 upper bound is 4.0 (df_2, does not meet requirement)
        self.assertEqual(
            self.SimpleThresholdStrategy(self.df_1, self.df_2, Size(), 1.0, 4.0), [Row(check_status="Warning")]
        )

        # Lower bound is 3.0 upper bound is 6.0 (df_1 does not meet requirement)
        self.assertEqual(
            self.SimpleThresholdStrategy(self.df_2, self.df_1, Size(), 3.0, 6.0), [Row(check_status="Warning")]
        )

        # Test with another analyzer
        self.assertEqual(
            self.SimpleThresholdStrategy(self.df_2, self.df_1, MinLength("c"), 2.0, 6.0), [Row(check_status="Success")]
        )

    def test_AbsoluteChangeStrategy(self):
        # MaxRateIncrease = 5, data should not increase by more than 5
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_1, self.df_2, Size(), maxRateIncrease=5.0),
            [Row(check_status="Success")],
        )

        # MaxRateIncrease = 2, data should not increase by more than 2 (4 cells)
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_1, self.df_2, Size(), maxRateIncrease=2.0),
            [Row(check_status="Warning")],
        )

        # MaxRateDecrease = 1.0, data should not decrease by more than 1 (4 rows)
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_2, self.df_1, Size(), maxRateDecrease=1.0),
            [Row(check_status="Warning")],
        )

        # MaxRateDecrease = -4, data should not decrease by more than 4 (1 row)
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_2, self.df_1, Size(), maxRateDecrease=-4.0),
            [Row(check_status="Success")],
        )

        # MaxRateDecrease = -3.0, data should not decrease by more than .2x
        # MaxRateIncrease = 1.0, data should not decrease by more than 1x
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_2, self.df_1, Size(), maxRateDecrease=-3.0, maxRateIncrease=1.0),
            [Row(check_status="Success")],
        )

        # Test with another analyzer
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_2, self.df_1, MinLength("c"), maxRateDecrease=0.0, maxRateIncrease=4.0),
            [Row(check_status="Success")],
        )

    def test_RelativeRateOfChangeStrategy(self):

        # MaxRateIncrease = 2.5, data should not increase by more than 2.5x
        self.assertEqual(
            self.RelativeRateOfChangeStrategy(self.df_1, self.df_2, Size(), maxRateIncrease=2.5),
            [Row(check_status="Success")],
        )

        # MaxRateIncrease = 2, data should not increase by more than 2x
        self.assertEqual(
            self.RelativeRateOfChangeStrategy(self.df_1, self.df_2, Size(), maxRateIncrease=2.0),
            [Row(check_status="Warning")],
        )

        # MaxRateDecrease = 1.0, data should not decrease by more than 1x
        self.assertEqual(
            self.RelativeRateOfChangeStrategy(self.df_2, self.df_1, Size(), maxRateDecrease=1.0),
            [Row(check_status="Warning")],
        )

        # MaxRateDecrease = .4, data should not decrease by more than .4x
        # From this test we see that RelaticeRateOfChangeStrategy approves values <=.4
        self.assertEqual(
            self.RelativeRateOfChangeStrategy(self.df_2, self.df_1, Size(), maxRateDecrease=0.4),
            [Row(check_status="Success")],
        )

        # MaxRateDecrease = .2, data should not decrease by more than .2x
        # MaxRateIncrease = 1.0, data should not decrease by more than 1x
        self.assertEqual(
            self.RelativeRateOfChangeStrategy(self.df_2, self.df_1, Size(), maxRateDecrease=0.2, maxRateIncrease=1.0),
            [Row(check_status="Success")],
        )

        # Test with another analyzer
        # Valid for anything smaller than 2
        self.assertEqual(
            self.AbsoluteChangeStrategy(self.df_2, self.df_1, MinLength("c"), maxRateDecrease=2.0),
            [Row(check_status="Success")],
        )

    # Todo: test anomaly detector
    # Doesn't work in verification suite
    def get_anomalyDetector(self, anomaly):
        anomaly._set_jvm(self._jvm)
        strategy_jvm = anomaly._anomaly_jvm

        AnomalyDetector._set_jvm(self._jvm, strategy_jvm)
        return AnomalyDetector._anomaly_jvm

    @pytest.mark.skip("Not implemented yet!")
    def test_anomalyDetector(self):
        self.get_anomalyDetector(SimpleThresholdStrategy(1.0, 3.0))

    def test_SimpleThresholdStrategy_Error(self):
        config = AnomalyCheckConfig(description='test error case', level=CheckLevel.Error)
        # Lower bound is 1 upper bound is 6 (Range: 1-6 rows)
        self.assertEqual(
            self.SimpleThresholdStrategy(self.df_1, self.df_2, Size(), 1.0, 4.0, config), [Row(check_status="Error")]
        )

    def test_SimpleThresholdStrategy_Warning(self):
        config = AnomalyCheckConfig(description='test error case', level=CheckLevel.Warning)
        # Lower bound is 1 upper bound is 6 (Range: 1-6 rows)
        self.assertEqual(
            self.SimpleThresholdStrategy(self.df_1, self.df_2, Size(), 1.0, 4.0, config), [Row(check_status="Warning")]
        )

    #
    # def test_RelativeRateOfChangeStrategy(self):
    #     metricsRepository = InMemoryMetricsRepository(self.spark)
    #     yesterdaysKey = ResultKey(self.spark, ResultKey.current_milli_time() - 24 * 60 * 60* 1000)
    #
    #     self.df_yesterday = self.sc.parallelize([
    #         Row(a=3, b=0, ),
    #         Row(a=3, b=5, )]).toDF()
    #
    #     # MaxRateIncrease = 2, data should not increase by more than 2x
    #     prevResult = VerificationSuite(self.spark).onData(self.df_yesterday) \
    #         .useRepository(metricsRepository) \
    #         .saveOrAppendResult(yesterdaysKey) \
    #         .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateIncrease=2.0), Size()) \
    #         .run()
    #
    #     todaysKey = ResultKey(self.spark, ResultKey.current_milli_time())
    #
    #     self.df_today = self.sc.parallelize([
    #         Row(a=3, b=0, ),
    #         Row(a=3, b=5, ),
    #         Row(a=100, b=5,),
    #         Row(a=2, b=30, ),
    #         Row(a=10, b=5)]).toDF()
    #
    #     currResult = VerificationSuite(self.spark).onData(self.df_today) \
    #         .useRepository(metricsRepository) \
    #         .saveOrAppendResult(todaysKey) \
    #         .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateIncrease=2.0), Size()) \
    #         .run()
    #
    #      print(VerificationResult.successMetricsAsJson(self.spark, currResult))
    #
    #     df = VerificationResult.checkResultsAsDataFrame(self.spark, currResult)
    #
    #     print(df.collect())
    #     print(df.select('check_status').collect())
    #
    # if (currResult.status != "Success"):
    #     print("Anomaly detected in the Size() metric!")
    #     metricsRepository.load().forAnalyzers([Size()]).getSuccessMetricsAsDataFrame().show()
