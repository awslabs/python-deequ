# -*- coding: utf-8 -*-
import unittest

from py4j.protocol import Py4JError
from pyspark.sql import Row

from pydeequ.analyzers import AnalyzerContext, AnalysisRunner, ApproxCountDistinct
from pydeequ.checks import Check, CheckLevel
from pydeequ.repository import FileSystemMetricsRepository, InMemoryMetricsRepository, ResultKey
from pydeequ.verification import VerificationResult, VerificationSuite
from tests.conftest import setup_pyspark


class TestRepository(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = setup_pyspark().appName("test-analyzers-local").getOrCreate()
        cls.AnalysisRunner = AnalysisRunner(cls.spark)
        cls.VerificationSuite = VerificationSuite(cls.spark)
        cls.sc = cls.spark.sparkContext
        cls.df = cls.sc.parallelize([Row(a="foo", b=1, c=5), Row(a="bar", b=2, c=6), Row(a="baz", b=3, c=None)]).toDF()

    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext._gateway.shutdown_callback_server()
        cls.spark.stop()

    def test_analyzers_FSmetrep(self):
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(self.spark, "metrics.json")
        print(f"metrics filepath: {metrics_file}")
        repository = FileSystemMetricsRepository(self.spark, metrics_file)
        key_tags = {"tag": "FS metrep analyzers"}
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        result = (
            self.AnalysisRunner.onData(self.df)
            .addAnalyzer(ApproxCountDistinct("b"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_df = result_df.select("value").collect()

        # TEST: Check JSON for tags
        result_metrep_json = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsJson()
        )

        print(result_metrep_json[0]["tag"], key_tags["tag"])
        self.assertEqual(result_metrep_json[0]["tag"], key_tags["tag"])

        # TEST: Check DF parity
        withTags = [key_tags["tag"], "just_another_tag"]
        result_metrep = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsDataFrame(withTags)
        )

        result_metrep_df = result_metrep.select("value").collect()
        print(result_df, result_metrep_df, [Row(value=3)])
        self.assertEqual(result_df, result_metrep_df)
        self.assertEqual(result_metrep_df, [Row(value=3)])
        self.assertEqual(result_df, [Row(value=3)])

    def test_analyzers_FSmetrep_noTags_noFile(self):
        repository = FileSystemMetricsRepository(self.spark)
        print(f"metrics filepath: {repository.path}")
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time())
        result = (
            self.AnalysisRunner.onData(self.df)
            .addAnalyzer(ApproxCountDistinct("b"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_df = result_df.select("value").collect()

        # TEST: Check JSON for tags
        result_metrep_json = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsJson()
        )

        self.assertNotIn("tags", result_metrep_json[0].keys())

        # TEST: Check DF parity
        result_metrep = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsDataFrame()
        )

        result_metrep_df = result_metrep.select("value").collect()
        print(result_df, result_metrep_df, [Row(value=3)])
        self.assertEqual(result_df, result_metrep_df)
        self.assertEqual(result_df, [Row(value=3)])
        self.assertEqual(result_metrep_df, [Row(value=3)])

    def test_verifications_FSmetrep(self):
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(self.spark, "metrics.json")
        print(f"metrics filepath: {metrics_file}")
        repository = FileSystemMetricsRepository(self.spark, metrics_file)
        key_tags = {"tag": "FS metrep verification"}
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        result = (
            self.VerificationSuite.onData(self.df)
            .addCheck(check.hasSize(lambda x: x == 3, "size of dataframe should be 3"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )

        # TEST: Check JSON for tags
        result_metrep_json = repository.load().before(ResultKey.current_milli_time()).getSuccessMetricsAsJson()

        print(result_metrep_json[0]["tag"], key_tags["tag"])
        self.assertEqual(result_metrep_json[0]["tag"], key_tags["tag"])

        result_metrep = repository.load().before(ResultKey.current_milli_time()).getSuccessMetricsAsDataFrame()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        print(df.collect())
        print(result_metrep.collect())

    def test_verifications_FSmetrep_noTags_noFile(self):
        repository = FileSystemMetricsRepository(self.spark)
        print(f"metrics filepath: {repository.path}")
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time())
        check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        result = (
            self.VerificationSuite.onData(self.df)
            .addCheck(check.hasSize(lambda x: x == 3, "size of dataframe should be 3"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )

        # TEST: Check DF parity
        result_metrep = repository.load().before(ResultKey.current_milli_time()).getSuccessMetricsAsDataFrame()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        print(df.collect())
        print(result_metrep.collect())

    def test_analyzers_IMmetrep(self):
        repository = InMemoryMetricsRepository(self.spark)
        key_tags = {"tag": "FS metrep analyzers"}
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        result = (
            self.AnalysisRunner.onData(self.df)
            .addAnalyzer(ApproxCountDistinct("b"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_df = result_df.select("value").collect()

        # TEST: Check JSON for tags
        result_metrep_json = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsJson()
        )

        print(result_metrep_json[0]["tag"], key_tags["tag"])
        self.assertEqual(result_metrep_json[0]["tag"], key_tags["tag"])

        # TEST: Check DF parity
        withTags = [key_tags["tag"], "just_another_tag"]
        result_metrep = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsDataFrame(withTags)
        )

        result_metrep_df = result_metrep.select("value").collect()
        print(result_df, result_metrep_df, [Row(value=3)])
        self.assertEqual(result_df, result_metrep_df)
        self.assertEqual(result_metrep_df, [Row(value=3)])
        self.assertEqual(result_df, [Row(value=3)])

    def test_analyzers_IMmetrep_noTags_noFile(self):
        repository = InMemoryMetricsRepository(self.spark)
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time())
        result = (
            self.AnalysisRunner.onData(self.df)
            .addAnalyzer(ApproxCountDistinct("b"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )
        result_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, result)
        result_df = result_df.select("value").collect()

        # TEST: Check JSON for tags
        result_metrep_json = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsJson()
        )

        self.assertNotIn("tags", result_metrep_json[0].keys())

        # TEST: Check DF parity
        result_metrep = (
            repository.load()
            .before(ResultKey.current_milli_time())
            .forAnalyzers([ApproxCountDistinct("b")])
            .getSuccessMetricsAsDataFrame()
        )

        result_metrep_df = result_metrep.select("value").collect()
        print(result_df, result_metrep_df, [Row(value=3)])
        self.assertEqual(result_df, result_metrep_df)
        self.assertEqual(result_df, [Row(value=3)])
        self.assertEqual(result_metrep_df, [Row(value=3)])

    def test_verifications_IMmetrep(self):
        repository = InMemoryMetricsRepository(self.spark)
        key_tags = {"tag": "IM metrep verification"}
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        result = (
            self.VerificationSuite.onData(self.df)
            .addCheck(check.hasSize(lambda x: x == 3, "size of dataframe should be 3"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )

        # TEST: Check JSON for tags
        result_metrep_json = repository.load().before(ResultKey.current_milli_time()).getSuccessMetricsAsJson()

        print(result_metrep_json[0]["tag"], key_tags["tag"])
        self.assertEqual(result_metrep_json[0]["tag"], key_tags["tag"])

        result_metrep = repository.load().before(ResultKey.current_milli_time()).getSuccessMetricsAsDataFrame()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        print(df.collect())
        print(result_metrep.collect())

    def test_verifications_IMmetrep_noTags_noFile(self):
        repository = InMemoryMetricsRepository(self.spark)
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time())
        check = Check(self.spark, CheckLevel.Warning, "test hasSize")
        result = (
            self.VerificationSuite.onData(self.df)
            .addCheck(check.hasSize(lambda x: x == 3, "size of dataframe should be 3"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )

        # TEST: Check DF parity
        result_metrep = repository.load().before(ResultKey.current_milli_time()).getSuccessMetricsAsDataFrame()

        df = VerificationResult.checkResultsAsDataFrame(self.spark, result)
        print(df.collect())
        print(result_metrep.collect())

    def test_fail_no_useRepository(self):
        """This run fails because it doesn't call useRepository() before saveOrAppendResult()."""
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(self.spark, "metrics.json")
        print(f"metrics filepath: {metrics_file}")
        key_tags = {"tag": "FS metrep analyzers -- FAIL"}
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)

        # MISSING useRepository()
        with self.assertRaises(Py4JError) as err:
            _ = (
                self.AnalysisRunner.onData(self.df)
                .addAnalyzer(ApproxCountDistinct("b"))
                .saveOrAppendResult(resultKey)
                .run()
            )

        self.assertIn(
            "Method saveOrAppendResult([class com.amazon.deequ.repository.ResultKey]) does not exist",
            str(err.exception)
        )

    def test_fail_no_load(self):
        """This run fails because we do not load() for the repository reading."""
        metrics_file = FileSystemMetricsRepository.helper_metrics_file(self.spark, "metrics.json")
        print(f"metrics filepath: {metrics_file}")
        repository = FileSystemMetricsRepository(self.spark, metrics_file)
        key_tags = {"tag": "FS metrep analyzers"}
        resultKey = ResultKey(self.spark, ResultKey.current_milli_time(), key_tags)
        _ = (
            self.AnalysisRunner.onData(self.df)
            .addAnalyzer(ApproxCountDistinct("b"))
            .useRepository(repository)
            .saveOrAppendResult(resultKey)
            .run()
        )

        # MISSING: repository.load()
        with self.assertRaises(AttributeError) as err:
            _ = (
                repository.before(ResultKey.current_milli_time())
                .forAnalyzers([ApproxCountDistinct("b")])
                .getSuccessMetricsAsJson()
            )

        self.assertEqual(
            "'FileSystemMetricsRepository' object has no attribute 'RepositoryLoader'",
            str(err.exception)
        )
