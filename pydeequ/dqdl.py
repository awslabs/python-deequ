# -*- coding: utf-8 -*-
"""Evaluate data quality rules written in DQDL (Data Quality Definition Language).

See https://docs.aws.amazon.com/glue/latest/dg/dqdl.html for the DQDL syntax.
"""
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from pydeequ.pandas_utils import ensure_pyspark_df
from pydeequ.scala_utils import to_scala_map


class EvaluateDataQuality:
    """Validates a DataFrame against a ruleset defined in DQDL.

    Example::

        ruleset = '''Rules=[
            IsComplete "id",
            DataFreshness "updated_at" <= 24 hours
        ]'''
        outcomes = EvaluateDataQuality.process(spark, df, ruleset)
    """

    ORIGINAL_DATA_KEY = "originalData"
    RULE_OUTCOMES_KEY = "ruleOutcomes"
    ROW_LEVEL_OUTCOMES_KEY = "rowLevelOutcomes"

    @classmethod
    def process(
        cls,
        spark_session: SparkSession,
        data: DataFrame,
        rulesetDefinition: str,
        additionalDataSources: Optional[Dict[str, DataFrame]] = None,
        pandas: bool = False,
    ):
        """
        Evaluates a DQDL ruleset and returns one row per rule.

        :param SparkSession spark_session: SparkSession
        :param DataFrame data: DataFrame to validate
        :param str rulesetDefinition: DQDL ruleset, e.g. 'Rules=[DataFreshness "ts" <= 24 hours]'
        :param dict additionalDataSources: alias -> DataFrame for dataset comparison rules
                (e.g. RowCountMatch, ReferentialIntegrity)
        :param bool pandas: If True, return a Pandas DataFrame instead of PySpark
        :return: DataFrame with columns Rule, Outcome, FailureReason, EvaluatedMetrics, EvaluatedRule
        """
        jdf = cls._evaluator(spark_session).process(
            *cls._arguments(spark_session, data, rulesetDefinition, additionalDataSources)
        )
        df = DataFrame(jdf, spark_session)
        return df.toPandas() if pandas else df

    @classmethod
    def processRows(
        cls,
        spark_session: SparkSession,
        data: DataFrame,
        rulesetDefinition: str,
        additionalDataSources: Optional[Dict[str, DataFrame]] = None,
        pandas: bool = False,
    ) -> Dict[str, DataFrame]:
        """
        Evaluates a DQDL ruleset and returns both rule-level and row-level outcomes.

        :param SparkSession spark_session: SparkSession
        :param DataFrame data: DataFrame to validate
        :param str rulesetDefinition: DQDL ruleset
        :param dict additionalDataSources: alias -> DataFrame for dataset comparison rules
        :param bool pandas: If True, the returned DataFrames are Pandas DataFrames
        :return: dict with keys "originalData" (the input data), "ruleOutcomes" (one row per rule)
                and "rowLevelOutcomes" (input rows with per-row passed/failed/skipped rule arrays)
        """
        results = cls._evaluator(spark_session).processRows(
            *cls._arguments(spark_session, data, rulesetDefinition, additionalDataSources)
        )
        keys = (cls.ORIGINAL_DATA_KEY, cls.RULE_OUTCOMES_KEY, cls.ROW_LEVEL_OUTCOMES_KEY)
        dfs = {key: DataFrame(results.apply(key), spark_session) for key in keys}
        return {key: df.toPandas() for key, df in dfs.items()} if pandas else dfs

    @staticmethod
    def _evaluator(spark_session: SparkSession):
        return spark_session._jvm.com.amazon.deequ.dqdl.EvaluateDataQuality

    @staticmethod
    def _arguments(spark_session, data, rulesetDefinition, additionalDataSources):
        if not isinstance(rulesetDefinition, str):
            raise TypeError(f"Expected str for rulesetDefinition, not {type(rulesetDefinition)}")
        data = ensure_pyspark_df(spark_session, data)
        sources = {
            alias: ensure_pyspark_df(spark_session, df)._jdf
            for alias, df in (additionalDataSources or {}).items()
        }
        return data._jdf, rulesetDefinition, to_scala_map(spark_session, sources)
