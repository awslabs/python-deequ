# -*- coding: utf-8 -*-
"""
Spark execution engine for PyDeequ.

This module provides a Spark-based execution engine that wraps the existing
v2 Spark Connect API, providing a unified engine interface.

Example usage:
    from pyspark.sql import SparkSession
    import pydeequ
    from pydeequ.v2.verification import AnalysisRunner
    from pydeequ.v2.analyzers import Size, Completeness

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])

    engine = pydeequ.connect(spark)
    result = (AnalysisRunner(engine)
        .onData(dataframe=df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness("a"))
        .run())
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

import pandas as pd

from pydeequ.engines import (
    BaseEngine,
    ColumnProfile,
    ConstraintResult,
    ConstraintSuggestion,
    ConstraintStatus,
    CheckStatus,
    MetricResult,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pydeequ.v2.analyzers import _ConnectAnalyzer
    from pydeequ.v2.checks import Check


class SparkEngine(BaseEngine):
    """
    Spark-based execution engine.

    This engine wraps the existing v2 Spark Connect API to provide
    a unified engine interface. It delegates execution to the
    Deequ plugin running on the Spark cluster.

    Attributes:
        spark: SparkSession
        table: Optional table name
        dataframe: Optional DataFrame to analyze
    """

    def __init__(
        self,
        spark: "SparkSession",
        table: Optional[str] = None,
        dataframe: Optional["DataFrame"] = None,
    ):
        """
        Create a new SparkEngine.

        Args:
            spark: SparkSession (Spark Connect)
            table: Optional table name to analyze
            dataframe: Optional DataFrame to analyze (preferred over table)
        """
        self.spark = spark
        self.table = table
        self._dataframe = dataframe

    def for_table(self, table: str) -> "SparkEngine":
        """Return a new SparkEngine bound to the given table."""
        return SparkEngine(self.spark, table=table)

    def for_dataframe(self, df: "DataFrame") -> "SparkEngine":
        """Return a new SparkEngine bound to the given dataframe."""
        return SparkEngine(self.spark, dataframe=df)

    def _get_dataframe(self) -> "DataFrame":
        """Get the DataFrame to analyze."""
        if self._dataframe is not None:
            return self._dataframe
        if self.table:
            return self.spark.table(self.table)
        raise ValueError("Either dataframe or table must be provided")

    def get_schema(self) -> Dict[str, str]:
        """Get the schema of the data source."""
        df = self._get_dataframe()
        return {field.name: str(field.dataType) for field in df.schema.fields}

    def compute_metrics(
        self, analyzers: Sequence["_ConnectAnalyzer"]
    ) -> List[MetricResult]:
        """
        Compute metrics using the Spark Connect Deequ plugin.

        Args:
            analyzers: Sequence of analyzers to compute metrics for

        Returns:
            List of MetricResult objects
        """
        from pydeequ.v2.verification import _SparkAnalysisRunBuilder

        df = self._get_dataframe()

        # Build and run the analysis using internal builder
        builder = _SparkAnalysisRunBuilder(self.spark, df)
        for analyzer in analyzers:
            builder = builder.addAnalyzer(analyzer)

        result_df = builder.run()

        # Convert Spark DataFrame result to MetricResult objects
        results: List[MetricResult] = []
        for row in result_df.collect():
            results.append(MetricResult(
                name=row["name"],
                instance=row["instance"],
                entity=row["entity"],
                value=float(row["value"]) if row["value"] is not None else None,
            ))

        return results

    def run_checks(self, checks: Sequence["Check"]) -> List[ConstraintResult]:
        """
        Run verification checks using the Spark Connect Deequ plugin.

        Args:
            checks: Sequence of Check objects to evaluate

        Returns:
            List of ConstraintResult objects
        """
        from pydeequ.v2.verification import _SparkVerificationRunBuilder

        df = self._get_dataframe()

        # Build and run the verification using internal builder
        builder = _SparkVerificationRunBuilder(self.spark, df)
        for check in checks:
            builder = builder.addCheck(check)

        result_df = builder.run()

        # Convert Spark DataFrame result to ConstraintResult objects
        results: List[ConstraintResult] = []
        for row in result_df.collect():
            results.append(ConstraintResult(
                check_description=row["check"],
                check_level=row["check_level"],
                check_status=row["check_status"],
                constraint=row["constraint"],
                constraint_status=row["constraint_status"],
                constraint_message=row["constraint_message"],
            ))

        return results

    def profile_columns(
        self,
        columns: Optional[Sequence[str]] = None,
        low_cardinality_threshold: int = 0,
    ) -> List[ColumnProfile]:
        """
        Profile columns using the Spark Connect Deequ plugin.

        Args:
            columns: Optional list of columns to profile
            low_cardinality_threshold: Threshold for histogram computation

        Returns:
            List of ColumnProfile objects
        """
        from pydeequ.v2.profiles import _SparkColumnProfilerRunBuilder

        df = self._get_dataframe()

        # Build and run the profiler using internal builder
        runner = _SparkColumnProfilerRunBuilder(self.spark, df)

        if columns:
            runner = runner.restrictToColumns(columns)

        if low_cardinality_threshold > 0:
            runner = runner.withLowCardinalityHistogramThreshold(low_cardinality_threshold)

        result_df = runner.run()

        # Convert Spark DataFrame result to ColumnProfile objects
        profiles: List[ColumnProfile] = []
        for row in result_df.collect():
            profiles.append(ColumnProfile(
                column=row["column"],
                completeness=float(row["completeness"]) if row["completeness"] is not None else 0.0,
                approx_distinct_values=int(row["approx_distinct_values"]) if row["approx_distinct_values"] is not None else 0,
                data_type=row["data_type"] if row["data_type"] else "Unknown",
                is_data_type_inferred=bool(row["is_data_type_inferred"]) if "is_data_type_inferred" in row else True,
                type_counts=row["type_counts"] if "type_counts" in row else None,
                histogram=row["histogram"] if "histogram" in row else None,
                mean=float(row["mean"]) if "mean" in row and row["mean"] is not None else None,
                minimum=float(row["minimum"]) if "minimum" in row and row["minimum"] is not None else None,
                maximum=float(row["maximum"]) if "maximum" in row and row["maximum"] is not None else None,
                sum=float(row["sum"]) if "sum" in row and row["sum"] is not None else None,
                std_dev=float(row["std_dev"]) if "std_dev" in row and row["std_dev"] is not None else None,
            ))

        return profiles

    def suggest_constraints(
        self,
        columns: Optional[Sequence[str]] = None,
        rules: Optional[Sequence[str]] = None,
    ) -> List[ConstraintSuggestion]:
        """
        Suggest constraints using the Spark Connect Deequ plugin.

        Args:
            columns: Optional list of columns to analyze
            rules: Optional list of rule sets to apply

        Returns:
            List of ConstraintSuggestion objects
        """
        from pydeequ.v2.suggestions import _SparkConstraintSuggestionRunBuilder, Rules

        df = self._get_dataframe()

        # Build and run the suggestion runner using internal builder
        runner = _SparkConstraintSuggestionRunBuilder(self.spark, df)

        if columns:
            runner = runner.restrictToColumns(columns)

        # Map rule strings to Rules enum (accept both strings and enum values)
        if rules:
            rule_map = {
                "DEFAULT": Rules.DEFAULT,
                "STRING": Rules.STRING,
                "NUMERICAL": Rules.NUMERICAL,
                "COMMON": Rules.COMMON,
                "EXTENDED": Rules.EXTENDED,
            }
            for rule in rules:
                # Accept both Rules enum and string values
                if isinstance(rule, Rules):
                    runner = runner.addConstraintRules(rule)
                elif rule in rule_map:
                    runner = runner.addConstraintRules(rule_map[rule])
        else:
            runner = runner.addConstraintRules(Rules.DEFAULT)

        result_df = runner.run()

        # Convert Spark DataFrame result to ConstraintSuggestion objects
        suggestions: List[ConstraintSuggestion] = []
        for row in result_df.collect():
            suggestions.append(ConstraintSuggestion(
                column_name=row["column_name"],
                constraint_name=row["constraint_name"],
                current_value=row["current_value"] if "current_value" in row else None,
                description=row["description"],
                suggesting_rule=row["suggesting_rule"],
                code_for_constraint=row["code_for_constraint"],
            ))

        return suggestions
