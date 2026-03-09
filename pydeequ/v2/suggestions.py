# -*- coding: utf-8 -*-
"""
Constraint Suggestions for PyDeequ v2.

This module provides automatic constraint suggestion capabilities that analyze
columns and suggest appropriate data quality constraints based on the data
characteristics.

Example usage with DuckDB:
    import duckdb
    import pydeequ
    from pydeequ.v2.suggestions import ConstraintSuggestionRunner, Rules

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id, 'foo' as name")
    engine = pydeequ.connect(con)

    suggestions = (ConstraintSuggestionRunner(engine)
        .onData(table="test")
        .addConstraintRules(Rules.DEFAULT)
        .run())

Example usage with Spark Connect:
    from pyspark.sql import SparkSession
    import pydeequ
    from pydeequ.v2.suggestions import ConstraintSuggestionRunner, Rules

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    engine = pydeequ.connect(spark)

    suggestions = (ConstraintSuggestionRunner(engine)
        .onData(dataframe=df)
        .addConstraintRules(Rules.DEFAULT)
        .run())
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

import pandas as pd

from pydeequ.v2.profiles import KLLParameters

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pydeequ.engines import BaseEngine


class Rules(Enum):
    """
    Constraint suggestion rule sets.

    - DEFAULT: Core rules for completeness, type retention, categorical ranges
    - STRING: String-specific rules for length constraints
    - NUMERICAL: Numeric rules for min/max/mean/stddev
    - COMMON: Common patterns like uniqueness
    - EXTENDED: All rules combined
    """

    DEFAULT = "DEFAULT"
    STRING = "STRING"
    NUMERICAL = "NUMERICAL"
    COMMON = "COMMON"
    EXTENDED = "EXTENDED"


class ConstraintSuggestionRunner:
    """
    Entry point for generating constraint suggestions.

    Takes an engine in the constructor and uses ``onData()`` to bind data.

    Example:
        suggestions = (ConstraintSuggestionRunner(engine)
            .onData(table="users")
            .addConstraintRules(Rules.DEFAULT)
            .run())
    """

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine

    def onData(
        self, *, table: Optional[str] = None, dataframe: "Optional[DataFrame]" = None
    ) -> "EngineConstraintSuggestionRunBuilder":
        """
        Bind data for constraint suggestion.

        Args:
            table: Table name (keyword-only)
            dataframe: DataFrame (keyword-only)

        Returns:
            EngineConstraintSuggestionRunBuilder for method chaining
        """
        if table is not None and dataframe is not None:
            raise ValueError("Provide either 'table' or 'dataframe', not both")
        if table is not None:
            bound_engine = self._engine.for_table(table)
        elif dataframe is not None:
            bound_engine = self._engine.for_dataframe(dataframe)
        else:
            raise ValueError("Must provide either 'table' or 'dataframe'")
        return EngineConstraintSuggestionRunBuilder(bound_engine)


class EngineConstraintSuggestionRunBuilder:
    """Builder for configuring and executing engine-based constraint suggestions."""

    def __init__(self, engine: "BaseEngine"):
        self._engine = engine
        self._rules: List[Rules] = []
        self._restrict_to_columns: Optional[Sequence[str]] = None

    def addConstraintRules(self, rules: Rules) -> "EngineConstraintSuggestionRunBuilder":
        self._rules.append(rules)
        return self

    def restrictToColumns(
        self, columns: Sequence[str]
    ) -> "EngineConstraintSuggestionRunBuilder":
        self._restrict_to_columns = columns
        return self

    def run(self) -> pd.DataFrame:
        """Execute the suggestion analysis and return results as a pandas DataFrame."""
        if not self._rules:
            raise ValueError(
                "At least one constraint rule set must be added. "
                "Use .addConstraintRules(Rules.DEFAULT) to add rules."
            )
        rule_strs = [r.value for r in self._rules]
        suggestions = self._engine.suggest_constraints(
            columns=self._restrict_to_columns,
            rules=rule_strs,
        )
        return self._engine.suggestions_to_dataframe(suggestions)


# ---------------------------------------------------------------------------
# Private Spark Connect builder (used internally by SparkEngine)
# ---------------------------------------------------------------------------

class _SparkConstraintSuggestionRunBuilder:
    """Internal builder for Spark Connect suggestions (protobuf-based)."""

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        self._spark = spark
        self._df = df
        self._rules: List[Rules] = []
        self._restrict_to_columns: Optional[Sequence[str]] = None
        self._low_cardinality_threshold: int = 0
        self._enable_kll: bool = False
        self._kll_parameters: Optional[KLLParameters] = None
        self._predefined_types: Optional[Dict[str, str]] = None
        self._testset_ratio: float = 0.0
        self._testset_seed: Optional[int] = None

    def addConstraintRules(self, rules: Rules) -> "_SparkConstraintSuggestionRunBuilder":
        self._rules.append(rules)
        return self

    def restrictToColumns(
        self, columns: Sequence[str]
    ) -> "_SparkConstraintSuggestionRunBuilder":
        self._restrict_to_columns = columns
        return self

    def withLowCardinalityHistogramThreshold(
        self, threshold: int
    ) -> "_SparkConstraintSuggestionRunBuilder":
        self._low_cardinality_threshold = threshold
        return self

    def withKLLProfiling(self) -> "_SparkConstraintSuggestionRunBuilder":
        self._enable_kll = True
        return self

    def setKLLParameters(
        self, params: KLLParameters
    ) -> "_SparkConstraintSuggestionRunBuilder":
        self._kll_parameters = params
        return self

    def setPredefinedTypes(
        self, types: Dict[str, str]
    ) -> "_SparkConstraintSuggestionRunBuilder":
        self._predefined_types = types
        return self

    def useTrainTestSplitWithTestsetRatio(
        self, ratio: float, seed: Optional[int] = None
    ) -> "_SparkConstraintSuggestionRunBuilder":
        if not 0.0 < ratio < 1.0:
            raise ValueError("testset_ratio must be between 0.0 and 1.0 (exclusive)")
        self._testset_ratio = ratio
        self._testset_seed = seed
        return self

    def run(self) -> "DataFrame":
        from google.protobuf import any_pb2
        from pydeequ.v2.proto import deequ_connect_pb2 as proto
        from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

        if not self._rules:
            raise ValueError(
                "At least one constraint rule set must be added. "
                "Use .addConstraintRules(Rules.DEFAULT) to add rules."
            )

        msg = proto.DeequConstraintSuggestionRelation()

        for rule in self._rules:
            msg.constraint_rules.append(rule.value)
        if self._restrict_to_columns:
            msg.restrict_to_columns.extend(self._restrict_to_columns)
        if self._low_cardinality_threshold > 0:
            msg.low_cardinality_histogram_threshold = self._low_cardinality_threshold
        msg.enable_kll_profiling = self._enable_kll
        if self._kll_parameters:
            msg.kll_parameters.CopyFrom(self._kll_parameters.to_proto())
        if self._predefined_types:
            for col, dtype in self._predefined_types.items():
                msg.predefined_types[col] = dtype
        if self._testset_ratio > 0:
            msg.testset_ratio = self._testset_ratio
            if self._testset_seed is not None:
                msg.testset_split_random_seed = self._testset_seed

        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")
        plan = create_deequ_plan(extension)
        return dataframe_from_plan(plan, self._spark)


# Export all public symbols
__all__ = [
    "ConstraintSuggestionRunner",
    "EngineConstraintSuggestionRunBuilder",
    "Rules",
]
