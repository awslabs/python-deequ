# -*- coding: utf-8 -*-
"""
Constraint Suggestions for Deequ Spark Connect.

This module provides automatic constraint suggestion capabilities that analyze
DataFrame columns and suggest appropriate data quality constraints based on
the data characteristics.

Example usage:
    from pyspark.sql import SparkSession
    from pydeequ.v2.suggestions import ConstraintSuggestionRunner, Rules

    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Basic suggestions with default rules
    suggestions = (ConstraintSuggestionRunner(spark)
        .onData(df)
        .addConstraintRules(Rules.DEFAULT)
        .run())

    # With train/test evaluation
    suggestions = (ConstraintSuggestionRunner(spark)
        .onData(df)
        .addConstraintRules(Rules.EXTENDED)
        .useTrainTestSplitWithTestsetRatio(0.2, seed=42)
        .run())

    suggestions.show()  # Result is a DataFrame with suggested constraints
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence

from google.protobuf import any_pb2

from pydeequ.v2.profiles import KLLParameters
from pydeequ.v2.proto import deequ_connect_pb2 as proto
from pydeequ.v2.spark_helpers import create_deequ_plan, dataframe_from_plan

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class Rules(Enum):
    """
    Constraint suggestion rule sets.

    Different rule sets analyze different aspects of the data:

    - DEFAULT: Core rules for completeness, type retention, categorical ranges
    - STRING: String-specific rules for length constraints
    - NUMERICAL: Numeric rules for min/max/mean/stddev
    - COMMON: Common patterns like uniqueness
    - EXTENDED: All rules combined
    """

    DEFAULT = "DEFAULT"
    """Core rules: CompleteIfComplete, RetainCompleteness, RetainType,
    CategoricalRange, FractionalCategoricalRange, NonNegativeNumbers"""

    STRING = "STRING"
    """String rules: HasMinLength, HasMaxLength"""

    NUMERICAL = "NUMERICAL"
    """Numeric rules: HasMin, HasMax, HasMean, HasStandardDeviation"""

    COMMON = "COMMON"
    """Common patterns: UniqueIfApproximatelyUnique"""

    EXTENDED = "EXTENDED"
    """All rules combined: DEFAULT + STRING + NUMERICAL + COMMON"""


class ConstraintSuggestionRunner:
    """
    Entry point for generating constraint suggestions.

    ConstraintSuggestionRunner analyzes DataFrame columns to suggest
    appropriate data quality constraints based on the data characteristics.

    Example:
        suggestions = (ConstraintSuggestionRunner(spark)
            .onData(df)
            .addConstraintRules(Rules.DEFAULT)
            .run())
    """

    def __init__(self, spark: "SparkSession"):
        """
        Create a new ConstraintSuggestionRunner.

        Args:
            spark: SparkSession (can be either local or Spark Connect)
        """
        self._spark = spark

    def onData(self, df: "DataFrame") -> "ConstraintSuggestionRunBuilder":
        """
        Specify the DataFrame to analyze.

        Args:
            df: DataFrame to analyze for constraint suggestions

        Returns:
            ConstraintSuggestionRunBuilder for method chaining
        """
        return ConstraintSuggestionRunBuilder(self._spark, df)


class ConstraintSuggestionRunBuilder:
    """
    Builder for configuring and executing a constraint suggestion run.

    This class collects suggestion options and executes the analysis
    when run() is called.
    """

    def __init__(self, spark: "SparkSession", df: "DataFrame"):
        """
        Create a new ConstraintSuggestionRunBuilder.

        Args:
            spark: SparkSession
            df: DataFrame to analyze
        """
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

    def addConstraintRules(self, rules: Rules) -> "ConstraintSuggestionRunBuilder":
        """
        Add a constraint rule set.

        Can be called multiple times to add multiple rule sets.

        Args:
            rules: Rules enum value specifying which rules to use

        Returns:
            self for method chaining
        """
        self._rules.append(rules)
        return self

    def restrictToColumns(
        self, columns: Sequence[str]
    ) -> "ConstraintSuggestionRunBuilder":
        """
        Restrict suggestions to specific columns.

        Args:
            columns: List of column names to analyze

        Returns:
            self for method chaining
        """
        self._restrict_to_columns = columns
        return self

    def withLowCardinalityHistogramThreshold(
        self, threshold: int
    ) -> "ConstraintSuggestionRunBuilder":
        """
        Set threshold for computing histograms during profiling.

        Args:
            threshold: Maximum distinct values for histogram computation

        Returns:
            self for method chaining
        """
        self._low_cardinality_threshold = threshold
        return self

    def withKLLProfiling(self) -> "ConstraintSuggestionRunBuilder":
        """
        Enable KLL sketch profiling for numeric columns.

        Returns:
            self for method chaining
        """
        self._enable_kll = True
        return self

    def setKLLParameters(
        self, params: KLLParameters
    ) -> "ConstraintSuggestionRunBuilder":
        """
        Set KLL sketch parameters.

        Args:
            params: KLLParameters configuration

        Returns:
            self for method chaining
        """
        self._kll_parameters = params
        return self

    def setPredefinedTypes(
        self, types: Dict[str, str]
    ) -> "ConstraintSuggestionRunBuilder":
        """
        Set predefined data types for columns.

        Args:
            types: Dictionary mapping column names to type names

        Returns:
            self for method chaining
        """
        self._predefined_types = types
        return self

    def useTrainTestSplitWithTestsetRatio(
        self, ratio: float, seed: Optional[int] = None
    ) -> "ConstraintSuggestionRunBuilder":
        """
        Enable train/test split for evaluating suggestions.

        When enabled, the data is split into training and test sets.
        Suggestions are generated from the training set and then
        evaluated against the test set.

        Args:
            ratio: Fraction of data to use as test set (0.0-1.0)
            seed: Optional random seed for reproducibility

        Returns:
            self for method chaining
        """
        if not 0.0 < ratio < 1.0:
            raise ValueError("testset_ratio must be between 0.0 and 1.0 (exclusive)")
        self._testset_ratio = ratio
        self._testset_seed = seed
        return self

    def run(self) -> "DataFrame":
        """
        Execute the suggestion analysis and return results as a DataFrame.

        The result DataFrame contains columns:
        - column_name: Column the constraint applies to
        - constraint_name: Type of constraint (e.g., "Completeness", "IsIn")
        - current_value: Current metric value that triggered suggestion
        - description: Human-readable description
        - suggesting_rule: Rule that generated this suggestion
        - code_for_constraint: Python code snippet for the constraint

        If train/test split is enabled:
        - evaluation_status: "Success" or "Failure" on test set
        - evaluation_metric_value: Actual metric on test set

        Returns:
            DataFrame with constraint suggestions

        Raises:
            RuntimeError: If the Deequ plugin is not available on the server
            ValueError: If no rules have been added
        """
        if not self._rules:
            raise ValueError(
                "At least one constraint rule set must be added. "
                "Use .addConstraintRules(Rules.DEFAULT) to add rules."
            )

        # Build the protobuf message
        suggestion_msg = self._build_suggestion_message()

        # V2 only supports Spark Connect
        return self._run_via_spark_connect(suggestion_msg)

    def _build_suggestion_message(self) -> proto.DeequConstraintSuggestionRelation:
        """Build the protobuf suggestion message."""
        msg = proto.DeequConstraintSuggestionRelation()

        # Add constraint rules
        for rule in self._rules:
            msg.constraint_rules.append(rule.value)

        # Set column restrictions
        if self._restrict_to_columns:
            msg.restrict_to_columns.extend(self._restrict_to_columns)

        # Set histogram threshold
        if self._low_cardinality_threshold > 0:
            msg.low_cardinality_histogram_threshold = self._low_cardinality_threshold

        # Set KLL profiling
        msg.enable_kll_profiling = self._enable_kll
        if self._kll_parameters:
            msg.kll_parameters.CopyFrom(self._kll_parameters.to_proto())

        # Set predefined types
        if self._predefined_types:
            for col, dtype in self._predefined_types.items():
                msg.predefined_types[col] = dtype

        # Set train/test split
        if self._testset_ratio > 0:
            msg.testset_ratio = self._testset_ratio
            if self._testset_seed is not None:
                msg.testset_split_random_seed = self._testset_seed

        return msg

    def _run_via_spark_connect(
        self, msg: proto.DeequConstraintSuggestionRelation
    ) -> "DataFrame":
        """Execute suggestion analysis via Spark Connect plugin."""
        # Get the input DataFrame's plan as serialized bytes
        input_plan = self._df._plan.to_proto(self._spark._client)
        msg.input_relation = input_plan.root.SerializeToString()

        # Wrap our Deequ message in a google.protobuf.Any
        extension = any_pb2.Any()
        extension.Pack(msg, type_url_prefix="type.googleapis.com")

        # Create a proper LogicalPlan subclass with the extension
        plan = create_deequ_plan(extension)

        # Create DataFrame from the plan (handles Spark 3.x vs 4.x)
        return dataframe_from_plan(plan, self._spark)


# Export all public symbols
__all__ = [
    "ConstraintSuggestionRunner",
    "ConstraintSuggestionRunBuilder",
    "Rules",
]
