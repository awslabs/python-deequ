# -*- coding: utf-8 -*-
"""
PyDeequ Spark Connect Module.

This module provides Spark Connect compatible implementations of PyDeequ's
data quality verification capabilities. It replaces the Py4J-based bridge
with a protobuf-based communication protocol that works with Spark Connect's
client-server architecture.

Key differences from the legacy Py4J-based PyDeequ:
1. Uses serializable predicates instead of Python lambdas
2. Communicates via protobuf messages over gRPC
3. No direct JVM access required

Example usage:
    from pyspark.sql import SparkSession
    from pydeequ.v2 import VerificationSuite, Check, CheckLevel
    from pydeequ.v2.predicates import gte, eq

    # Connect to Spark Connect server
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    # Create a check with constraints
    check = (Check(CheckLevel.Error, "Data quality check")
        .isComplete("id")
        .hasCompleteness("email", gte(0.95))
        .hasSize(eq(1000)))

    # Run verification
    result = (VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run())

    # Result is a DataFrame with check results
    result.show()
"""

# Import analyzers
from pydeequ.v2.analyzers import (
    ApproxCountDistinct,
    ApproxQuantile,
    Completeness,
    Compliance,
    Correlation,
    CountDistinct,
    DataType,
    Distinctness,
    Entropy,
    Histogram,
    Maximum,
    MaxLength,
    Mean,
    Minimum,
    MinLength,
    MutualInformation,
    PatternMatch,
    Size,
    StandardDeviation,
    Sum,
    Uniqueness,
    UniqueValueRatio,
)

# Import checks
from pydeequ.v2.checks import (
    Check,
    CheckLevel,
)

# Import predicates
from pydeequ.v2.predicates import (
    Predicate,
    between,
    eq,
    gt,
    gte,
    is_non_negative,
    is_one,
    is_positive,
    is_zero,
    lt,
    lte,
    neq,
)

# Import profiles
from pydeequ.v2.profiles import (
    ColumnProfilerRunner,
    ColumnProfilerRunBuilder,
    KLLParameters,
)

# Import suggestions
from pydeequ.v2.suggestions import (
    ConstraintSuggestionRunner,
    ConstraintSuggestionRunBuilder,
    Rules,
)

# Import verification
from pydeequ.v2.verification import (
    AnalysisRunBuilder,
    AnalysisRunner,
    VerificationRunBuilder,
    VerificationSuite,
)

__all__ = [
    # Predicates
    "Predicate",
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "between",
    "is_one",
    "is_zero",
    "is_positive",
    "is_non_negative",
    # Checks
    "Check",
    "CheckLevel",
    # Analyzers
    "Size",
    "Completeness",
    "Mean",
    "Sum",
    "Maximum",
    "Minimum",
    "StandardDeviation",
    "Distinctness",
    "Uniqueness",
    "UniqueValueRatio",
    "CountDistinct",
    "ApproxCountDistinct",
    "ApproxQuantile",
    "Correlation",
    "MutualInformation",
    "MaxLength",
    "MinLength",
    "PatternMatch",
    "Compliance",
    "Entropy",
    "Histogram",
    "DataType",
    # Profiles
    "ColumnProfilerRunner",
    "ColumnProfilerRunBuilder",
    "KLLParameters",
    # Suggestions
    "ConstraintSuggestionRunner",
    "ConstraintSuggestionRunBuilder",
    "Rules",
    # Verification
    "VerificationSuite",
    "VerificationRunBuilder",
    "AnalysisRunner",
    "AnalysisRunBuilder",
]
