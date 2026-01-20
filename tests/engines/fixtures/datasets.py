# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test dataset definitions ported from Deequ Scala FixtureSupport.

These datasets provide comprehensive edge case coverage for testing
analyzers, constraints, profiles, and suggestions.
"""

from typing import Any, Dict, List, Tuple
import math

import pandas as pd


def create_df_full() -> pd.DataFrame:
    """Basic complete data with no nulls (4 rows).

    Purpose: Basic complete data testing
    Edge cases: No nulls, simple values
    """
    return pd.DataFrame({
        "att1": ["a", "b", "c", "a"],
        "att2": ["d", "e", "f", "d"],
        "item": [1, 2, 3, 4],
        "price": [10.0, 20.0, 30.0, 40.0],
    })


def create_df_missing() -> pd.DataFrame:
    """Dataset with NULL handling patterns (12 rows).

    Purpose: NULL handling tests
    Edge cases: att1: 50% complete, att2: 75% complete
    """
    return pd.DataFrame({
        "att1": ["a", "b", None, None, "e", "f", None, None, "i", "j", None, None],
        "att2": ["d", "e", "f", None, "h", "i", "j", None, "l", "m", "n", None],
        "item": list(range(1, 13)),
    })


def create_df_numeric() -> pd.DataFrame:
    """Dataset for statistical tests (6 rows).

    Purpose: Statistical analyzer tests (Mean, Sum, Min, Max, StdDev)
    Edge cases: Mean=3.5, includes NULL column
    Values: 1, 2, 3, 4, 5, 6 -> Mean=3.5, Sum=21, Min=1, Max=6
    StdDev (population) = sqrt(17.5/6) ≈ 1.7078
    """
    return pd.DataFrame({
        "att1": [1, 2, 3, 4, 5, 6],
        "att2": [1.0, 2.0, 3.0, 4.0, 5.0, None],  # One NULL
        "item": ["a", "b", "c", "d", "e", "f"],
    })


def create_df_unique() -> pd.DataFrame:
    """Dataset for uniqueness pattern tests (6 rows).

    Purpose: Uniqueness analyzer tests
    Edge cases: Various uniqueness scenarios
    - unique_col: All unique values (uniqueness=1.0)
    - half_null: 50% null (completeness=0.5)
    - non_unique: Duplicates present
    """
    return pd.DataFrame({
        "unique_col": [1, 2, 3, 4, 5, 6],
        "half_null": [1, None, 3, None, 5, None],
        "non_unique": [1, 1, 2, 2, 3, 3],
        "all_same": [1, 1, 1, 1, 1, 1],
    })


def create_df_distinct() -> pd.DataFrame:
    """Dataset for distinctness testing with duplicates (6 rows).

    Purpose: Distinctness and uniqueness ratio testing
    Edge cases: 3 distinct values in att1 with duplicates
    - att1: ["a", "a", "b", "b", "c", "c"] -> 3 distinct, 0 unique
    - att2: ["x", "y", "z", "w", "v", "u"] -> 6 distinct, 6 unique
    """
    return pd.DataFrame({
        "att1": ["a", "a", "b", "b", "c", "c"],
        "att2": ["x", "y", "z", "w", "v", "u"],
        "item": [1, 2, 3, 4, 5, 6],
    })


def create_df_string_lengths() -> pd.DataFrame:
    """Dataset for string length edge cases (5 rows).

    Purpose: MinLength and MaxLength analyzer tests
    Edge cases: Empty string (""), varying lengths
    Lengths: 0, 1, 2, 3, 4
    """
    return pd.DataFrame({
        "att1": ["", "a", "bb", "ccc", "dddd"],
        "att2": ["hello", "world", "test", "data", "value"],
        "item": [1, 2, 3, 4, 5],
    })


def create_df_empty() -> pd.DataFrame:
    """Empty dataset with schema (0 rows).

    Purpose: Edge case testing with zero rows
    Edge cases: Size=0, Completeness=1.0 (vacuously true for empty)
    """
    return pd.DataFrame({
        "att1": pd.Series([], dtype="object"),
        "att2": pd.Series([], dtype="object"),
        "item": pd.Series([], dtype="int64"),
    })


def create_df_single() -> pd.DataFrame:
    """Minimal dataset with single row (1 row).

    Purpose: Minimal dataset edge case testing
    Edge cases: StdDev undefined/NaN, Uniqueness=1.0
    """
    return pd.DataFrame({
        "att1": ["a"],
        "att2": ["d"],
        "item": [1],
        "price": [10.0],
    })


def create_df_all_null() -> pd.DataFrame:
    """Dataset with all-NULL column (3 rows).

    Purpose: 0% completeness edge case testing
    Edge cases: Completeness=0, Mean=NULL
    """
    return pd.DataFrame({
        "value": [None, None, None],
        "item": [1, 2, 3],
    })


def create_df_escape() -> pd.DataFrame:
    """Dataset with special characters (8 rows).

    Purpose: Special character and regex escaping tests
    Edge cases: Quotes, special characters (@#$%^&)
    """
    return pd.DataFrame({
        "att1": [
            'hello "world"',
            "it's working",
            "test@example.com",
            "#hashtag",
            "$money$",
            "%percent%",
            "^caret^",
            "&ampersand&",
        ],
        "att2": ["normal", "values", "here", "for", "comparison", "testing", "edge", "cases"],
        "item": list(range(1, 9)),
    })


def create_df_correlation() -> pd.DataFrame:
    """Dataset for correlation testing (5 rows).

    Purpose: Correlation analyzer tests
    Edge cases: Perfect +1.0 and -1.0 correlation
    - x and y: perfectly positively correlated (1.0)
    - x and z: perfectly negatively correlated (-1.0)
    """
    return pd.DataFrame({
        "x": [1.0, 2.0, 3.0, 4.0, 5.0],
        "y": [2.0, 4.0, 6.0, 8.0, 10.0],  # y = 2x, correlation = 1.0
        "z": [5.0, 4.0, 3.0, 2.0, 1.0],   # z = 6-x, correlation = -1.0
        "w": [1.0, 1.0, 1.0, 1.0, 1.0],   # constant, correlation undefined
    })


def create_df_entropy() -> pd.DataFrame:
    """Dataset for entropy testing (4 rows).

    Purpose: Entropy analyzer tests
    Edge cases: Uniform vs skewed distribution
    - uniform: 4 distinct values each appearing once -> entropy = ln(4) ≈ 1.386
    - skewed: 1 value appearing 3 times, 1 appearing once -> entropy < 1.386
    """
    return pd.DataFrame({
        "uniform": ["a", "b", "c", "d"],  # Entropy = ln(4) ≈ 1.386
        "skewed": ["a", "a", "a", "b"],   # Entropy = -(3/4)ln(3/4) - (1/4)ln(1/4) ≈ 0.562
        "constant": ["x", "x", "x", "x"], # Entropy = 0 (single value)
        "item": [1, 2, 3, 4],
    })


def create_df_where() -> pd.DataFrame:
    """Dataset for WHERE clause filtering tests (4 rows).

    Purpose: WHERE clause filter testing
    Edge cases: Mixed completeness by filter
    - When filtered by category='A': att1 is complete
    - When filtered by category='B': att1 has nulls
    """
    return pd.DataFrame({
        "category": ["A", "A", "B", "B"],
        "att1": ["x", "y", None, "w"],  # A: 2/2 complete, B: 1/2 complete
        "att2": [1, None, 3, 4],        # A: 1/2 complete, B: 2/2 complete
        "value": [10.0, 20.0, 30.0, 40.0],
    })


def create_df_pattern() -> pd.DataFrame:
    """Dataset for pattern matching tests (6 rows).

    Purpose: PatternMatch analyzer and regex compliance tests
    Edge cases: Email patterns, phone patterns, mixed valid/invalid
    """
    return pd.DataFrame({
        "email": [
            "test@example.com",
            "user@domain.org",
            "invalid-email",
            "another@test.co.uk",
            "bad@",
            "good.name@company.com",
        ],
        "phone": [
            "123-456-7890",
            "987-654-3210",
            "invalid",
            "555-123-4567",
            "1234567890",
            "800-555-1234",
        ],
        "code": ["ABC123", "DEF456", "xyz789", "GHI012", "JKL345", "mno678"],
        "item": list(range(1, 7)),
    })


def create_df_compliance() -> pd.DataFrame:
    """Dataset for compliance predicate tests (6 rows).

    Purpose: Compliance and satisfies constraint tests
    Edge cases: Positive/negative numbers, boundary conditions
    """
    return pd.DataFrame({
        "positive": [1, 2, 3, 4, 5, 6],
        "negative": [-1, -2, -3, -4, -5, -6],
        "mixed": [-2, -1, 0, 1, 2, 3],
        "with_null": [1, 2, None, 4, 5, None],
        "item": list(range(1, 7)),
    })


def create_df_quantile() -> pd.DataFrame:
    """Dataset for quantile testing (10 rows).

    Purpose: ApproxQuantile analyzer tests
    Edge cases: Sorted values for predictable quantiles
    Values: 1-10, Median (50th percentile) = 5.5
    """
    return pd.DataFrame({
        "value": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "item": list(range(1, 11)),
    })


def create_df_contained_in() -> pd.DataFrame:
    """Dataset for isContainedIn constraint tests (6 rows).

    Purpose: Testing containment in allowed value sets
    Edge cases: All valid, some invalid, NULL handling
    """
    return pd.DataFrame({
        "status": ["active", "inactive", "pending", "active", "inactive", "active"],
        "category": ["A", "B", "C", "A", "B", "D"],  # D is not in typical allowed set
        "priority": [1, 2, 3, 1, 2, 4],  # 4 might be outside allowed range
        "item": list(range(1, 7)),
    })


def create_df_histogram() -> pd.DataFrame:
    """Dataset for histogram testing (10 rows).

    Purpose: Histogram analyzer tests
    Edge cases: Low cardinality categorical data
    """
    return pd.DataFrame({
        "category": ["A", "A", "A", "B", "B", "C", "C", "C", "C", "D"],
        "status": ["active", "active", "inactive", "active", "inactive",
                   "active", "active", "inactive", "inactive", "active"],
        "item": list(range(1, 11)),
    })


def create_df_mutual_info() -> pd.DataFrame:
    """Dataset for mutual information testing (8 rows).

    Purpose: MutualInformation analyzer tests
    Edge cases: Perfectly dependent vs independent columns
    """
    return pd.DataFrame({
        "x": ["a", "a", "b", "b", "c", "c", "d", "d"],
        "y_dependent": ["a", "a", "b", "b", "c", "c", "d", "d"],  # Perfectly dependent on x
        "y_independent": ["p", "q", "r", "s", "p", "q", "r", "s"],  # Less dependent
        "item": list(range(1, 9)),
    })


def create_df_data_type() -> pd.DataFrame:
    """Dataset for DataType analyzer testing.

    Purpose: Testing data type inference
    Edge cases: Mixed numeric strings, pure numeric, non-numeric
    """
    return pd.DataFrame({
        "numeric_strings": ["1", "2", "3", "4", "5"],
        "mixed": ["1", "2", "three", "4", "five"],
        "pure_numeric": [1.0, 2.0, 3.0, 4.0, 5.0],
        "strings": ["a", "b", "c", "d", "e"],
        "item": list(range(1, 6)),
    })


# Expected values registry for DuckDB-only tests
# Key: (dataset_name, analyzer_name, instance) -> expected_value
# instance can be a column name, tuple of columns, or None for dataset-level metrics
EXPECTED_VALUES: Dict[Tuple[str, str, Any], float] = {
    # Size analyzer
    ("df_full", "Size", None): 4.0,
    ("df_missing", "Size", None): 12.0,
    ("df_numeric", "Size", None): 6.0,
    ("df_empty", "Size", None): 0.0,
    ("df_single", "Size", None): 1.0,

    # Completeness analyzer
    ("df_full", "Completeness", "att1"): 1.0,
    ("df_full", "Completeness", "att2"): 1.0,
    ("df_missing", "Completeness", "att1"): 0.5,  # 6/12
    ("df_missing", "Completeness", "att2"): 0.75,  # 9/12
    ("df_all_null", "Completeness", "value"): 0.0,
    ("df_single", "Completeness", "att1"): 1.0,
    ("df_unique", "Completeness", "unique_col"): 1.0,
    ("df_unique", "Completeness", "half_null"): 0.5,  # 3/6

    # Mean analyzer
    ("df_numeric", "Mean", "att1"): 3.5,  # (1+2+3+4+5+6)/6
    ("df_numeric", "Mean", "att2"): 3.0,  # (1+2+3+4+5)/5, NULL excluded
    ("df_single", "Mean", "item"): 1.0,
    ("df_single", "Mean", "price"): 10.0,

    # Sum analyzer
    ("df_numeric", "Sum", "att1"): 21.0,  # 1+2+3+4+5+6
    ("df_numeric", "Sum", "att2"): 15.0,  # 1+2+3+4+5, NULL excluded
    ("df_single", "Sum", "item"): 1.0,
    ("df_single", "Sum", "price"): 10.0,

    # Minimum analyzer
    ("df_numeric", "Minimum", "att1"): 1.0,
    ("df_numeric", "Minimum", "att2"): 1.0,
    ("df_single", "Minimum", "item"): 1.0,
    ("df_single", "Minimum", "price"): 10.0,

    # Maximum analyzer
    ("df_numeric", "Maximum", "att1"): 6.0,
    ("df_numeric", "Maximum", "att2"): 5.0,  # 6 is NULL position
    ("df_single", "Maximum", "item"): 1.0,
    ("df_single", "Maximum", "price"): 10.0,

    # StandardDeviation analyzer (population stddev)
    ("df_numeric", "StandardDeviation", "att1"): 1.7078251276599330,  # sqrt(17.5/6)

    # String length analyzers
    ("df_string_lengths", "MinLength", "att1"): 0.0,  # Empty string
    ("df_string_lengths", "MaxLength", "att1"): 4.0,  # "dddd"
    ("df_string_lengths", "MinLength", "att2"): 4.0,  # "test", "data"
    ("df_string_lengths", "MaxLength", "att2"): 5.0,  # "hello", "world", "value"

    # Distinctness analyzer (distinct values / total rows)
    ("df_distinct", "Distinctness", "att1"): 0.5,  # 3 distinct / 6 rows
    ("df_distinct", "Distinctness", "att2"): 1.0,  # 6 distinct / 6 rows
    ("df_unique", "Distinctness", "all_same"): 1/6,  # 1 distinct / 6 rows

    # Uniqueness analyzer (rows with unique values / total rows)
    ("df_distinct", "Uniqueness", "att1"): 0.0,  # No unique values (all duplicated)
    ("df_distinct", "Uniqueness", "att2"): 1.0,  # All values are unique
    ("df_unique", "Uniqueness", "unique_col"): 1.0,  # All values unique
    ("df_unique", "Uniqueness", "non_unique"): 0.0,  # All values duplicated

    # UniqueValueRatio analyzer (unique values / distinct values)
    ("df_distinct", "UniqueValueRatio", "att1"): 0.0,  # 0 unique / 3 distinct
    ("df_distinct", "UniqueValueRatio", "att2"): 1.0,  # 6 unique / 6 distinct

    # Correlation analyzer
    ("df_correlation", "Correlation", ("x", "y")): 1.0,   # Perfect positive
    ("df_correlation", "Correlation", ("x", "z")): -1.0,  # Perfect negative

    # Entropy analyzer
    ("df_entropy", "Entropy", "uniform"): 1.3862943611198906,  # ln(4) for 4 uniform values
    ("df_entropy", "Entropy", "constant"): 0.0,  # Single value = 0 entropy

    # ApproxCountDistinct analyzer
    ("df_full", "ApproxCountDistinct", "att1"): 3.0,  # "a", "b", "c" (a appears twice)
    ("df_full", "ApproxCountDistinct", "item"): 4.0,  # 1, 2, 3, 4
    ("df_distinct", "ApproxCountDistinct", "att1"): 3.0,  # "a", "b", "c"
    ("df_distinct", "ApproxCountDistinct", "att2"): 6.0,  # All distinct

    # CountDistinct analyzer
    ("df_full", "CountDistinct", "att1"): 3.0,
    ("df_distinct", "CountDistinct", "att1"): 3.0,
    ("df_distinct", "CountDistinct", "att2"): 6.0,

    # PatternMatch analyzer (fraction of rows matching pattern)
    # These will be tested with specific patterns in the tests

    # Compliance analyzer (fraction of rows satisfying predicate)
    ("df_compliance", "Compliance", "positive > 0"): 1.0,  # All positive
    ("df_compliance", "Compliance", "negative < 0"): 1.0,  # All negative
    ("df_compliance", "Compliance", "mixed > 0"): 0.5,  # 3/6 > 0

    # Quantile analyzer (approximate)
    ("df_quantile", "ApproxQuantile", ("value", 0.5)): 5.5,  # Median
    ("df_quantile", "ApproxQuantile", ("value", 0.25)): 3.0,  # 25th percentile (approx)
    ("df_quantile", "ApproxQuantile", ("value", 0.75)): 8.0,  # 75th percentile (approx)
}


# Tolerance levels for comparing floating-point results
FLOAT_EPSILON = 1e-9      # Exact comparisons: Size, Completeness, Uniqueness
FLOAT_TOLERANCE = 1e-6    # Statistical: Mean, StdDev, Correlation
APPROX_TOLERANCE = 0.1    # Approximate algorithms: ApproxCountDistinct (10% relative)


# Mapping of analyzer types to their expected tolerance
ANALYZER_TOLERANCES: Dict[str, float] = {
    # Exact metrics
    "Size": FLOAT_EPSILON,
    "Completeness": FLOAT_EPSILON,
    "Uniqueness": FLOAT_EPSILON,
    "Distinctness": FLOAT_EPSILON,
    "UniqueValueRatio": FLOAT_EPSILON,
    "CountDistinct": FLOAT_EPSILON,
    "MinLength": FLOAT_EPSILON,
    "MaxLength": FLOAT_EPSILON,
    "PatternMatch": FLOAT_EPSILON,
    "Compliance": FLOAT_EPSILON,

    # Statistical metrics
    "Mean": FLOAT_TOLERANCE,
    "Sum": FLOAT_TOLERANCE,
    "Minimum": FLOAT_TOLERANCE,
    "Maximum": FLOAT_TOLERANCE,
    "StandardDeviation": FLOAT_TOLERANCE,
    "Correlation": FLOAT_TOLERANCE,
    "Entropy": FLOAT_TOLERANCE,
    "MutualInformation": FLOAT_TOLERANCE,
    "ApproxQuantile": FLOAT_TOLERANCE,

    # Approximate metrics
    "ApproxCountDistinct": APPROX_TOLERANCE,
}


def get_tolerance(analyzer_name: str) -> float:
    """Get the appropriate tolerance for an analyzer type."""
    return ANALYZER_TOLERANCES.get(analyzer_name, FLOAT_TOLERANCE)


def is_close(actual: float, expected: float, tolerance: float) -> bool:
    """Check if two values are close within tolerance.

    For APPROX_TOLERANCE, uses relative comparison.
    For smaller tolerances, uses absolute comparison.
    """
    if expected is None or actual is None:
        return expected is None and actual is None

    if tolerance >= APPROX_TOLERANCE:
        # Relative tolerance for approximate algorithms
        if expected == 0:
            return abs(actual) < tolerance
        return abs(actual - expected) / abs(expected) < tolerance
    else:
        # Absolute tolerance for exact/statistical metrics
        return abs(actual - expected) < tolerance


# Dataset factory registry
DATASET_FACTORIES = {
    "df_full": create_df_full,
    "df_missing": create_df_missing,
    "df_numeric": create_df_numeric,
    "df_unique": create_df_unique,
    "df_distinct": create_df_distinct,
    "df_string_lengths": create_df_string_lengths,
    "df_empty": create_df_empty,
    "df_single": create_df_single,
    "df_all_null": create_df_all_null,
    "df_escape": create_df_escape,
    "df_correlation": create_df_correlation,
    "df_entropy": create_df_entropy,
    "df_where": create_df_where,
    "df_pattern": create_df_pattern,
    "df_compliance": create_df_compliance,
    "df_quantile": create_df_quantile,
    "df_contained_in": create_df_contained_in,
    "df_histogram": create_df_histogram,
    "df_mutual_info": create_df_mutual_info,
    "df_data_type": create_df_data_type,
}


def get_dataset(name: str) -> pd.DataFrame:
    """Get a dataset by name."""
    if name not in DATASET_FACTORIES:
        raise ValueError(f"Unknown dataset: {name}. Available: {list(DATASET_FACTORIES.keys())}")
    return DATASET_FACTORIES[name]()
