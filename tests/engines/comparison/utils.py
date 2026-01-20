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

"""Comparison utilities for cross-engine testing.

Provides utilities for comparing results between DuckDB and Spark engines
with appropriate tolerance levels for different metric types.
"""

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from pydeequ.engines import MetricResult, ConstraintResult, ColumnProfile


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


def values_equal(
    actual: Any,
    expected: Any,
    tolerance: float = FLOAT_TOLERANCE
) -> bool:
    """Check if two values are equal within tolerance.

    Handles None, NaN, strings, and numeric values appropriately.

    Args:
        actual: The actual value from DuckDB
        expected: The expected value from Spark
        tolerance: The tolerance for numeric comparison

    Returns:
        True if values are considered equal
    """
    # Handle None/null
    if actual is None and expected is None:
        return True
    if actual is None or expected is None:
        return False

    # Handle NaN
    if isinstance(actual, float) and isinstance(expected, float):
        if math.isnan(actual) and math.isnan(expected):
            return True
        if math.isnan(actual) or math.isnan(expected):
            return False

    # Handle strings
    if isinstance(actual, str) and isinstance(expected, str):
        return actual == expected

    # Handle numeric values
    try:
        actual_float = float(actual)
        expected_float = float(expected)

        if tolerance >= APPROX_TOLERANCE:
            # Relative tolerance for approximate algorithms
            if expected_float == 0:
                return abs(actual_float) < tolerance
            return abs(actual_float - expected_float) / abs(expected_float) < tolerance
        else:
            # Absolute tolerance for exact/statistical metrics
            return abs(actual_float - expected_float) < tolerance
    except (TypeError, ValueError):
        # Fall back to exact equality
        return actual == expected


@dataclass
class MetricDifference:
    """Represents a difference between two metric results."""
    name: str
    instance: Optional[str]
    spark_value: Any
    duckdb_value: Any
    tolerance: float
    is_match: bool
    message: str = ""


@dataclass
class ComparisonReport:
    """Report comparing results from two engines."""
    total_metrics: int = 0
    matching_metrics: int = 0
    differing_metrics: int = 0
    spark_only_metrics: int = 0
    duckdb_only_metrics: int = 0
    differences: List[MetricDifference] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True if all metrics match within tolerance."""
        return self.differing_metrics == 0 and self.spark_only_metrics == 0 and self.duckdb_only_metrics == 0

    def summary(self) -> str:
        """Generate a summary string."""
        lines = [
            f"Comparison Report:",
            f"  Total metrics: {self.total_metrics}",
            f"  Matching: {self.matching_metrics}",
            f"  Differing: {self.differing_metrics}",
            f"  Spark-only: {self.spark_only_metrics}",
            f"  DuckDB-only: {self.duckdb_only_metrics}",
        ]
        if self.differences:
            lines.append("  Differences:")
            for diff in self.differences:
                lines.append(f"    - {diff.name}({diff.instance}): Spark={diff.spark_value}, DuckDB={diff.duckdb_value}")
        return "\n".join(lines)


def index_metrics(metrics: List[MetricResult]) -> Dict[Tuple[str, str], MetricResult]:
    """Index metrics by (name, instance) tuple for efficient lookup."""
    return {(m.name, m.instance or ""): m for m in metrics}


def compare_metrics(
    spark_metrics: List[MetricResult],
    duckdb_metrics: List[MetricResult]
) -> ComparisonReport:
    """Compare metric results from Spark and DuckDB engines.

    Args:
        spark_metrics: Metrics computed by Spark engine
        duckdb_metrics: Metrics computed by DuckDB engine

    Returns:
        ComparisonReport with detailed comparison results
    """
    report = ComparisonReport()

    # Index by (name, instance)
    spark_index = index_metrics(spark_metrics)
    duckdb_index = index_metrics(duckdb_metrics)

    all_keys = set(spark_index.keys()) | set(duckdb_index.keys())
    report.total_metrics = len(all_keys)

    for key in all_keys:
        name, instance = key
        tolerance = get_tolerance(name)

        spark_metric = spark_index.get(key)
        duckdb_metric = duckdb_index.get(key)

        if spark_metric is None:
            report.duckdb_only_metrics += 1
            report.differences.append(MetricDifference(
                name=name,
                instance=instance,
                spark_value=None,
                duckdb_value=duckdb_metric.value if duckdb_metric else None,
                tolerance=tolerance,
                is_match=False,
                message="Metric only in DuckDB"
            ))
        elif duckdb_metric is None:
            report.spark_only_metrics += 1
            report.differences.append(MetricDifference(
                name=name,
                instance=instance,
                spark_value=spark_metric.value,
                duckdb_value=None,
                tolerance=tolerance,
                is_match=False,
                message="Metric only in Spark"
            ))
        else:
            is_match = values_equal(spark_metric.value, duckdb_metric.value, tolerance)
            if is_match:
                report.matching_metrics += 1
            else:
                report.differing_metrics += 1
                report.differences.append(MetricDifference(
                    name=name,
                    instance=instance,
                    spark_value=spark_metric.value,
                    duckdb_value=duckdb_metric.value,
                    tolerance=tolerance,
                    is_match=False,
                    message=f"Values differ (tolerance={tolerance})"
                ))

    return report


def compare_constraint_results(
    spark_results: List[ConstraintResult],
    duckdb_results: List[ConstraintResult]
) -> ComparisonReport:
    """Compare constraint results from Spark and DuckDB engines.

    Comparison is done by position within each check group, since constraint
    names may differ between engines (e.g., Spark uses 'SizeConstraint(Size(None))'
    while DuckDB uses 'hasSize(assertion)').

    Args:
        spark_results: Constraint results from Spark engine
        duckdb_results: Constraint results from DuckDB engine

    Returns:
        ComparisonReport with detailed comparison results
    """
    report = ComparisonReport()

    # Group results by check_description to maintain ordering within checks
    def group_by_check(results: List[ConstraintResult]) -> Dict[str, List[ConstraintResult]]:
        groups: Dict[str, List[ConstraintResult]] = {}
        for r in results:
            key = r.check_description
            if key not in groups:
                groups[key] = []
            groups[key].append(r)
        return groups

    spark_groups = group_by_check(spark_results)
    duckdb_groups = group_by_check(duckdb_results)

    all_checks = set(spark_groups.keys()) | set(duckdb_groups.keys())

    for check_desc in all_checks:
        spark_list = spark_groups.get(check_desc, [])
        duckdb_list = duckdb_groups.get(check_desc, [])

        # Compare by position within each check
        max_len = max(len(spark_list), len(duckdb_list))
        report.total_metrics += max_len

        for i in range(max_len):
            spark_result = spark_list[i] if i < len(spark_list) else None
            duckdb_result = duckdb_list[i] if i < len(duckdb_list) else None

            if spark_result is None:
                report.duckdb_only_metrics += 1
            elif duckdb_result is None:
                report.spark_only_metrics += 1
            else:
                # Compare constraint status
                spark_status = spark_result.constraint_status
                duckdb_status = duckdb_result.constraint_status

                if spark_status == duckdb_status:
                    report.matching_metrics += 1
                else:
                    report.differing_metrics += 1
                    report.differences.append(MetricDifference(
                        name="ConstraintStatus",
                        instance=f"{check_desc}[{i}]",
                        spark_value=str(spark_status),
                        duckdb_value=str(duckdb_status),
                        tolerance=0,
                        is_match=False,
                        message="Constraint status differs"
                    ))

    return report


def compare_profiles(
    spark_profiles: List[ColumnProfile],
    duckdb_profiles: List[ColumnProfile]
) -> ComparisonReport:
    """Compare column profiles from Spark and DuckDB engines.

    Args:
        spark_profiles: Column profiles from Spark engine
        duckdb_profiles: Column profiles from DuckDB engine

    Returns:
        ComparisonReport with detailed comparison results
    """
    report = ComparisonReport()

    # Index by column name
    spark_index = {p.column: p for p in spark_profiles}
    duckdb_index = {p.column: p for p in duckdb_profiles}

    all_columns = set(spark_index.keys()) | set(duckdb_index.keys())

    for column in all_columns:
        spark_profile = spark_index.get(column)
        duckdb_profile = duckdb_index.get(column)

        if spark_profile is None:
            report.duckdb_only_metrics += 1
            continue
        if duckdb_profile is None:
            report.spark_only_metrics += 1
            continue

        # Compare profile attributes
        attrs_to_compare = [
            ("completeness", FLOAT_EPSILON),
            ("approx_distinct_values", APPROX_TOLERANCE),
            ("mean", FLOAT_TOLERANCE),
            ("minimum", FLOAT_TOLERANCE),
            ("maximum", FLOAT_TOLERANCE),
            ("sum", FLOAT_TOLERANCE),
            ("std_dev", FLOAT_TOLERANCE),
        ]

        for attr, tolerance in attrs_to_compare:
            spark_val = getattr(spark_profile, attr, None)
            duckdb_val = getattr(duckdb_profile, attr, None)

            report.total_metrics += 1

            if values_equal(spark_val, duckdb_val, tolerance):
                report.matching_metrics += 1
            else:
                report.differing_metrics += 1
                report.differences.append(MetricDifference(
                    name=attr,
                    instance=column,
                    spark_value=spark_val,
                    duckdb_value=duckdb_val,
                    tolerance=tolerance,
                    is_match=False,
                    message=f"Profile attribute {attr} differs"
                ))

    return report


def assert_metrics_match(
    spark_metrics: List[MetricResult],
    duckdb_metrics: List[MetricResult],
    msg: str = ""
) -> None:
    """Assert that metrics from both engines match within tolerance.

    Raises:
        AssertionError: If metrics don't match
    """
    report = compare_metrics(spark_metrics, duckdb_metrics)
    if not report.success:
        raise AssertionError(f"{msg}\n{report.summary()}")


def assert_constraints_match(
    spark_results: List[ConstraintResult],
    duckdb_results: List[ConstraintResult],
    msg: str = ""
) -> None:
    """Assert that constraint results from both engines match.

    Raises:
        AssertionError: If results don't match
    """
    report = compare_constraint_results(spark_results, duckdb_results)
    if not report.success:
        raise AssertionError(f"{msg}\n{report.summary()}")


def assert_profiles_match(
    spark_profiles: List[ColumnProfile],
    duckdb_profiles: List[ColumnProfile],
    msg: str = ""
) -> None:
    """Assert that profiles from both engines match within tolerance.

    Raises:
        AssertionError: If profiles don't match
    """
    report = compare_profiles(spark_profiles, duckdb_profiles)
    if not report.success:
        raise AssertionError(f"{msg}\n{report.summary()}")
