# -*- coding: utf-8 -*-
"""
DuckDB execution engine for PyDeequ.

This module provides a DuckDB-based execution engine that runs data quality
checks directly via SQL queries, without requiring a Spark cluster.

Example usage:
    import duckdb
    from pydeequ.engines.duckdb import DuckDBEngine
    from pydeequ.v2.analyzers import Size, Completeness, Mean

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id, 2 as value")

    engine = DuckDBEngine(con, table="test")
    metrics = engine.compute_metrics([Size(), Completeness("id"), Mean("value")])
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
from pydeequ.engines.operators import OperatorFactory

if TYPE_CHECKING:
    import duckdb
    from pydeequ.v2.analyzers import _ConnectAnalyzer
    from pydeequ.v2.checks import Check
    from pydeequ.v2.predicates import Predicate


class DuckDBEngine(BaseEngine):
    """
    DuckDB-based execution engine.

    This engine executes data quality checks using DuckDB SQL queries.
    It supports most analyzers through standard SQL aggregations.

    Attributes:
        con: DuckDB connection
        table: Name of the table to analyze
    """

    def __init__(self, con: "duckdb.DuckDBPyConnection", table: str):
        """
        Create a new DuckDBEngine.

        Args:
            con: DuckDB connection object
            table: Name of the table to analyze
        """
        self.con = con
        self.table = table
        self._schema: Optional[Dict[str, str]] = None

    def get_schema(self) -> Dict[str, str]:
        """Get the schema of the table."""
        if self._schema is None:
            df = self.con.execute(f"PRAGMA table_info('{self.table}')").fetchdf()
            self._schema = {}
            for _, row in df.iterrows():
                # Normalize type names to uppercase for consistency
                col_type = str(row["type"]).upper()
                # Extract base type (e.g., "DECIMAL(10,2)" -> "DECIMAL")
                base_type = col_type.split("(")[0]
                self._schema[row["name"]] = base_type
        return self._schema

    def _execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        return self.con.execute(query).fetchdf()

    def _get_row_count(self, where: Optional[str] = None) -> int:
        """Get the row count, optionally filtered."""
        if where:
            query = f"SELECT COUNT(*) as cnt FROM {self.table} WHERE {where}"
        else:
            query = f"SELECT COUNT(*) as cnt FROM {self.table}"
        result = self._execute_query(query)
        return int(result["cnt"].iloc[0])

    # =========================================================================
    # Main compute_metrics implementation using operators
    # =========================================================================

    def compute_metrics(
        self, analyzers: Sequence["_ConnectAnalyzer"]
    ) -> List[MetricResult]:
        """
        Compute metrics for the given analyzers.

        This method uses the operator abstraction to:
        1. Create operators from analyzers via OperatorFactory
        2. Batch scan operators into a single SQL query
        3. Execute grouping operators individually
        4. Handle metadata operators using schema access
        5. Extract results using operator-specific logic
        """
        results: List[MetricResult] = []

        # Separate analyzers by operator type
        scan_operators = []
        grouping_operators = []
        metadata_operators = []

        for analyzer in analyzers:
            if OperatorFactory.is_scan_operator(analyzer):
                operator = OperatorFactory.create(analyzer)
                if operator:
                    scan_operators.append(operator)
            elif OperatorFactory.is_grouping_operator(analyzer):
                operator = OperatorFactory.create(analyzer)
                if operator:
                    grouping_operators.append(operator)
            elif OperatorFactory.is_metadata_operator(analyzer):
                operator = OperatorFactory.create(analyzer)
                if operator:
                    metadata_operators.append(operator)
            else:
                # Unsupported analyzer
                results.append(MetricResult(
                    name=type(analyzer).__name__,
                    instance=getattr(analyzer, 'column', '*'),
                    entity="Column" if hasattr(analyzer, 'column') else "Dataset",
                    value=None,
                    success=False,
                    message=f"Analyzer {type(analyzer).__name__} not implemented"
                ))

        # Execute batched scan query
        if scan_operators:
            try:
                # Collect all aggregations
                aggregations = []
                for operator in scan_operators:
                    aggregations.extend(operator.get_aggregations())

                # Build and execute single query
                query = f"SELECT {', '.join(aggregations)} FROM {self.table}"
                scan_result = self._execute_query(query)

                # Extract results from each operator
                for operator in scan_operators:
                    try:
                        result = operator.extract_result(scan_result)
                        results.append(result)
                    except Exception as e:
                        results.append(MetricResult(
                            name=operator.metric_name,
                            instance=operator.instance,
                            entity=operator.entity,
                            value=None,
                            success=False,
                            message=str(e)
                        ))

            except Exception as e:
                # If batch query fails, report error for all scan operators
                for operator in scan_operators:
                    results.append(MetricResult(
                        name=operator.metric_name,
                        instance=operator.instance,
                        entity=operator.entity,
                        value=None,
                        success=False,
                        message=f"Batch query failed: {str(e)}"
                    ))

        # Execute grouping operators individually
        for operator in grouping_operators:
            try:
                query = operator.build_query(self.table)
                df = self._execute_query(query)
                result = operator.extract_result(df)
                results.append(result)
            except Exception as e:
                results.append(MetricResult(
                    name=operator.metric_name,
                    instance=operator.instance,
                    entity=operator.entity,
                    value=None,
                    success=False,
                    message=str(e)
                ))

        # Execute metadata operators using schema
        schema = self.get_schema()
        for operator in metadata_operators:
            try:
                result = operator.compute_from_schema(schema)
                results.append(result)
            except Exception as e:
                results.append(MetricResult(
                    name=operator.metric_name,
                    instance=operator.instance,
                    entity=operator.entity,
                    value=None,
                    success=False,
                    message=str(e)
                ))

        return results

    # =========================================================================
    # Constraint checking
    # =========================================================================

    def run_checks(self, checks: Sequence["Check"]) -> List[ConstraintResult]:
        """Run verification checks and return constraint results."""
        from pydeequ.v2.checks import CheckLevel
        from pydeequ.engines.constraints import ConstraintEvaluatorFactory

        results: List[ConstraintResult] = []

        for check in checks:
            check_description = check.description
            check_level = check.level.value

            # Track overall check status
            check_has_failure = False

            for constraint in check._constraints:
                constraint_message = None
                constraint_passed = False

                try:
                    # Create evaluator for this constraint
                    evaluator = ConstraintEvaluatorFactory.create(constraint)

                    if evaluator:
                        # Compute the metric value
                        value = evaluator.compute_value(self.table, self._execute_query)

                        # Evaluate the constraint
                        constraint_passed = evaluator.evaluate(value)

                        # Get constraint description
                        constraint_str = evaluator.to_string()

                        if not constraint_passed:
                            if value is not None:
                                constraint_message = f"Value: {value:.6g}"
                            else:
                                constraint_message = "Could not compute metric"
                    else:
                        constraint_str = constraint.type
                        constraint_message = f"Unknown constraint type: {constraint.type}"

                except Exception as e:
                    constraint_str = constraint.type
                    constraint_message = f"Error: {str(e)}"
                    constraint_passed = False

                if not constraint_passed:
                    check_has_failure = True

                results.append(ConstraintResult(
                    check_description=check_description,
                    check_level=check_level,
                    check_status=CheckStatus.ERROR.value if check_has_failure else CheckStatus.SUCCESS.value,
                    constraint=constraint_str,
                    constraint_status=ConstraintStatus.SUCCESS.value if constraint_passed else ConstraintStatus.FAILURE.value,
                    constraint_message=constraint_message,
                ))

            # Update check status for all constraints in this check
            final_status = CheckStatus.ERROR.value if check_has_failure else CheckStatus.SUCCESS.value
            if check.level == CheckLevel.Warning and check_has_failure:
                final_status = CheckStatus.WARNING.value

            for i in range(len(results) - len(check._constraints), len(results)):
                results[i] = ConstraintResult(
                    check_description=results[i].check_description,
                    check_level=results[i].check_level,
                    check_status=final_status,
                    constraint=results[i].constraint,
                    constraint_status=results[i].constraint_status,
                    constraint_message=results[i].constraint_message,
                )

        return results

    # =========================================================================
    # Column profiling
    # =========================================================================

    def profile_columns(
        self,
        columns: Optional[Sequence[str]] = None,
        low_cardinality_threshold: int = 0,
    ) -> List[ColumnProfile]:
        """
        Profile columns in the table.

        Uses ColumnProfileOperator to compute statistics for each column
        including completeness, distinct values, and (for numeric columns)
        min, max, mean, sum, stddev, and percentiles.

        Args:
            columns: Optional list of columns to profile. If None, profile all.
            low_cardinality_threshold: Threshold for histogram computation.
                If > 0 and distinct values <= threshold, compute histogram.

        Returns:
            List of ColumnProfile objects
        """
        from pydeequ.engines.operators.profiling_operators import ColumnProfileOperator

        schema = self.get_schema()

        # Determine which columns to profile
        if columns:
            cols_to_profile = [c for c in columns if c in schema]
        else:
            cols_to_profile = list(schema.keys())

        profiles: List[ColumnProfile] = []

        for col in cols_to_profile:
            col_type = schema[col]

            # Create operator for this column
            operator = ColumnProfileOperator(
                column=col,
                column_type=col_type,
                compute_percentiles=True,
                compute_histogram=(low_cardinality_threshold > 0),
                histogram_limit=low_cardinality_threshold,
            )

            # Execute base query and extract stats
            base_query = operator.build_base_query(self.table)
            base_result = self._execute_query(base_query)
            base_stats = operator.extract_base_result(base_result)

            # Execute percentile query for numeric columns
            percentiles = None
            if operator.compute_percentiles:
                try:
                    percentile_query = operator.build_percentile_query(self.table)
                    percentile_result = self._execute_query(percentile_query)
                    percentiles = operator.extract_percentile_result(percentile_result)
                except Exception:
                    # Percentile computation may fail for some types
                    pass

            # Execute histogram query for low cardinality columns
            histogram = None
            if operator.compute_histogram and base_stats["distinct_count"] <= low_cardinality_threshold:
                hist_query = operator.build_histogram_query(self.table)
                hist_result = self._execute_query(hist_query)
                histogram = operator.extract_histogram_result(hist_result)

            # Build and append profile
            profile = operator.build_profile(base_stats, percentiles, histogram)
            profiles.append(profile)

        return profiles

    # =========================================================================
    # Constraint suggestions
    # =========================================================================

    def suggest_constraints(
        self,
        columns: Optional[Sequence[str]] = None,
        rules: Optional[Sequence[str]] = None,
    ) -> List[ConstraintSuggestion]:
        """
        Suggest constraints based on data characteristics.

        Uses the SuggestionRunner to apply modular suggestion rules against
        column profiles. Rules are organized into sets:
        - DEFAULT: completeness, non-negative, categorical
        - NUMERICAL: min, max, mean
        - STRING: min/max length
        - COMMON: uniqueness
        - EXTENDED: all rules

        Args:
            columns: Optional list of columns to analyze. If None, analyze all.
            rules: Optional list of rule sets to apply. Defaults to ["DEFAULT"].

        Returns:
            List of ConstraintSuggestion objects
        """
        from pydeequ.engines.suggestions import SuggestionRunner
        from pydeequ.v2.suggestions import Rules

        # Default rules - normalize to strings for SuggestionRunner
        if rules is None:
            rule_strings = ["DEFAULT"]
        else:
            # Accept both Rules enum and string values
            rule_strings = []
            for rule in rules:
                if isinstance(rule, Rules):
                    rule_strings.append(rule.value)
                else:
                    rule_strings.append(rule)

        # Profile columns with histograms for categorical detection
        profiles = self.profile_columns(columns, low_cardinality_threshold=100)

        # Get row count for uniqueness checks
        row_count = self._get_row_count()

        # Run suggestion rules
        runner = SuggestionRunner(rule_sets=rule_strings)
        return runner.run(
            profiles,
            execute_fn=self._execute_query,
            table=self.table,
            row_count=row_count,
        )
