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

    # With profiling enabled
    engine = DuckDBEngine(con, table="test", enable_profiling=True)
    engine.compute_metrics([Size(), Completeness("id")])
    stats = engine.get_query_stats()
    print(f"Total queries: {engine.get_query_count()}")
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

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
from pydeequ.engines.operators import GroupingOperatorBatcher, OperatorFactory

if TYPE_CHECKING:
    import duckdb
    from pydeequ.engines.duckdb_config import DuckDBEngineConfig
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
        enable_profiling: Whether to collect query timing statistics
        config: Optional configuration for DuckDB optimization
    """

    def __init__(
        self,
        con: "duckdb.DuckDBPyConnection",
        table: Optional[str] = None,
        enable_profiling: bool = False,
        config: Optional["DuckDBEngineConfig"] = None,
    ):
        """
        Create a new DuckDBEngine.

        Args:
            con: DuckDB connection object
            table: Optional name of the table to analyze.
                   Can be specified later via ``for_table()``.
            enable_profiling: Whether to collect query timing statistics
            config: Optional DuckDB configuration for optimization
        """
        self.con = con
        self.table = table
        self._schema: Optional[Dict[str, str]] = None
        self._enable_profiling = enable_profiling
        self._query_stats: List[Dict] = []

        # Apply configuration if provided
        if config is not None:
            config.apply(con)

    def for_table(self, table: str) -> "DuckDBEngine":
        """Return a new DuckDBEngine bound to the given table."""
        return DuckDBEngine(
            self.con,
            table=table,
            enable_profiling=self._enable_profiling,
        )

    def for_dataframe(self, df: Any) -> "DuckDBEngine":
        """Return a new DuckDBEngine bound to the given dataframe.

        The dataframe is registered as a temporary view on the underlying
        DuckDB connection under a unique name. Any object DuckDB's
        ``con.register`` accepts works (pandas DataFrame, pyarrow Table,
        polars DataFrame, etc.).
        """
        view_name = f"_pydeequ_df_{uuid.uuid4().hex[:12]}"
        self.con.register(view_name, df)
        return DuckDBEngine(
            self.con,
            table=view_name,
            enable_profiling=self._enable_profiling,
        )

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
        if self._enable_profiling:
            start = time.perf_counter()
            result = self.con.execute(query).fetchdf()
            elapsed = time.perf_counter() - start
            self._query_stats.append({
                'query': query[:200] + ('...' if len(query) > 200 else ''),
                'time_ms': elapsed * 1000,
                'rows': len(result),
            })
            return result
        return self.con.execute(query).fetchdf()

    def get_query_stats(self) -> pd.DataFrame:
        """Return profiling statistics as DataFrame."""
        return pd.DataFrame(self._query_stats)

    def get_query_count(self) -> int:
        """Return number of queries executed."""
        return len(self._query_stats)

    def explain_query(self, query: str) -> str:
        """Get DuckDB query plan with EXPLAIN ANALYZE."""
        return self.con.execute(f"EXPLAIN ANALYZE {query}").fetchdf().to_string()

    def reset_profiling(self) -> None:
        """Reset profiling statistics."""
        self._query_stats = []

    def _get_row_count(self, where: Optional[str] = None) -> int:
        """Get the row count, optionally filtered."""
        if where:
            query = f"SELECT COUNT(*) as cnt FROM {self.table} WHERE {where}"
        else:
            query = f"SELECT COUNT(*) as cnt FROM {self.table}"
        result = self._execute_query(query)
        return int(result["cnt"].iloc[0])

    # =========================================================================
    # compute_metrics — dispatcher over three operator-family planners
    # =========================================================================

    def compute_metrics(
        self, analyzers: Sequence["_ConnectAnalyzer"]
    ) -> List[MetricResult]:
        """
        Compute metrics by dispatching analyzers to operator-family planners.

        Each planner owns one operator family:
        - ``_run_scan_operators``: single-pass aggregations, batched into one SELECT
        - ``_run_grouping_operators``: GROUP BY queries, fused where compatible
        - ``_run_metadata_operators``: schema-based, no SQL

        Unsupported analyzers fall through to a typed failure result.
        """
        scan_ops: List = []
        grouping_ops: List = []
        metadata_ops: List = []
        results: List[MetricResult] = []

        for analyzer in analyzers:
            if OperatorFactory.is_scan_operator(analyzer):
                op = OperatorFactory.create(analyzer)
                if op:
                    scan_ops.append(op)
            elif OperatorFactory.is_grouping_operator(analyzer):
                op = OperatorFactory.create(analyzer)
                if op:
                    grouping_ops.append(op)
            elif OperatorFactory.is_metadata_operator(analyzer):
                op = OperatorFactory.create(analyzer)
                if op:
                    metadata_ops.append(op)
            else:
                results.append(self._unsupported_analyzer_result(analyzer))

        if scan_ops:
            results.extend(self._run_scan_operators(scan_ops))
        if grouping_ops:
            results.extend(self._run_grouping_operators(grouping_ops))
        if metadata_ops:
            results.extend(self._run_metadata_operators(metadata_ops))

        return results

    # -------------------------------------------------------------------------
    # Planner: scan operators
    # -------------------------------------------------------------------------

    def _run_scan_operators(self, operators: List) -> List[MetricResult]:
        """Run scan operators by fusing their aggregations into one query.

        On batch failure, each operator gets a typed failure result.
        On per-operator extract failure, only that operator fails.
        """
        aggregations: List[str] = []
        for op in operators:
            aggregations.extend(op.get_aggregations())
        query = f"SELECT {', '.join(aggregations)} FROM {self.table}"

        try:
            df = self._execute_query(query)
        except Exception as e:
            return [
                self._failure_result(op, f"Batch query failed: {e}") for op in operators
            ]

        return [self._safe_extract(op, lambda o=op: o.extract_result(df)) for op in operators]

    # -------------------------------------------------------------------------
    # Planner: grouping operators
    # -------------------------------------------------------------------------

    def _run_grouping_operators(self, operators: List) -> List[MetricResult]:
        """Run grouping operators with fused-batch + per-operator fallback.

        Operators sharing (columns, where) run in a single fused query;
        unbatchable operators run one query each. Individual operator
        failures are returned as failure results so other operators still
        produce values.
        """
        batcher = GroupingOperatorBatcher(operators)
        results: List[MetricResult] = []

        try:
            results.extend(batcher.execute_batched(self.table, self._execute_query))
        except Exception as e:
            # Fused query failed — synthesise a failure for every batchable op.
            unbatchable = set(map(id, batcher.get_unbatchable_operators()))
            for op in operators:
                if id(op) not in unbatchable:
                    results.append(self._failure_result(op, f"Batched query failed: {e}"))

        for op in batcher.get_unbatchable_operators():
            results.append(
                self._safe_extract(
                    op, lambda o=op: o.extract_result(self._execute_query(o.build_query(self.table)))
                )
            )

        return results

    # -------------------------------------------------------------------------
    # Planner: metadata operators
    # -------------------------------------------------------------------------

    def _run_metadata_operators(self, operators: List) -> List[MetricResult]:
        """Run metadata operators against the cached table schema."""
        schema = self.get_schema()
        return [
            self._safe_extract(op, lambda o=op: o.compute_from_schema(schema))
            for op in operators
        ]

    # -------------------------------------------------------------------------
    # Failure helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _failure_result(operator, message: str) -> MetricResult:
        return MetricResult(
            name=operator.metric_name,
            instance=operator.instance,
            entity=operator.entity,
            value=None,
            success=False,
            message=message,
        )

    @classmethod
    def _safe_extract(cls, operator, fn) -> MetricResult:
        """Run ``fn``, returning a typed failure result on any exception."""
        try:
            return fn()
        except Exception as e:
            return cls._failure_result(operator, str(e))

    @staticmethod
    def _unsupported_analyzer_result(analyzer) -> MetricResult:
        name = type(analyzer).__name__
        return MetricResult(
            name=name,
            instance=getattr(analyzer, "column", "*"),
            entity="Column" if hasattr(analyzer, "column") else "Dataset",
            value=None,
            success=False,
            message=f"Analyzer {name} not implemented",
        )

    # =========================================================================
    # Constraint checking
    # =========================================================================

    def run_checks(self, checks: Sequence["Check"]) -> List[ConstraintResult]:
        """Run verification checks and return constraint results.

        Uses ConstraintBatchEvaluator to batch compatible constraints,
        reducing the number of SQL queries executed.
        """
        from pydeequ.v2.checks import CheckLevel
        from pydeequ.engines.constraints import (
            ConstraintBatchEvaluator,
            ConstraintEvaluatorFactory,
        )

        results: List[ConstraintResult] = []

        # Phase 1: Create all evaluators and collect metadata
        all_evaluators = []
        constraint_info = []  # (check, constraint, evaluator) tuples

        for check in checks:
            for constraint in check._constraints:
                evaluator = ConstraintEvaluatorFactory.create(constraint)
                if evaluator:
                    all_evaluators.append(evaluator)
                    constraint_info.append((check, constraint, evaluator))
                else:
                    constraint_info.append((check, constraint, None))

        # Phase 2: Batch execute all evaluators
        computed_values: Dict = {}
        if all_evaluators:
            batcher = ConstraintBatchEvaluator(all_evaluators)
            computed_values = batcher.execute(self.table, self._execute_query)

        # Phase 3: Process results by check
        info_idx = 0
        for check in checks:
            check_description = check.description
            check_level = check.level.value
            check_has_failure = False

            for constraint in check._constraints:
                _, _, evaluator = constraint_info[info_idx]
                info_idx += 1

                constraint_message = None
                constraint_passed = False

                try:
                    if evaluator:
                        # Get pre-computed value from batch execution
                        value = computed_values.get(evaluator)

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

        Uses MultiColumnProfileOperator to batch profile statistics across
        multiple columns, significantly reducing the number of SQL queries
        from 2-3 per column to 2-3 total.

        Args:
            columns: Optional list of columns to profile. If None, profile all.
            low_cardinality_threshold: Threshold for histogram computation.
                If > 0 and distinct values <= threshold, compute histogram.

        Returns:
            List of ColumnProfile objects
        """
        from pydeequ.engines.operators.profiling_operators import (
            ColumnProfileOperator,
            MultiColumnProfileOperator,
        )

        schema = self.get_schema()

        # Determine which columns to profile
        if columns:
            cols_to_profile = [c for c in columns if c in schema]
        else:
            cols_to_profile = list(schema.keys())

        if not cols_to_profile:
            return []

        # Use MultiColumnProfileOperator for batched profiling
        operator = MultiColumnProfileOperator(cols_to_profile, schema)

        # Query 1: Completeness and distinct counts for all columns
        completeness_query = operator.build_completeness_query(self.table)
        completeness_df = self._execute_query(completeness_query)

        # Query 2: Numeric stats for all numeric columns (if any)
        numeric_df = None
        numeric_query = operator.build_numeric_stats_query(self.table)
        if numeric_query:
            numeric_df = self._execute_query(numeric_query)

        # Query 3: Percentiles for all numeric columns (if any)
        percentile_df = None
        percentile_query = operator.build_percentile_query(self.table)
        if percentile_query:
            try:
                percentile_df = self._execute_query(percentile_query)
            except Exception:
                # Percentile computation may fail for some types
                pass

        # Extract profiles from batched results
        profiles = operator.extract_profiles(completeness_df, numeric_df, percentile_df)

        # Add histograms for low cardinality columns (requires per-column queries)
        if low_cardinality_threshold > 0:
            for profile in profiles:
                if profile.approx_distinct_values <= low_cardinality_threshold:
                    col_type = schema.get(profile.column, "VARCHAR")
                    col_operator = ColumnProfileOperator(
                        column=profile.column,
                        column_type=col_type,
                        compute_percentiles=False,
                        compute_histogram=True,
                        histogram_limit=low_cardinality_threshold,
                    )
                    hist_query = col_operator.build_histogram_query(self.table)
                    hist_result = self._execute_query(hist_query)
                    profile.histogram = col_operator.extract_histogram_result(hist_result)

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
