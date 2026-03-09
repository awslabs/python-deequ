# -*- coding: utf-8 -*-
"""
Engine abstraction for PyDeequ.

This module provides the engine abstraction layer that enables PyDeequ
to work with different execution backends (Spark, DuckDB, etc.).

Key design principles (inspired by DuckDQ):
1. State computation is engine-dependent (SQL queries, Spark jobs)
2. State merging is engine-independent (pure Python)
3. This separation enables incremental validation and easy backend additions

Example usage:
    import duckdb
    import pydeequ
    from pydeequ.v2.verification import AnalysisRunner
    from pydeequ.v2.analyzers import Size

    con = duckdb.connect()
    con.execute("CREATE TABLE test AS SELECT 1 as id, 2 as value")
    engine = pydeequ.connect(con)

    result = (AnalysisRunner(engine)
        .onData(table="test")
        .addAnalyzer(Size())
        .run())
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import pandas as pd

if TYPE_CHECKING:
    from pydeequ.v2.analyzers import _ConnectAnalyzer
    from pydeequ.v2.checks import Check


class ConstraintStatus(Enum):
    """Status of a constraint evaluation."""
    SUCCESS = "Success"
    FAILURE = "Failure"

    # Aliases for backwards compatibility
    Success = "Success"
    Failure = "Failure"


class CheckStatus(Enum):
    """Status of a check evaluation."""
    SUCCESS = "Success"
    WARNING = "Warning"
    ERROR = "Error"

    # Aliases for backwards compatibility
    Success = "Success"
    Warning = "Warning"
    Error = "Error"


@dataclass
class MetricResult:
    """Result of computing a metric."""
    name: str
    instance: str
    entity: str
    value: Optional[float]
    success: bool = True
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "name": self.name,
            "instance": self.instance,
            "entity": self.entity,
            "value": self.value,
        }


@dataclass
class ConstraintResult:
    """Result of evaluating a constraint."""
    check_description: str
    check_level: str
    check_status: Union[str, "CheckStatus"]
    constraint: str
    constraint_status: Union[str, "ConstraintStatus"]
    constraint_message: Optional[str] = None

    def __post_init__(self):
        """Convert string status values to enum values."""
        # Handle check_status
        if isinstance(self.check_status, str):
            for status in CheckStatus:
                if status.value == self.check_status:
                    self.check_status = status
                    break
        # Handle constraint_status
        if isinstance(self.constraint_status, str):
            for status in ConstraintStatus:
                if status.value == self.constraint_status:
                    self.constraint_status = status
                    break

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        check_status_val = self.check_status.value if isinstance(self.check_status, CheckStatus) else self.check_status
        constraint_status_val = self.constraint_status.value if isinstance(self.constraint_status, ConstraintStatus) else self.constraint_status
        return {
            "check": self.check_description,
            "check_level": self.check_level,
            "check_status": check_status_val,
            "constraint": self.constraint,
            "constraint_status": constraint_status_val,
            "constraint_message": self.constraint_message or "",
        }


@dataclass
class ColumnProfile:
    """Profile of a single column."""
    column: str
    completeness: float
    approx_distinct_values: int
    data_type: str
    is_data_type_inferred: bool = True
    type_counts: Optional[str] = None
    histogram: Optional[str] = None
    mean: Optional[float] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    sum: Optional[float] = None
    std_dev: Optional[float] = None
    approx_percentiles: Optional[str] = None
    kll_buckets: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "column": self.column,
            "completeness": self.completeness,
            "approx_distinct_values": self.approx_distinct_values,
            "data_type": self.data_type,
            "is_data_type_inferred": self.is_data_type_inferred,
            "type_counts": self.type_counts,
            "histogram": self.histogram,
            "mean": self.mean,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "sum": self.sum,
            "std_dev": self.std_dev,
            "approx_percentiles": self.approx_percentiles,
            "kll_buckets": self.kll_buckets,
        }


@dataclass
class ConstraintSuggestion:
    """A suggested constraint."""
    column_name: str
    constraint_name: str
    current_value: Optional[str]
    description: str
    suggesting_rule: str
    code_for_constraint: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation."""
        return {
            "column_name": self.column_name,
            "constraint_name": self.constraint_name,
            "current_value": self.current_value,
            "description": self.description,
            "suggesting_rule": self.suggesting_rule,
            "code_for_constraint": self.code_for_constraint,
        }


class BaseEngine(ABC):
    """
    Abstract base class for execution engines.

    Engines are responsible for:
    1. Computing metrics from data (engine-dependent)
    2. Evaluating constraints against computed metrics
    3. Profiling columns
    4. Suggesting constraints

    Subclasses must implement the core computation methods for their
    specific backend (DuckDB, Spark, etc.).
    """

    @abstractmethod
    def compute_metrics(
        self, analyzers: Sequence["_ConnectAnalyzer"]
    ) -> List[MetricResult]:
        """
        Compute metrics for the given analyzers.

        Args:
            analyzers: Sequence of analyzers to compute metrics for

        Returns:
            List of MetricResult objects
        """
        pass

    @abstractmethod
    def run_checks(self, checks: Sequence["Check"]) -> List[ConstraintResult]:
        """
        Run verification checks and return constraint results.

        Args:
            checks: Sequence of Check objects to evaluate

        Returns:
            List of ConstraintResult objects
        """
        pass

    @abstractmethod
    def profile_columns(
        self,
        columns: Optional[Sequence[str]] = None,
        low_cardinality_threshold: int = 0,
    ) -> List[ColumnProfile]:
        """
        Profile columns in the data source.

        Args:
            columns: Optional list of columns to profile. If None, profile all.
            low_cardinality_threshold: Threshold for histogram computation

        Returns:
            List of ColumnProfile objects
        """
        pass

    @abstractmethod
    def suggest_constraints(
        self,
        columns: Optional[Sequence[str]] = None,
        rules: Optional[Sequence[str]] = None,
    ) -> List[ConstraintSuggestion]:
        """
        Suggest constraints based on data characteristics.

        Args:
            columns: Optional list of columns to analyze
            rules: Optional list of rule sets to apply

        Returns:
            List of ConstraintSuggestion objects
        """
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, str]:
        """
        Get the schema of the data source.

        Returns:
            Dictionary mapping column names to data types
        """
        pass

    def metrics_to_dataframe(self, metrics: List[MetricResult]) -> pd.DataFrame:
        """Convert metrics to a pandas DataFrame."""
        if not metrics:
            return pd.DataFrame(columns=["name", "instance", "entity", "value"])
        return pd.DataFrame([m.to_dict() for m in metrics])

    def constraints_to_dataframe(
        self, results: List[ConstraintResult]
    ) -> pd.DataFrame:
        """Convert constraint results to a pandas DataFrame."""
        if not results:
            return pd.DataFrame(
                columns=[
                    "check", "check_level", "check_status",
                    "constraint", "constraint_status", "constraint_message"
                ]
            )
        return pd.DataFrame([r.to_dict() for r in results])

    def profiles_to_dataframe(self, profiles: List[ColumnProfile]) -> pd.DataFrame:
        """Convert column profiles to a pandas DataFrame."""
        if not profiles:
            return pd.DataFrame(columns=["column", "completeness", "data_type"])
        return pd.DataFrame([p.to_dict() for p in profiles])

    def suggestions_to_dataframe(
        self, suggestions: List[ConstraintSuggestion]
    ) -> pd.DataFrame:
        """Convert suggestions to a pandas DataFrame."""
        if not suggestions:
            return pd.DataFrame(
                columns=[
                    "column_name", "constraint_name", "current_value",
                    "description", "suggesting_rule", "code_for_constraint"
                ]
            )
        return pd.DataFrame([s.to_dict() for s in suggestions])

    @abstractmethod
    def for_table(self, table: str) -> "BaseEngine":
        """Return a new engine instance bound to the given table."""
        pass

    def for_dataframe(self, df: Any) -> "BaseEngine":
        """Return a new engine instance bound to the given dataframe."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support dataframes. "
            "Use onData(table='...') instead."
        )


def connect(
    connection: Any,
    table: Optional[str] = None,
    dataframe: Optional[Any] = None,
) -> BaseEngine:
    """
    Create an engine from a connection object with auto-detection.

    This function inspects the connection type and creates the appropriate
    engine backend. It supports:
    - DuckDB connections (duckdb.DuckDBPyConnection)
    - Spark sessions (pyspark.sql.SparkSession) - wraps existing v2 API

    The returned engine can be passed to runner constructors. Use
    ``onData(table=...)`` or ``onData(dataframe=...)`` on the runner to
    bind data for each run.

    Args:
        connection: A database connection or Spark session
        table: Optional table name (can also be specified via onData)
        dataframe: Optional DataFrame (can also be specified via onData)

    Returns:
        An engine instance appropriate for the connection type

    Raises:
        ValueError: If connection type is not supported

    Example:
        import duckdb
        import pydeequ
        from pydeequ.v2.verification import AnalysisRunner
        from pydeequ.v2.analyzers import Size

        con = duckdb.connect()
        con.execute("CREATE TABLE reviews AS SELECT * FROM 'reviews.csv'")
        engine = pydeequ.connect(con)
        result = (AnalysisRunner(engine)
            .onData(table="reviews")
            .addAnalyzer(Size())
            .run())
    """
    # Try DuckDB
    try:
        import duckdb
        if isinstance(connection, duckdb.DuckDBPyConnection):
            from pydeequ.engines.duckdb import DuckDBEngine
            return DuckDBEngine(connection, table)
    except ImportError:
        pass

    # Try Spark (regular and Connect sessions are separate classes)
    try:
        from pyspark.sql import SparkSession
        if isinstance(connection, SparkSession):
            from pydeequ.engines.spark import SparkEngine
            return SparkEngine(connection, table=table, dataframe=dataframe)
    except ImportError:
        pass
    try:
        from pyspark.sql.connect.session import SparkSession as ConnectSession
        if isinstance(connection, ConnectSession):
            from pydeequ.engines.spark import SparkEngine
            return SparkEngine(connection, table=table, dataframe=dataframe)
    except ImportError:
        pass

    raise ValueError(
        f"Unsupported connection type: {type(connection).__name__}. "
        "Supported types:\n"
        "  - duckdb.DuckDBPyConnection (pip install pydeequ[duckdb])\n"
        "  - pyspark.sql.SparkSession (pip install pydeequ[spark])"
    )


# Export public API
__all__ = [
    # Base classes
    "BaseEngine",
    # Result types
    "MetricResult",
    "ConstraintResult",
    "ConstraintStatus",
    "CheckStatus",
    "ColumnProfile",
    "ConstraintSuggestion",
    # Factory function
    "connect",
]


# Lazy import for DuckDB config to avoid import errors when duckdb is not installed
def __getattr__(name: str) -> Any:
    if name == "DuckDBEngineConfig":
        from pydeequ.engines.duckdb_config import DuckDBEngineConfig
        return DuckDBEngineConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
