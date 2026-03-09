"""Benchmark experiment logic extracted from original benchmark_cli.py."""

import os
import time
from typing import List, Dict, Any, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
import pydeequ

from pydeequ.v2.verification import VerificationSuite
from pydeequ.v2.checks import Check, CheckLevel
from pydeequ.v2.predicates import gte, lte, between
from pydeequ.v2.profiles import ColumnProfilerRunner

from .config import ExperimentConfig


# =============================================================================
# Data Generation
# =============================================================================


def generate_rich_data(n_rows: int, n_extra_cols: int = 0) -> pd.DataFrame:
    """
    Generate mixed-type data for benchmarking with optional extra numeric columns.

    Base schema (10 columns):
    - id: string (unique identifier)
    - category: string (5 categorical values)
    - status: string (3 categorical values)
    - email: string (email-like pattern)
    - amount: float [0, 10000]
    - quantity: int [0, 1000]
    - score: float [0, 100] (normal distribution)
    - rating: int [1, 5]
    - price: float [0.01, 9999.99]
    - discount: float [0, 0.5]

    Args:
        n_rows: Number of rows to generate
        n_extra_cols: Number of additional numeric columns

    Returns:
        DataFrame with mixed-type columns + optional extra numeric columns
    """
    np.random.seed(42)

    data = {
        "id": [f"ID{i:012d}" for i in range(n_rows)],
        "category": np.random.choice(
            ["electronics", "clothing", "food", "books", "toys"], n_rows
        ),
        "status": np.random.choice(["active", "inactive", "pending"], n_rows),
        "email": [f"user{i}@example.com" for i in range(n_rows)],
        "amount": np.random.uniform(0, 10000, n_rows),
        "quantity": np.random.randint(0, 1001, n_rows),
        "score": np.random.normal(50, 15, n_rows).clip(0, 100),
        "rating": np.random.randint(1, 6, n_rows),
        "price": np.random.uniform(0.01, 9999.99, n_rows),
        "discount": np.random.uniform(0, 0.5, n_rows),
    }

    for i in range(n_extra_cols):
        data[f"extra_{i}"] = np.random.uniform(-10000, 10000, n_rows)

    return pd.DataFrame(data)


def save_to_parquet(df: pd.DataFrame, cache_dir: str, name: str, target_row_groups: int = 64) -> str:
    """
    Save DataFrame to Parquet with dynamic row group size for Spark parallelism.

    Args:
        df: DataFrame to save
        cache_dir: Cache directory path
        name: Cache file name
        target_row_groups: Target number of row groups

    Returns:
        Path to the saved Parquet file
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(cache_dir, exist_ok=True)
    path = os.path.join(cache_dir, f"{name}.parquet")

    if not os.path.exists(path):
        n_rows = len(df)
        row_group_size = max(10_000, n_rows // target_row_groups)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, path, compression="snappy", row_group_size=row_group_size)

    return path


def get_cached_parquet(cache_dir: str, name: str) -> Optional[str]:
    """Get path to cached Parquet file, or None if not cached."""
    path = os.path.join(cache_dir, f"{name}.parquet")
    return path if os.path.exists(path) else None


# =============================================================================
# Check Building
# =============================================================================


def build_rich_check(n_extra_cols: int = 0) -> Check:
    """
    Build a Check suite with rich validations on base columns + simple checks on extras.

    Args:
        n_extra_cols: Number of extra numeric columns to add checks for

    Returns:
        Check instance with all constraints configured
    """
    check = (
        Check(CheckLevel.Warning, "Rich Benchmark Check")
        # Completeness checks (3)
        .isComplete("id")
        .isComplete("category")
        .hasCompleteness("email", gte(0.95))
        # Uniqueness checks (2)
        .isUnique("id")
        .hasDistinctness(["category"], gte(0.001))
        # Numeric range checks (6)
        .hasMin("amount", gte(0))
        .hasMax("amount", lte(10000))
        .hasMean("score", between(0, 100))
        .hasStandardDeviation("score", lte(50))
        .isNonNegative("quantity")
        .isPositive("price")
        # String checks (5)
        .hasMinLength("id", gte(8))
        .hasMaxLength("id", lte(20))
        .hasPattern("email", r".*@.*\..*", gte(0.9))
        .isContainedIn("status", ["active", "inactive", "pending"])
        .isContainedIn("rating", ["1", "2", "3", "4", "5"])
    )

    for i in range(n_extra_cols):
        col = f"extra_{i}"
        check = check.isComplete(col).hasMin(col, gte(-10000)).hasMax(col, lte(10000))

    return check


def count_checks(n_extra_cols: int = 0) -> int:
    """Return total number of checks for given extra columns."""
    base_checks = 16
    extra_checks = n_extra_cols * 3
    return base_checks + extra_checks


# =============================================================================
# DuckDB Setup and Benchmarking
# =============================================================================


def setup_duckdb_from_parquet(parquet_path: str) -> Tuple[Any, duckdb.DuckDBPyConnection]:
    """Setup DuckDB engine to read from Parquet file."""
    con = duckdb.connect()
    engine = pydeequ.connect(con, table=f"read_parquet('{parquet_path}')")
    return engine, con


def setup_duckdb_for_profiling(parquet_path: str) -> Tuple[Any, duckdb.DuckDBPyConnection]:
    """
    Setup DuckDB engine for profiling by creating a view from parquet.
    This is needed because PRAGMA table_info() doesn't work with read_parquet().
    """
    con = duckdb.connect()
    con.execute(f"CREATE VIEW benchmark_data AS SELECT * FROM read_parquet('{parquet_path}')")
    engine = pydeequ.connect(con, table="benchmark_data")
    return engine, con


def benchmark_duckdb_validation(engine: Any, check: Check, n_runs: int) -> float:
    """Time DuckDB VerificationSuite.run() over N runs, return average."""
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        result = VerificationSuite(engine).onData(table="benchmark_data").addCheck(check).run()
        _ = len(result)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return sum(times) / len(times)


def benchmark_duckdb_profiling(engine: Any, n_runs: int) -> float:
    """Time DuckDB ColumnProfilerRunner.run() over N runs, return average."""
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        result = ColumnProfilerRunner(engine).onData(table="benchmark_data").run()
        _ = len(result)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return sum(times) / len(times)


# =============================================================================
# Spark Setup and Benchmarking
# =============================================================================


def setup_spark(spark_remote: str) -> Tuple[Any, float]:
    """Create SparkSession for Spark Connect. Returns (spark, startup_time)."""
    from pyspark.sql import SparkSession

    start = time.perf_counter()
    spark = SparkSession.builder.remote(spark_remote).getOrCreate()
    startup_time = time.perf_counter() - start

    return spark, startup_time


def load_spark_from_parquet(spark: Any, parquet_path: str) -> Tuple[Any, float]:
    """Load Parquet file into Spark. Returns (spark_df, load_time)."""
    start = time.perf_counter()
    spark_df = spark.read.parquet(parquet_path)
    spark_df.count()  # Force materialization
    load_time = time.perf_counter() - start

    return spark_df, load_time


def benchmark_spark_validation(spark: Any, spark_df: Any, check: Check, n_runs: int) -> float:
    """Time Spark VerificationSuite.run() over N runs, return average."""
    engine = pydeequ.connect(spark)
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        result = VerificationSuite(engine).onData(dataframe=spark_df).addCheck(check).run()
        _ = len(result)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return sum(times) / len(times)


def benchmark_spark_profiling(spark: Any, spark_df: Any, n_runs: int) -> float:
    """Time Spark ColumnProfilerRunner.run() over N runs, return average."""
    engine = pydeequ.connect(spark)
    times = []
    for _ in range(n_runs):
        start = time.perf_counter()
        result = ColumnProfilerRunner(engine).onData(dataframe=spark_df).run()
        _ = len(result)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
    return sum(times) / len(times)


# =============================================================================
# DuckDB Experiment Runners
# =============================================================================


def run_varying_rows_experiment_duckdb(config: ExperimentConfig) -> List[Dict[str, Any]]:
    """Run varying rows experiment for DuckDB engine."""
    print("\n" + "=" * 70)
    print(f"EXPERIMENT 1 (DuckDB): VARYING ROWS (Fixed Columns = {config.base_cols})")
    print("=" * 70)

    results = []

    for n_rows in config.row_counts:
        n_checks = count_checks(0)
        print(f"\n--- {n_rows:,} rows x {config.base_cols} cols ({n_checks} checks) ---")

        cache_name = f"rich_rows_{n_rows}"
        parquet_path = get_cached_parquet(config.cache_dir, cache_name)

        if parquet_path:
            print(f"Using cached data: {parquet_path}")
        else:
            print("Generating rich mixed-type data and saving to Parquet...")
            df = generate_rich_data(n_rows, n_extra_cols=0)
            parquet_path = save_to_parquet(df, config.cache_dir, cache_name)
            del df

        check = build_rich_check(n_extra_cols=0)

        print("Setting up DuckDB (from Parquet)...")
        duck_engine, duck_con = setup_duckdb_from_parquet(parquet_path)

        print(f"Running DuckDB validation ({config.n_runs} runs)...")
        duck_validation = benchmark_duckdb_validation(duck_engine, check, config.n_runs)
        print(f"  DuckDB Validation: {duck_validation:.3f}s (avg)")

        duck_con.close()

        results.append({
            "rows": n_rows,
            "cols": config.base_cols,
            "checks": n_checks,
            "duckdb_validation": duck_validation,
        })

    return results


def run_varying_cols_experiment_duckdb(config: ExperimentConfig) -> List[Dict[str, Any]]:
    """Run varying columns experiment for DuckDB engine."""
    print("\n" + "=" * 70)
    print(f"EXPERIMENT 2 (DuckDB): VARYING COLUMNS (Fixed Rows = {config.fixed_rows:,})")
    print("=" * 70)

    results = []

    for n_cols in config.column_counts:
        n_extra_cols = n_cols - config.base_cols
        n_checks = count_checks(n_extra_cols)
        print(f"\n--- {config.fixed_rows:,} rows x {n_cols} cols ({n_checks} checks) ---")

        cache_name = f"rich_cols_{n_cols}"
        parquet_path = get_cached_parquet(config.cache_dir, cache_name)

        if parquet_path:
            print(f"Using cached data: {parquet_path}")
        else:
            print("Generating rich mixed-type data and saving to Parquet...")
            df = generate_rich_data(config.fixed_rows, n_extra_cols=n_extra_cols)
            parquet_path = save_to_parquet(df, config.cache_dir, cache_name)
            del df

        check = build_rich_check(n_extra_cols=n_extra_cols)

        print("Setting up DuckDB (from Parquet)...")
        duck_engine, duck_con = setup_duckdb_from_parquet(parquet_path)

        print(f"Running DuckDB validation ({config.n_runs} runs)...")
        duck_validation = benchmark_duckdb_validation(duck_engine, check, config.n_runs)
        print(f"  DuckDB Validation: {duck_validation:.3f}s (avg)")

        duck_con.close()

        results.append({
            "rows": config.fixed_rows,
            "cols": n_cols,
            "checks": n_checks,
            "duckdb_validation": duck_validation,
        })

    return results


def run_profiling_experiment_duckdb(config: ExperimentConfig) -> List[Dict[str, Any]]:
    """Run column profiling experiment for DuckDB engine."""
    print("\n" + "=" * 70)
    print(f"EXPERIMENT 3 (DuckDB): COLUMN PROFILING (Fixed Columns = {config.base_cols})")
    print("=" * 70)

    results = []

    for n_rows in config.profiling_row_counts:
        print(f"\n--- {n_rows:,} rows x {config.base_cols} cols (profiling) ---")

        cache_name = f"rich_rows_{n_rows}"
        parquet_path = get_cached_parquet(config.cache_dir, cache_name)

        if parquet_path:
            print(f"Using cached data: {parquet_path}")
        else:
            print("Generating rich mixed-type data and saving to Parquet...")
            df = generate_rich_data(n_rows, n_extra_cols=0)
            parquet_path = save_to_parquet(df, config.cache_dir, cache_name)
            del df

        print("Setting up DuckDB for profiling...")
        duck_engine, duck_con = setup_duckdb_for_profiling(parquet_path)

        print(f"Running DuckDB profiling ({config.n_runs} runs)...")
        duck_profiling = benchmark_duckdb_profiling(duck_engine, config.n_runs)
        print(f"  DuckDB Profiling: {duck_profiling:.3f}s (avg)")

        duck_con.close()

        results.append({
            "rows": n_rows,
            "cols": config.base_cols,
            "duckdb_profiling": duck_profiling,
        })

    return results


# =============================================================================
# Spark Experiment Runners
# =============================================================================


def run_varying_rows_experiment_spark(
    spark: Any, spark_startup_time: float, config: ExperimentConfig
) -> List[Dict[str, Any]]:
    """Run varying rows experiment for Spark engine."""
    print("\n" + "=" * 70)
    print(f"EXPERIMENT 1 (Spark): VARYING ROWS (Fixed Columns = {config.base_cols})")
    print("=" * 70)

    results = []

    for n_rows in config.row_counts:
        n_checks = count_checks(0)
        print(f"\n--- {n_rows:,} rows x {config.base_cols} cols ({n_checks} checks) ---")

        cache_name = f"rich_rows_{n_rows}"
        parquet_path = get_cached_parquet(config.cache_dir, cache_name)

        if parquet_path:
            print(f"Using cached data: {parquet_path}")
        else:
            print("Generating rich mixed-type data and saving to Parquet...")
            df = generate_rich_data(n_rows, n_extra_cols=0)
            parquet_path = save_to_parquet(df, config.cache_dir, cache_name)
            del df

        check = build_rich_check(n_extra_cols=0)

        spark_load = None
        spark_validation = None

        try:
            print("Loading Parquet into Spark...")
            spark_df, spark_load = load_spark_from_parquet(spark, parquet_path)
            print(f"  Spark Data Load: {spark_load:.3f}s")

            print(f"Running Spark validation ({config.n_runs} runs)...")
            spark_validation = benchmark_spark_validation(spark, spark_df, check, config.n_runs)
            print(f"  Spark Validation: {spark_validation:.3f}s (avg)")
        except Exception as e:
            print(f"  Spark error: {str(e)[:80]}")

        results.append({
            "rows": n_rows,
            "cols": config.base_cols,
            "checks": n_checks,
            "spark_startup": spark_startup_time,
            "spark_load": spark_load,
            "spark_validation": spark_validation,
        })

    return results


def run_varying_cols_experiment_spark(
    spark: Any, spark_startup_time: float, config: ExperimentConfig
) -> List[Dict[str, Any]]:
    """Run varying columns experiment for Spark engine."""
    print("\n" + "=" * 70)
    print(f"EXPERIMENT 2 (Spark): VARYING COLUMNS (Fixed Rows = {config.fixed_rows:,})")
    print("=" * 70)

    results = []

    for n_cols in config.column_counts:
        n_extra_cols = n_cols - config.base_cols
        n_checks = count_checks(n_extra_cols)
        print(f"\n--- {config.fixed_rows:,} rows x {n_cols} cols ({n_checks} checks) ---")

        cache_name = f"rich_cols_{n_cols}"
        parquet_path = get_cached_parquet(config.cache_dir, cache_name)

        if parquet_path:
            print(f"Using cached data: {parquet_path}")
        else:
            print("Generating rich mixed-type data and saving to Parquet...")
            df = generate_rich_data(config.fixed_rows, n_extra_cols=n_extra_cols)
            parquet_path = save_to_parquet(df, config.cache_dir, cache_name)
            del df

        check = build_rich_check(n_extra_cols=n_extra_cols)

        spark_load = None
        spark_validation = None

        try:
            print("Loading Parquet into Spark...")
            spark_df, spark_load = load_spark_from_parquet(spark, parquet_path)
            print(f"  Spark Data Load: {spark_load:.3f}s")

            print(f"Running Spark validation ({config.n_runs} runs)...")
            spark_validation = benchmark_spark_validation(spark, spark_df, check, config.n_runs)
            print(f"  Spark Validation: {spark_validation:.3f}s (avg)")
        except Exception as e:
            print(f"  Spark error: {str(e)[:80]}")

        results.append({
            "rows": config.fixed_rows,
            "cols": n_cols,
            "checks": n_checks,
            "spark_startup": spark_startup_time,
            "spark_load": spark_load,
            "spark_validation": spark_validation,
        })

    return results


def run_profiling_experiment_spark(
    spark: Any, spark_startup_time: float, config: ExperimentConfig
) -> List[Dict[str, Any]]:
    """Run column profiling experiment for Spark engine."""
    print("\n" + "=" * 70)
    print(f"EXPERIMENT 3 (Spark): COLUMN PROFILING (Fixed Columns = {config.base_cols})")
    print("=" * 70)

    results = []

    for n_rows in config.profiling_row_counts:
        print(f"\n--- {n_rows:,} rows x {config.base_cols} cols (profiling) ---")

        cache_name = f"rich_rows_{n_rows}"
        parquet_path = get_cached_parquet(config.cache_dir, cache_name)

        if parquet_path:
            print(f"Using cached data: {parquet_path}")
        else:
            print("Generating rich mixed-type data and saving to Parquet...")
            df = generate_rich_data(n_rows, n_extra_cols=0)
            parquet_path = save_to_parquet(df, config.cache_dir, cache_name)
            del df

        spark_load = None
        spark_profiling = None

        try:
            print("Loading Parquet into Spark...")
            spark_df, spark_load = load_spark_from_parquet(spark, parquet_path)
            print(f"  Spark Data Load: {spark_load:.3f}s")

            print(f"Running Spark profiling ({config.n_runs} runs)...")
            spark_profiling = benchmark_spark_profiling(spark, spark_df, config.n_runs)
            print(f"  Spark Profiling: {spark_profiling:.3f}s (avg)")
        except Exception as e:
            print(f"  Spark error: {str(e)[:80]}")

        results.append({
            "rows": n_rows,
            "cols": config.base_cols,
            "spark_startup": spark_startup_time,
            "spark_load": spark_load,
            "spark_profiling": spark_profiling,
        })

    return results
