#!/usr/bin/env python3
"""
Subprocess worker for running benchmarks in isolated process.

This module is designed to be run as:
    python -m benchmark.worker --config <json_path> --engine {duckdb,spark} --output <json_path>

Each engine runs in a fresh subprocess to ensure clean JVM/Python state.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from typing import Optional

from .config import BenchmarkConfig, ExperimentConfig
from .results import (
    BenchmarkRun,
    collect_environment_info,
    generate_run_id,
)
from .experiments import (
    run_varying_rows_experiment_duckdb,
    run_varying_cols_experiment_duckdb,
    run_profiling_experiment_duckdb,
    run_varying_rows_experiment_spark,
    run_varying_cols_experiment_spark,
    run_profiling_experiment_spark,
    setup_spark,
)


def run_duckdb_worker(config: BenchmarkConfig, run_id: str) -> BenchmarkRun:
    """Run all DuckDB experiments."""
    start_time = time.time()

    run = BenchmarkRun(
        run_id=run_id,
        timestamp=datetime.now().isoformat(),
        engine="duckdb",
        config=config.to_dict(),
        environment=collect_environment_info(),
    )

    exp_config = config.experiment

    try:
        run.varying_rows_results = run_varying_rows_experiment_duckdb(exp_config)
    except Exception as e:
        run.errors.append(f"DuckDB varying rows experiment failed: {e}")
        print(f"Error in varying rows experiment: {e}")

    try:
        run.varying_cols_results = run_varying_cols_experiment_duckdb(exp_config)
    except Exception as e:
        run.errors.append(f"DuckDB varying cols experiment failed: {e}")
        print(f"Error in varying cols experiment: {e}")

    try:
        run.profiling_results = run_profiling_experiment_duckdb(exp_config)
    except Exception as e:
        run.errors.append(f"DuckDB profiling experiment failed: {e}")
        print(f"Error in profiling experiment: {e}")

    run.total_duration_seconds = time.time() - start_time
    return run


def run_spark_worker(config: BenchmarkConfig, run_id: str) -> BenchmarkRun:
    """Run all Spark experiments."""
    start_time = time.time()

    run = BenchmarkRun(
        run_id=run_id,
        timestamp=datetime.now().isoformat(),
        engine="spark",
        config=config.to_dict(),
        environment=collect_environment_info(),
    )

    exp_config = config.experiment

    # Setup Spark
    spark = None
    spark_startup_time = 0.0

    try:
        print("\nSetting up Spark Connect...")
        spark, spark_startup_time = setup_spark(config.spark_remote)
        print(f"  Spark Startup: {spark_startup_time:.3f}s")
        run.spark_startup_time = spark_startup_time
    except Exception as e:
        error_msg = f"Spark setup failed: {e}"
        run.errors.append(error_msg)
        print(f"Error: {error_msg}")
        run.total_duration_seconds = time.time() - start_time
        return run

    try:
        run.varying_rows_results = run_varying_rows_experiment_spark(
            spark, spark_startup_time, exp_config
        )
    except Exception as e:
        run.errors.append(f"Spark varying rows experiment failed: {e}")
        print(f"Error in varying rows experiment: {e}")

    try:
        run.varying_cols_results = run_varying_cols_experiment_spark(
            spark, spark_startup_time, exp_config
        )
    except Exception as e:
        run.errors.append(f"Spark varying cols experiment failed: {e}")
        print(f"Error in varying cols experiment: {e}")

    try:
        run.profiling_results = run_profiling_experiment_spark(
            spark, spark_startup_time, exp_config
        )
    except Exception as e:
        run.errors.append(f"Spark profiling experiment failed: {e}")
        print(f"Error in profiling experiment: {e}")

    run.total_duration_seconds = time.time() - start_time
    return run


def main():
    parser = argparse.ArgumentParser(description="Benchmark worker subprocess")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to JSON config file",
    )
    parser.add_argument(
        "--engine",
        choices=["duckdb", "spark"],
        required=True,
        help="Engine to benchmark",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON results",
    )
    parser.add_argument(
        "--run-id",
        help="Run ID to use (optional, generated if not provided)",
    )

    args = parser.parse_args()

    # Load config from JSON file
    with open(args.config) as f:
        config_data = json.load(f)
    config = BenchmarkConfig.from_dict(config_data)

    # Use provided run ID or generate new one
    run_id = args.run_id or generate_run_id()

    print(f"Benchmark Worker: {args.engine}")
    print(f"Run ID: {run_id}")
    print("=" * 70)

    # Run the appropriate engine
    if args.engine == "duckdb":
        result = run_duckdb_worker(config, run_id)
    else:
        result = run_spark_worker(config, run_id)

    # Write results to output file
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(result.to_dict(), f, indent=2)

    print(f"\nResults written to: {args.output}")
    print(f"Total duration: {result.total_duration_seconds:.1f}s")

    if result.errors:
        print(f"Errors encountered: {len(result.errors)}")
        for error in result.errors:
            print(f"  - {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
