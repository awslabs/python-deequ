#!/usr/bin/env python3
"""
PyDeequ Engine Benchmark CLI

Orchestrates benchmark runs with process isolation and auto Spark server management.

Usage:
    # Run DuckDB only (no Spark server needed)
    python benchmark_cli.py run --engine duckdb

    # Run Spark only (auto-spark is enabled by default)
    python benchmark_cli.py run --engine spark

    # Run both engines
    python benchmark_cli.py run --engine all

    # Run without auto Spark server management (assumes server is running)
    python benchmark_cli.py run --engine spark --no-auto-spark

    # Generate report from saved results (folder or file path)
    python benchmark_cli.py report benchmark_results/benchmark_2024-01-19T14-30-45/
    python benchmark_cli.py report benchmark_results/benchmark_2024-01-19T14-30-45/results.json

    # Generate report to custom location
    python benchmark_cli.py report benchmark_results/benchmark_2024-01-19T14-30-45/ -o MY_RESULTS.md

    # Generate visualization PNG from results
    python benchmark_cli.py visualize benchmark_results/benchmark_2024-01-19T14-30-45/
    python benchmark_cli.py visualize benchmark_results/benchmark_2024-01-19T14-30-45/ -o charts.png
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Optional

from benchmark.config import BenchmarkConfig, ExperimentConfig, SparkServerConfig
from benchmark.results import (
    BenchmarkRun,
    generate_run_id,
    save_results,
    load_results,
    merge_results,
    collect_environment_info,
)
from benchmark.spark_server import managed_spark_server
from benchmark.report import save_report
from benchmark.visualize import generate_visualization


def run_engine_in_subprocess(
    engine: str,
    config: BenchmarkConfig,
    run_id: str,
) -> Optional[BenchmarkRun]:
    """
    Run benchmark for a single engine in an isolated subprocess.

    Args:
        engine: Engine to run ("duckdb" or "spark")
        config: Benchmark configuration
        run_id: Run ID to use

    Returns:
        BenchmarkRun result, or None on failure
    """
    print(f"\n{'=' * 70}")
    print(f"Running {engine.upper()} benchmarks in subprocess...")
    print("=" * 70)

    # Write config to temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config.to_dict(), f, indent=2)
        config_path = f.name

    # Temp output file for results (will be cleaned up after loading)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        output_path = f.name

    try:
        # Run worker subprocess
        cmd = [
            sys.executable,
            "-m",
            "benchmark.worker",
            "--config", config_path,
            "--engine", engine,
            "--output", output_path,
            "--run-id", run_id,
        ]

        print(f"Command: {' '.join(cmd)}")
        print()

        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        )

        if result.returncode != 0:
            print(f"\n{engine.upper()} worker exited with code {result.returncode}")
            # Try to load partial results if they exist
            if os.path.exists(output_path):
                return load_results(output_path)
            return None

        # Load and return results
        return load_results(output_path)

    except Exception as e:
        print(f"Error running {engine} worker: {e}")
        return None

    finally:
        # Clean up temp files
        if os.path.exists(config_path):
            os.unlink(config_path)
        if os.path.exists(output_path):
            os.unlink(output_path)


def cmd_run(args: argparse.Namespace) -> int:
    """Execute the 'run' subcommand."""
    # Build configuration
    exp_config = ExperimentConfig()
    if args.n_runs:
        exp_config.n_runs = args.n_runs

    spark_config = SparkServerConfig()
    if args.spark_home:
        spark_config.spark_home = args.spark_home
    if args.java_home:
        spark_config.java_home = args.java_home

    config = BenchmarkConfig(
        engine=args.engine,
        output_dir=args.output_dir,
        experiment=exp_config,
        spark_server=spark_config,
    )

    # Generate run ID and create run directory
    run_id = generate_run_id()
    run_dir = os.path.join(args.output_dir, run_id)
    os.makedirs(run_dir, exist_ok=True)

    auto_spark = not args.no_auto_spark

    print("PyDeequ Engine Benchmark")
    print("=" * 70)
    print(f"Run ID: {run_id}")
    print(f"Engine: {args.engine}")
    print(f"Output directory: {run_dir}")
    print(f"Auto Spark: {auto_spark}")
    print(f"Validation runs: {exp_config.n_runs}")
    print(f"Row counts: {exp_config.row_counts}")
    print(f"Column counts: {exp_config.column_counts}")
    print(f"Cache directory: {exp_config.cache_dir}")

    start_time = time.time()

    run_duckdb = args.engine in ("all", "duckdb")
    run_spark = args.engine in ("all", "spark")

    duckdb_result: Optional[BenchmarkRun] = None
    spark_result: Optional[BenchmarkRun] = None

    # Run DuckDB (doesn't need Spark server)
    if run_duckdb:
        duckdb_result = run_engine_in_subprocess("duckdb", config, run_id)

    # Run Spark (may need server management)
    if run_spark:
        if auto_spark:
            # Use managed server context
            with managed_spark_server(spark_config) as server:
                startup_time = server.start()
                if server.is_running():
                    spark_result = run_engine_in_subprocess("spark", config, run_id)
                else:
                    print("Spark server failed to start, skipping Spark benchmarks")
        else:
            # Assume server is already running
            spark_result = run_engine_in_subprocess("spark", config, run_id)

    # Merge results if both engines ran
    if duckdb_result and spark_result:
        final_result = merge_results(duckdb_result, spark_result)
    elif duckdb_result:
        final_result = duckdb_result
    elif spark_result:
        final_result = spark_result
    else:
        print("\nNo benchmark results produced!")
        return 1

    # Update total duration
    final_result.total_duration_seconds = time.time() - start_time

    # Save combined results to run directory
    results_path = save_results(final_result, run_dir)
    print(f"\n{'=' * 70}")
    print(f"Results saved to: {results_path}")

    # Generate markdown report in run directory
    report_path = os.path.join(run_dir, "BENCHMARK_RESULTS.md")
    save_report(final_result, report_path)
    print(f"Report saved to: {report_path}")

    print(f"Total duration: {final_result.total_duration_seconds:.1f}s")

    if final_result.errors:
        print(f"\nErrors encountered: {len(final_result.errors)}")
        for error in final_result.errors:
            print(f"  - {error}")
        return 1

    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """Execute the 'report' subcommand."""
    if not os.path.exists(args.json_file):
        print(f"Error: File not found: {args.json_file}")
        return 1

    try:
        run = load_results(args.json_file)
    except Exception as e:
        print(f"Error loading results: {e}")
        return 1

    output_path = args.output or "BENCHMARK_RESULTS.md"
    save_report(run, output_path)
    print(f"Report generated: {output_path}")

    return 0


def cmd_visualize(args: argparse.Namespace) -> int:
    """Execute the 'visualize' subcommand."""
    if not os.path.exists(args.results_path):
        print(f"Error: Path not found: {args.results_path}")
        return 1

    try:
        run = load_results(args.results_path)
    except Exception as e:
        print(f"Error loading results: {e}")
        return 1

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        # Default: save in the same directory as results
        if os.path.isdir(args.results_path):
            output_path = os.path.join(args.results_path, "benchmark_chart.png")
        else:
            output_path = os.path.join(os.path.dirname(args.results_path), "benchmark_chart.png")

    try:
        generate_visualization(run, output_path)
        print(f"Visualization saved to: {output_path}")
    except Exception as e:
        print(f"Error generating visualization: {e}")
        return 1

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="PyDeequ Engine Benchmark CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # 'run' subcommand
    run_parser = subparsers.add_parser("run", help="Run benchmark experiments")
    run_parser.add_argument(
        "--engine",
        choices=["all", "duckdb", "spark"],
        default="all",
        help="Engine to benchmark (default: all)",
    )
    run_parser.add_argument(
        "--output-dir",
        default="benchmark_results",
        help="Output directory for results (default: benchmark_results/)",
    )
    run_parser.add_argument(
        "--no-auto-spark",
        action="store_true",
        dest="no_auto_spark",
        help="Disable automatic Spark Connect server management (assumes server is already running)",
    )
    run_parser.add_argument(
        "--spark-home",
        help="Path to Spark installation",
    )
    run_parser.add_argument(
        "--java-home",
        help="Path to Java installation",
    )
    run_parser.add_argument(
        "--n-runs",
        type=int,
        help="Number of validation runs for averaging",
    )

    # 'report' subcommand
    report_parser = subparsers.add_parser("report", help="Generate markdown report from JSON results")
    report_parser.add_argument(
        "json_file",
        help="Path to JSON results file or run directory containing results.json",
    )
    report_parser.add_argument(
        "-o", "--output",
        help="Output path for markdown report (default: BENCHMARK_RESULTS.md)",
    )

    # 'visualize' subcommand
    visualize_parser = subparsers.add_parser("visualize", help="Generate PNG visualization from results")
    visualize_parser.add_argument(
        "results_path",
        help="Path to JSON results file or run directory containing results.json",
    )
    visualize_parser.add_argument(
        "-o", "--output",
        help="Output path for PNG file (default: benchmark_chart.png in results directory)",
    )

    args = parser.parse_args()

    if args.command == "run":
        sys.exit(cmd_run(args))
    elif args.command == "report":
        sys.exit(cmd_report(args))
    elif args.command == "visualize":
        sys.exit(cmd_visualize(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
