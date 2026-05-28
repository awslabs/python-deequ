"""Markdown report generation for benchmark results."""

import os
from typing import Optional

from .results import BenchmarkRun
from .experiments import count_checks


def format_value(value: Optional[float], precision: int = 3) -> str:
    """Format a numeric value or return 'N/A' if None."""
    if value is None:
        return "N/A"
    return f"{value:.{precision}f}"


def calculate_speedup(spark_time: Optional[float], duckdb_time: Optional[float]) -> str:
    """Calculate speedup ratio (spark/duckdb) or return 'N/A'."""
    if spark_time is None or duckdb_time is None or duckdb_time <= 0:
        return "N/A"
    return f"{spark_time / duckdb_time:.1f}x"


def generate_markdown_report(run: BenchmarkRun) -> str:
    """
    Generate a markdown report from benchmark results.

    Args:
        run: BenchmarkRun instance with results

    Returns:
        Markdown string
    """
    # Extract config values
    config = run.config
    exp_config = config.get("experiment", {})
    n_runs = exp_config.get("n_runs", 3)
    base_cols = exp_config.get("base_cols", 10)
    fixed_rows = exp_config.get("fixed_rows", 1_000_000)
    row_counts = exp_config.get("row_counts", [])
    column_counts = exp_config.get("column_counts", [])

    spark_startup = run.spark_startup_time or 0.0

    report = f"""# PyDeequ Engine Benchmark Results

## Run Information

| Field | Value |
|-------|-------|
| Run ID | `{run.run_id}` |
| Timestamp | {run.timestamp} |
| Engine | {run.engine} |
| Total Duration | {format_value(run.total_duration_seconds, 1)}s |

## Environment

| Component | Version |
|-----------|---------|
| Python | {run.environment.python_version} |
| Platform | {run.environment.platform_system} {run.environment.platform_release} ({run.environment.platform_machine}) |
| CPU Count | {run.environment.cpu_count} |
| DuckDB | {run.environment.duckdb_version or 'N/A'} |
| PySpark | {run.environment.pyspark_version or 'N/A'} |
| PyDeequ | {run.environment.pydeequ_version or 'N/A'} |
| Pandas | {run.environment.pandas_version or 'N/A'} |
| NumPy | {run.environment.numpy_version or 'N/A'} |
| PyArrow | {run.environment.pyarrow_version or 'N/A'} |

## Methodology

Based on duckdq-exp experiments:

- **Data Source**: Both engines read from the same Parquet files
- **Rich Dataset**: Mixed-type columns (strings + numerics) with realistic data patterns
- **Validation Runs**: {n_runs} iterations, reporting average
- **Base Checks**: {count_checks(0)} rich checks on {base_cols} mixed-type columns

### Rich Dataset Schema ({base_cols} base columns)

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Unique identifier (ID000000000000) |
| `category` | string | Categorical (5 values) |
| `status` | string | Categorical (3 values) |
| `email` | string | Email pattern |
| `amount` | float | Numeric value [0, 10000] |
| `quantity` | int | Non-negative integer [0, 1000] |
| `score` | float | Normal distribution [0, 100] |
| `rating` | int | Star rating [1, 5] |
| `price` | float | Positive numeric [0.01, 9999.99] |
| `discount` | float | Percentage [0, 0.5] |

## Experiment 1: Varying Rows (Fixed Columns = {base_cols}, {count_checks(0)} checks)

| Rows | Cols | Checks | DuckDB (s) | Spark (s) | Speedup |
|------|------|--------|------------|-----------|---------|
"""

    for r in run.varying_rows_results:
        duck_s = r.get("duckdb_validation")
        spark_s = r.get("spark_validation")
        checks = r.get("checks", count_checks(0))
        speedup = calculate_speedup(spark_s, duck_s)
        report += f"| {r['rows']:,} | {r['cols']} | {checks} | {format_value(duck_s)} | {format_value(spark_s)} | {speedup} |\n"

    report += f"""
## Experiment 2: Varying Columns (Fixed Rows = {fixed_rows:,})

Column counts: {column_counts} (base {base_cols} mixed-type + extra numeric columns)

| Rows | Cols | Checks | DuckDB (s) | Spark (s) | Speedup |
|------|------|--------|------------|-----------|---------|
"""

    for r in run.varying_cols_results:
        duck_s = r.get("duckdb_validation")
        spark_s = r.get("spark_validation")
        checks = r.get("checks", "N/A")
        speedup = calculate_speedup(spark_s, duck_s)
        report += f"| {r['rows']:,} | {r['cols']} | {checks} | {format_value(duck_s)} | {format_value(spark_s)} | {speedup} |\n"

    report += f"""
## Experiment 3: Column Profiling (Fixed Columns = {base_cols})

Uses `ColumnProfilerRunner` to profile all columns.

| Rows | Cols | DuckDB (s) | Spark (s) | Speedup |
|------|------|------------|-----------|---------|
"""

    for r in run.profiling_results:
        duck_s = r.get("duckdb_profiling")
        spark_s = r.get("spark_profiling")
        speedup = calculate_speedup(spark_s, duck_s)
        report += f"| {r['rows']:,} | {r['cols']} | {format_value(duck_s)} | {format_value(spark_s)} | {speedup} |\n"

    report += f"""
## Timing Details

### Spark Overhead (Excluded from Validation Time)

| Phase | Time (s) |
|-------|----------|
| Startup (SparkSession) | {format_value(spark_startup)} |

**Note**: Data load time varies per experiment and is not included in validation/profiling time.

## Key Findings

1. **DuckDB is significantly faster** for single-node data quality validation
2. **No JVM overhead**: DuckDB runs natively in Python process
3. **Rich type support**: Both engines handle mixed string/numeric data effectively
4. **Parquet files**: Both engines read from the same files, eliminating gRPC serialization bottleneck
5. **Column profiling**: Full profiling available on both engines

## Running the Benchmark

```bash
# Run DuckDB only (no Spark server needed)
python benchmark_cli.py run --engine duckdb

# Run Spark only (auto-spark is enabled by default)
python benchmark_cli.py run --engine spark

# Run both engines
python benchmark_cli.py run --engine all

# Generate report from saved results (folder or file path)
python benchmark_cli.py report benchmark_results/{run.run_id}/
python benchmark_cli.py report benchmark_results/{run.run_id}/results.json

# Generate PNG visualization
python benchmark_cli.py visualize benchmark_results/{run.run_id}/
```

## Notes

- Both engines read from the same Parquet files, ensuring fair comparison
- Memory configuration (16GB+) prevents OOM errors for large datasets
- For distributed workloads across multiple nodes, Spark scales horizontally
- DuckDB is optimized for single-node analytical workloads
"""

    if run.errors:
        report += "\n## Errors\n\n"
        for error in run.errors:
            report += f"- {error}\n"

    return report


def save_report(run: BenchmarkRun, output_path: str) -> str:
    """
    Generate and save markdown report.

    Args:
        run: BenchmarkRun instance
        output_path: Path to save the report

    Returns:
        Path to the saved report
    """
    report = generate_markdown_report(run)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w") as f:
        f.write(report)

    return output_path
