# PyDeequ Benchmark

Benchmark harness for comparing DuckDB and Spark engine performance.

## Design Overview

### Architecture

```
benchmark_cli.py          # CLI entry point
benchmark/
├── config.py             # Configuration dataclasses
├── experiments.py        # Experiment logic (data gen, checks, profiling)
├── worker.py             # Subprocess worker for process isolation
├── spark_server.py       # Auto Spark Connect server management
├── results.py            # Results storage and merging
├── report.py             # Markdown report generation
└── visualize.py          # PNG chart generation
```

### Process Isolation

Each engine runs in a separate subprocess to ensure:
- Clean JVM state for Spark
- Independent memory allocation
- No cross-contamination between engines

### Data Pipeline

1. **Generate** synthetic mixed-type data (strings, floats, ints)
2. **Cache** as Parquet files with optimized row groups
3. **Load** from same Parquet files for both engines (fair comparison)

## Experiments

### 1. Varying Rows
- Fixed: 10 columns, 16 data quality checks
- Variable: 100K to 130M rows
- Measures: Validation time scaling with data size

### 2. Varying Columns
- Fixed: 1M rows
- Variable: 10 to 80 columns (16 to 226 checks)
- Measures: Validation time scaling with schema complexity

### 3. Column Profiling
- Fixed: 10 columns
- Variable: 100K to 10M rows
- Measures: Full column profiling performance

## Results

Benchmark run on Apple M3 Max (14 cores), macOS Darwin 25.2.0.

![Benchmark Results](imgs/benchmark_chart.png)

### Experiment 1: Varying Rows

| Rows | DuckDB (s) | Spark (s) | Speedup |
|------|------------|-----------|---------|
| 100K | 0.052 | 0.667 | **12.8x** |
| 1M | 0.090 | 1.718 | **19.1x** |
| 5M | 0.221 | 2.591 | **11.7x** |
| 10M | 0.335 | 3.504 | **10.5x** |
| 50M | 1.177 | 12.808 | **10.9x** |
| 130M | 2.897 | 29.570 | **10.2x** |

### Experiment 2: Varying Columns

| Cols | Checks | DuckDB (s) | Spark (s) | Speedup |
|------|--------|------------|-----------|---------|
| 10 | 16 | 0.118 | 1.656 | **14.1x** |
| 20 | 46 | 0.286 | 2.129 | **7.5x** |
| 40 | 106 | 0.713 | 2.869 | **4.0x** |
| 80 | 226 | 2.214 | 4.434 | **2.0x** |

### Experiment 3: Column Profiling

| Rows | DuckDB (s) | Spark (s) | Speedup |
|------|------------|-----------|---------|
| 100K | 0.086 | 0.599 | **7.0x** |
| 1M | 0.388 | 0.814 | **2.1x** |
| 5M | 1.470 | 2.399 | **1.6x** |
| 10M | 2.659 | 4.109 | **1.5x** |

### Key Takeaways

1. **DuckDB is 10-19x faster** for row-scaling validation workloads
2. **Speedup decreases with complexity** - more columns/checks narrow the gap (14x → 2x)
3. **Profiling converges** - at 10M rows, DuckDB is still 1.5x faster
4. **No JVM overhead** - DuckDB runs natively in Python, no startup cost

## Quick Start

### Run DuckDB Only (No Spark Required)

```bash
python benchmark_cli.py run --engine duckdb
```

### Run Both Engines

```bash
python benchmark_cli.py run --engine all
```

Auto-spark is enabled by default. The harness will:
1. Start a Spark Connect server
2. Run DuckDB benchmarks
3. Run Spark benchmarks
4. Stop the server
5. Merge results

### Run with External Spark Server

```bash
# Start server manually first, then:
python benchmark_cli.py run --engine spark --no-auto-spark
```

## Output Structure

Each run creates a timestamped folder:

```
benchmark_results/
└── benchmark_2024-01-19T14-30-45/
    ├── results.json           # Raw timing data
    └── BENCHMARK_RESULTS.md   # Markdown report
```

## Visualize Results

Generate a PNG chart comparing engine performance:

```bash
# From run folder
python benchmark_cli.py visualize benchmark_results/benchmark_2024-01-19T14-30-45/

# Custom output path
python benchmark_cli.py visualize benchmark_results/benchmark_2024-01-19T14-30-45/ -o comparison.png
```

The chart shows:
- **Top row**: Time comparisons (DuckDB vs Spark) for each experiment
- **Bottom row**: Speedup ratios (how many times faster DuckDB is)

## Regenerate Report

```bash
python benchmark_cli.py report benchmark_results/benchmark_2024-01-19T14-30-45/
```

## Configuration

Default experiment parameters (see `benchmark/config.py`):

| Parameter | Default |
|-----------|---------|
| Row counts | 100K, 1M, 5M, 10M, 50M, 130M |
| Column counts | 10, 20, 40, 80 |
| Profiling rows | 100K, 1M, 5M, 10M |
| Validation runs | 3 (averaged) |
| Cache directory | `~/.deequ_benchmark_data` |

## Requirements

- **DuckDB**: No additional setup
- **Spark**: Requires `SPARK_HOME` and `JAVA_HOME` environment variables (or use `--spark-home`/`--java-home` flags)

## Example Workflow

```bash
# 1. Run full benchmark
python benchmark_cli.py run --engine all

# 2. View results
cat benchmark_results/benchmark_*/BENCHMARK_RESULTS.md

# 3. Generate chart
python benchmark_cli.py visualize benchmark_results/benchmark_*/

# 4. Open chart
open benchmark_results/benchmark_*/benchmark_chart.png
```
