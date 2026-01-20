"""
Benchmark Visualization for PyDeequ Engine Comparison.

Generates PNG charts comparing DuckDB vs Spark performance from benchmark results.
"""

import os
from typing import List, Dict, Any, Optional

import matplotlib.pyplot as plt
import numpy as np

from .results import BenchmarkRun


def _format_row_count(n: int) -> str:
    """Format row count for display (e.g., 1000000 -> '1M')."""
    if n >= 1_000_000:
        return f"{n // 1_000_000}M"
    elif n >= 1_000:
        return f"{n // 1_000}K"
    return str(n)


def _extract_validation_data(
    results: List[Dict[str, Any]], x_key: str
) -> Dict[str, List]:
    """Extract validation timing data from results."""
    data = {
        "x_values": [],
        "x_labels": [],
        "duckdb": [],
        "spark": [],
        "checks": [],
    }

    for r in sorted(results, key=lambda x: x.get(x_key, 0)):
        x_val = r.get(x_key)
        if x_val is None:
            continue

        data["x_values"].append(x_val)
        if x_key == "rows":
            data["x_labels"].append(_format_row_count(x_val))
        else:
            checks = r.get("checks", "")
            data["x_labels"].append(f"{x_val}\n({checks})" if checks else str(x_val))

        data["duckdb"].append(r.get("duckdb_validation"))
        data["spark"].append(r.get("spark_validation"))
        data["checks"].append(r.get("checks"))

    return data


def _extract_profiling_data(results: List[Dict[str, Any]]) -> Dict[str, List]:
    """Extract profiling timing data from results."""
    data = {
        "x_values": [],
        "x_labels": [],
        "duckdb": [],
        "spark": [],
    }

    for r in sorted(results, key=lambda x: x.get("rows", 0)):
        rows = r.get("rows")
        if rows is None:
            continue

        data["x_values"].append(rows)
        data["x_labels"].append(_format_row_count(rows))
        data["duckdb"].append(r.get("duckdb_profiling"))
        data["spark"].append(r.get("spark_profiling"))

    return data


def _calculate_speedup(spark_times: List, duckdb_times: List) -> List[Optional[float]]:
    """Calculate speedup ratios (Spark time / DuckDB time)."""
    speedups = []
    for s, d in zip(spark_times, duckdb_times):
        if s is not None and d is not None and d > 0:
            speedups.append(s / d)
        else:
            speedups.append(None)
    return speedups


def _plot_comparison(
    ax: plt.Axes,
    x_labels: List[str],
    duckdb_times: List,
    spark_times: List,
    xlabel: str,
    ylabel: str,
    title: str,
    duckdb_color: str,
    spark_color: str,
    use_log_scale: bool = False,
) -> None:
    """Plot a side-by-side bar comparison chart."""
    # Filter out None values
    valid_indices = [
        i for i in range(len(x_labels))
        if duckdb_times[i] is not None or spark_times[i] is not None
    ]

    if not valid_indices:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title, fontsize=12, fontweight="bold")
        return

    labels = [x_labels[i] for i in valid_indices]
    duckdb = [duckdb_times[i] if duckdb_times[i] is not None else 0 for i in valid_indices]
    spark = [spark_times[i] if spark_times[i] is not None else 0 for i in valid_indices]

    x = np.arange(len(labels))
    width = 0.35

    has_duckdb = any(d > 0 for d in duckdb)
    has_spark = any(s > 0 for s in spark)

    if has_duckdb:
        bars1 = ax.bar(
            x - width / 2, duckdb, width, label="DuckDB",
            color=duckdb_color, edgecolor="black", linewidth=0.5
        )
        for bar in bars1:
            height = bar.get_height()
            if height > 0:
                ax.annotate(
                    f"{height:.2f}s",
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=7
                )

    if has_spark:
        bars2 = ax.bar(
            x + width / 2, spark, width, label="Spark",
            color=spark_color, edgecolor="black", linewidth=0.5
        )
        for bar in bars2:
            height = bar.get_height()
            if height > 0:
                ax.annotate(
                    f"{height:.1f}s",
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3), textcoords="offset points",
                    ha="center", va="bottom", fontsize=7
                )

    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(loc="upper left")

    if use_log_scale and has_duckdb and has_spark:
        ax.set_yscale("log")


def _plot_speedup(
    ax: plt.Axes,
    x_labels: List[str],
    speedups: List[Optional[float]],
    xlabel: str,
    title: str,
    speedup_color: str,
) -> None:
    """Plot a speedup bar chart."""
    valid_indices = [i for i in range(len(x_labels)) if speedups[i] is not None]

    if not valid_indices:
        ax.text(0.5, 0.5, "No speedup data\n(need both engines)", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title, fontsize=12, fontweight="bold")
        return

    labels = [x_labels[i] for i in valid_indices]
    values = [speedups[i] for i in valid_indices]

    x = np.arange(len(labels))
    bars = ax.bar(x, values, color=speedup_color, edgecolor="black", linewidth=0.5)

    ax.axhline(y=1, color="gray", linestyle="--", alpha=0.7)
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Speedup (x times faster)", fontsize=11)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)

    for bar in bars:
        height = bar.get_height()
        ax.annotate(
            f"{height:.1f}x",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3), textcoords="offset points",
            ha="center", va="bottom", fontsize=10, fontweight="bold"
        )


def generate_visualization(run: BenchmarkRun, output_path: str) -> str:
    """
    Generate benchmark visualization PNG from results.

    Args:
        run: BenchmarkRun instance with results
        output_path: Path to save the PNG file

    Returns:
        Path to the saved PNG file
    """
    # Extract data from results
    rows_data = _extract_validation_data(run.varying_rows_results, "rows")
    cols_data = _extract_validation_data(run.varying_cols_results, "cols")
    profiling_data = _extract_profiling_data(run.profiling_results)

    # Calculate speedups
    rows_speedup = _calculate_speedup(rows_data["spark"], rows_data["duckdb"])
    cols_speedup = _calculate_speedup(cols_data["spark"], cols_data["duckdb"])
    profiling_speedup = _calculate_speedup(profiling_data["spark"], profiling_data["duckdb"])

    # Color scheme
    duckdb_color = "#FFA500"  # Orange
    spark_color = "#E25A1C"   # Spark orange/red
    speedup_color = "#2E86AB"  # Blue

    # Set style and create figure
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, axes = plt.subplots(2, 3, figsize=(16, 10))

    # Row 1: Time comparisons
    _plot_comparison(
        axes[0, 0], rows_data["x_labels"], rows_data["duckdb"], rows_data["spark"],
        "Dataset Size (rows)", "Validation Time (seconds)",
        "Exp 1: Varying Rows (10 cols)",
        duckdb_color, spark_color, use_log_scale=True
    )

    _plot_comparison(
        axes[0, 1], cols_data["x_labels"], cols_data["duckdb"], cols_data["spark"],
        "Columns (Checks)", "Validation Time (seconds)",
        "Exp 2: Varying Columns (1M rows)",
        duckdb_color, spark_color
    )

    _plot_comparison(
        axes[0, 2], profiling_data["x_labels"], profiling_data["duckdb"], profiling_data["spark"],
        "Dataset Size (rows)", "Profiling Time (seconds)",
        "Exp 3: Column Profiling (10 cols)",
        duckdb_color, spark_color
    )

    # Row 2: Speedup charts
    _plot_speedup(
        axes[1, 0], rows_data["x_labels"], rows_speedup,
        "Dataset Size (rows)", "DuckDB Speedup: Varying Rows",
        speedup_color
    )

    _plot_speedup(
        axes[1, 1], cols_data["x_labels"], cols_speedup,
        "Columns (Checks)", "DuckDB Speedup: Varying Columns",
        speedup_color
    )

    _plot_speedup(
        axes[1, 2], profiling_data["x_labels"], profiling_speedup,
        "Dataset Size (rows)", "DuckDB Speedup: Column Profiling",
        speedup_color
    )

    # Title
    engine_label = run.engine.upper() if run.engine != "all" else "DuckDB vs Spark"
    fig.suptitle(
        f"PyDeequ Benchmark: {engine_label}\n{run.run_id}",
        fontsize=14, fontweight="bold", y=1.02
    )

    plt.tight_layout()

    # Save the figure
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white", edgecolor="none")
    plt.close(fig)

    return output_path
