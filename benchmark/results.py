"""Results dataclasses and JSON I/O for benchmark."""

import json
import os
import platform
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class ExperimentResult:
    """Result from a single experiment run."""

    rows: int
    cols: int
    checks: Optional[int] = None
    duckdb_validation: Optional[float] = None
    duckdb_profiling: Optional[float] = None
    spark_startup: Optional[float] = None
    spark_load: Optional[float] = None
    spark_validation: Optional[float] = None
    spark_profiling: Optional[float] = None
    error: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    @classmethod
    def from_dict(cls, data: dict) -> "ExperimentResult":
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class EnvironmentInfo:
    """Environment information for reproducibility."""

    python_version: str = ""
    platform_system: str = ""
    platform_release: str = ""
    platform_machine: str = ""
    cpu_count: int = 0
    duckdb_version: str = ""
    pyspark_version: str = ""
    pydeequ_version: str = ""
    pandas_version: str = ""
    numpy_version: str = ""
    pyarrow_version: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "EnvironmentInfo":
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class BenchmarkRun:
    """Complete benchmark run with all results."""

    run_id: str
    timestamp: str
    engine: str
    config: Dict[str, Any] = field(default_factory=dict)
    environment: EnvironmentInfo = field(default_factory=EnvironmentInfo)
    varying_rows_results: List[Dict[str, Any]] = field(default_factory=list)
    varying_cols_results: List[Dict[str, Any]] = field(default_factory=list)
    profiling_results: List[Dict[str, Any]] = field(default_factory=list)
    spark_startup_time: Optional[float] = None
    total_duration_seconds: Optional[float] = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "engine": self.engine,
            "config": self.config,
            "environment": self.environment.to_dict(),
            "varying_rows_results": self.varying_rows_results,
            "varying_cols_results": self.varying_cols_results,
            "profiling_results": self.profiling_results,
            "spark_startup_time": self.spark_startup_time,
            "total_duration_seconds": self.total_duration_seconds,
            "errors": self.errors,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BenchmarkRun":
        """Create from dictionary."""
        return cls(
            run_id=data.get("run_id", ""),
            timestamp=data.get("timestamp", ""),
            engine=data.get("engine", ""),
            config=data.get("config", {}),
            environment=EnvironmentInfo.from_dict(data.get("environment", {})),
            varying_rows_results=data.get("varying_rows_results", []),
            varying_cols_results=data.get("varying_cols_results", []),
            profiling_results=data.get("profiling_results", []),
            spark_startup_time=data.get("spark_startup_time"),
            total_duration_seconds=data.get("total_duration_seconds"),
            errors=data.get("errors", []),
        )


def generate_run_id() -> str:
    """Generate a unique run ID with timestamp."""
    return f"benchmark_{datetime.now().strftime('%Y-%m-%dT%H-%M-%S')}"


def save_results(run: BenchmarkRun, run_dir: str) -> str:
    """
    Save benchmark results to JSON file in run directory.

    Args:
        run: BenchmarkRun instance
        run_dir: Directory for this benchmark run (e.g., benchmark_results/benchmark_2024-01-19T14-30-45/)

    Returns:
        Path to the saved JSON file
    """
    os.makedirs(run_dir, exist_ok=True)
    path = os.path.join(run_dir, "results.json")

    with open(path, "w") as f:
        json.dump(run.to_dict(), f, indent=2)

    return path


def load_results(path: str) -> BenchmarkRun:
    """
    Load benchmark results from JSON file.

    Args:
        path: Path to JSON file or run directory containing results.json

    Returns:
        BenchmarkRun instance
    """
    # If path is a directory, look for results.json inside
    if os.path.isdir(path):
        path = os.path.join(path, "results.json")

    with open(path) as f:
        data = json.load(f)
    return BenchmarkRun.from_dict(data)


def collect_environment_info() -> EnvironmentInfo:
    """Collect environment information for reproducibility."""
    info = EnvironmentInfo(
        python_version=platform.python_version(),
        platform_system=platform.system(),
        platform_release=platform.release(),
        platform_machine=platform.machine(),
        cpu_count=os.cpu_count() or 0,
    )

    # Try to get package versions
    try:
        import duckdb

        info.duckdb_version = duckdb.__version__
    except (ImportError, AttributeError):
        pass

    try:
        import pyspark

        info.pyspark_version = pyspark.__version__
    except (ImportError, AttributeError):
        pass

    try:
        import pydeequ

        info.pydeequ_version = getattr(pydeequ, "__version__", "unknown")
    except (ImportError, AttributeError):
        pass

    try:
        import pandas

        info.pandas_version = pandas.__version__
    except (ImportError, AttributeError):
        pass

    try:
        import numpy

        info.numpy_version = numpy.__version__
    except (ImportError, AttributeError):
        pass

    try:
        import pyarrow

        info.pyarrow_version = pyarrow.__version__
    except (ImportError, AttributeError):
        pass

    return info


def merge_results(duckdb_run: Optional[BenchmarkRun], spark_run: Optional[BenchmarkRun]) -> BenchmarkRun:
    """
    Merge results from separate DuckDB and Spark runs into a single combined result.

    Args:
        duckdb_run: Results from DuckDB worker (may be None)
        spark_run: Results from Spark worker (may be None)

    Returns:
        Combined BenchmarkRun
    """
    # Use whichever run is available as the base
    base = duckdb_run or spark_run
    if base is None:
        raise ValueError("At least one run must be provided")

    merged = BenchmarkRun(
        run_id=base.run_id,
        timestamp=base.timestamp,
        engine="all" if duckdb_run and spark_run else base.engine,
        config=base.config,
        environment=base.environment,
        errors=base.errors.copy(),
    )

    # Merge varying rows results
    rows_by_key = {}
    for run in [duckdb_run, spark_run]:
        if run:
            for r in run.varying_rows_results:
                key = (r.get("rows"), r.get("cols"))
                if key not in rows_by_key:
                    rows_by_key[key] = r.copy()
                else:
                    rows_by_key[key].update({k: v for k, v in r.items() if v is not None})
            merged.errors.extend(e for e in run.errors if e not in merged.errors)
    merged.varying_rows_results = list(rows_by_key.values())

    # Merge varying cols results
    cols_by_key = {}
    for run in [duckdb_run, spark_run]:
        if run:
            for r in run.varying_cols_results:
                key = (r.get("rows"), r.get("cols"))
                if key not in cols_by_key:
                    cols_by_key[key] = r.copy()
                else:
                    cols_by_key[key].update({k: v for k, v in r.items() if v is not None})
    merged.varying_cols_results = list(cols_by_key.values())

    # Merge profiling results
    prof_by_key = {}
    for run in [duckdb_run, spark_run]:
        if run:
            for r in run.profiling_results:
                key = (r.get("rows"), r.get("cols"))
                if key not in prof_by_key:
                    prof_by_key[key] = r.copy()
                else:
                    prof_by_key[key].update({k: v for k, v in r.items() if v is not None})
    merged.profiling_results = list(prof_by_key.values())

    # Take Spark startup time from Spark run
    if spark_run and spark_run.spark_startup_time:
        merged.spark_startup_time = spark_run.spark_startup_time

    # Sum total durations
    total = 0.0
    if duckdb_run and duckdb_run.total_duration_seconds:
        total += duckdb_run.total_duration_seconds
    if spark_run and spark_run.total_duration_seconds:
        total += spark_run.total_duration_seconds
    merged.total_duration_seconds = total if total > 0 else None

    return merged
