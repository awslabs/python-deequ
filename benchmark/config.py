"""Configuration dataclasses for benchmark."""

import os
from dataclasses import dataclass, field, asdict
from typing import List, Optional


# Default experiment configurations (from original script)
DEFAULT_ROW_COUNTS = [100_000, 1_000_000, 5_000_000, 10_000_000, 50_000_000, 130_000_000]
DEFAULT_COLUMN_COUNTS = [10, 20, 40, 80]
DEFAULT_PROFILING_ROW_COUNTS = [100_000, 1_000_000, 5_000_000, 10_000_000]
DEFAULT_FIXED_ROWS = 1_000_000
DEFAULT_BASE_COLS = 10
DEFAULT_N_RUNS = 3

@dataclass
class ExperimentConfig:
    """Configuration for benchmark experiments."""

    n_runs: int = DEFAULT_N_RUNS
    row_counts: List[int] = field(default_factory=lambda: DEFAULT_ROW_COUNTS.copy())
    column_counts: List[int] = field(default_factory=lambda: DEFAULT_COLUMN_COUNTS.copy())
    profiling_row_counts: List[int] = field(
        default_factory=lambda: DEFAULT_PROFILING_ROW_COUNTS.copy()
    )
    fixed_rows: int = DEFAULT_FIXED_ROWS
    base_cols: int = DEFAULT_BASE_COLS
    cache_dir: str = field(
        default_factory=lambda: os.path.expanduser("~/.deequ_benchmark_data")
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ExperimentConfig":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class SparkServerConfig:
    """Configuration for Spark Connect server."""

    java_home: str = field(
        default_factory=lambda: os.environ.get(
            "JAVA_HOME",
            "/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home",
        )
    )
    spark_home: str = field(
        default_factory=lambda: os.environ.get(
            "SPARK_HOME", "/Volumes/workplace/deequ_rewrite/spark-3.5.0-bin-hadoop3"
        )
    )
    port: int = 15002
    startup_timeout: int = 60
    poll_interval: float = 1.0
    driver_memory: str = "16g"
    executor_memory: str = "16g"
    deequ_jar: str = field(
        default_factory=lambda: "/Volumes/workplace/deequ_rewrite/deequ/target/deequ_2.12-2.1.0b-spark-3.5.jar"
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "SparkServerConfig":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class BenchmarkConfig:
    """Overall benchmark configuration."""

    engine: str = "all"  # "all", "duckdb", or "spark"
    output_dir: str = "benchmark_results"
    experiment: ExperimentConfig = field(default_factory=ExperimentConfig)
    spark_server: SparkServerConfig = field(default_factory=SparkServerConfig)
    spark_remote: str = field(
        default_factory=lambda: os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
    )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "engine": self.engine,
            "output_dir": self.output_dir,
            "experiment": self.experiment.to_dict(),
            "spark_server": self.spark_server.to_dict(),
            "spark_remote": self.spark_remote,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BenchmarkConfig":
        """Create from dictionary."""
        return cls(
            engine=data.get("engine", "all"),
            output_dir=data.get("output_dir", "benchmark_results"),
            experiment=ExperimentConfig.from_dict(data.get("experiment", {})),
            spark_server=SparkServerConfig.from_dict(data.get("spark_server", {})),
            spark_remote=data.get("spark_remote", "sc://localhost:15002"),
        )
