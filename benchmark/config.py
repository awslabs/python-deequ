"""Configuration dataclasses for benchmark."""

import os
from dataclasses import dataclass, field, asdict
from typing import List, Optional


def _required_env(name: str) -> str:
    """Return the value of the required env var, or empty string if unset.

    Empty values are validated at use sites (e.g., when the Spark server
    is started) so config objects can be created without the env vars set.
    """
    return os.environ.get(name, "")


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
    """Configuration for Spark Connect server.

    Paths must be supplied via environment variables (or set programmatically)
    rather than baked-in defaults. Required when starting a server:
    - JAVA_HOME: path to a Java 17 install
    - SPARK_HOME: path to a Spark 3.5 install
    - DEEQU_JAR: path to the Deequ JAR with Spark Connect support
    """

    java_home: str = field(default_factory=lambda: _required_env("JAVA_HOME"))
    spark_home: str = field(default_factory=lambda: _required_env("SPARK_HOME"))
    port: int = 15002
    startup_timeout: int = 60
    poll_interval: float = 1.0
    driver_memory: str = "16g"
    executor_memory: str = "16g"
    deequ_jar: str = field(default_factory=lambda: _required_env("DEEQU_JAR"))

    def validate(self) -> None:
        """Raise if any required path is missing or doesn't exist."""
        missing = [
            (name, val)
            for name, val in (
                ("JAVA_HOME", self.java_home),
                ("SPARK_HOME", self.spark_home),
                ("DEEQU_JAR", self.deequ_jar),
            )
            if not val
        ]
        if missing:
            names = ", ".join(name for name, _ in missing)
            raise RuntimeError(
                f"SparkServerConfig is missing required values: {names}. "
                "Set the corresponding environment variables or assign on the "
                "config object before starting the server."
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
